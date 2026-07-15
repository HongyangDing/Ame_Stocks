from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.identity_adjudication import (
    AdjudicationEvidenceRef,
    CandidateManifestBinding,
    EvidenceSourceType,
    IdentityAdjudicationError,
    IdentityAdjudicationPlan,
    IdentityAdjudicationProposal,
    IdentityAdjudicationStore,
    IdentityCaseReference,
    IdentityDisposition,
)
from ame_stocks_api.silver.identity_bounce import (
    IdentityObservation,
    SourceSession,
    build_identity_case_candidate_manifest,
    detect_provider_figi_bounces,
    write_identity_case_candidate_manifest,
)
from ame_stocks_api.silver.identity_resolution import (
    IdentityResolutionBinding,
    IdentityResolutionError,
    ObservedIdentityMembership,
    canonical_asset_id,
    resolve_identity_at_cutoff,
    resolve_loaded_registry_at_cutoff,
)
from ame_stocks_api.silver.identity_source import (
    S7_SIX_RELEASE_BINDING_ID,
    S7_SOURCE_PINS,
)

FIGI_A = "BBG00000000A"
FIGI_B = "BBG00000000B"


@dataclass(frozen=True)
class Case:
    identity_case_id: str
    six_release_binding_id: str
    ticker: str
    left_outer_composite_figi: str
    middle_observed_composite_figi: str
    right_outer_composite_figi: str
    left_outer_source_record_id: str
    right_outer_source_record_id: str
    episode_valid_from_session: date
    episode_valid_through_session: date
    episode_source_record_ids: tuple[str, ...]
    episode_source_record_set_digest: str
    identity_case_available_session: date


@dataclass(frozen=True)
class Adjudication:
    identity_adjudication_id: str
    adjudication_series_id: str
    decision_version: int
    supersedes_identity_adjudication_id: str | None
    identity_case_id: str
    identity_case_available_session: date
    observed_ticker: str
    observed_composite_figi: str
    episode_valid_from_session: date
    episode_valid_through_session: date
    episode_source_record_set_digest: str
    disposition: str
    canonical_composite_figi: str | None
    canonical_asset_id: str | None
    canonical_override: bool
    approval_status: str
    adjudication_available_session: date
    outcome_or_backtest_evidence_used: bool
    source_identity_case_candidate_manifest_id: str
    source_identity_case_candidate_manifest_sha256: str


def _digest(label: str) -> str:
    return stable_digest({"fixture": label})


def _binding(*, cutoff: date = date(2024, 1, 5)) -> IdentityResolutionBinding:
    return IdentityResolutionBinding(
        cutoff_session=cutoff,
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
        candidate_manifest_id=_digest("candidate-manifest"),
        candidate_manifest_sha256=_digest("candidate-manifest-bytes"),
        candidate_manifest_available_session=date(2024, 1, 4),
        adjudication_release_id=_digest("registry-release"),
        adjudication_release_available_session=date(2024, 1, 5),
    )


def _rows() -> tuple[ObservedIdentityMembership, ...]:
    rows: list[ObservedIdentityMembership] = []
    for ticker in ("GEN", "BAD", "PEN"):
        for day, figi in ((1, FIGI_A), (2, FIGI_B), (3, FIGI_A)):
            rows.append(
                ObservedIdentityMembership(
                    session_date=date(2024, 1, day),
                    ticker=ticker,
                    active_on_date=True,
                    observed_composite_figi=figi,
                    source_record_id=_digest(f"{ticker}-{day}"),
                )
            )
    return tuple(rows)


def _cases(rows: tuple[ObservedIdentityMembership, ...]) -> tuple[Case, ...]:
    by_key = {(row.ticker, row.session_date.day): row for row in rows}
    output = []
    for ticker in ("GEN", "BAD", "PEN"):
        source_ids = (by_key[(ticker, 2)].source_record_id,)
        output.append(
            Case(
                identity_case_id=_digest(f"case-{ticker}"),
                six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
                ticker=ticker,
                left_outer_composite_figi=FIGI_A,
                middle_observed_composite_figi=FIGI_B,
                right_outer_composite_figi=FIGI_A,
                left_outer_source_record_id=by_key[(ticker, 1)].source_record_id,
                right_outer_source_record_id=by_key[(ticker, 3)].source_record_id,
                episode_valid_from_session=date(2024, 1, 2),
                episode_valid_through_session=date(2024, 1, 2),
                episode_source_record_ids=source_ids,
                episode_source_record_set_digest=stable_digest(sorted(source_ids)),
                identity_case_available_session=date(2024, 1, 3),
            )
        )
    return tuple(output)


def _adjudication(
    case: Case,
    *,
    disposition: str,
    canonical: str | None,
    version: int = 1,
    predecessor: str | None = None,
) -> Adjudication:
    identity_adjudication_id = _digest(f"decision-{case.ticker}-{version}-{disposition}")
    return Adjudication(
        identity_adjudication_id=identity_adjudication_id,
        adjudication_series_id=_digest(f"series-{case.ticker}"),
        decision_version=version,
        supersedes_identity_adjudication_id=predecessor,
        identity_case_id=case.identity_case_id,
        identity_case_available_session=case.identity_case_available_session,
        observed_ticker=case.ticker,
        observed_composite_figi=FIGI_B,
        episode_valid_from_session=case.episode_valid_from_session,
        episode_valid_through_session=case.episode_valid_through_session,
        episode_source_record_set_digest=case.episode_source_record_set_digest,
        disposition=disposition,
        canonical_composite_figi=canonical,
        canonical_asset_id=None if canonical is None else canonical_asset_id(canonical),
        canonical_override=disposition == "confirmed_provider_contamination",
        approval_status="approved",
        adjudication_available_session=date(2024, 1, 4),
        outcome_or_backtest_evidence_used=False,
        source_identity_case_candidate_manifest_id=_binding().candidate_manifest_id,
        source_identity_case_candidate_manifest_sha256=(_binding().candidate_manifest_sha256),
    )


def test_fixed_genuine_contamination_and_unresolved_cases() -> None:
    rows = _rows()
    cases = _cases(rows)
    genuine = _adjudication(
        cases[0],
        disposition="confirmed_genuine_transition",
        canonical=FIGI_B,
    )
    contamination = _adjudication(
        cases[1],
        disposition="confirmed_provider_contamination",
        canonical=FIGI_A,
    )

    result = resolve_identity_at_cutoff(
        rows,
        cases,
        (genuine, contamination),
        binding=_binding(),
    )
    middle = {
        item.ticker: item for item in result.decisions if item.session_date == date(2024, 1, 2)
    }
    assert middle["GEN"].canonical_composite_figi == FIGI_B
    assert middle["GEN"].identity_resolution_method == "approved_genuine_transition"
    assert middle["GEN"].backtest_identity_eligible is True
    assert middle["BAD"].canonical_composite_figi == FIGI_A
    assert middle["BAD"].observed_composite_figi == FIGI_B
    assert middle["BAD"].identity_resolution_status == "resolved_approved_override"
    assert middle["BAD"].backtest_identity_eligible is True
    assert middle["PEN"].canonical_composite_figi is None
    assert middle["PEN"].identity_disposition == "pending_unresolved"
    assert middle["PEN"].alias_emitted is False
    assert middle["PEN"].identity_quality_liquidation_signal is False
    assert result.audit.active_membership_rows == 9
    assert result.audit.suspected_provider_figi_bounce_rows == 3
    assert result.audit.pending_or_adjudicated_unresolved_rows == 1
    assert result.audit.approved_provider_contamination_override_rows == 1
    assert result.audit.unapproved_canonical_identity_override_rows == 0
    assert result.audit.suspected_provider_contamination_eligible_rows == 0
    assert result.audit.identity_quality_liquidation_signal_rows == 0


def test_detector_case_flows_directly_into_cutoff_resolver() -> None:
    source_ids = tuple(_digest(f"pipeline-{day}") for day in range(1, 4))
    spine = tuple(SourceSession(date(2024, 1, day)) for day in range(1, 4))
    observed = tuple(
        IdentityObservation(
            session_date=date(2024, 1, day),
            ticker="PIPE",
            observed_composite_figi=figi,
            source_record_id=source_id,
            source_available_session=date(2024, 1, day),
        )
        for day, figi, source_id in zip(
            range(1, 4),
            (FIGI_A, FIGI_B, FIGI_A),
            source_ids,
            strict=True,
        )
    )
    detection = detect_provider_figi_bounces(
        spine,
        observed,
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
        candidate_manifest_available_session=date(2024, 1, 4),
    )
    assert len(detection.cases) == 1
    case = detection.cases[0]
    decision = _adjudication(
        case,
        disposition="confirmed_provider_contamination",
        canonical=FIGI_A,
    )
    memberships = tuple(
        ObservedIdentityMembership(
            session_date=item.session_date,
            ticker=item.ticker,
            active_on_date=item.active_on_date,
            observed_composite_figi=item.observed_composite_figi,
            source_record_id=item.source_record_id,
        )
        for item in observed
    )
    result = resolve_identity_at_cutoff(
        memberships,
        detection.cases,
        (decision,),
        binding=_binding(),
    )
    middle = next(item for item in result.decisions if item.session_date.day == 2)
    assert middle.observed_composite_figi == FIGI_B
    assert middle.canonical_composite_figi == FIGI_A
    assert middle.identity_case_id == case.identity_case_id


def test_protocol_lookalike_cannot_enter_loaded_registry_resolver() -> None:
    source_ids = tuple(_digest(f"loaded-{day}") for day in range(1, 4))
    spine = tuple(SourceSession(date(2024, 1, day)) for day in range(1, 4))
    observed = tuple(
        IdentityObservation(
            session_date=date(2024, 1, day),
            ticker="LOAD",
            observed_composite_figi=figi,
            source_record_id=source_id,
            source_available_session=date(2024, 1, day),
        )
        for day, figi, source_id in zip(
            range(1, 4),
            (FIGI_A, FIGI_B, FIGI_A),
            source_ids,
            strict=True,
        )
    )
    detection = detect_provider_figi_bounces(
        spine,
        observed,
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
        candidate_manifest_available_session=date(2024, 1, 4),
    )
    manifest = build_identity_case_candidate_manifest(
        detection,
        created_at_utc=datetime(2024, 1, 4, 13, 0, tzinfo=UTC),
    )
    decision = replace(
        _adjudication(
            detection.cases[0],
            disposition="confirmed_provider_contamination",
            canonical=FIGI_A,
        ),
        source_identity_case_candidate_manifest_id=manifest.candidate_manifest_id,
        source_identity_case_candidate_manifest_sha256=manifest.sha256,
    )
    loaded = SimpleNamespace(
        release=SimpleNamespace(
            candidate_manifest_id=manifest.candidate_manifest_id,
            candidate_manifest_sha256=manifest.sha256,
            six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
            release_id=_digest("loaded-registry-release"),
            release_available_session=date(2024, 1, 5),
        ),
        candidate_manifest=manifest,
        decisions=(decision,),
    )
    memberships = tuple(
        ObservedIdentityMembership(
            session_date=item.session_date,
            ticker=item.ticker,
            active_on_date=True,
            observed_composite_figi=item.observed_composite_figi,
            source_record_id=item.source_record_id,
        )
        for item in observed
    )
    with pytest.raises(IdentityResolutionError, match=r"exact.*loader result"):
        resolve_loaded_registry_at_cutoff(
            memberships,
            loaded,
            cutoff_session=date(2024, 1, 5),
        )


def test_unattested_production_candidate_cannot_enter_control_store(tmp_path) -> None:
    sessions = tuple(
        SourceSession(session_date=value)
        for value in (date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4))
    )
    source_ids = tuple(_digest(f"store-loaded-{day}") for day in range(3))
    observations = tuple(
        IdentityObservation(
            session_date=session.session_date,
            ticker="LOAD",
            observed_composite_figi=figi,
            source_record_id=source_id,
            source_available_session=session.session_date,
        )
        for session, figi, source_id in zip(
            sessions,
            (FIGI_A, FIGI_B, FIGI_A),
            source_ids,
            strict=True,
        )
    )
    detection = detect_provider_figi_bounces(
        sessions,
        observations,
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
        candidate_manifest_available_session=date(2024, 1, 5),
    )
    manifest = build_identity_case_candidate_manifest(
        detection,
        created_at_utc=datetime(2024, 1, 4, 22, 0, tzinfo=UTC),
    )
    manifest_receipt = write_identity_case_candidate_manifest(tmp_path, manifest)
    candidate = CandidateManifestBinding(
        manifest_id=manifest.candidate_manifest_id,
        manifest_sha256=str(manifest_receipt["sha256"]),
        path=manifest.relative_path,
    )
    case = detection.cases[0]
    case_ref = IdentityCaseReference(
        identity_case_id=case.identity_case_id,
        identity_case_available_session=case.identity_case_available_session,
        observed_ticker=case.ticker,
        observed_composite_figi=case.middle_observed_composite_figi,
        left_outer_composite_figi=case.left_outer_composite_figi,
        right_outer_composite_figi=case.right_outer_composite_figi,
        episode_valid_from_session=case.episode_valid_from_session,
        episode_valid_through_session=case.episode_valid_through_session,
        episode_source_record_count=len(case.episode_source_record_ids),
        episode_source_record_set_digest=case.episode_source_record_set_digest,
    )
    provider_pin = S7_SOURCE_PINS["asset_observation_daily"]
    proposal = IdentityAdjudicationProposal(
        case=case_ref,
        decision_version=1,
        disposition=IdentityDisposition.CONFIRMED_PROVIDER_CONTAMINATION,
        canonical_composite_figi=FIGI_A,
        reason_code="provider_figi_episode_contamination",
        reason_detail="The exact bounded outer identity supports this reviewed mapping.",
        evidence_refs=(
            AdjudicationEvidenceRef(
                evidence_ref="candidate-manifest-outer-anchors",
                source_type=EvidenceSourceType.PINNED_MASSIVE_RELEASE,
                source_available_session=date(2024, 1, 5),
                source={
                    "dataset": provider_pin.table,
                    "release_id": provider_pin.release_id,
                    "source_record_id": source_ids[0],
                },
            ),
        ),
    )
    plan = IdentityAdjudicationPlan(
        candidate_manifest=candidate,
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
        availability_calendar_id="xnys-calendar-fixture-v1",
        availability_calendar_sha256=_digest("calendar-fixture"),
        proposed_by="identity-reviewer",
        proposed_at_utc=datetime(2024, 1, 5, 16, 0, tzinfo=UTC),
        proposals=(proposal,),
    )
    store = IdentityAdjudicationStore(tmp_path)
    with pytest.raises(IdentityAdjudicationError, match="source-bundle verification"):
        store.store_plan(plan)


def test_approved_unresolved_successor_withdraws_without_rewriting_predecessor() -> None:
    rows = _rows()
    case = _cases(rows)[0]
    first = _adjudication(
        case,
        disposition="confirmed_genuine_transition",
        canonical=FIGI_B,
    )
    successor = _adjudication(
        case,
        disposition="adjudicated_unresolved",
        canonical=None,
        version=2,
        predecessor=first.identity_adjudication_id,
    )
    result = resolve_identity_at_cutoff(
        tuple(row for row in rows if row.ticker == "GEN"),
        (case,),
        (first, successor),
        binding=_binding(),
    )
    middle = next(item for item in result.decisions if item.session_date.day == 2)
    assert middle.identity_disposition == "adjudicated_unresolved"
    assert middle.identity_adjudication_id == successor.identity_adjudication_id
    assert middle.backtest_identity_eligible is False


def test_confirmed_figi_does_not_waive_relationship_conflict() -> None:
    rows = _rows()
    case = _cases(rows)[0]
    conflicted = tuple(
        replace(row, relationship_conflict=True)
        if row.ticker == "GEN" and row.session_date.day == 2
        else row
        for row in rows
        if row.ticker == "GEN"
    )
    decision = _adjudication(
        case,
        disposition="confirmed_genuine_transition",
        canonical=FIGI_B,
    )
    result = resolve_identity_at_cutoff(
        conflicted,
        (case,),
        (decision,),
        binding=_binding(),
    )
    middle = next(item for item in result.decisions if item.session_date.day == 2)
    assert middle.canonical_composite_figi == FIGI_B
    assert middle.identity_resolution_status == "resolved_conflicted"
    assert middle.backtest_identity_eligible is False
    assert middle.alias_emitted is False


def test_case_absent_from_earlier_physical_manifest_uses_direct_observation() -> None:
    row = _rows()[1]
    earlier_binding = replace(
        _binding(),
        cutoff_session=date(2024, 1, 2),
        candidate_manifest_available_session=date(2024, 1, 2),
        adjudication_release_available_session=date(2024, 1, 2),
    )
    result = resolve_identity_at_cutoff((row,), (), (), binding=earlier_binding)
    assert result.decisions[0].canonical_composite_figi == FIGI_B
    assert result.decisions[0].identity_disposition == "observed_consistent"


def test_unapproved_decision_and_unanchored_override_fail_closed() -> None:
    rows = _rows()
    case = _cases(rows)[0]
    decision = _adjudication(
        case,
        disposition="confirmed_provider_contamination",
        canonical=FIGI_A,
    )
    with pytest.raises(IdentityResolutionError, match="lacks independent"):
        resolve_identity_at_cutoff(
            tuple(
                replace(row, relationship_conflict=True) if row.session_date.day != 2 else row
                for row in rows
                if row.ticker == "GEN"
            ),
            (case,),
            (decision,),
            binding=_binding(),
        )
    with pytest.raises(IdentityResolutionError, match="unapproved"):
        resolve_identity_at_cutoff(
            tuple(row for row in rows if row.ticker == "GEN"),
            (case,),
            (replace(decision, approval_status="proposed"),),
            binding=_binding(),
        )


def test_case_digest_and_post_cutoff_controls_fail_closed() -> None:
    rows = _rows()
    case = _cases(rows)[0]
    with pytest.raises(IdentityResolutionError, match="digest mismatch"):
        resolve_identity_at_cutoff(
            tuple(row for row in rows if row.ticker == "GEN"),
            (replace(case, episode_source_record_set_digest=_digest("wrong")),),
            (),
            binding=_binding(),
        )
    with pytest.raises(IdentityResolutionError, match="unavailable at the cutoff"):
        replace(
            _binding(),
            cutoff_session=date(2024, 1, 4),
        )
    with pytest.raises(IdentityResolutionError, match="later than the physical cutoff"):
        resolve_identity_at_cutoff(
            (
                ObservedIdentityMembership(
                    session_date=date(2024, 1, 6),
                    ticker="FUTURE",
                    active_on_date=True,
                    observed_composite_figi=FIGI_A,
                    source_record_id=_digest("future-membership-row"),
                ),
            ),
            (),
            (),
            binding=_binding(),
        )


def test_asset_id_fixed_vector() -> None:
    assert canonical_asset_id(FIGI_A) == (
        "53613427718de3074faafed6214578d53d8c8a2b5a08cb626395cd51b10c2d85"
    )
