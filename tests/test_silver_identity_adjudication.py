from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ame_stocks_api.artifacts import sha256_file, stable_digest
from ame_stocks_api.silver.calendar_artifact import (
    build_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.identity_adjudication import (
    AdjudicationControlState,
    AdjudicationEvidenceRef,
    AdjudicationReviewDecision,
    ApprovedIdentityDecision,
    CandidateManifestBinding,
    EvidenceSourceType,
    ExternalAuthorityClass,
    ExternalEvidenceCapture,
    IdentityAdjudicationError,
    IdentityAdjudicationPlan,
    IdentityAdjudicationProposal,
    IdentityAdjudicationRegistryRelease,
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
from ame_stocks_api.silver.identity_source import S7_SIX_RELEASE_BINDING_ID

SIX_RELEASE_BINDING_ID = "b" * 64
CALENDAR_ARTIFACT = build_xnys_calendar_artifact(date(2024, 1, 2), date(2024, 1, 31))
CALENDAR_ID = CALENDAR_ARTIFACT.calendar_artifact_id
CALENDAR_SHA = CALENDAR_ARTIFACT.sha256
FIGI_A = "BBG00000000A"
FIGI_B = "BBG00000000B"
CASE_ID = stable_digest(
    {
        "detector_rule_version": "s7_provider_figi_bounce_detector_v1",
        "episode_source_record_set_digest": stable_digest(["s4-b-001", "s4-b-002"]),
        "episode_valid_from_session": "2024-01-03",
        "episode_valid_through_session": "2024-01-04",
        "left_outer_composite_figi": FIGI_A,
        "left_outer_source_record_id": "s4-a-left",
        "middle_observed_composite_figi": FIGI_B,
        "namespace": "ame_stocks.identity.provider_figi_bounce_case",
        "right_outer_composite_figi": FIGI_A,
        "right_outer_source_record_id": "s4-a-right",
        "rule_version": "s7_provider_figi_bounce_case_id_v1",
        "six_release_binding_id": SIX_RELEASE_BINDING_ID,
        "ticker": "TEST",
    }
)


@pytest.fixture(autouse=True)
def _install_frozen_calendar(tmp_path: Path) -> None:
    write_xnys_calendar_artifact(tmp_path, CALENDAR_ARTIFACT)


def _case() -> IdentityCaseReference:
    return IdentityCaseReference(
        identity_case_id=CASE_ID,
        identity_case_available_session=date(2024, 1, 8),
        observed_ticker="TEST",
        observed_composite_figi=FIGI_B,
        left_outer_composite_figi=FIGI_A,
        right_outer_composite_figi=FIGI_A,
        episode_valid_from_session=date(2024, 1, 3),
        episode_valid_through_session=date(2024, 1, 4),
        episode_source_record_count=2,
        episode_source_record_set_digest=stable_digest(["s4-b-001", "s4-b-002"]),
    )


def _provider_ref(*, available: date = date(2024, 1, 5)) -> AdjudicationEvidenceRef:
    return AdjudicationEvidenceRef(
        evidence_ref="s4:left-and-right-anchor",
        source_type=EvidenceSourceType.PINNED_MASSIVE_RELEASE,
        source_available_session=available,
        source={
            "dataset": "asset_observation_daily",
            "release_id": "d" * 64,
            "source_record_id": "s4-a-left",
        },
    )


def _external_capture(content: bytes = b"fixture SEC filing bytes\n") -> ExternalEvidenceCapture:
    return ExternalEvidenceCapture(
        identity_case_id=CASE_ID,
        source_authority_class=ExternalAuthorityClass.REGULATOR_OFFICIAL,
        source_name="U.S. Securities and Exchange Commission",
        source_url="https://WWW.SEC.GOV/Archives/example.txt",
        source_published_at_utc=datetime(2024, 1, 5, 14, 0, tzinfo=UTC),
        observed_at_utc=datetime(2024, 1, 5, 14, 1, tzinfo=UTC),
        as_of_at_utc=datetime(2024, 1, 2, 21, 0, tzinfo=UTC),
        captured_at_utc=datetime(2024, 1, 5, 14, 2, tzinfo=UTC),
        source_available_session=date(2024, 1, 8),
        asserted_fields=("composite_figi", "ticker"),
        assertion={
            "relationship": "same security",
            "supports": {"canonical_composite_figi": FIGI_A},
        },
        media_type="text/plain",
        license_name="SEC public filing",
        license_url="https://www.sec.gov/os/accessing-edgar-data",
        captured_content=content,
    )


def _candidate_binding(
    root: Path,
    *,
    six_release_binding_id: str = SIX_RELEASE_BINDING_ID,
) -> CandidateManifestBinding:
    sessions = tuple(
        SourceSession(session_date=session)
        for session in (
            date(2024, 1, 2),
            date(2024, 1, 3),
            date(2024, 1, 4),
            date(2024, 1, 5),
        )
    )
    observations = tuple(
        IdentityObservation(
            session_date=session,
            ticker="TEST",
            observed_composite_figi=figi,
            source_record_id=source_record_id,
            source_available_session=session,
        )
        for session, figi, source_record_id in (
            (date(2024, 1, 2), FIGI_A, "s4-a-left"),
            (date(2024, 1, 3), FIGI_B, "s4-b-001"),
            (date(2024, 1, 4), FIGI_B, "s4-b-002"),
            (date(2024, 1, 5), FIGI_A, "s4-a-right"),
        )
    )
    detection = detect_provider_figi_bounces(
        sessions,
        observations,
        six_release_binding_id=six_release_binding_id,
        candidate_manifest_available_session=date(2024, 1, 8),
    )
    if six_release_binding_id == SIX_RELEASE_BINDING_ID:
        assert detection.cases[0].identity_case_id == CASE_ID
    manifest = build_identity_case_candidate_manifest(
        detection,
        created_at_utc=datetime(2024, 1, 5, 22, 0, tzinfo=UTC),
    )
    receipt = write_identity_case_candidate_manifest(root, manifest)
    return CandidateManifestBinding(
        manifest_id=manifest.candidate_manifest_id,
        manifest_sha256=str(receipt["sha256"]),
        path=manifest.relative_path,
    )


def _plan(
    root: Path,
    proposal: IdentityAdjudicationProposal,
    *,
    external_id: str | None = None,
    external_sha: str | None = None,
    proposed_at_utc: datetime = datetime(2024, 1, 8, 16, 0, tzinfo=UTC),
) -> IdentityAdjudicationPlan:
    return IdentityAdjudicationPlan(
        candidate_manifest=_candidate_binding(root),
        six_release_binding_id=SIX_RELEASE_BINDING_ID,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
        proposed_by="identity-reviewer",
        proposed_at_utc=proposed_at_utc,
        proposals=(proposal,),
        external_evidence_manifest_id=external_id,
        external_evidence_manifest_sha256=external_sha,
    )


def _contamination_proposal(
    *evidence: AdjudicationEvidenceRef,
    version: int = 1,
    supersedes: str | None = None,
) -> IdentityAdjudicationProposal:
    return IdentityAdjudicationProposal(
        case=_case(),
        decision_version=version,
        disposition=IdentityDisposition.CONFIRMED_PROVIDER_CONTAMINATION,
        canonical_composite_figi=FIGI_A,
        reason_code="provider_figi_episode_contamination",
        reason_detail=(
            "Official identity evidence supports the independently anchored outer security."
        ),
        evidence_refs=tuple(evidence) or (_provider_ref(),),
        supersedes_identity_adjudication_id=supersedes,
    )


def test_external_evidence_is_captured_as_content_addressed_immutable_bytes(
    tmp_path: Path,
) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    manifest, stored = store.capture_external_evidence(
        (_external_capture(),),
        six_release_binding_id=SIX_RELEASE_BINDING_ID,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
    )

    assert stored.path.endswith(f"manifest_id={manifest.manifest_id}.json")
    assert stored.sha256 == sha256_file(tmp_path / stored.path)
    record = manifest.records[0]
    assert record.normalized_url == "https://www.sec.gov/Archives/example.txt"
    assert record.source_name == "U.S. Securities and Exchange Commission"
    assert record.observed_at_utc == datetime(2024, 1, 5, 14, 1, tzinfo=UTC)
    assert record.as_of_at_utc == datetime(2024, 1, 2, 21, 0, tzinfo=UTC)
    assert record.captured_at_utc == datetime(2024, 1, 5, 14, 2, tzinfo=UTC)
    assert record.license_name == "SEC public filing"
    artifact = tmp_path / record.archived_artifact_path
    assert artifact.read_bytes() == b"fixture SEC filing bytes\n"
    assert record.archived_artifact_sha256 == hashlib.sha256(artifact.read_bytes()).hexdigest()

    loaded, loaded_document = store.load_external_evidence(manifest.manifest_id)
    assert loaded == manifest
    assert loaded_document == stored

    repeated, repeated_document = store.capture_external_evidence(
        (_external_capture(),),
        six_release_binding_id=SIX_RELEASE_BINDING_ID,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
    )
    assert repeated == manifest
    assert repeated_document == stored


def test_external_evidence_requires_bytes_and_rejects_secret_bearing_urls() -> None:
    with pytest.raises(IdentityAdjudicationError, match="non-empty immutable captured bytes"):
        _external_capture(b"")

    values = _external_capture().__dict__ if hasattr(_external_capture(), "__dict__") else None
    assert values is None  # frozen slot dataclass intentionally has no mutable dictionary
    with pytest.raises(IdentityAdjudicationError, match="sensitive query"):
        ExternalEvidenceCapture(
            identity_case_id=CASE_ID,
            source_authority_class=ExternalAuthorityClass.REGULATOR_OFFICIAL,
            source_name="SEC",
            source_url="https://sec.gov/filing?api_key=do-not-store",
            source_published_at_utc=datetime(2024, 1, 5, 14, 0, tzinfo=UTC),
            observed_at_utc=datetime(2024, 1, 5, 14, 1, tzinfo=UTC),
            as_of_at_utc=datetime(2024, 1, 2, 21, 0, tzinfo=UTC),
            captured_at_utc=datetime(2024, 1, 5, 14, 2, tzinfo=UTC),
            source_available_session=date(2024, 1, 8),
            asserted_fields=("ticker",),
            assertion={"ticker": "TEST"},
            media_type="text/plain",
            license_name="public",
            captured_content=b"bytes",
        )
    with pytest.raises(IdentityAdjudicationError, match="outcome or backtest"):
        replace(_external_capture(), asserted_fields=("return_1d",))
    with pytest.raises(IdentityAdjudicationError, match="outcome or backtest"):
        replace(
            _external_capture(),
            assertion={"identity": {"support": [{"diagnostics": {"sharpe": 2.0}}]}},
        )


def test_external_capture_and_reload_require_exact_frozen_calendar(tmp_path: Path) -> None:
    with pytest.raises(IdentityAdjudicationError, match="calendar ID/SHA trust chain"):
        IdentityAdjudicationStore(tmp_path / "missing-root").capture_external_evidence(
            (_external_capture(),),
            six_release_binding_id=SIX_RELEASE_BINDING_ID,
            availability_calendar_id=CALENDAR_ID,
            availability_calendar_sha256=CALENDAR_SHA,
        )

    store = IdentityAdjudicationStore(tmp_path)
    with pytest.raises(IdentityAdjudicationError, match="calendar ID/SHA trust chain"):
        store.capture_external_evidence(
            (_external_capture(),),
            six_release_binding_id=SIX_RELEASE_BINDING_ID,
            availability_calendar_id=CALENDAR_ID,
            availability_calendar_sha256="0" * 64,
        )

    manifest, _ = store.capture_external_evidence(
        (_external_capture(),),
        six_release_binding_id=SIX_RELEASE_BINDING_ID,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
    )
    calendar_path = tmp_path / CALENDAR_ARTIFACT.relative_path
    calendar_path.chmod(0o644)
    calendar_path.write_bytes(b"{}\n")
    with pytest.raises(IdentityAdjudicationError, match="calendar ID/SHA trust chain"):
        store.load_external_evidence(manifest.manifest_id)


def test_production_provider_evidence_must_use_one_exact_six_release_pin() -> None:
    proposal = _contamination_proposal(_provider_ref())
    with pytest.raises(IdentityAdjudicationError, match="exact six-release"):
        IdentityAdjudicationPlan(
            candidate_manifest=CandidateManifestBinding(
                manifest_id="e" * 64,
                manifest_sha256="f" * 64,
                path=(
                    "manifests/silver/identity-case-candidates/"
                    f"candidate_manifest_id={'e' * 64}.json"
                ),
            ),
            six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
            availability_calendar_id=CALENDAR_ID,
            availability_calendar_sha256=CALENDAR_SHA,
            proposed_by="identity-reviewer",
            proposed_at_utc=datetime(2024, 1, 8, 16, 0, tzinfo=UTC),
            proposals=(proposal,),
        )


def test_provider_evidence_record_must_belong_to_exact_candidate_lineage(
    tmp_path: Path,
) -> None:
    forged_provider_ref = replace(
        _provider_ref(),
        source={
            "dataset": "asset_observation_daily",
            "release_id": "d" * 64,
            "source_record_id": "record-not-present-in-candidate",
        },
    )
    proposal = _contamination_proposal(forged_provider_ref)

    with pytest.raises(IdentityAdjudicationError, match="outside exact candidate lineage"):
        IdentityAdjudicationStore(tmp_path).store_plan(_plan(tmp_path, proposal))


def test_unattested_production_candidate_cannot_write_or_load_empty_registry(
    tmp_path: Path,
) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    candidate = _candidate_binding(
        tmp_path,
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
    )
    release_args = {
        "candidate_manifest": candidate,
        "six_release_binding_id": S7_SIX_RELEASE_BINDING_ID,
        "availability_calendar_id": CALENDAR_ID,
        "availability_calendar_sha256": CALENDAR_SHA,
        "published_at_utc": datetime(2024, 1, 8, 18, 0, tzinfo=UTC),
        "release_available_session": date(2024, 1, 9),
    }

    with pytest.raises(IdentityAdjudicationError, match="source-bundle verification"):
        store.write_registry_release((), **release_args)

    forged_release = IdentityAdjudicationRegistryRelease(
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
        candidate_manifest_id=candidate.manifest_id,
        candidate_manifest_sha256=candidate.manifest_sha256,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
        published_at_utc=datetime(2024, 1, 8, 18, 0, tzinfo=UTC),
        release_available_session=date(2024, 1, 9),
        decisions=(),
    )
    forged_path = (
        tmp_path
        / "manifests/silver/identity/adjudication-registry-releases"
        / f"release_id={forged_release.release_id}.json"
    )
    forged_path.parent.mkdir(parents=True, exist_ok=True)
    forged_path.write_bytes(
        json.dumps(
            forged_release.to_dict(),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )
    with pytest.raises(IdentityAdjudicationError, match="source-bundle verification"):
        store.load_registry_release(forged_release.release_id)


def test_registry_load_rejects_candidate_release_binding_mismatch(tmp_path: Path) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    official_candidate = _candidate_binding(
        tmp_path,
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
    )
    mismatched_release = IdentityAdjudicationRegistryRelease(
        six_release_binding_id=SIX_RELEASE_BINDING_ID,
        candidate_manifest_id=official_candidate.manifest_id,
        candidate_manifest_sha256=official_candidate.manifest_sha256,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
        published_at_utc=datetime(2024, 1, 8, 18, 0, tzinfo=UTC),
        release_available_session=date(2024, 1, 9),
        decisions=(),
    )
    path = (
        tmp_path
        / "manifests/silver/identity/adjudication-registry-releases"
        / f"release_id={mismatched_release.release_id}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        json.dumps(
            mismatched_release.to_dict(),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )

    with pytest.raises(IdentityAdjudicationError, match="six-release bindings differ"):
        store.load_registry_release(mismatched_release.release_id)


def test_approved_override_requires_exact_plan_receipt_and_registry_chain(
    tmp_path: Path,
) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    external, external_document = store.capture_external_evidence(
        (_external_capture(),),
        six_release_binding_id=SIX_RELEASE_BINDING_ID,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
    )
    external_ref = external.records[0].to_evidence_ref(external.manifest_id)
    proposal = _contamination_proposal(_provider_ref(), external_ref)
    plan = _plan(
        tmp_path,
        proposal,
        external_id=external.manifest_id,
        external_sha=external_document.sha256,
    )
    plan_document = store.store_plan(plan)

    records = store.list_control_records()
    assert [(item.identity_adjudication_id, item.state) for item in records] == [
        (proposal.identity_adjudication_id, AdjudicationControlState.PROPOSED)
    ]
    with pytest.raises(IdentityAdjudicationError, match="no unique approved"):
        store.require_approved_decision(proposal.identity_adjudication_id)

    with pytest.raises(IdentityAdjudicationError, match="first XNYS open"):
        store.review(
            plan.plan_id,
            proposal.identity_adjudication_id,
            decision=AdjudicationReviewDecision.APPROVED,
            reviewed_by="reviewer@example.test",
            reviewed_at_utc=datetime(2024, 1, 8, 18, 0, tzinfo=UTC),
            review_reason="A backdated approval availability must fail.",
            approval_available_session=date(2024, 1, 8),
        )

    result = store.review(
        plan.plan_id,
        proposal.identity_adjudication_id,
        decision=AdjudicationReviewDecision.APPROVED,
        reviewed_by="reviewer@example.test",
        reviewed_at_utc=datetime(2024, 1, 8, 18, 0, tzinfo=UTC),
        review_reason="Exact archived evidence and the bounded episode were reviewed.",
        approval_available_session=date(2024, 1, 9),
    )
    assert result.approved_decision is not None
    approved = store.require_approved_decision(proposal.identity_adjudication_id)
    assert approved.identity_case_id == CASE_ID
    assert approved.identity_adjudication_id == proposal.identity_adjudication_id
    assert approved.adjudication_series_id == proposal.adjudication_series_id
    assert approved.decision_version == 1
    assert approved.observed_ticker == "TEST"
    assert approved.observed_composite_figi == FIGI_B
    assert approved.disposition is IdentityDisposition.CONFIRMED_PROVIDER_CONTAMINATION
    assert approved.canonical_composite_figi == FIGI_A
    assert approved.canonical_override is True
    assert approved.episode_valid_from_session == date(2024, 1, 3)
    assert approved.episode_valid_through_session == date(2024, 1, 4)
    assert approved.episode_source_record_set_digest == stable_digest(["s4-b-001", "s4-b-002"])
    assert approved.identity_case_available_session == date(2024, 1, 8)
    assert approved.adjudication_available_session == date(2024, 1, 9)
    assert approved.approval_status == "approved"
    assert approved.supersedes_identity_adjudication_id is None
    assert approved.outcome_or_backtest_evidence_used is False
    assert approved.source_decision_plan_path == plan_document.path
    assert store.list_control_records()[0].state is AdjudicationControlState.APPROVED

    with pytest.raises(IdentityAdjudicationError, match="already has a review receipt"):
        store.review(
            plan.plan_id,
            proposal.identity_adjudication_id,
            decision=AdjudicationReviewDecision.REJECTED,
            reviewed_by="different-reviewer",
            reviewed_at_utc=datetime(2024, 1, 9, 18, 0, tzinfo=UTC),
            review_reason="Conflicting second receipt must fail.",
        )


def test_rejected_proposal_never_enters_approved_registry(tmp_path: Path) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    proposal = _contamination_proposal(_provider_ref())
    plan = _plan(tmp_path, proposal)
    store.store_plan(plan)
    result = store.review(
        plan.plan_id,
        proposal.identity_adjudication_id,
        decision=AdjudicationReviewDecision.REJECTED,
        reviewed_by="identity-reviewer",
        reviewed_at_utc=datetime(2024, 1, 8, 18, 0, tzinfo=UTC),
        review_reason="The supplied evidence does not establish a canonical override.",
    )

    assert result.approved_decision is None
    assert store.load_approved_decisions() == ()
    assert store.list_control_records()[0].state is AdjudicationControlState.REJECTED
    with pytest.raises(IdentityAdjudicationError, match="no unique approved"):
        store.require_approved_decision(proposal.identity_adjudication_id)


def test_approved_receipt_cannot_be_reused_across_plan_or_proposal(
    tmp_path: Path,
) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    first = _contamination_proposal(_provider_ref())
    first_plan = _plan(tmp_path, first)
    first_document = store.store_plan(first_plan)
    approved = store.review(
        first_plan.plan_id,
        first.identity_adjudication_id,
        decision=AdjudicationReviewDecision.APPROVED,
        reviewed_by="identity-reviewer",
        reviewed_at_utc=datetime(2024, 1, 8, 18, 0, tzinfo=UTC),
        review_reason="The exact first proposal was approved.",
        approval_available_session=date(2024, 1, 9),
    )
    assert approved.approved_decision is not None

    alternate = replace(
        first,
        disposition=IdentityDisposition.ADJUDICATED_UNRESOLVED,
        canonical_composite_figi=None,
        reason_code="identity_evidence_unresolved",
        reason_detail="The same observations do not support a canonical identity.",
    )
    alternate_plan = _plan(tmp_path, alternate)
    alternate_document = store.store_plan(alternate_plan)
    with pytest.raises(IdentityAdjudicationError, match="exactly bound"):
        ApprovedIdentityDecision.create(
            alternate,
            alternate_plan,
            alternate_document,
            approved.receipt,
            approved.receipt_document,
        )

    different_plan = replace(first_plan, proposed_by="different-reviewer")
    different_plan_document = store.store_plan(different_plan)
    with pytest.raises(IdentityAdjudicationError, match="exactly bound"):
        ApprovedIdentityDecision.create(
            first,
            different_plan,
            different_plan_document,
            approved.receipt,
            approved.receipt_document,
        )

    assert first_document.path != different_plan_document.path

    forged = replace(
        approved.receipt,
        plan_id=alternate_plan.plan_id,
        plan_path=alternate_document.path,
        plan_sha256=alternate_document.sha256,
    )
    forged_path = (
        tmp_path
        / "manifests/silver/identity/adjudication-receipts"
        / f"receipt_id={forged.receipt_id}.json"
    )
    forged_path.parent.mkdir(parents=True, exist_ok=True)
    forged_path.write_bytes(
        json.dumps(
            forged.to_dict(),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )
    with pytest.raises(IdentityAdjudicationError, match="no unique requested proposal"):
        store.list_control_records()


def test_plan_requires_an_exact_real_detector_case_and_immutable_manifest(
    tmp_path: Path,
) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    candidate = _candidate_binding(tmp_path)
    mismatched = IdentityAdjudicationProposal(
        case=replace(_case(), observed_ticker="WRONG"),
        decision_version=1,
        disposition=IdentityDisposition.CONFIRMED_PROVIDER_CONTAMINATION,
        canonical_composite_figi=FIGI_A,
        reason_code="provider_figi_episode_contamination",
        reason_detail="Official evidence supports the bounded outer identity.",
        evidence_refs=(_provider_ref(),),
    )
    mismatched_plan = IdentityAdjudicationPlan(
        candidate_manifest=candidate,
        six_release_binding_id=SIX_RELEASE_BINDING_ID,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
        proposed_by="identity-reviewer",
        proposed_at_utc=datetime(2024, 1, 8, 16, 0, tzinfo=UTC),
        proposals=(mismatched,),
    )
    with pytest.raises(IdentityAdjudicationError, match="exactly match"):
        store.store_plan(mismatched_plan)

    valid = _contamination_proposal(_provider_ref())
    valid_plan = IdentityAdjudicationPlan(
        candidate_manifest=candidate,
        six_release_binding_id=SIX_RELEASE_BINDING_ID,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
        proposed_by="identity-reviewer",
        proposed_at_utc=datetime(2024, 1, 8, 16, 0, tzinfo=UTC),
        proposals=(valid,),
    )
    candidate_path = tmp_path / candidate.path
    candidate_path.chmod(0o644)
    candidate_path.write_bytes(candidate_path.read_bytes() + b" ")
    with pytest.raises(IdentityAdjudicationError, match="trust chain failed"):
        store.store_plan(valid_plan)


def test_plan_and_review_timestamps_are_causal(tmp_path: Path) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    proposal = _contamination_proposal(_provider_ref())
    too_early = _plan(
        tmp_path,
        proposal,
        proposed_at_utc=datetime(2024, 1, 8, 14, 0, tzinfo=UTC),
    )
    with pytest.raises(IdentityAdjudicationError, match="cannot precede"):
        store.store_plan(too_early)

    plan = _plan(tmp_path, proposal)
    store.store_plan(plan)
    with pytest.raises(IdentityAdjudicationError, match="cannot precede"):
        store.review(
            plan.plan_id,
            proposal.identity_adjudication_id,
            decision=AdjudicationReviewDecision.REJECTED,
            reviewed_by="identity-reviewer",
            reviewed_at_utc=datetime(2024, 1, 8, 15, 59, tzinfo=UTC),
            review_reason="A review cannot exist before its immutable plan.",
        )


def test_plan_and_review_reload_their_exact_calendar_binding(tmp_path: Path) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    proposal = _contamination_proposal(_provider_ref())
    plan = _plan(tmp_path, proposal)
    with pytest.raises(IdentityAdjudicationError, match="calendar ID/SHA trust chain"):
        store.store_plan(replace(plan, availability_calendar_sha256="0" * 64))

    store.store_plan(plan)
    calendar_path = tmp_path / CALENDAR_ARTIFACT.relative_path
    calendar_path.chmod(0o644)
    calendar_path.write_bytes(b"{}\n")
    with pytest.raises(IdentityAdjudicationError, match="calendar ID/SHA trust chain"):
        store.review(
            plan.plan_id,
            proposal.identity_adjudication_id,
            decision=AdjudicationReviewDecision.REJECTED,
            reviewed_by="identity-reviewer",
            reviewed_at_utc=datetime(2024, 1, 8, 18, 0, tzinfo=UTC),
            review_reason="A review cannot proceed after its frozen calendar is corrupted.",
        )


def test_approved_successor_supersedes_without_mutating_predecessor(
    tmp_path: Path,
) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    first = _contamination_proposal(_provider_ref())
    first_plan = _plan(tmp_path, first)
    store.store_plan(first_plan)
    first_result = store.review(
        first_plan.plan_id,
        first.identity_adjudication_id,
        decision=AdjudicationReviewDecision.APPROVED,
        reviewed_by="identity-reviewer",
        reviewed_at_utc=datetime(2024, 1, 8, 18, 0, tzinfo=UTC),
        review_reason="Initial bounded mapping was approved.",
        approval_available_session=date(2024, 1, 9),
    )
    assert first_result.approved_decision_document is not None
    first_bytes = (tmp_path / first_result.approved_decision_document.path).read_bytes()
    first_release, first_release_document = store.write_registry_release(
        (first.identity_adjudication_id,),
        candidate_manifest=first_plan.candidate_manifest,
        six_release_binding_id=SIX_RELEASE_BINDING_ID,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
        published_at_utc=datetime(2024, 1, 12, 14, 0, tzinfo=UTC),
        release_available_session=date(2024, 1, 12),
    )
    loaded_first = store.load_registry_release(first_release.release_id)
    assert [item.identity_adjudication_id for item in loaded_first.decisions] == [
        first.identity_adjudication_id
    ]
    assert loaded_first.candidate_manifest.candidate_manifest_id == (
        first_plan.candidate_manifest.manifest_id
    )

    second = IdentityAdjudicationProposal(
        case=_case(),
        decision_version=2,
        disposition=IdentityDisposition.ADJUDICATED_UNRESOLVED,
        canonical_composite_figi=None,
        reason_code="withdraw_prior_identity_mapping",
        reason_detail="New official evidence makes the prior relationship unresolved.",
        evidence_refs=(_provider_ref(available=date(2024, 1, 10)),),
        supersedes_identity_adjudication_id=first.identity_adjudication_id,
    )
    second_plan = _plan(
        tmp_path,
        second,
        proposed_at_utc=datetime(2024, 1, 10, 16, 0, tzinfo=UTC),
    )
    store.store_plan(second_plan)
    second_result = store.review(
        second_plan.plan_id,
        second.identity_adjudication_id,
        decision=AdjudicationReviewDecision.APPROVED,
        reviewed_by="identity-reviewer-2",
        reviewed_at_utc=datetime(2024, 1, 10, 18, 0, tzinfo=UTC),
        review_reason="Append-only withdrawal was independently reviewed.",
        approval_available_session=date(2024, 1, 11),
    )
    assert second_result.approved_decision is not None
    assert second_result.approved_decision.canonical_override is False
    assert second_result.approved_decision.outcome_or_backtest_evidence_used is False

    decisions = store.load_approved_decisions()
    assert [item.decision_version for item in decisions] == [1, 2]
    assert decisions[1].supersedes_identity_adjudication_id == first.identity_adjudication_id
    assert (tmp_path / first_result.approved_decision_document.path).read_bytes() == first_bytes
    states = {item.identity_adjudication_id: item.state for item in store.list_control_records()}
    assert states[first.identity_adjudication_id] is AdjudicationControlState.SUPERSEDED
    assert states[second.identity_adjudication_id] is AdjudicationControlState.APPROVED

    with pytest.raises(IdentityAdjudicationError, match="first XNYS open"):
        store.write_registry_release(
            (first.identity_adjudication_id, second.identity_adjudication_id),
            candidate_manifest=second_plan.candidate_manifest,
            six_release_binding_id=SIX_RELEASE_BINDING_ID,
            availability_calendar_id=CALENDAR_ID,
            availability_calendar_sha256=CALENDAR_SHA,
            published_at_utc=datetime(2024, 1, 15, 14, 0, tzinfo=UTC),
            release_available_session=date(2024, 1, 15),
        )

    with pytest.raises(IdentityAdjudicationError, match="calendar ID/SHA trust chain"):
        store.write_registry_release(
            (first.identity_adjudication_id, second.identity_adjudication_id),
            candidate_manifest=second_plan.candidate_manifest,
            six_release_binding_id=SIX_RELEASE_BINDING_ID,
            availability_calendar_id=CALENDAR_ID,
            availability_calendar_sha256="0" * 64,
            published_at_utc=datetime(2024, 1, 15, 14, 0, tzinfo=UTC),
            release_available_session=date(2024, 1, 16),
        )

    second_release, _ = store.write_registry_release(
        (first.identity_adjudication_id, second.identity_adjudication_id),
        candidate_manifest=second_plan.candidate_manifest,
        six_release_binding_id=SIX_RELEASE_BINDING_ID,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
        published_at_utc=datetime(2024, 1, 15, 14, 0, tzinfo=UTC),
        release_available_session=date(2024, 1, 16),
    )
    assert second_release.release_id != first_release.release_id
    assert [
        item.decision_version
        for item in store.load_registry_release(second_release.release_id).decisions
    ] == [1, 2]
    reloaded_first = store.load_registry_release(first_release.release_id)
    assert [item.decision_version for item in reloaded_first.decisions] == [1]
    assert reloaded_first.release_document == first_release_document

    calendar_path = tmp_path / CALENDAR_ARTIFACT.relative_path
    calendar_path.chmod(0o644)
    calendar_path.write_bytes(b"{}\n")
    with pytest.raises(IdentityAdjudicationError, match="calendar ID/SHA trust chain"):
        store.load_registry_release(second_release.release_id)


def test_successor_cannot_be_backdated_before_predecessor(tmp_path: Path) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    first = _contamination_proposal(_provider_ref())
    first_plan = _plan(tmp_path, first)
    store.store_plan(first_plan)
    store.review(
        first_plan.plan_id,
        first.identity_adjudication_id,
        decision=AdjudicationReviewDecision.APPROVED,
        reviewed_by="identity-reviewer",
        reviewed_at_utc=datetime(2024, 1, 10, 18, 0, tzinfo=UTC),
        review_reason="The predecessor was approved at its recorded review time.",
        approval_available_session=date(2024, 1, 11),
    )

    successor = IdentityAdjudicationProposal(
        case=_case(),
        decision_version=2,
        disposition=IdentityDisposition.ADJUDICATED_UNRESOLVED,
        canonical_composite_figi=None,
        reason_code="withdraw_prior_identity_mapping",
        reason_detail="The recorded evidence leaves the identity unresolved.",
        evidence_refs=(_provider_ref(),),
        supersedes_identity_adjudication_id=first.identity_adjudication_id,
    )
    successor_plan = _plan(tmp_path, successor)
    store.store_plan(successor_plan)
    with pytest.raises(IdentityAdjudicationError, match="chronology precedes"):
        store.review(
            successor_plan.plan_id,
            successor.identity_adjudication_id,
            decision=AdjudicationReviewDecision.APPROVED,
            reviewed_by="identity-reviewer-2",
            reviewed_at_utc=datetime(2024, 1, 9, 18, 0, tzinfo=UTC),
            review_reason="A successor cannot be logically backdated.",
            approval_available_session=date(2024, 1, 10),
        )


def test_registry_publication_cannot_predate_included_approval(tmp_path: Path) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    proposal = _contamination_proposal(_provider_ref())
    plan = _plan(tmp_path, proposal)
    store.store_plan(plan)
    store.review(
        plan.plan_id,
        proposal.identity_adjudication_id,
        decision=AdjudicationReviewDecision.APPROVED,
        reviewed_by="identity-reviewer",
        reviewed_at_utc=datetime(2024, 1, 8, 18, 0, tzinfo=UTC),
        review_reason="The bounded identity evidence supports this decision.",
        approval_available_session=date(2024, 1, 9),
    )
    with pytest.raises(IdentityAdjudicationError, match="cannot precede an included approval"):
        store.write_registry_release(
            (proposal.identity_adjudication_id,),
            candidate_manifest=plan.candidate_manifest,
            six_release_binding_id=SIX_RELEASE_BINDING_ID,
            availability_calendar_id=CALENDAR_ID,
            availability_calendar_sha256=CALENDAR_SHA,
            published_at_utc=datetime(2024, 1, 8, 17, 0, tzinfo=UTC),
            release_available_session=date(2024, 1, 9),
        )


def test_external_archive_tampering_fails_the_manifest_trust_chain(
    tmp_path: Path,
) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    manifest, _ = store.capture_external_evidence(
        (_external_capture(),),
        six_release_binding_id=SIX_RELEASE_BINDING_ID,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
    )
    artifact = tmp_path / manifest.records[0].archived_artifact_path
    artifact.chmod(0o644)
    artifact.write_bytes(b"tampered")

    with pytest.raises(IdentityAdjudicationError, match="trust chain failed"):
        store.load_external_evidence(manifest.manifest_id)


def test_plan_reload_revalidates_external_evidence_bytes(tmp_path: Path) -> None:
    store = IdentityAdjudicationStore(tmp_path)
    external, external_document = store.capture_external_evidence(
        (_external_capture(),),
        six_release_binding_id=SIX_RELEASE_BINDING_ID,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
    )
    proposal = _contamination_proposal(
        _provider_ref(),
        external.records[0].to_evidence_ref(external.manifest_id),
    )
    plan = _plan(
        tmp_path,
        proposal,
        external_id=external.manifest_id,
        external_sha=external_document.sha256,
    )
    store.store_plan(plan)

    artifact = tmp_path / external.records[0].archived_artifact_path
    artifact.chmod(0o644)
    artifact.write_bytes(b"tampered after the plan was frozen")
    with pytest.raises(IdentityAdjudicationError, match="trust chain failed"):
        store.load_plan(plan.plan_id)
