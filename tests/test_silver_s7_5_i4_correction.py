from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import replace
from datetime import date
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_silver_s7_5_i3_checkpoint import _checkpoint, _parent_manifest
from test_silver_s7_5_i3_runner import (
    CALENDAR,
    MEMBER_AVAILABLE,
    _decision_record,
    _legacy_row,
    _snapshot,
)
from test_silver_s7_5_incremental_contract import _projection as _gate_projection

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_i3_runner as i3_runner
from ame_stocks_api.silver import incremental_i4_correction as i4_correction
from ame_stocks_api.silver.incremental_contract import (
    ArtifactPin,
    CheckpointReceipt,
    PartitionReceipt,
    PartitionReplacement,
    ReleaseType,
    RowSemanticProofReceipt,
    RowVersionOperation,
    RowVersionReceipt,
    RowVersionReference,
    control_object_pin,
    correction_scope_digest,
    logical_change_set_digest,
)
from ame_stocks_api.silver.incremental_gate import (
    CORRECTION_AUTHORIZATION_LITERAL_VERSION,
    CorrectionAuthorization,
    CorrectionAuthorizedAction,
    GateArtifactPin,
    GateEvidencePin,
    PinnedCorrectionAuthorization,
)
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    NATIVE_V2_RELEASE_FAMILY,
    IdentityPolicyBundle,
    IdentityRegistryKind,
    IdentityRegistryReleasePin,
    NativeV2ParentReleasePin,
    OpenAliasState,
    ResolvedPartitionState,
    i3_resolved_state_digest,
)
from ame_stocks_api.silver.incremental_i3_contract import I3_V2_CONTRACTS
from ame_stocks_api.silver.incremental_i3_dispatch import (
    IdentityObservation,
    IdentityPolicySnapshot,
    RegistryDecision,
    RegistrySourceScopeRow,
    _mint_production_identity_policy_snapshot,
)
from ame_stocks_api.silver.incremental_i4_correction import (
    AliasBoundaryProof,
    CanonicalIdentityProjection,
    ExactGroupExpansionRequired,
    ExactGroupSessionSlot,
    ExactIdentityGroup,
    I4AliasStateLedgerEntry,
    I4AliasStateLedgerRelease,
    I4ApprovalEvent,
    I4ApprovalEventAttestation,
    I4ApprovalLedgerEntry,
    I4ApprovalLedgerRelease,
    I4CorrectionError,
    I4CorrectionPlan,
    I4LateSourceChangeLedgerRelease,
    I4LateSourceLedgerEntry,
    I4LateSourceSnapshot,
    I4ProductionCorrectionCause,
    I4RegistryChangeLedgerRelease,
    I4RegistryLedgerEntry,
    ProductionI4CorrectionCapability,
    RegistryChange,
    RegistryChangeOperation,
    SessionPartitionImage,
    SourceIdentityKey,
    attest_i4_approval_event_exact,
    freeze_bounded_correction_scope,
    mint_production_i4_correction_capability,
    production_i4_source_binding_digest,
    select_first_stable_alias_boundary,
    validate_canonical_row_correction,
)
from ame_stocks_api.silver.incremental_identity import (
    AliasResolutionDisposition,
    AliasResolutionMethod,
    AliasResolutionStatus,
    AliasResolutionVersion,
    AliasSegmentIdentity,
    ShareClassResolutionMethod,
    canonical_asset_id,
    canonical_issuer_id,
    canonical_share_class_id,
)

SESSION = date(2026, 7, 10)
APPROVAL_AVAILABLE = date(2026, 8, 1)
CUTOFF = date(2026, 8, 2)
OBSERVED = "BBG000KRLLH9"
CANONICAL = "BBG000DFMXT3"
SECOND_CANONICAL = "BBG000B9XRY4"
SHARE = "BBG001S87NT0"
OTHER_SHARE = "BBG001S5N8V8"
CIK = "0000320193"


def _digest(label: str) -> str:
    return stable_digest({"i4-fixture": label})


def _artifact(label: str, *, path: str | None = None) -> ArtifactPin:
    content = label.encode()
    return ArtifactPin(
        path=path or f"fixtures/i4/{label}.json",
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _gate_artifact(label: str, *, path: str | None = None) -> GateArtifactPin:
    content = label.encode()
    return GateArtifactPin(
        path=path or f"fixtures/i4/{label}.json",
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _approval_event() -> I4ApprovalEvent:
    return I4ApprovalEvent(
        authorized_action=CorrectionAuthorizedAction.PUBLISH_EXACT_CORRECTION,
        parent_release_id=_digest("parent-release"),
        expected_change_set_digest=_digest("change-set"),
        source_binding_digest=_digest("source-binding"),
        schema_digest=_digest("schema"),
        transform_semantics_digest=_digest("transform"),
        calendar_digest=_digest("calendar"),
        identity_policy_before_id=_digest("policy-before"),
        identity_policy_after_id=_digest("policy-after"),
        scope_digest=_digest("gate-scope"),
        approver_id="research_owner",
        event_available_session=APPROVAL_AVAILABLE,
    )


def _authorization(
    event: I4ApprovalEvent | None = None,
) -> PinnedCorrectionAuthorization:
    selected_event = event or _approval_event()
    body = CorrectionAuthorization(
        authorized_action=CorrectionAuthorizedAction.PUBLISH_EXACT_CORRECTION,
        literal_version=CORRECTION_AUTHORIZATION_LITERAL_VERSION,
        parent_release_id=_digest("parent-release"),
        expected_change_set_digest=_digest("change-set"),
        source_binding_digest=_digest("source-binding"),
        schema_digest=_digest("schema"),
        transform_semantics_digest=_digest("transform"),
        calendar_digest=_digest("calendar"),
        identity_policy_before_id=_digest("policy-before"),
        identity_policy_after_id=_digest("policy-after"),
        scope_digest=_digest("gate-scope"),
        approval_event_id=selected_event.approval_event_id,
        approval_event_sha256=hashlib.sha256(selected_event.canonical_bytes()).hexdigest(),
        approver_id="research_owner",
        approval_available_session=APPROVAL_AVAILABLE,
        evidence_pins=(
            GateEvidencePin(
                artifact=_gate_artifact("evidence"),
                available_session=date(2026, 7, 31),
            ),
        ),
    )
    return PinnedCorrectionAuthorization.freeze(
        body,
        path="fixtures/i4/correction-authorization.json",
    )


def _source(
    *,
    session: date = SESSION,
    source_id: str | None = None,
    locale: str = "us",
    country: str = "GB",
    observed: str = OBSERVED,
    share: str = SHARE,
) -> SourceIdentityKey:
    return SourceIdentityKey(
        provider_id="massive",
        provider_market="stocks",
        provider_locale=locale,
        ticker="AAPL",
        session_date=session,
        source_record_id=source_id or _digest(f"source-{session}-{locale}"),
        observed_composite_figi=observed,
        observed_composite_country=country,
        observed_share_class_figi=share,
        active_on_date=True,
    )


def _projection(
    source: SourceIdentityKey,
    *,
    composite: str = OBSERVED,
    share: str = SHARE,
    eligible: bool = True,
    decision_ids: tuple[str, ...] = (),
    share_decision_ids: tuple[str, ...] = (),
    method: str = "direct_observed",
    disposition: str = "observed_consistent",
    issuer_id: str | None = None,
    cik: str | None = CIK,
) -> CanonicalIdentityProjection:
    return CanonicalIdentityProjection(
        source=source,
        canonical_composite_figi=composite if eligible else None,
        canonical_asset_id=canonical_asset_id(composite) if eligible else None,
        canonical_share_class_figi=share if eligible else None,
        canonical_share_class_id=canonical_share_class_id(share) if eligible else None,
        canonical_issuer_id=(
            issuer_id
            if issuer_id is not None
            else canonical_issuer_id(cik)
            if cik is not None
            else None
        ),
        canonical_cik_normalized=cik,
        backtest_identity_eligible=eligible,
        resolution_method=method,
        resolution_status="resolved" if eligible else "unresolved",
        disposition=disposition,
        share_class_resolution_method=(
            "approved_share_class_adjudication" if share_decision_ids else "direct_observed"
        ),
        decision_lineage_ids=decision_ids,
        share_class_decision_lineage_ids=share_decision_ids,
        alias_segment_id=_digest("alias-segment"),
        alias_resolution_version_id=_digest(
            f"alias-resolution-{composite}-{share}-{method}-{disposition}"
        ),
    )


def _registry_pair(
    kind: IdentityRegistryKind,
    *,
    source: SourceIdentityKey,
    canonical_before: str = CANONICAL,
    canonical_after: str = SECOND_CANONICAL,
    share_before: str = SHARE,
    share_after: str = OTHER_SHARE,
) -> tuple[RegistryChange, object, object]:
    kwargs: dict[str, object] = {
        "session": source.session_date,
        "source_record_id": source.source_record_id,
    }
    if kind is IdentityRegistryKind.SHARE_CLASS_ADJUDICATION:
        kwargs.update(
            {
                "observed_composite_figi": source.observed_composite_figi,
                "composite_scope_figi": source.observed_composite_figi,
                "observed_share_class_figi": share_before,
                "canonical_share_class_figi": share_after,
            }
        )
        before_kwargs = {**kwargs, "canonical_share_class_figi": canonical_before}
    elif kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION:
        kwargs.update(
            {
                "observed_composite_figi": source.observed_composite_figi,
                "observed_share_class_figi": source.observed_share_class_figi,
                "canonical_composite_figi": canonical_after,
            }
        )
        before_kwargs = {**kwargs, "canonical_composite_figi": canonical_before}
    elif kind is IdentityRegistryKind.IDENTITY_ADJUDICATION:
        kwargs.update(
            {
                "observed_composite_figi": source.observed_composite_figi,
                "canonical_composite_figi": canonical_after,
            }
        )
        before_kwargs = {**kwargs, "canonical_composite_figi": canonical_before}
    elif kind is IdentityRegistryKind.ASSET_TRANSITION:
        kwargs.update(
            {
                "predecessor_asset_id": canonical_asset_id(OBSERVED),
                "successor_asset_id": canonical_asset_id(canonical_after),
            }
        )
        before_kwargs = {
            **kwargs,
            "successor_asset_id": canonical_asset_id(canonical_before),
        }
    else:
        raise AssertionError("fixture pair supports correction registries only")
    before_snapshot = _snapshot((kind, _decision_record(kind, **before_kwargs)))
    after_snapshot = _snapshot((kind, _decision_record(kind, **kwargs)))
    predecessor = before_snapshot.decisions[0]
    successor = after_snapshot.decisions[0]
    return (
        RegistryChange(
            operation=RegistryChangeOperation.SUCCESSOR,
            predecessor=predecessor,
            successor=successor,
            change_available_session=MEMBER_AVAILABLE,
        ),
        before_snapshot,
        after_snapshot,
    )


def test_i3_observation_projection_and_us_exact_group_are_closed() -> None:
    observation = IdentityObservation(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker="AAPL",
        session_date=SESSION,
        observed_composite_figi=OBSERVED,
        observed_composite_country="GB",
        observed_share_class_figi=SHARE,
        primary_exchange="XNAS",
        source_record_id=_digest("i3-observation"),
        active_on_date=True,
    )
    source = SourceIdentityKey.from_i3_observation(observation)
    group = ExactIdentityGroup("massive", "stocks", "us", "AAPL")

    assert group.matches(source)
    assert source.to_dict()["observed_composite_figi"] == OBSERVED
    with pytest.raises(I4CorrectionError, match="locale=us"):
        ExactIdentityGroup("massive", "stocks", "ca", "AAPL")


def test_i4_exact_group_preserves_mixed_case_provider_ticker() -> None:
    source = replace(_source(), ticker="AANw")
    group = ExactIdentityGroup("massive", "stocks", "us", "AANw")

    assert group.matches(source)
    assert source.ticker == "AANw"


def test_alias_scope_stops_at_first_reproduced_i3_open_alias_frontier() -> None:
    checkpoint = _checkpoint()
    group = ExactIdentityGroup("massive", "stocks", "us", "AAPL")
    source = _source()
    slot = ExactGroupSessionSlot(group, SESSION, (source,))
    shared = _digest("lookback")
    future = _digest("future-policy-effect")
    proof = AliasBoundaryProof(
        group=group,
        session_date=SESSION,
        source_slot_digest=slot.slot_digest,
        parent_open_alias=checkpoint.open_aliases[0],
        corrected_open_alias=checkpoint.open_aliases[0],
        parent_fixed_lookback_digest=shared,
        corrected_fixed_lookback_digest=shared,
        parent_future_registry_effect_digest=future,
        corrected_future_registry_effect_digest=future,
        exact_group_history_complete=True,
        correction_effect_exhausted=True,
    )

    boundary, selected = select_first_stable_alias_boundary(
        group=group,
        earliest_affected_session=SESSION,
        exact_group_slots=(slot,),
        boundary_proofs=(proof,),
    )
    scope = freeze_bounded_correction_scope(
        group=group,
        direct_source_rows=(source,),
        exact_group_slots=(slot,),
        boundary_proofs=(proof,),
        ordered_calendar_sessions=(SESSION,),
        authorization=_authorization(),
        availability_cutoff_session=CUTOFF,
    )

    assert boundary == SESSION
    assert selected == (slot,)
    assert scope.recompute_sessions == (SESSION,)
    assert scope.exact_scope_id == stable_digest(scope.logical_payload())

    with pytest.raises(I4CorrectionError, match="stable-boundary factory"):
        type(scope)(
            group=group,
            direct_source_rows=(source,),
            exact_group_slots=(slot,),
            alias_recompute_from_session=SESSION,
            alias_stable_boundary_session=SESSION,
            authorization_id=_digest("fake-authorization"),
            authorization_available_session=APPROVAL_AVAILABLE,
            availability_cutoff_session=CUTOFF,
            _seal=object(),
        )


def test_alias_scope_without_convergence_fails_closed_to_same_exact_group() -> None:
    group = ExactIdentityGroup("massive", "stocks", "us", "AAPL")
    source = _source()
    slot = ExactGroupSessionSlot(group, SESSION, (source,))
    proof = AliasBoundaryProof(
        group=group,
        session_date=SESSION,
        source_slot_digest=slot.slot_digest,
        parent_open_alias=None,
        corrected_open_alias=None,
        parent_fixed_lookback_digest=_digest("parent-lookback"),
        corrected_fixed_lookback_digest=_digest("corrected-lookback"),
        parent_future_registry_effect_digest=_digest("future"),
        corrected_future_registry_effect_digest=_digest("future"),
        exact_group_history_complete=True,
        correction_effect_exhausted=True,
    )

    with pytest.raises(ExactGroupExpansionRequired) as caught:
        select_first_stable_alias_boundary(
            group=group,
            earliest_affected_session=SESSION,
            exact_group_slots=(slot,),
            boundary_proofs=(proof,),
        )

    assert caught.value.group == group
    assert caught.value.from_session == SESSION
    assert "expand only exact group" in str(caught.value)


def test_alias_boundary_cannot_skip_an_unproved_earlier_session() -> None:
    group = ExactIdentityGroup("massive", "stocks", "us", "AAPL")
    first = _source(session=CALENDAR[1])
    second = _source(session=CALENDAR[2])
    slots = (
        ExactGroupSessionSlot(group, CALENDAR[1], (first,)),
        ExactGroupSessionSlot(group, CALENDAR[2], (second,)),
    )
    proof = AliasBoundaryProof(
        group=group,
        session_date=CALENDAR[2],
        source_slot_digest=slots[1].slot_digest,
        parent_open_alias=None,
        corrected_open_alias=None,
        parent_fixed_lookback_digest=_digest("same-lookback"),
        corrected_fixed_lookback_digest=_digest("same-lookback"),
        parent_future_registry_effect_digest=_digest("same-future"),
        corrected_future_registry_effect_digest=_digest("same-future"),
        exact_group_history_complete=True,
        correction_effect_exhausted=True,
    )

    with pytest.raises(ExactGroupExpansionRequired, match="missing boundary proof"):
        select_first_stable_alias_boundary(
            group=group,
            earliest_affected_session=CALENDAR[1],
            exact_group_slots=slots,
            boundary_proofs=(proof,),
        )


def test_registry_successor_and_withdrawal_preserve_exact_responsibility() -> None:
    source = _source(session=CALENDAR[2])
    change, _, _ = _registry_pair(
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        source=source,
    )

    assert change.registry_kind is IdentityRegistryKind.IDENTITY_ADJUDICATION
    assert change.decision_after is change.successor
    withdrawal = RegistryChange(
        operation=RegistryChangeOperation.WITHDRAWAL,
        predecessor=change.predecessor,
        successor=None,
        change_available_session=MEMBER_AVAILABLE,
    )
    assert withdrawal.decision_after is None

    widened = replace(change.successor, source_record_id=_digest("other-source"))
    with pytest.raises(I4CorrectionError, match="widened"):
        RegistryChange(
            operation=RegistryChangeOperation.SUCCESSOR,
            predecessor=change.predecessor,
            successor=widened,
            change_available_session=MEMBER_AVAILABLE,
        )


def test_share_class_successor_cannot_change_composite_asset_or_issuer() -> None:
    source = _source(
        session=CALENDAR[2],
        country="US",
        observed=CANONICAL,
        share=SHARE,
    )
    change, _, target = _registry_pair(
        IdentityRegistryKind.SHARE_CLASS_ADJUDICATION,
        source=source,
        share_after=OTHER_SHARE,
    )
    successor_id = change.successor.decision_id
    old = _projection(source, composite=CANONICAL, share=SHARE)
    corrected = _projection(
        source,
        composite=CANONICAL,
        share=OTHER_SHARE,
        share_decision_ids=(successor_id,),
        disposition="transient_duplicate_share_class",
    )

    validate_canonical_row_correction(
        old,
        corrected,
        registry_changes=(change,),
        target_policy_snapshot=target,
    )
    with pytest.raises(I4CorrectionError, match="Share Class correction crossed"):
        validate_canonical_row_correction(
            old,
            replace(
                corrected,
                canonical_composite_figi=SECOND_CANONICAL,
                canonical_asset_id=canonical_asset_id(SECOND_CANONICAL),
            ),
            registry_changes=(change,),
            target_policy_snapshot=target,
        )
    with pytest.raises(I4CorrectionError, match="issuer authority"):
        validate_canonical_row_correction(
            old,
            replace(
                corrected,
                canonical_cik_normalized="0000789019",
                canonical_issuer_id=canonical_issuer_id("0000789019"),
            ),
            registry_changes=(change,),
            target_policy_snapshot=target,
        )


def test_cross_registry_collision_is_ineligible_for_automatic_correction() -> None:
    source = _source(session=CALENDAR[2])
    change, _, _ = _registry_pair(
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        source=source,
    )
    identity_record = _decision_record(
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        session=source.session_date,
        source_record_id=source.source_record_id,
        observed_composite_figi=OBSERVED,
        canonical_composite_figi=SECOND_CANONICAL,
    )
    cross_record = _decision_record(
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        session=source.session_date,
        source_record_id=source.source_record_id,
        observed_composite_figi=OBSERVED,
        observed_share_class_figi=SHARE,
        canonical_composite_figi=CANONICAL,
    )
    colliding_target = _snapshot(
        (
            IdentityRegistryKind.IDENTITY_ADJUDICATION,
            identity_record,
        ),
        (
            IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
            cross_record,
        ),
    )
    old = _projection(source)
    corrected = _projection(
        source,
        composite=SECOND_CANONICAL,
        decision_ids=(change.successor.decision_id,),
        method="approved_provider_contamination_override",
        disposition="confirmed_provider_contamination",
    )

    with pytest.raises(I4CorrectionError, match="cross-registry Composite collision"):
        validate_canonical_row_correction(
            old,
            corrected,
            registry_changes=(change,),
            target_policy_snapshot=colliding_target,
        )


def test_composite_registry_cannot_silently_correct_share_class() -> None:
    source = _source(session=CALENDAR[2])
    change, _, target = _registry_pair(
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        source=source,
    )
    corrected = _projection(
        source,
        composite=SECOND_CANONICAL,
        share=OTHER_SHARE,
        decision_ids=(change.successor.decision_id,),
        method="approved_provider_contamination_override",
        disposition="confirmed_provider_contamination",
    )

    with pytest.raises(I4CorrectionError, match="crossed Share Class"):
        validate_canonical_row_correction(
            _projection(source),
            corrected,
            registry_changes=(change,),
            target_policy_snapshot=target,
        )


def test_asset_transition_is_lineage_only_and_cannot_change_eligibility() -> None:
    source = _source(session=CALENDAR[2], country="US")
    change, _, target = _registry_pair(
        IdentityRegistryKind.ASSET_TRANSITION,
        source=source,
    )

    validate_canonical_row_correction(
        _projection(source),
        _projection(source),
        registry_changes=(change,),
        target_policy_snapshot=target,
    )
    with pytest.raises(I4CorrectionError, match="lineage only"):
        validate_canonical_row_correction(
            _projection(source),
            _projection(source, composite=SECOND_CANONICAL),
            registry_changes=(change,),
            target_policy_snapshot=target,
        )


def test_foreign_locale_identity_cannot_be_overridden_by_us_registry() -> None:
    foreign = _source(
        session=CALENDAR[2],
        locale="ca",
        country="CA",
        observed=OBSERVED,
    )
    old = _projection(foreign)
    changed = replace(
        old,
        canonical_composite_figi=CANONICAL,
        canonical_asset_id=canonical_asset_id(CANONICAL),
    )

    validate_canonical_row_correction(
        old,
        old,
        registry_changes=(),
        target_policy_snapshot=_snapshot(),
    )
    with pytest.raises(I4CorrectionError, match="foreign-locale"):
        validate_canonical_row_correction(
            old,
            changed,
            registry_changes=(),
            target_policy_snapshot=_snapshot(),
        )


def test_partition_image_requires_complete_sorted_whole_session_rows() -> None:
    source = _source()
    projection = _projection(source)
    receipt = PartitionReceipt(
        table_name="universe_daily",
        partition_key=SESSION.isoformat(),
        receipt=_artifact("partition"),
        row_count=1,
        schema_digest=_digest("schema"),
        availability_session=APPROVAL_AVAILABLE,
    )

    image = SessionPartitionImage(receipt=receipt, rows=(projection,))
    assert image.session_date == SESSION
    with pytest.raises(I4CorrectionError, match="row count"):
        SessionPartitionImage(receipt=replace(receipt, row_count=2), rows=(projection,))


def test_approval_event_attestation_binds_authorization_bytes_and_availability() -> None:
    event = _approval_event()
    authorization = _authorization(event)
    body = authorization.authorization
    event_pin = event.exact_pin(path="fixtures/i4/approval-event.json")
    ledger = I4ApprovalLedgerRelease(
        release_sequence=1,
        previous_ledger_release_id=None,
        release_available_session=APPROVAL_AVAILABLE,
        entries=(
            I4ApprovalLedgerEntry(
                ledger_index=1,
                authorization_id=body.authorization_id,
                authorization_artifact=authorization.artifact,
                approval_event_id=event.approval_event_id,
                event_artifact=event_pin,
                recorded_available_session=APPROVAL_AVAILABLE,
            ),
        ),
    )
    ledger_pin = ledger.exact_pin(path="fixtures/i4/approval-ledger.json")
    artifacts = {
        authorization.artifact.path: (
            json.dumps(
                authorization.authorization.to_dict(),
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
            + b"\n"
        ),
        event_pin.path: event.canonical_bytes(),
        ledger_pin.path: ledger.canonical_bytes(),
    }
    attestation = attest_i4_approval_event_exact(
        authorization=authorization,
        event=event,
        event_artifact=event_pin,
        ledger=ledger,
        ledger_artifact=ledger_pin,
        availability_cutoff_session=CUTOFF,
        artifact_reader=artifacts.__getitem__,
    )

    attestation.validate(authorization, availability_cutoff_session=CUTOFF)
    with pytest.raises(I4CorrectionError, match="unavailable"):
        attestation.validate(
            authorization,
            availability_cutoff_session=date(2026, 7, 31),
        )
    with pytest.raises(I4CorrectionError, match="exact-reader factory"):
        I4ApprovalEventAttestation(
            authorization_id=body.authorization_id,
            approval_event_id=body.approval_event_id,
            event_artifact=event_pin,
            ledger_release_id=ledger.ledger_release_id,
            ledger_artifact=ledger_pin,
            attestation_available_session=APPROVAL_AVAILABLE,
            _seal=object(),
        )


def test_correction_plan_cannot_be_constructed_without_factory_seal() -> None:
    with pytest.raises(I4CorrectionError, match="validated factory"):
        I4CorrectionPlan(
            parent_checkpoint_id=_digest("checkpoint"),
            parent_release_id=_digest("release"),
            exact_scope=None,  # type: ignore[arg-type]
            partition_replacements=(),
            added_row_version_receipts=(),
            superseded_row_version_ids=(),
            registry_changes=(),
            authorization_id=_digest("authorization"),
            approval_attestation=None,  # type: ignore[arg-type]
            change_set_digest=_digest("change-set"),
            identity_policy_before_id=_digest("before"),
            identity_policy_after_id=_digest("after"),
            _seal=object(),
        )


PRODUCTION_SESSIONS = tuple(date(2026, 7, day) for day in (7, 8, 9, 10))
PRODUCTION_PARENT_AVAILABLE = date(2026, 8, 3)
PRODUCTION_APPROVAL_AVAILABLE = date(2026, 8, 6)
PRODUCTION_REPLACEMENT_AVAILABLE = date(2026, 8, 7)
PRODUCTION_CUTOFF = date(2026, 8, 10)


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )


def _bytes_pin(path: str, content: bytes) -> ArtifactPin:
    return ArtifactPin(
        path=path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _parquet_bytes(rows: tuple[dict[str, object], ...]) -> bytes:
    table = pa.Table.from_pylist(
        list(rows),
        schema=I3_V2_CONTRACTS["universe_daily"].arrow_schema,
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="NONE")
    return sink.getvalue().to_pybytes()


def _table_parquet_bytes(
    table_name: str,
    rows: tuple[dict[str, object], ...],
) -> bytes:
    table = pa.Table.from_pylist(list(rows), schema=I3_V2_CONTRACTS[table_name].arrow_schema)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="NONE")
    return sink.getvalue().to_pybytes()


def _jsonable(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in sorted(value.items())}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


def _row_semantic_proof_content(proof: RowSemanticProofReceipt) -> bytes:
    body = {
        "artifact_type": "s7_5_i3_production_row_semantic_proof",
        "operation": proof.operation.value,
        "predecessor_payload_digest": proof.predecessor_payload_digest,
        "predecessor_row_version_id": proof.predecessor_row_version_id,
        "row_payload_digest": proof.row_payload_digest,
        "row_version_id": proof.row_version_id,
        "rule_version": "s7_5_i3_production_row_semantic_proof_v1",
        "stable_row_key": proof.stable_row_key,
        "table_name": proof.table_name,
        "validator_semantics_digest": proof.validator_semantics_digest,
    }
    return _canonical_bytes({"proof_id": stable_digest(body), **body})


def _row_version_references(
    rows: tuple[dict[str, object], ...],
) -> tuple[RowVersionReference, ...]:
    references = {
        (table_name, str(row[column]))
        for row in rows
        for table_name, column in (
            ("asset_master", "asset_master_version_id"),
            ("issuer_master", "issuer_master_version_id"),
            ("ticker_alias", "alias_resolution_version_id"),
        )
        if row[column] is not None
    }
    return tuple(
        RowVersionReference(table_name=table_name, row_version_id=row_version_id)
        for table_name, row_version_id in sorted(references)
    )


def _native_row(
    session: date,
    *,
    ticker: str,
    source_label: str,
    observed_composite: str,
    observed_market: str,
    canonical_composite: str,
    policy_bundle: IdentityPolicyBundle,
    row_available_session: date,
    alias_segment_id: str,
    alias_resolution_version_id: str,
    identity_adjudication_id: str | None = None,
    cross_market_adjudication_id: str | None = None,
) -> dict[str, object]:
    row = _legacy_row(
        session,
        ticker=ticker,
        observed_composite=observed_composite,
        observed_market=observed_market,
        observed_share=SHARE,
        canonical_composite=canonical_composite,
        canonical_share=SHARE,
        identity_method=(
            "approved_identity_adjudication"
            if identity_adjudication_id is not None
            else "approved_cross_market_provider_contamination_override"
            if cross_market_adjudication_id is not None
            else "source_composite_figi_exact"
        ),
        identity_disposition=(
            "confirmed_provider_contamination"
            if identity_adjudication_id is not None or cross_market_adjudication_id is not None
            else "observed_consistent"
        ),
        identity_adjudication_id=identity_adjudication_id,
        cross_market_adjudication_id=cross_market_adjudication_id,
        policy_bundle=policy_bundle,
        source_label=source_label,
    )
    row.pop("ticker_alias_id")
    row.update(
        {
            "alias_segment_id": alias_segment_id,
            "alias_resolution_version_id": alias_resolution_version_id,
            "asset_master_version_id": _digest(f"production-asset-version-{canonical_composite}"),
            "issuer_master_version_id": _digest("production-issuer-version"),
            "identity_policy_bundle_id": policy_bundle.identity_policy_bundle_id,
            "row_available_session": row_available_session,
        }
    )
    return row


def _production_alias_state(
    row: dict[str, object],
    *,
    canonical_composite: str,
    policy_bundle: IdentityPolicyBundle,
    available_session: date,
    decision_id: str | None = None,
    segment: AliasSegmentIdentity | None = None,
) -> OpenAliasState:
    session = row["session_date"]
    assert isinstance(session, date)
    segment = segment or AliasSegmentIdentity(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker=str(row["ticker"]),
        observed_composite_figi=str(row["observed_composite_figi"]),
        observed_share_class_figi=str(row["observed_share_class_figi"]),
        observed_cik_normalized=CIK,
        valid_from_session=session,
        segment_origin_source_record_id=str(row["selected_source_record_id"]),
    )
    adjudicated = decision_id is not None
    resolution = AliasResolutionVersion.for_segment(
        segment,
        canonical_asset_id=canonical_asset_id(canonical_composite),
        canonical_composite_figi=canonical_composite,
        canonical_share_class_id=canonical_share_class_id(SHARE),
        canonical_share_class_figi=SHARE,
        canonical_issuer_id=canonical_issuer_id(CIK),
        canonical_cik_normalized=CIK,
        resolution_method=(
            AliasResolutionMethod.APPROVED_PROVIDER_CONTAMINATION_OVERRIDE
            if adjudicated
            else AliasResolutionMethod.DIRECT_OBSERVED
        ),
        resolution_status=AliasResolutionStatus.RESOLVED,
        disposition=(
            AliasResolutionDisposition.CONFIRMED_PROVIDER_CONTAMINATION
            if adjudicated
            else AliasResolutionDisposition.OBSERVED_CONSISTENT
        ),
        decision_lineage_ids=(decision_id,) if decision_id is not None else (),
        share_class_resolution_method=ShareClassResolutionMethod.DIRECT_OBSERVED,
        share_class_decision_lineage_ids=(),
        identity_policy_bundle_id=policy_bundle.identity_policy_bundle_id,
        identity_cutoff_session=policy_bundle.policy_cutoff_session,
        resolution_available_session=min(
            available_session,
            policy_bundle.policy_cutoff_session,
        ),
        evidence_cutoff_session=policy_bundle.policy_cutoff_session,
        evidence_available_session=min(
            available_session,
            policy_bundle.policy_cutoff_session,
        ),
        valid_through_session=session,
        source_record_set_digest=stable_digest(
            {"source_record_ids": [row["selected_source_record_id"]]}
        ),
        predecessor_alias_resolution_version_id=None,
        is_tombstone=False,
        tombstone_reason_code=None,
    )
    return OpenAliasState(segment=segment, resolution=resolution)


def _source_from_native_row(row: dict[str, object]) -> SourceIdentityKey:
    return SourceIdentityKey(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker=str(row["ticker"]),
        session_date=row["session_date"],  # type: ignore[arg-type]
        source_record_id=str(row["selected_source_record_id"]),
        observed_composite_figi=str(row["observed_composite_figi"]),
        observed_composite_country=str(row["observed_composite_market_code"]),
        observed_share_class_figi=str(row["observed_share_class_figi"]),
        active_on_date=bool(row["active_on_date"]),
    )


def _reauthorize_production_case(
    case: SimpleNamespace,
    *,
    artifacts: dict[str, bytes],
    replacement_receipts: tuple[PartitionReceipt, ...],
    target_policy: IdentityPolicySnapshot,
    target_bundle_pin: ArtifactPin,
    alias_state_ledger: I4AliasStateLedgerRelease,
    alias_state_ledger_pin: ArtifactPin,
    registry_ledger: I4RegistryChangeLedgerRelease | None,
    registry_ledger_pin: ArtifactPin | None,
    late_source_ledger: I4LateSourceChangeLedgerRelease | None,
    late_source_ledger_pin: ArtifactPin | None,
    label: str,
    added_row_version_receipts: tuple[RowVersionReceipt, ...] | None = None,
    superseded_row_version_ids: tuple[str, ...] | None = None,
) -> dict[str, object]:
    row_receipts = (
        case.kwargs["added_row_version_receipts"]
        if added_row_version_receipts is None
        else added_row_version_receipts
    )
    superseded = (
        case.kwargs["superseded_row_version_ids"]
        if superseded_row_version_ids is None
        else superseded_row_version_ids
    )
    parent_by_session = {
        date.fromisoformat(item.partition_key): item
        for item in case.parent_manifest.added_partition_receipts
    }
    replacements = tuple(
        PartitionReplacement(
            parent_by_session[date.fromisoformat(item.partition_key)],
            item,
        )
        for item in replacement_receipts
    )
    change_set = logical_change_set_digest(
        added_partition_receipts=(),
        partition_replacements=replacements,
        added_row_version_receipts=row_receipts,
        superseded_row_version_ids=superseded,
    )
    replacement_sessions = {date.fromisoformat(item.partition_key) for item in replacement_receipts}
    source_binding = production_i4_source_binding_digest(
        parent_manifest_pin=case.kwargs["parent_manifest_pin"],
        parent_run_receipt_artifact=case.parent_manifest.run_receipt_pin.artifact,
        parent_checkpoint_artifact=case.kwargs["parent_checkpoint_artifact"],
        parent_partition_artifacts=tuple(
            item.artifact
            for item in case.checkpoint.resolved_partition_map
            if item.session_date in replacement_sessions
        ),
        replacement_partition_receipts=replacement_receipts,
        prior_policy_bundle_artifact=case.checkpoint.identity_policy_bundle_artifact,
        target_policy_bundle_artifact=target_bundle_pin,
        alias_state_ledger_artifact=alias_state_ledger_pin,
        registry_ledger_artifact=registry_ledger_pin,
        late_source_ledger_artifact=late_source_ledger_pin,
        added_row_version_receipts=row_receipts,
    )
    scope = correction_scope_digest(
        parent_release_id=case.parent_manifest.release_id,
        change_set_digest=change_set,
    )
    target_bundle = target_policy.policy_bundle
    event = I4ApprovalEvent(
        authorized_action=CorrectionAuthorizedAction.PUBLISH_EXACT_CORRECTION,
        parent_release_id=case.parent_manifest.release_id,
        expected_change_set_digest=change_set,
        source_binding_digest=source_binding,
        schema_digest=case.parent_manifest.schema_digest,
        transform_semantics_digest=case.parent_manifest.transform_semantics_digest,
        calendar_digest=case.parent_manifest.calendar_digest,
        identity_policy_before_id=(
            case.checkpoint.identity_policy_bundle.identity_policy_bundle_id
        ),
        identity_policy_after_id=target_bundle.identity_policy_bundle_id,
        scope_digest=scope,
        approver_id="joe",
        event_available_session=PRODUCTION_APPROVAL_AVAILABLE,
    )
    event_pin = event.exact_pin(path=f"approvals/i4/{label}-event.json")
    body = CorrectionAuthorization(
        authorized_action=CorrectionAuthorizedAction.PUBLISH_EXACT_CORRECTION,
        literal_version=CORRECTION_AUTHORIZATION_LITERAL_VERSION,
        parent_release_id=case.parent_manifest.release_id,
        expected_change_set_digest=change_set,
        source_binding_digest=source_binding,
        schema_digest=case.parent_manifest.schema_digest,
        transform_semantics_digest=case.parent_manifest.transform_semantics_digest,
        calendar_digest=case.parent_manifest.calendar_digest,
        identity_policy_before_id=(
            case.checkpoint.identity_policy_bundle.identity_policy_bundle_id
        ),
        identity_policy_after_id=target_bundle.identity_policy_bundle_id,
        scope_digest=scope,
        approval_event_id=event.approval_event_id,
        approval_event_sha256=event_pin.sha256,
        approver_id="joe",
        approval_available_session=PRODUCTION_APPROVAL_AVAILABLE,
        evidence_pins=(
            GateEvidencePin(
                artifact=_gate_artifact(
                    f"{label}-evidence",
                    path=f"approvals/i4/{label}-evidence.json",
                ),
                available_session=PRODUCTION_APPROVAL_AVAILABLE,
            ),
        ),
    )
    authorization = PinnedCorrectionAuthorization.freeze(
        body,
        path=f"approvals/i4/{label}-authorization.json",
    )
    ledger = I4ApprovalLedgerRelease(
        release_sequence=1,
        previous_ledger_release_id=None,
        release_available_session=PRODUCTION_APPROVAL_AVAILABLE,
        entries=(
            I4ApprovalLedgerEntry(
                ledger_index=1,
                authorization_id=body.authorization_id,
                authorization_artifact=authorization.artifact,
                approval_event_id=event.approval_event_id,
                event_artifact=event_pin,
                recorded_available_session=PRODUCTION_APPROVAL_AVAILABLE,
            ),
        ),
    )
    ledger_pin = ledger.exact_pin(path=f"approvals/i4/{label}-ledger.json")
    artifacts[authorization.artifact.path] = _canonical_bytes(body.to_dict())
    artifacts[event_pin.path] = event.canonical_bytes()
    artifacts[ledger_pin.path] = ledger.canonical_bytes()
    return {
        **case.kwargs,
        "replacement_partition_receipts": replacement_receipts,
        "target_policy_snapshot": target_policy,
        "target_policy_bundle_artifact": target_bundle_pin,
        "registry_ledger": registry_ledger,
        "registry_ledger_artifact": registry_ledger_pin,
        "late_source_ledger": late_source_ledger,
        "late_source_ledger_artifact": late_source_ledger_pin,
        "alias_state_ledger": alias_state_ledger,
        "alias_state_ledger_artifact": alias_state_ledger_pin,
        "added_row_version_receipts": row_receipts,
        "superseded_row_version_ids": superseded,
        "authorization": authorization,
        "approval_event": event,
        "approval_event_artifact": event_pin,
        "approval_ledger": ledger,
        "approval_ledger_artifact": ledger_pin,
        "artifact_reader": artifacts.__getitem__,
    }


def _production_case(
    *,
    omitted_source_scope_row: bool = False,
    tamper_unaffected_projection: bool = False,
) -> SimpleNamespace:
    base_checkpoint = _checkpoint()
    prior_bundle = base_checkpoint.identity_policy_bundle
    prior_release = next(
        item
        for item in prior_bundle.registry_releases
        if item.registry_kind is IdentityRegistryKind.IDENTITY_ADJUDICATION
    )
    target_release_content = b"production identity adjudication successor release\n"
    target_release = IdentityRegistryReleasePin(
        registry_kind=IdentityRegistryKind.IDENTITY_ADJUDICATION,
        release_id=_digest("production-successor-release"),
        artifact=_bytes_pin(
            "registries/i4/identity-adjudication-successor.json",
            target_release_content,
        ),
        decision_cutoff_session=date(2026, 8, 5),
        release_available_session=PRODUCTION_APPROVAL_AVAILABLE,
    )
    target_bundle = IdentityPolicyBundle(
        tuple(
            target_release
            if item.registry_kind is IdentityRegistryKind.IDENTITY_ADJUDICATION
            else item
            for item in prior_bundle.registry_releases
        ),
        bundle_available_session=PRODUCTION_APPROVAL_AVAILABLE,
    )

    direct_seed = _legacy_row(
        PRODUCTION_SESSIONS[0],
        observed_composite=OBSERVED,
        canonical_composite=CANONICAL,
        observed_share=SHARE,
        canonical_share=SHARE,
        source_label="production-aapl-direct",
        policy_bundle=prior_bundle,
    )
    direct_scope = RegistrySourceScopeRow(
        session_date=PRODUCTION_SESSIONS[0],
        source_record_id=str(direct_seed["selected_source_record_id"]),
        source_dataset="universe_source_daily",
        source_s4_release_set_id=str(direct_seed["source_s4_release_set_id"]),
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker="AAPL",
        observed_composite_figi=OBSERVED,
        observed_share_class_figi=SHARE,
        primary_exchange_mic="XNAS",
    )
    direct_scope_rows = (direct_scope,)
    if omitted_source_scope_row:
        direct_scope_rows = tuple(
            sorted(
                (
                    direct_scope,
                    replace(
                        direct_scope,
                        session_date=PRODUCTION_SESSIONS[1],
                        source_record_id=_digest("production-omitted-source-row"),
                    ),
                )
            )
        )
    predecessor_id = _digest("production-predecessor-decision")
    successor_id = _digest("production-successor-decision")
    representative_source_id = min(item.source_record_id for item in direct_scope_rows)
    predecessor = RegistryDecision(
        registry_kind=IdentityRegistryKind.IDENTITY_ADJUDICATION,
        registry_release_id=prior_release.release_id,
        decision_id=predecessor_id,
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker="AAPL",
        source_record_id=representative_source_id,
        identity_disposition="confirmed_provider_contamination",
        decision_available_session=prior_release.release_available_session,
        effective_from_session=PRODUCTION_SESSIONS[0],
        effective_to_session=(
            PRODUCTION_SESSIONS[1] if omitted_source_scope_row else PRODUCTION_SESSIONS[0]
        ),
        observed_composite_figi=OBSERVED,
        canonical_composite_figi=CANONICAL,
        source_scope=direct_scope_rows,
        production_registry_row={"decision_version": 1, "reviewed": True},
    )
    successor = replace(
        predecessor,
        registry_release_id=target_release.release_id,
        decision_id=successor_id,
        canonical_composite_figi=SECOND_CANONICAL,
        decision_available_session=PRODUCTION_APPROVAL_AVAILABLE,
        production_registry_row={"decision_version": 2, "reviewed": True},
    )

    cross_seed = _legacy_row(
        PRODUCTION_SESSIONS[0],
        ticker="MSFT",
        observed_composite=OBSERVED,
        observed_market="GB",
        canonical_composite=CANONICAL,
        observed_share=SHARE,
        canonical_share=SHARE,
        source_label="production-msft-cross-market",
        policy_bundle=prior_bundle,
    )
    cross_scope = RegistrySourceScopeRow(
        session_date=PRODUCTION_SESSIONS[0],
        source_record_id=str(cross_seed["selected_source_record_id"]),
        source_dataset="universe_source_daily",
        source_s4_release_set_id=str(cross_seed["source_s4_release_set_id"]),
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker="MSFT",
        observed_composite_figi=OBSERVED,
        observed_share_class_figi=SHARE,
        primary_exchange_mic="XNAS",
    )
    cross_release = next(
        item
        for item in prior_bundle.registry_releases
        if item.registry_kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION
    )
    cross_decision = RegistryDecision(
        registry_kind=IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        registry_release_id=cross_release.release_id,
        decision_id=_digest("production-legal-cross-market-decision"),
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker="MSFT",
        source_record_id=cross_scope.source_record_id,
        identity_disposition="confirmed_provider_contamination",
        decision_available_session=cross_release.release_available_session,
        effective_from_session=PRODUCTION_SESSIONS[0],
        effective_to_session=PRODUCTION_SESSIONS[0],
        observed_composite_figi=OBSERVED,
        canonical_composite_figi=CANONICAL,
        observed_composite_market_code="GB",
        canonical_composite_market_code="US",
        observed_share_class_figi=SHARE,
        source_scope=(cross_scope,),
        production_registry_row={"decision_version": 1, "reviewed": True},
    )
    prior_policy = _mint_production_identity_policy_snapshot(
        prior_bundle,
        tuple(sorted((predecessor, cross_decision), key=lambda item: item.decision_id)),
        production_release_set_binding_digest=_digest("production-prior-release-binding"),
    )
    target_policy = _mint_production_identity_policy_snapshot(
        target_bundle,
        tuple(sorted((successor, cross_decision), key=lambda item: item.decision_id)),
        production_release_set_binding_digest=_digest("production-target-release-binding"),
    )

    parent_rows_by_session: dict[date, tuple[dict[str, object], ...]] = {}
    replacement_rows_by_session: dict[date, tuple[dict[str, object], ...]] = {}
    alias_entries: list[I4AliasStateLedgerEntry] = []
    aapl_segment: AliasSegmentIdentity | None = None
    for index, session in enumerate(PRODUCTION_SESSIONS):
        alias_segment_id = _digest("production-aapl-alias")
        stable_alias_version = _digest(f"production-aapl-stable-resolution-{session}")
        if index == 0:
            parent_aapl = _native_row(
                session,
                ticker="AAPL",
                source_label="production-aapl-direct",
                observed_composite=OBSERVED,
                observed_market="US",
                canonical_composite=CANONICAL,
                policy_bundle=prior_bundle,
                row_available_session=PRODUCTION_PARENT_AVAILABLE,
                alias_segment_id=alias_segment_id,
                alias_resolution_version_id=_digest("production-aapl-old-resolution"),
                identity_adjudication_id=predecessor_id,
            )
            replacement_aapl = _native_row(
                session,
                ticker="AAPL",
                source_label="production-aapl-direct",
                observed_composite=OBSERVED,
                observed_market="US",
                canonical_composite=SECOND_CANONICAL,
                policy_bundle=target_bundle,
                row_available_session=PRODUCTION_REPLACEMENT_AVAILABLE,
                alias_segment_id=alias_segment_id,
                alias_resolution_version_id=_digest("production-aapl-new-resolution"),
                identity_adjudication_id=successor_id,
            )
            parent_alias_state = _production_alias_state(
                parent_aapl,
                canonical_composite=CANONICAL,
                policy_bundle=prior_bundle,
                available_session=PRODUCTION_PARENT_AVAILABLE,
                decision_id=predecessor_id,
            )
            aapl_segment = parent_alias_state.segment
            corrected_alias_state = _production_alias_state(
                replacement_aapl,
                canonical_composite=SECOND_CANONICAL,
                policy_bundle=target_bundle,
                available_session=PRODUCTION_REPLACEMENT_AVAILABLE,
                decision_id=successor_id,
                segment=aapl_segment,
            )
            replacement_aapl["asset_master_version_id"] = parent_aapl["asset_master_version_id"]
            replacement_aapl["identity_evidence_available_session"] = PRODUCTION_APPROVAL_AVAILABLE
        else:
            assert aapl_segment is not None
            parent_aapl = _native_row(
                session,
                ticker="AAPL",
                source_label=f"production-aapl-{session}",
                observed_composite=OBSERVED,
                observed_market="US",
                canonical_composite=CANONICAL,
                policy_bundle=prior_bundle,
                row_available_session=PRODUCTION_PARENT_AVAILABLE,
                alias_segment_id=alias_segment_id,
                alias_resolution_version_id=stable_alias_version,
                identity_adjudication_id=predecessor_id,
            )
            replacement_aapl = _native_row(
                session,
                ticker="AAPL",
                source_label=f"production-aapl-{session}",
                observed_composite=OBSERVED,
                observed_market="US",
                canonical_composite=CANONICAL,
                policy_bundle=target_bundle,
                row_available_session=PRODUCTION_REPLACEMENT_AVAILABLE,
                alias_segment_id=alias_segment_id,
                alias_resolution_version_id=stable_alias_version,
                identity_adjudication_id=predecessor_id,
            )
            parent_alias_state = _production_alias_state(
                parent_aapl,
                canonical_composite=CANONICAL,
                policy_bundle=prior_bundle,
                available_session=PRODUCTION_PARENT_AVAILABLE,
                decision_id=predecessor_id,
                segment=aapl_segment,
            )
            corrected_alias_state = parent_alias_state
        for row, state in (
            (parent_aapl, parent_alias_state),
            (replacement_aapl, corrected_alias_state),
        ):
            row["alias_segment_id"] = state.segment.alias_segment_id
            row["alias_resolution_version_id"] = state.resolution.alias_resolution_version_id
        alias_entries.append(
            I4AliasStateLedgerEntry(
                entry_sequence=index + 1,
                group=ExactIdentityGroup(
                    provider_id="massive",
                    provider_market="stocks",
                    provider_locale="us",
                    ticker="AAPL",
                ),
                session_date=session,
                parent_open_alias=parent_alias_state,
                corrected_open_alias=corrected_alias_state,
            )
        )
        parent_rows: tuple[dict[str, object], ...] = (parent_aapl,)
        replacement_rows: tuple[dict[str, object], ...] = (replacement_aapl,)
        if index == 0:
            foreign_alias = _digest("production-msft-cross-market-alias")
            foreign_resolution = _digest("production-msft-cross-market-resolution")
            parent_msft = _native_row(
                session,
                ticker="MSFT",
                source_label="production-msft-cross-market",
                observed_composite=OBSERVED,
                observed_market="GB",
                canonical_composite=CANONICAL,
                policy_bundle=prior_bundle,
                row_available_session=PRODUCTION_PARENT_AVAILABLE,
                alias_segment_id=foreign_alias,
                alias_resolution_version_id=foreign_resolution,
                cross_market_adjudication_id=cross_decision.decision_id,
            )
            replacement_msft = _native_row(
                session,
                ticker="MSFT",
                source_label="production-msft-cross-market",
                observed_composite=OBSERVED,
                observed_market="GB",
                canonical_composite=(
                    SECOND_CANONICAL if tamper_unaffected_projection else CANONICAL
                ),
                policy_bundle=target_bundle,
                row_available_session=PRODUCTION_REPLACEMENT_AVAILABLE,
                alias_segment_id=foreign_alias,
                alias_resolution_version_id=foreign_resolution,
                cross_market_adjudication_id=cross_decision.decision_id,
            )
            parent_rows = (parent_aapl, parent_msft)
            replacement_rows = (replacement_aapl, replacement_msft)
        parent_rows_by_session[session] = parent_rows
        replacement_rows_by_session[session] = replacement_rows

    terminal_parent_alias = alias_entries[-1].parent_open_alias
    first_entry = alias_entries[0]
    corrected_first_resolution = replace(
        first_entry.corrected_open_alias.resolution,
        segment=first_entry.corrected_open_alias.segment,
        predecessor_alias_resolution_version_id=(
            terminal_parent_alias.resolution.alias_resolution_version_id
        ),
    )
    corrected_first_state = OpenAliasState(
        segment=first_entry.corrected_open_alias.segment,
        resolution=corrected_first_resolution,
    )
    alias_entries[0] = replace(
        first_entry,
        corrected_open_alias=corrected_first_state,
    )
    for row in replacement_rows_by_session[PRODUCTION_SESSIONS[0]]:
        if row["ticker"] == "AAPL":
            row["alias_resolution_version_id"] = (
                corrected_first_resolution.alias_resolution_version_id
            )

    artifacts: dict[str, bytes] = {}
    frontiers: list[ResolvedPartitionState] = []
    parent_receipts: list[PartitionReceipt] = []
    replacement_receipts: list[PartitionReceipt] = []
    for session in PRODUCTION_SESSIONS:
        parent_rows = parent_rows_by_session[session]
        replacement_rows = replacement_rows_by_session[session]
        parent_content = _parquet_bytes(parent_rows)
        replacement_content = _parquet_bytes(replacement_rows)
        parent_pin = _bytes_pin(
            f"silver/i4/parent/{session.isoformat()}.parquet",
            parent_content,
        )
        replacement_pin = _bytes_pin(
            f"silver/i4/replacement/{session.isoformat()}.parquet",
            replacement_content,
        )
        artifacts[parent_pin.path] = parent_content
        artifacts[replacement_pin.path] = replacement_content
        parent_receipt = PartitionReceipt(
            table_name="universe_daily",
            partition_key=session.isoformat(),
            receipt=parent_pin,
            row_count=len(parent_rows),
            schema_digest=I3_V2_CONTRACTS["universe_daily"].schema_digest,
            availability_session=PRODUCTION_PARENT_AVAILABLE,
            row_version_references=_row_version_references(parent_rows),
        )
        replacement_receipt = PartitionReceipt(
            table_name="universe_daily",
            partition_key=session.isoformat(),
            receipt=replacement_pin,
            row_count=len(replacement_rows),
            schema_digest=I3_V2_CONTRACTS["universe_daily"].schema_digest,
            availability_session=PRODUCTION_REPLACEMENT_AVAILABLE,
            row_version_references=_row_version_references(replacement_rows),
        )
        parent_receipts.append(parent_receipt)
        replacement_receipts.append(replacement_receipt)
        frontiers.append(
            ResolvedPartitionState(
                session_date=session,
                partition_receipt_id=_digest(f"production-parent-receipt-{session}"),
                artifact=parent_pin,
                row_count=len(parent_rows),
                availability_session=PRODUCTION_PARENT_AVAILABLE,
            )
        )

    untouched_session = date(2026, 7, 6)
    untouched_row = _native_row(
        untouched_session,
        ticker="AAPL",
        source_label="production-aapl-untouched-history",
        observed_composite=CANONICAL,
        observed_market="US",
        canonical_composite=CANONICAL,
        policy_bundle=prior_bundle,
        row_available_session=PRODUCTION_PARENT_AVAILABLE,
        alias_segment_id=_digest("production-untouched-alias"),
        alias_resolution_version_id=_digest("production-untouched-resolution"),
    )
    untouched_rows = (untouched_row,)
    untouched_content = _parquet_bytes(untouched_rows)
    untouched_pin = _bytes_pin(
        f"silver/i4/parent/{untouched_session.isoformat()}.parquet",
        untouched_content,
    )
    artifacts[untouched_pin.path] = untouched_content
    untouched_receipt = PartitionReceipt(
        table_name="universe_daily",
        partition_key=untouched_session.isoformat(),
        receipt=untouched_pin,
        row_count=1,
        schema_digest=I3_V2_CONTRACTS["universe_daily"].schema_digest,
        availability_session=PRODUCTION_PARENT_AVAILABLE,
        row_version_references=_row_version_references(untouched_rows),
    )
    parent_receipts.insert(0, untouched_receipt)
    frontiers.insert(
        0,
        ResolvedPartitionState(
            session_date=untouched_session,
            partition_receipt_id=_digest("production-parent-receipt-untouched"),
            artifact=untouched_pin,
            row_count=1,
            availability_session=PRODUCTION_PARENT_AVAILABLE,
        ),
    )

    frontier_tuple = tuple(frontiers)
    parent_open_aliases = (terminal_parent_alias,)
    parent_terminal_rows = tuple(
        replace(
            item,
            stable_row_key=terminal_parent_alias.segment.alias_segment_id,
            row_version_id=(terminal_parent_alias.resolution.alias_resolution_version_id),
            row_payload_digest=stable_digest(terminal_parent_alias.to_dict()),
            availability_session=PRODUCTION_PARENT_AVAILABLE,
        )
        if item.table_name == "ticker_alias"
        else item
        for item in base_checkpoint.terminal_row_versions
    )
    resolved_state_digest = i3_resolved_state_digest(
        last_session=base_checkpoint.last_session,
        source_cutoff_session=base_checkpoint.source_cutoff_session,
        availability_cutoff_session=base_checkpoint.availability_cutoff_session,
        s4_terminal_pins=base_checkpoint.s4_terminal_pins,
        calendar_digest=base_checkpoint.calendar_digest,
        schema_digest=base_checkpoint.schema_digest,
        transform_semantics_digest=base_checkpoint.transform_semantics_digest,
        identity_policy_bundle=prior_bundle,
        identity_policy_bundle_artifact=base_checkpoint.identity_policy_bundle_artifact,
        open_aliases=parent_open_aliases,
        asset_aggregates=base_checkpoint.asset_aggregates,
        issuer_aggregates=base_checkpoint.issuer_aggregates,
        unresolved_subjects=base_checkpoint.unresolved_subjects,
        resolved_partition_map=frontier_tuple,
        terminal_row_versions=parent_terminal_rows,
    )
    native_manifest = replace(
        _parent_manifest(prior_bundle, resolved_state_digest=resolved_state_digest),
        release_family=NATIVE_V2_RELEASE_FAMILY,
    )
    checkpoint = replace(
        base_checkpoint,
        parent_release=NativeV2ParentReleasePin.from_manifest(
            native_manifest,
            path="manifests/i4/native-v2-parent.json",
        ),
        open_aliases=parent_open_aliases,
        resolved_partition_map=frontier_tuple,
        terminal_row_versions=parent_terminal_rows,
    )
    checkpoint_pin = checkpoint.exact_pin(path="checkpoints/i4/parent.json")
    artifacts[checkpoint.parent_release.manifest.path] = native_manifest.canonical_bytes()
    artifacts[checkpoint_pin.path] = checkpoint.canonical_bytes()
    artifacts[checkpoint.identity_policy_bundle_artifact.path] = prior_bundle.canonical_bytes()
    for release in prior_bundle.registry_releases:
        label = release.artifact.path.rsplit("/", 1)[-1].removesuffix(".json")
        artifacts[release.artifact.path] = f"{label}\n".encode()
    artifacts[target_release.artifact.path] = target_release_content
    target_bundle_pin = target_bundle.exact_pin(path="registries/i4/target-bundle.json")
    artifacts[target_bundle_pin.path] = target_bundle.canonical_bytes()

    _, parent_receipt_seed, parent_manifest_seed, _ = _gate_projection(
        added_partitions=tuple(parent_receipts),
        source_cutoff_session=PRODUCTION_SESSIONS[-1],
        release_available_session=PRODUCTION_PARENT_AVAILABLE,
    )
    checkpoint_receipt = CheckpointReceipt(
        artifact=checkpoint_pin,
        parent_release_id=None,
        run_spec_id=parent_receipt_seed.run_spec_id,
        last_session=checkpoint.last_session,
        resolved_content_digest=checkpoint.resolved_state_digest,
        rebuild_basis_digest=checkpoint.rebuild_basis_digest,
    )
    parent_run_receipt = replace(parent_receipt_seed, checkpoint=checkpoint_receipt)
    parent_manifest = replace(
        parent_manifest_seed,
        schema_digest=checkpoint.schema_digest,
        transform_semantics_digest=checkpoint.transform_semantics_digest,
        identity_policy_bundle_id=prior_bundle.identity_policy_bundle_id,
        calendar_digest=checkpoint.calendar_digest,
        source_cutoff_session=checkpoint.source_cutoff_session,
        availability_cutoff_session=checkpoint.availability_cutoff_session,
        release_available_session=checkpoint.parent_release.release_available_session,
        resolved_content_digest=checkpoint.resolved_state_digest,
        run_receipt_pin=control_object_pin(
            parent_run_receipt,
            path=parent_manifest_seed.run_receipt_pin.artifact.path,
        ),
    )
    parent_manifest_pin = parent_manifest.exact_pin(
        manifest_path="manifests/i4/parent-release.json"
    )
    artifacts[parent_manifest_pin.path] = parent_manifest.canonical_bytes()
    artifacts[parent_manifest.run_receipt_pin.artifact.path] = _canonical_bytes(
        parent_run_receipt.to_dict()
    )

    predecessor_bytes = _canonical_bytes(predecessor.to_dict())
    successor_bytes = _canonical_bytes(successor.to_dict())
    predecessor_pin = _bytes_pin("registries/i4/decisions/predecessor.json", predecessor_bytes)
    successor_pin = _bytes_pin("registries/i4/decisions/successor.json", successor_bytes)
    artifacts[predecessor_pin.path] = predecessor_bytes
    artifacts[successor_pin.path] = successor_bytes
    registry_entry = I4RegistryLedgerEntry(
        entry_sequence=1,
        registry_kind=IdentityRegistryKind.IDENTITY_ADJUDICATION,
        operation=RegistryChangeOperation.SUCCESSOR,
        predecessor_decision_id=predecessor.decision_id,
        predecessor_decision_artifact=predecessor_pin,
        predecessor_registry_release_id=prior_release.release_id,
        predecessor_registry_release_artifact=prior_release.artifact,
        change_decision_id=successor.decision_id,
        change_decision_artifact=successor_pin,
        change_registry_release_id=target_release.release_id,
        change_registry_release_artifact=target_release.artifact,
        change_available_session=PRODUCTION_APPROVAL_AVAILABLE,
    )
    registry_ledger = I4RegistryChangeLedgerRelease(
        release_sequence=1,
        previous_ledger_release_id=None,
        release_available_session=PRODUCTION_APPROVAL_AVAILABLE,
        entries=(registry_entry,),
    )
    registry_ledger_pin = registry_ledger.exact_pin(path="registries/i4/change-ledger.json")
    artifacts[registry_ledger_pin.path] = registry_ledger.canonical_bytes()
    alias_state_ledger = I4AliasStateLedgerRelease(
        release_sequence=1,
        previous_ledger_release_id=None,
        release_available_session=PRODUCTION_REPLACEMENT_AVAILABLE,
        entries=tuple(alias_entries),
    )
    alias_state_ledger_pin = alias_state_ledger.exact_pin(path="aliases/i4/state-ledger.json")
    artifacts[alias_state_ledger_pin.path] = alias_state_ledger.canonical_bytes()

    corrected_alias_state = alias_entries[0].corrected_open_alias
    corrected_alias_source = next(
        row
        for row in replacement_rows_by_session[PRODUCTION_SESSIONS[0]]
        if row["ticker"] == "AAPL"
    )
    alias_row = i3_runner._alias_physical_row(
        corrected_alias_source,
        segment=corrected_alias_state.segment,
        resolution=corrected_alias_state.resolution,
        availability_session=PRODUCTION_REPLACEMENT_AVAILABLE,
        calendar_index={session: index for index, session in enumerate(PRODUCTION_SESSIONS)},
    )
    alias_row_content = _table_parquet_bytes("ticker_alias", (alias_row,))
    alias_row_pin = _bytes_pin(
        "silver/i4/ticker-alias/reviewed-correction.parquet",
        alias_row_content,
    )
    artifacts[alias_row_pin.path] = alias_row_content
    alias_row_payload_digest = stable_digest(_jsonable(alias_row))
    terminal_alias_row = next(
        item
        for item in parent_terminal_rows
        if item.table_name == "ticker_alias"
        and item.stable_row_key == corrected_alias_state.segment.alias_segment_id
    )
    alias_validator_semantics_digest = stable_digest(
        {
            "operation": RowVersionOperation.REVIEWED_CORRECTION.value,
            "rule_version": "s7_5_i4_ticker_alias_reviewed_correction_v1",
            "schema_digest": I3_V2_CONTRACTS["ticker_alias"].schema_digest,
            "table_name": "ticker_alias",
        }
    )
    alias_proof_body = {
        "artifact_type": "s7_5_i3_production_row_semantic_proof",
        "operation": RowVersionOperation.REVIEWED_CORRECTION.value,
        "predecessor_payload_digest": terminal_alias_row.row_payload_digest,
        "predecessor_row_version_id": terminal_alias_row.row_version_id,
        "row_payload_digest": alias_row_payload_digest,
        "row_version_id": corrected_alias_state.resolution.alias_resolution_version_id,
        "rule_version": "s7_5_i3_production_row_semantic_proof_v1",
        "stable_row_key": corrected_alias_state.segment.alias_segment_id,
        "table_name": "ticker_alias",
        "validator_semantics_digest": alias_validator_semantics_digest,
    }
    alias_proof_content = _canonical_bytes(
        {"proof_id": stable_digest(alias_proof_body), **alias_proof_body}
    )
    alias_proof_pin = _bytes_pin(
        "proofs/i4/ticker-alias-reviewed-correction.json",
        alias_proof_content,
    )
    artifacts[alias_proof_pin.path] = alias_proof_content
    alias_semantic_proof = RowSemanticProofReceipt(
        table_name="ticker_alias",
        stable_row_key=corrected_alias_state.segment.alias_segment_id,
        row_version_id=corrected_alias_state.resolution.alias_resolution_version_id,
        predecessor_row_version_id=terminal_alias_row.row_version_id,
        operation=RowVersionOperation.REVIEWED_CORRECTION,
        row_payload_digest=alias_row_payload_digest,
        predecessor_payload_digest=terminal_alias_row.row_payload_digest,
        validator_semantics_digest=alias_validator_semantics_digest,
        artifact=alias_proof_pin,
    )
    alias_row_receipt = RowVersionReceipt(
        table_name="ticker_alias",
        stable_row_key=corrected_alias_state.segment.alias_segment_id,
        row_version_id=corrected_alias_state.resolution.alias_resolution_version_id,
        predecessor_row_version_id=terminal_alias_row.row_version_id,
        operation=RowVersionOperation.REVIEWED_CORRECTION,
        availability_session=PRODUCTION_REPLACEMENT_AVAILABLE,
        index_artifact=alias_row_pin,
        row_locator="row_index=0",
        row_payload_digest=alias_row_payload_digest,
        semantic_proof=alias_semantic_proof,
    )
    added_row_version_receipts = (alias_row_receipt,)
    superseded_row_version_ids = (terminal_alias_row.row_version_id,)

    parent_receipt_by_session = {
        date.fromisoformat(item.partition_key): item for item in parent_receipts
    }
    replacements = tuple(
        PartitionReplacement(
            parent_receipt_by_session[date.fromisoformat(replacement.partition_key)],
            replacement,
        )
        for replacement in replacement_receipts
    )
    change_set_digest = logical_change_set_digest(
        added_partition_receipts=(),
        partition_replacements=replacements,
        added_row_version_receipts=added_row_version_receipts,
        superseded_row_version_ids=superseded_row_version_ids,
    )
    source_binding_digest = production_i4_source_binding_digest(
        parent_manifest_pin=parent_manifest_pin,
        parent_run_receipt_artifact=parent_manifest.run_receipt_pin.artifact,
        parent_checkpoint_artifact=checkpoint_pin,
        parent_partition_artifacts=tuple(
            item.artifact for item in frontier_tuple if item.session_date in PRODUCTION_SESSIONS
        ),
        replacement_partition_receipts=tuple(replacement_receipts),
        prior_policy_bundle_artifact=checkpoint.identity_policy_bundle_artifact,
        target_policy_bundle_artifact=target_bundle_pin,
        alias_state_ledger_artifact=alias_state_ledger_pin,
        registry_ledger_artifact=registry_ledger_pin,
        added_row_version_receipts=added_row_version_receipts,
    )
    scope_digest = correction_scope_digest(
        parent_release_id=parent_manifest.release_id,
        change_set_digest=change_set_digest,
    )
    approval_event = I4ApprovalEvent(
        authorized_action=CorrectionAuthorizedAction.PUBLISH_EXACT_CORRECTION,
        parent_release_id=parent_manifest.release_id,
        expected_change_set_digest=change_set_digest,
        source_binding_digest=source_binding_digest,
        schema_digest=parent_manifest.schema_digest,
        transform_semantics_digest=parent_manifest.transform_semantics_digest,
        calendar_digest=parent_manifest.calendar_digest,
        identity_policy_before_id=prior_bundle.identity_policy_bundle_id,
        identity_policy_after_id=target_bundle.identity_policy_bundle_id,
        scope_digest=scope_digest,
        approver_id="joe",
        event_available_session=PRODUCTION_APPROVAL_AVAILABLE,
    )
    approval_event_pin = approval_event.exact_pin(path="approvals/i4/approval-event.json")
    authorization_body = CorrectionAuthorization(
        authorized_action=CorrectionAuthorizedAction.PUBLISH_EXACT_CORRECTION,
        literal_version=CORRECTION_AUTHORIZATION_LITERAL_VERSION,
        parent_release_id=parent_manifest.release_id,
        expected_change_set_digest=change_set_digest,
        source_binding_digest=source_binding_digest,
        schema_digest=parent_manifest.schema_digest,
        transform_semantics_digest=parent_manifest.transform_semantics_digest,
        calendar_digest=parent_manifest.calendar_digest,
        identity_policy_before_id=prior_bundle.identity_policy_bundle_id,
        identity_policy_after_id=target_bundle.identity_policy_bundle_id,
        scope_digest=scope_digest,
        approval_event_id=approval_event.approval_event_id,
        approval_event_sha256=approval_event_pin.sha256,
        approver_id="joe",
        approval_available_session=PRODUCTION_APPROVAL_AVAILABLE,
        evidence_pins=(
            GateEvidencePin(
                artifact=_gate_artifact(
                    "production-i4-evidence",
                    path="approvals/i4/evidence.json",
                ),
                available_session=PRODUCTION_APPROVAL_AVAILABLE,
            ),
        ),
    )
    authorization = PinnedCorrectionAuthorization.freeze(
        authorization_body,
        path="approvals/i4/authorization.json",
    )
    approval_ledger = I4ApprovalLedgerRelease(
        release_sequence=1,
        previous_ledger_release_id=None,
        release_available_session=PRODUCTION_APPROVAL_AVAILABLE,
        entries=(
            I4ApprovalLedgerEntry(
                ledger_index=1,
                authorization_id=authorization_body.authorization_id,
                authorization_artifact=authorization.artifact,
                approval_event_id=approval_event.approval_event_id,
                event_artifact=approval_event_pin,
                recorded_available_session=PRODUCTION_APPROVAL_AVAILABLE,
            ),
        ),
    )
    approval_ledger_pin = approval_ledger.exact_pin(path="approvals/i4/approval-ledger.json")
    artifacts[authorization.artifact.path] = _canonical_bytes(authorization_body.to_dict())
    artifacts[approval_event_pin.path] = approval_event.canonical_bytes()
    artifacts[approval_ledger_pin.path] = approval_ledger.canonical_bytes()

    kwargs = {
        "parent_manifest": parent_manifest,
        "parent_manifest_pin": parent_manifest_pin,
        "parent_run_receipt": parent_run_receipt,
        "checkpoint": checkpoint,
        "parent_checkpoint_artifact": checkpoint_pin,
        "replacement_partition_receipts": tuple(replacement_receipts),
        "prior_policy_snapshot": prior_policy,
        "target_policy_snapshot": target_policy,
        "target_policy_bundle_artifact": target_bundle_pin,
        "registry_ledger": registry_ledger,
        "registry_ledger_artifact": registry_ledger_pin,
        "late_source_ledger": None,
        "late_source_ledger_artifact": None,
        "alias_state_ledger": alias_state_ledger,
        "alias_state_ledger_artifact": alias_state_ledger_pin,
        "added_row_version_receipts": added_row_version_receipts,
        "superseded_row_version_ids": superseded_row_version_ids,
        "authorization": authorization,
        "approval_event": approval_event,
        "approval_event_artifact": approval_event_pin,
        "approval_ledger": approval_ledger,
        "approval_ledger_artifact": approval_ledger_pin,
        "availability_cutoff_session": PRODUCTION_CUTOFF,
        "artifact_reader": artifacts.__getitem__,
    }
    return SimpleNamespace(
        kwargs=kwargs,
        artifacts=artifacts,
        parent_manifest=parent_manifest,
        checkpoint=checkpoint,
        predecessor=predecessor,
        successor=successor,
        target_policy=target_policy,
        target_release=target_release,
        target_bundle_pin=target_bundle_pin,
        registry_entry=registry_entry,
        registry_ledger=registry_ledger,
        registry_ledger_pin=registry_ledger_pin,
        alias_state_ledger=alias_state_ledger,
        alias_state_ledger_pin=alias_state_ledger_pin,
        approval_event=approval_event,
        approval_event_pin=approval_event_pin,
        authorization=authorization,
        alias_row=alias_row,
        alias_row_pin=alias_row_pin,
        alias_proof_pin=alias_proof_pin,
        alias_row_receipt=alias_row_receipt,
    )


def _forge_alias_nonprojection_evidence(
    case: SimpleNamespace,
    *,
    resolution_changes: dict[str, object],
    label: str,
    replacement_raw_changes: dict[str, object] | None = None,
) -> SimpleNamespace:
    artifacts = dict(case.artifacts)
    entry = case.alias_state_ledger.entries[0]
    segment = entry.corrected_open_alias.segment
    resolution = replace(
        entry.corrected_open_alias.resolution,
        segment=segment,
        **resolution_changes,
    )
    state = OpenAliasState(segment=segment, resolution=resolution)
    ledger = replace(
        case.alias_state_ledger,
        entries=(
            replace(entry, corrected_open_alias=state),
            *case.alias_state_ledger.entries[1:],
        ),
    )
    ledger_pin = ledger.exact_pin(path=f"aliases/i4/{label}-state-ledger.json")
    artifacts[ledger_pin.path] = ledger.canonical_bytes()

    first_receipt = case.kwargs["replacement_partition_receipts"][0]
    rows = tuple(pq.read_table(pa.BufferReader(artifacts[first_receipt.receipt.path])).to_pylist())
    for row in rows:
        if row["ticker"] == "AAPL":
            row.update(replacement_raw_changes or {})
            row["alias_resolution_version_id"] = resolution.alias_resolution_version_id
    replacement_content = _parquet_bytes(rows)
    replacement_pin = _bytes_pin(
        f"silver/i4/replacement/{label}.parquet",
        replacement_content,
    )
    artifacts[replacement_pin.path] = replacement_content
    replacement_receipt = replace(
        first_receipt,
        receipt=replacement_pin,
        row_version_references=_row_version_references(rows),
    )
    replacement_receipts = (
        replacement_receipt,
        *case.kwargs["replacement_partition_receipts"][1:],
    )

    source = next(row for row in rows if row["ticker"] == "AAPL")
    alias_row = i3_runner._alias_physical_row(
        source,
        segment=segment,
        resolution=resolution,
        availability_session=PRODUCTION_REPLACEMENT_AVAILABLE,
        calendar_index={session: index for index, session in enumerate(PRODUCTION_SESSIONS)},
    )
    alias_content = _table_parquet_bytes("ticker_alias", (alias_row,))
    alias_pin = _bytes_pin(
        f"silver/i4/ticker-alias/{label}.parquet",
        alias_content,
    )
    artifacts[alias_pin.path] = alias_content
    payload_digest = stable_digest(_jsonable(alias_row))
    proof_seed = replace(
        case.alias_row_receipt.semantic_proof,
        row_version_id=resolution.alias_resolution_version_id,
        row_payload_digest=payload_digest,
    )
    proof_content = _row_semantic_proof_content(proof_seed)
    proof_pin = _bytes_pin(f"proofs/i4/{label}.json", proof_content)
    artifacts[proof_pin.path] = proof_content
    proof = replace(proof_seed, artifact=proof_pin)
    receipt = replace(
        case.alias_row_receipt,
        row_version_id=resolution.alias_resolution_version_id,
        index_artifact=alias_pin,
        row_payload_digest=payload_digest,
        semantic_proof=proof,
    )
    kwargs = {
        **case.kwargs,
        "replacement_partition_receipts": replacement_receipts,
        "alias_state_ledger": ledger,
        "alias_state_ledger_artifact": ledger_pin,
        "added_row_version_receipts": (receipt,),
        "artifact_reader": artifacts.__getitem__,
    }
    return SimpleNamespace(
        kwargs=kwargs,
        artifacts=artifacts,
        replacement_receipts=replacement_receipts,
        alias_state_ledger=ledger,
        alias_state_ledger_pin=ledger_pin,
        row_receipt=receipt,
    )


def _late_source_case() -> SimpleNamespace:
    case = _production_case()
    artifacts = dict(case.artifacts)
    session = PRODUCTION_SESSIONS[0]
    first_replacement = case.kwargs["replacement_partition_receipts"][0]
    replacement_rows = tuple(
        pq.read_table(pa.BufferReader(artifacts[first_replacement.receipt.path])).to_pylist()
    )
    replacement_msft = next(row for row in replacement_rows if row["ticker"] == "MSFT")
    prior_bundle = case.checkpoint.identity_policy_bundle
    corrected_aapl = _native_row(
        session,
        ticker="AAPL",
        source_label="production-aapl-exact-late-source",
        observed_composite=SECOND_CANONICAL,
        observed_market="US",
        canonical_composite=SECOND_CANONICAL,
        policy_bundle=prior_bundle,
        row_available_session=PRODUCTION_REPLACEMENT_AVAILABLE,
        alias_segment_id=_digest("temporary-late-source-alias"),
        alias_resolution_version_id=_digest("temporary-late-source-resolution"),
    )
    corrected_state = _production_alias_state(
        corrected_aapl,
        canonical_composite=SECOND_CANONICAL,
        policy_bundle=prior_bundle,
        available_session=PRODUCTION_REPLACEMENT_AVAILABLE,
    )
    corrected_aapl["alias_segment_id"] = corrected_state.segment.alias_segment_id
    corrected_aapl["alias_resolution_version_id"] = (
        corrected_state.resolution.alias_resolution_version_id
    )
    corrected_rows = tuple(
        sorted((corrected_aapl, replacement_msft), key=lambda row: str(row["ticker"]))
    )
    corrected_content = _parquet_bytes(corrected_rows)
    corrected_pin = _bytes_pin(
        f"silver/i4/late-source/{session.isoformat()}.parquet",
        corrected_content,
    )
    artifacts[corrected_pin.path] = corrected_content
    corrected_receipt = PartitionReceipt(
        table_name="universe_daily",
        partition_key=session.isoformat(),
        receipt=corrected_pin,
        row_count=len(corrected_rows),
        schema_digest=I3_V2_CONTRACTS["universe_daily"].schema_digest,
        availability_session=PRODUCTION_REPLACEMENT_AVAILABLE,
        row_version_references=_row_version_references(corrected_rows),
    )
    replacement_receipts = (
        corrected_receipt,
        *case.kwargs["replacement_partition_receipts"][1:],
    )

    alias_state_ledger = replace(
        case.alias_state_ledger,
        entries=(
            replace(
                case.alias_state_ledger.entries[0],
                corrected_open_alias=corrected_state,
            ),
            *case.alias_state_ledger.entries[1:],
        ),
    )
    alias_state_ledger_pin = alias_state_ledger.exact_pin(
        path="aliases/i4/late-source-state-ledger.json"
    )
    artifacts[alias_state_ledger_pin.path] = alias_state_ledger.canonical_bytes()

    parent_frontier = next(
        item for item in case.checkpoint.resolved_partition_map if item.session_date == session
    )
    parent_rows = tuple(
        pq.read_table(pa.BufferReader(artifacts[parent_frontier.artifact.path])).to_pylist()
    )
    parent_snapshot = I4LateSourceSnapshot(
        source_release_id=_digest("late-source-parent-release"),
        session_date=session,
        source_available_session=PRODUCTION_PARENT_AVAILABLE,
        rows=tuple(
            sorted(
                (_source_from_native_row(row) for row in parent_rows),
                key=lambda row: (
                    row.provider_id,
                    row.provider_market,
                    row.provider_locale,
                    row.ticker,
                    row.source_record_id,
                ),
            )
        ),
    )
    corrected_snapshot = I4LateSourceSnapshot(
        source_release_id=_digest("late-source-corrected-release"),
        session_date=session,
        source_available_session=PRODUCTION_REPLACEMENT_AVAILABLE,
        rows=tuple(
            sorted(
                (_source_from_native_row(row) for row in corrected_rows),
                key=lambda row: (
                    row.provider_id,
                    row.provider_market,
                    row.provider_locale,
                    row.ticker,
                    row.source_record_id,
                ),
            )
        ),
    )
    parent_snapshot_pin = parent_snapshot.exact_pin(path="sources/i4/late-source-parent.json")
    corrected_snapshot_pin = corrected_snapshot.exact_pin(
        path="sources/i4/late-source-corrected.json"
    )
    artifacts[parent_snapshot_pin.path] = parent_snapshot.canonical_bytes()
    artifacts[corrected_snapshot_pin.path] = corrected_snapshot.canonical_bytes()
    late_source_ledger = I4LateSourceChangeLedgerRelease(
        release_sequence=1,
        previous_ledger_release_id=None,
        release_available_session=PRODUCTION_REPLACEMENT_AVAILABLE,
        entries=(
            I4LateSourceLedgerEntry(
                entry_sequence=1,
                session_date=session,
                parent_snapshot_artifact=parent_snapshot_pin,
                corrected_snapshot_artifact=corrected_snapshot_pin,
                change_available_session=PRODUCTION_REPLACEMENT_AVAILABLE,
            ),
        ),
    )
    late_source_ledger_pin = late_source_ledger.exact_pin(path="sources/i4/late-source-ledger.json")
    artifacts[late_source_ledger_pin.path] = late_source_ledger.canonical_bytes()
    kwargs = {
        **case.kwargs,
        "replacement_partition_receipts": replacement_receipts,
        "registry_ledger": None,
        "registry_ledger_artifact": None,
        "late_source_ledger": late_source_ledger,
        "late_source_ledger_artifact": late_source_ledger_pin,
        "alias_state_ledger": alias_state_ledger,
        "alias_state_ledger_artifact": alias_state_ledger_pin,
        "artifact_reader": artifacts.__getitem__,
    }
    return SimpleNamespace(
        base=case,
        kwargs=kwargs,
        artifacts=artifacts,
        parent_snapshot=parent_snapshot,
        corrected_snapshot=corrected_snapshot,
        parent_snapshot_pin=parent_snapshot_pin,
        corrected_snapshot_pin=corrected_snapshot_pin,
        late_source_ledger=late_source_ledger,
        late_source_ledger_pin=late_source_ledger_pin,
        alias_state_ledger=alias_state_ledger,
        alias_state_ledger_pin=alias_state_ledger_pin,
    )


def _replace_late_source_evidence(
    case: SimpleNamespace,
    *,
    parent_snapshot: I4LateSourceSnapshot,
    corrected_snapshot: I4LateSourceSnapshot,
    label: str,
    change_available_session: date = PRODUCTION_REPLACEMENT_AVAILABLE,
) -> SimpleNamespace:
    artifacts = dict(case.artifacts)
    parent_pin = parent_snapshot.exact_pin(path=f"sources/i4/{label}-parent.json")
    corrected_pin = corrected_snapshot.exact_pin(path=f"sources/i4/{label}-corrected.json")
    artifacts[parent_pin.path] = parent_snapshot.canonical_bytes()
    artifacts[corrected_pin.path] = corrected_snapshot.canonical_bytes()
    ledger = I4LateSourceChangeLedgerRelease(
        release_sequence=1,
        previous_ledger_release_id=None,
        release_available_session=max(
            PRODUCTION_REPLACEMENT_AVAILABLE,
            change_available_session,
        ),
        entries=(
            I4LateSourceLedgerEntry(
                entry_sequence=1,
                session_date=PRODUCTION_SESSIONS[0],
                parent_snapshot_artifact=parent_pin,
                corrected_snapshot_artifact=corrected_pin,
                change_available_session=change_available_session,
            ),
        ),
    )
    ledger_pin = ledger.exact_pin(path=f"sources/i4/{label}-ledger.json")
    artifacts[ledger_pin.path] = ledger.canonical_bytes()
    return SimpleNamespace(
        kwargs={
            **case.kwargs,
            "late_source_ledger": ledger,
            "late_source_ledger_artifact": ledger_pin,
            "artifact_reader": artifacts.__getitem__,
        },
        artifacts=artifacts,
        parent_pin=parent_pin,
        corrected_pin=corrected_pin,
        ledger=ledger,
        ledger_pin=ledger_pin,
    )


def _trusted_correction_parent_case() -> SimpleNamespace:
    case = _production_case()
    artifacts = dict(case.artifacts)
    predecessor_pin = case.kwargs["parent_manifest_pin"]
    replacements = tuple(
        PartitionReplacement(
            replace(
                receipt,
                receipt=_artifact(
                    f"trusted-parent-prior-{receipt.partition_key}",
                    path=f"silver/i4/trusted-parent-prior/{receipt.partition_key}.parquet",
                ),
            ),
            receipt,
        )
        for receipt in case.parent_manifest.added_partition_receipts
    )
    change_set = logical_change_set_digest(
        added_partition_receipts=(),
        partition_replacements=replacements,
        added_row_version_receipts=(),
        superseded_row_version_ids=(),
    )
    source_binding = _digest("trusted-correction-parent-source-binding")
    gate_scope = correction_scope_digest(
        parent_release_id=predecessor_pin.release_id,
        change_set_digest=change_set,
    )
    policy_id = case.checkpoint.identity_policy_bundle.identity_policy_bundle_id
    event = I4ApprovalEvent(
        authorized_action=CorrectionAuthorizedAction.PUBLISH_EXACT_CORRECTION,
        parent_release_id=predecessor_pin.release_id,
        expected_change_set_digest=change_set,
        source_binding_digest=source_binding,
        schema_digest=case.parent_manifest.schema_digest,
        transform_semantics_digest=case.parent_manifest.transform_semantics_digest,
        calendar_digest=case.parent_manifest.calendar_digest,
        identity_policy_before_id=policy_id,
        identity_policy_after_id=policy_id,
        scope_digest=gate_scope,
        approver_id="joe",
        event_available_session=APPROVAL_AVAILABLE,
    )
    event_pin = event.exact_pin(path="approvals/i4/trusted-parent-event.json")
    body = CorrectionAuthorization(
        authorized_action=CorrectionAuthorizedAction.PUBLISH_EXACT_CORRECTION,
        literal_version=CORRECTION_AUTHORIZATION_LITERAL_VERSION,
        parent_release_id=predecessor_pin.release_id,
        expected_change_set_digest=change_set,
        source_binding_digest=source_binding,
        schema_digest=case.parent_manifest.schema_digest,
        transform_semantics_digest=case.parent_manifest.transform_semantics_digest,
        calendar_digest=case.parent_manifest.calendar_digest,
        identity_policy_before_id=policy_id,
        identity_policy_after_id=policy_id,
        scope_digest=gate_scope,
        approval_event_id=event.approval_event_id,
        approval_event_sha256=event_pin.sha256,
        approver_id="joe",
        approval_available_session=APPROVAL_AVAILABLE,
        evidence_pins=(
            GateEvidencePin(
                artifact=_gate_artifact(
                    "trusted-parent-evidence",
                    path="approvals/i4/trusted-parent-evidence.json",
                ),
                available_session=APPROVAL_AVAILABLE,
            ),
        ),
    )
    authorization = PinnedCorrectionAuthorization.freeze(
        body,
        path="approvals/i4/trusted-parent-authorization.json",
    )
    approval_ledger = I4ApprovalLedgerRelease(
        release_sequence=1,
        previous_ledger_release_id=None,
        release_available_session=APPROVAL_AVAILABLE,
        entries=(
            I4ApprovalLedgerEntry(
                ledger_index=1,
                authorization_id=body.authorization_id,
                authorization_artifact=authorization.artifact,
                approval_event_id=event.approval_event_id,
                event_artifact=event_pin,
                recorded_available_session=APPROVAL_AVAILABLE,
            ),
        ),
    )
    approval_ledger_pin = approval_ledger.exact_pin(path="approvals/i4/trusted-parent-ledger.json")
    artifacts[authorization.artifact.path] = _canonical_bytes(body.to_dict())
    artifacts[event_pin.path] = event.canonical_bytes()
    artifacts[approval_ledger_pin.path] = approval_ledger.canonical_bytes()
    attestation = attest_i4_approval_event_exact(
        authorization=authorization,
        event=event,
        event_artifact=event_pin,
        ledger=approval_ledger,
        ledger_artifact=approval_ledger_pin,
        availability_cutoff_session=case.parent_manifest.availability_cutoff_session,
        artifact_reader=artifacts.__getitem__,
    )

    seed_receipt = case.kwargs["parent_run_receipt"]
    qa_receipt = replace(
        seed_receipt.qa_receipt,
        source_binding_digest=source_binding,
        change_set_digest=change_set,
    )
    checkpoint_receipt = replace(
        seed_receipt.checkpoint,
        parent_release_id=predecessor_pin.release_id,
    )
    run_receipt = replace(
        seed_receipt,
        actual_input_set_digest=source_binding,
        output_set_digest=change_set,
        qa_receipt=qa_receipt,
        checkpoint=checkpoint_receipt,
    )
    manifest = replace(
        case.parent_manifest,
        release_type=ReleaseType.CORRECTION,
        parent_release_pin=predecessor_pin,
        source_binding_digest=source_binding,
        added_partition_receipts=(),
        partition_replacements=replacements,
        correction_authorization_id=body.authorization_id,
        qa_receipt_id=qa_receipt.qa_receipt_id,
        run_receipt_pin=control_object_pin(
            run_receipt,
            path="controls/i4/trusted-parent-run-receipt.json",
        ),
    )
    manifest_pin = manifest.exact_pin(manifest_path="manifests/i4/trusted-correction-parent.json")
    artifacts[manifest_pin.manifest_path] = manifest.canonical_bytes()
    artifacts[manifest.run_receipt_pin.artifact.path] = _canonical_bytes(run_receipt.to_dict())
    return SimpleNamespace(
        base=case,
        artifacts=artifacts,
        manifest=manifest,
        manifest_pin=manifest_pin,
        run_receipt=run_receipt,
        authorization=authorization,
        attestation=attestation,
    )


def test_production_factory_derives_scope_boundary_and_preserves_cross_market_row() -> None:
    case = _production_case()

    capability = mint_production_i4_correction_capability(**case.kwargs)

    assert isinstance(capability, ProductionI4CorrectionCapability)
    assert capability.exact_scope.group.ticker == "AAPL"
    assert capability.exact_scope.direct_source_rows[0].session_date == PRODUCTION_SESSIONS[0]
    assert capability.exact_scope.recompute_sessions == PRODUCTION_SESSIONS
    assert capability.exact_scope.alias_stable_boundary_session == PRODUCTION_SESSIONS[-1]
    assert len(capability.partition_replacements) == len(PRODUCTION_SESSIONS)
    assert capability.registry_changes[0].successor == case.successor
    assert capability.correction_cause is I4ProductionCorrectionCause.REGISTRY_CHANGE
    assert capability.late_source_ledger_release_id is None
    assert capability.added_row_version_receipts == (case.alias_row_receipt,)
    assert capability.superseded_row_version_ids == (
        case.alias_row_receipt.predecessor_row_version_id,
    )
    assert capability.alias_state_ledger_release_id == (case.alias_state_ledger.ledger_release_id)
    assert case.alias_state_ledger.entries[0].parent_open_alias != (
        case.alias_state_ledger.entries[0].corrected_open_alias
    )
    assert case.alias_state_ledger.entries[-1].parent_open_alias == (
        case.alias_state_ledger.entries[-1].corrected_open_alias
    )
    with pytest.raises(I4CorrectionError, match="sealed exact-derivation factory"):
        ProductionI4CorrectionCapability(
            parent_manifest_pin=capability.parent_manifest_pin,
            parent_checkpoint_pin=capability.parent_checkpoint_pin,
            parent_checkpoint_id=capability.parent_checkpoint_id,
            exact_scope=capability.exact_scope,
            partition_replacements=capability.partition_replacements,
            added_row_version_receipts=capability.added_row_version_receipts,
            superseded_row_version_ids=capability.superseded_row_version_ids,
            registry_changes=capability.registry_changes,
            correction_cause=capability.correction_cause,
            registry_ledger_release_id=capability.registry_ledger_release_id,
            late_source_ledger_release_id=capability.late_source_ledger_release_id,
            alias_state_ledger_release_id=capability.alias_state_ledger_release_id,
            unaffected_partition_receipts_digest=(capability.unaffected_partition_receipts_digest),
            authorization_id=capability.authorization_id,
            approval_attestation=capability.approval_attestation,
            source_binding_digest=capability.source_binding_digest,
            change_set_digest=capability.change_set_digest,
            identity_policy_before_id=capability.identity_policy_before_id,
            identity_policy_after_id=capability.identity_policy_after_id,
            _seal=object(),
        )


def test_production_alias_requires_exactly_one_ticker_alias_receipt() -> None:
    case = _production_case()

    with pytest.raises(I4CorrectionError, match="exactly one ticker_alias"):
        mint_production_i4_correction_capability(
            **{
                **case.kwargs,
                "added_row_version_receipts": (),
                "superseded_row_version_ids": (),
            }
        )

    alias = case.alias_row_receipt
    extra_predecessor = _digest("extra-asset-predecessor")
    extra_proof = RowSemanticProofReceipt(
        table_name="asset_master",
        stable_row_key=_digest("extra-asset-stable-key"),
        row_version_id=_digest("extra-asset-version"),
        predecessor_row_version_id=extra_predecessor,
        operation=RowVersionOperation.REVIEWED_CORRECTION,
        row_payload_digest=_digest("extra-asset-payload"),
        predecessor_payload_digest=_digest("extra-asset-predecessor-payload"),
        validator_semantics_digest=_digest("extra-asset-validator"),
        artifact=case.alias_proof_pin,
    )
    extra = RowVersionReceipt(
        table_name="asset_master",
        stable_row_key=extra_proof.stable_row_key,
        row_version_id=extra_proof.row_version_id,
        predecessor_row_version_id=extra_predecessor,
        operation=RowVersionOperation.REVIEWED_CORRECTION,
        availability_session=PRODUCTION_REPLACEMENT_AVAILABLE,
        index_artifact=case.alias_row_pin,
        row_locator="row_index=0",
        row_payload_digest=extra_proof.row_payload_digest,
        semantic_proof=extra_proof,
    )
    receipts = tuple(sorted((extra, alias), key=lambda item: item.key))
    superseded = tuple(sorted((extra_predecessor, alias.predecessor_row_version_id)))
    with pytest.raises(I4CorrectionError, match="exactly one ticker_alias"):
        mint_production_i4_correction_capability(
            **{
                **case.kwargs,
                "added_row_version_receipts": receipts,
                "superseded_row_version_ids": superseded,
            }
        )


def test_production_alias_rejects_new_root_and_untrusted_predecessor() -> None:
    case = _production_case()
    receipt = case.alias_row_receipt
    new_root_proof = replace(
        receipt.semantic_proof,
        predecessor_row_version_id=None,
        predecessor_payload_digest=None,
        operation=RowVersionOperation.NEW_ROOT,
    )
    new_root = replace(
        receipt,
        predecessor_row_version_id=None,
        operation=RowVersionOperation.NEW_ROOT,
        semantic_proof=new_root_proof,
    )
    with pytest.raises(I4CorrectionError, match="rejects NEW_ROOT"):
        mint_production_i4_correction_capability(
            **{
                **case.kwargs,
                "added_row_version_receipts": (new_root,),
                "superseded_row_version_ids": (),
            }
        )

    forged_predecessor = _digest("untrusted-alias-predecessor")
    forged_proof = replace(
        receipt.semantic_proof,
        predecessor_row_version_id=forged_predecessor,
        predecessor_payload_digest=_digest("untrusted-alias-predecessor-payload"),
    )
    forged = replace(
        receipt,
        predecessor_row_version_id=forged_predecessor,
        semantic_proof=forged_proof,
    )
    with pytest.raises(I4CorrectionError, match="authenticated parent terminal"):
        mint_production_i4_correction_capability(
            **{
                **case.kwargs,
                "added_row_version_receipts": (forged,),
                "superseded_row_version_ids": (forged_predecessor,),
            }
        )


def test_production_alias_replays_exact_proof_locator_and_physical_row() -> None:
    case = _production_case()
    receipt = case.alias_row_receipt

    artifacts = dict(case.artifacts)
    artifacts[case.alias_proof_pin.path] += b"forged"
    with pytest.raises(I4CorrectionError, match="stored artifact bytes differ"):
        mint_production_i4_correction_capability(
            **{**case.kwargs, "artifact_reader": artifacts.__getitem__}
        )

    with pytest.raises(I4CorrectionError, match="exactly row_index=0"):
        mint_production_i4_correction_capability(
            **{
                **case.kwargs,
                "added_row_version_receipts": (replace(receipt, row_locator="row_index=1"),),
            }
        )

    appended_content = _table_parquet_bytes(
        "ticker_alias",
        (case.alias_row, case.alias_row),
    )
    appended_pin = _bytes_pin(
        "silver/i4/ticker-alias/appended-unreceipted-row.parquet",
        appended_content,
    )
    appended_artifacts = {
        **case.artifacts,
        appended_pin.path: appended_content,
    }
    with pytest.raises(I4CorrectionError, match="exactly row_index=0"):
        mint_production_i4_correction_capability(
            **{
                **case.kwargs,
                "added_row_version_receipts": (replace(receipt, index_artifact=appended_pin),),
                "artifact_reader": appended_artifacts.__getitem__,
            }
        )

    forged_row = dict(case.alias_row)
    forged_row["canonical_composite_figi"] = CANONICAL
    row_content = _table_parquet_bytes("ticker_alias", (forged_row,))
    row_pin = _bytes_pin("silver/i4/ticker-alias/forged-state-row.parquet", row_content)
    payload_digest = stable_digest(_jsonable(forged_row))
    proof_seed = replace(
        receipt.semantic_proof,
        row_payload_digest=payload_digest,
    )
    proof_content = _row_semantic_proof_content(proof_seed)
    proof_pin = _bytes_pin("proofs/i4/forged-state-row.json", proof_content)
    proof = replace(proof_seed, artifact=proof_pin)
    forged_receipt = replace(
        receipt,
        index_artifact=row_pin,
        row_payload_digest=payload_digest,
        semantic_proof=proof,
    )
    forged_artifacts = {
        **case.artifacts,
        row_pin.path: row_content,
        proof_pin.path: proof_content,
    }
    with pytest.raises(I4CorrectionError, match="canonical_composite_figi"):
        mint_production_i4_correction_capability(
            **{
                **case.kwargs,
                "added_row_version_receipts": (forged_receipt,),
                "artifact_reader": forged_artifacts.__getitem__,
            }
        )


def test_production_alias_state_resolution_must_match_replacement_projection() -> None:
    case = _production_case()
    state = case.alias_state_ledger.entries[0].corrected_open_alias
    first = case.kwargs["replacement_partition_receipts"][0]
    rows = pq.read_table(pa.BufferReader(case.artifacts[first.receipt.path])).to_pylist()
    projection = i4_correction._projection_from_native_row(
        next(row for row in rows if row["ticker"] == "AAPL")
    )
    forged_resolution = replace(
        state.resolution,
        segment=state.segment,
        decision_lineage_ids=(_digest("forged-alias-state-lineage"),),
    )
    forged_state = OpenAliasState(segment=state.segment, resolution=forged_resolution)

    with pytest.raises(I4CorrectionError, match="replacement decision lineage"):
        i4_correction._validate_corrected_alias_projection(forged_state, projection)


def test_production_alias_nonprojection_evidence_is_module_derived() -> None:
    case = _production_case()
    attacks = (
        (
            "policy bundle",
            {"identity_policy_bundle_id": _digest("forged-alias-policy-bundle")},
        ),
        (
            "source-record-set digest",
            {"source_record_set_digest": _digest("forged-alias-source-record-set")},
        ),
        (
            "evidence availability",
            {"evidence_available_session": date(2026, 8, 5)},
        ),
    )
    for message, changes in attacks:
        forged = _forge_alias_nonprojection_evidence(
            case,
            resolution_changes=changes,
            label=message.replace(" ", "-"),
        )
        with pytest.raises(I4CorrectionError, match=message):
            mint_production_i4_correction_capability(**forged.kwargs)


def test_joint_forgery_cannot_turn_replacement_evidence_into_authority() -> None:
    case = _production_case()
    forged_policy_id = _digest("joint-forged-target-policy")
    forged_evidence_available = date(2026, 8, 5)
    forged = _forge_alias_nonprojection_evidence(
        case,
        resolution_changes={
            "identity_policy_bundle_id": forged_policy_id,
            "source_record_set_digest": _digest("joint-forged-source-record-set"),
            "evidence_available_session": forged_evidence_available,
        },
        replacement_raw_changes={
            "identity_policy_bundle_id": forged_policy_id,
            "identity_evidence_available_session": forged_evidence_available,
        },
        label="joint-authorized-evidence-forgery",
    )
    kwargs = _reauthorize_production_case(
        case,
        artifacts=forged.artifacts,
        replacement_receipts=forged.replacement_receipts,
        target_policy=case.target_policy,
        target_bundle_pin=case.target_bundle_pin,
        alias_state_ledger=forged.alias_state_ledger,
        alias_state_ledger_pin=forged.alias_state_ledger_pin,
        registry_ledger=case.registry_ledger,
        registry_ledger_pin=case.registry_ledger_pin,
        late_source_ledger=None,
        late_source_ledger_pin=None,
        label="joint-authorized-evidence-forgery",
        added_row_version_receipts=(forged.row_receipt,),
        superseded_row_version_ids=case.kwargs["superseded_row_version_ids"],
    )

    with pytest.raises(I4CorrectionError, match="replacement identity policy bundle"):
        mint_production_i4_correction_capability(**kwargs)


def test_evidence_only_joint_forgery_hits_evidence_gate_after_reauthorization() -> None:
    case = _production_case()
    forged_evidence_available = date(2026, 8, 5)
    forged = _forge_alias_nonprojection_evidence(
        case,
        resolution_changes={
            "evidence_available_session": forged_evidence_available,
        },
        replacement_raw_changes={
            "identity_evidence_available_session": forged_evidence_available,
        },
        label="evidence-only-authorized-forgery",
    )
    kwargs = _reauthorize_production_case(
        case,
        artifacts=forged.artifacts,
        replacement_receipts=forged.replacement_receipts,
        target_policy=case.target_policy,
        target_bundle_pin=case.target_bundle_pin,
        alias_state_ledger=forged.alias_state_ledger,
        alias_state_ledger_pin=forged.alias_state_ledger_pin,
        registry_ledger=case.registry_ledger,
        registry_ledger_pin=case.registry_ledger_pin,
        late_source_ledger=None,
        late_source_ledger_pin=None,
        label="evidence-only-authorized-forgery",
        added_row_version_receipts=(forged.row_receipt,),
        superseded_row_version_ids=case.kwargs["superseded_row_version_ids"],
    )

    with pytest.raises(I4CorrectionError, match="replacement evidence availability"):
        mint_production_i4_correction_capability(**kwargs)


def test_production_alias_closes_superseded_and_replacement_reference_sets() -> None:
    case = _production_case()
    with pytest.raises(I4CorrectionError, match="superseded IDs differ"):
        mint_production_i4_correction_capability(
            **{
                **case.kwargs,
                "superseded_row_version_ids": (_digest("wrong-superseded-version"),),
            }
        )

    artifacts = dict(case.artifacts)
    first = case.kwargs["replacement_partition_receipts"][0]
    rows = tuple(pq.read_table(pa.BufferReader(artifacts[first.receipt.path])).to_pylist())
    for row in rows:
        if row["ticker"] == "AAPL":
            row["asset_master_version_id"] = _digest("unreceipted-asset-version")
    content = _parquet_bytes(rows)
    pin = _bytes_pin("silver/i4/replacement/unreceipted-ref.parquet", content)
    artifacts[pin.path] = content
    forged_partition = replace(
        first,
        receipt=pin,
        row_version_references=_row_version_references(rows),
    )
    with pytest.raises(I4CorrectionError, match="new row-version references"):
        mint_production_i4_correction_capability(
            **{
                **case.kwargs,
                "replacement_partition_receipts": (
                    forged_partition,
                    *case.kwargs["replacement_partition_receipts"][1:],
                ),
                "artifact_reader": artifacts.__getitem__,
            }
        )


def test_correction_parent_requires_and_accepts_exact_prior_gate_evidence() -> None:
    case = _trusted_correction_parent_case()
    common = {
        "parent_manifest": case.manifest,
        "parent_manifest_pin": case.manifest_pin,
        "parent_run_receipt": case.run_receipt,
        "checkpoint": case.base.checkpoint,
        "parent_checkpoint_artifact": case.base.kwargs["parent_checkpoint_artifact"],
        "artifact_reader": case.artifacts.__getitem__,
    }

    with pytest.raises(I4CorrectionError, match="requires its exact authorization"):
        i4_correction._authenticate_production_parent(
            **common,
            parent_correction_authorization=None,
            parent_correction_approval_attestation=None,
            cache=i4_correction._ExactReadCache(),
        )

    i4_correction._authenticate_production_parent(
        **common,
        parent_correction_authorization=case.authorization,
        parent_correction_approval_attestation=case.attestation,
        cache=i4_correction._ExactReadCache(),
    )


def test_i4_evidence_packages_are_genesis_only() -> None:
    case = _production_case()
    late = _late_source_case()
    releases = (
        (case.alias_state_ledger, "alias-state evidence"),
        (late.late_source_ledger, "late-source evidence"),
        (case.registry_ledger, "registry evidence"),
        (case.kwargs["approval_ledger"], "approval evidence"),
    )
    for release, label in releases:
        for sequence, previous in (
            (2, None),
            (True, None),
            (1.0, None),
            (1, _digest(f"{label}-forged-previous")),
        ):
            with pytest.raises(I4CorrectionError, match=f"{label}.*genesis-only"):
                replace(
                    release,
                    release_sequence=sequence,
                    previous_ledger_release_id=previous,
                )


def test_production_factory_reads_only_authenticated_bounded_parent_sessions() -> None:
    case = _production_case()
    read_paths: list[str] = []

    def reader(path: str) -> bytes:
        read_paths.append(path)
        return case.artifacts[path]

    capability = mint_production_i4_correction_capability(
        **{**case.kwargs, "artifact_reader": reader}
    )

    untouched_path = "silver/i4/parent/2026-07-06.parquet"
    assert untouched_path not in read_paths
    assert {
        f"silver/i4/parent/{session.isoformat()}.parquet" for session in PRODUCTION_SESSIONS
    }.issubset(read_paths)
    assert capability.unaffected_partition_receipts_digest == stable_digest(
        {
            "parent_checkpoint_id": case.checkpoint.checkpoint_id,
            "unchanged_partition_frontier": [case.checkpoint.resolved_partition_map[0].to_dict()],
        }
    )


def test_production_factory_rejects_alias_state_not_bound_to_partition_ids() -> None:
    case = _production_case()
    first = case.alias_state_ledger.entries[0]
    forged_ledger = replace(
        case.alias_state_ledger,
        entries=(
            replace(first, corrected_open_alias=first.parent_open_alias),
            *case.alias_state_ledger.entries[1:],
        ),
    )
    forged_pin = forged_ledger.exact_pin(path="aliases/i4/forged-state-ledger.json")
    artifacts = {**case.artifacts, forged_pin.path: forged_ledger.canonical_bytes()}

    with pytest.raises(I4CorrectionError, match="partition row-version IDs"):
        mint_production_i4_correction_capability(
            **{
                **case.kwargs,
                "alias_state_ledger": forged_ledger,
                "alias_state_ledger_artifact": forged_pin,
                "artifact_reader": artifacts.__getitem__,
            }
        )


def test_production_factory_requests_exact_group_expansion_when_real_alias_never_converges() -> (
    None
):
    case = _production_case()
    artifacts = dict(case.artifacts)
    final_entry = case.alias_state_ledger.entries[-1]
    divergent_resolution = replace(
        final_entry.parent_open_alias.resolution,
        segment=final_entry.parent_open_alias.segment,
        source_record_set_digest=_digest("never-converged-source-record-set"),
        predecessor_alias_resolution_version_id=(
            final_entry.parent_open_alias.resolution.alias_resolution_version_id
        ),
    )
    divergent_state = OpenAliasState(
        segment=final_entry.parent_open_alias.segment,
        resolution=divergent_resolution,
    )
    final_receipt = case.kwargs["replacement_partition_receipts"][-1]
    final_rows = tuple(
        pq.read_table(pa.BufferReader(artifacts[final_receipt.receipt.path])).to_pylist()
    )
    for row in final_rows:
        if row["ticker"] == "AAPL":
            row["alias_resolution_version_id"] = (
                divergent_state.resolution.alias_resolution_version_id
            )
    divergent_content = _parquet_bytes(final_rows)
    divergent_pin = _bytes_pin(
        "silver/i4/replacement/never-converged.parquet",
        divergent_content,
    )
    artifacts[divergent_pin.path] = divergent_content
    divergent_receipt = replace(
        final_receipt,
        receipt=divergent_pin,
        row_version_references=_row_version_references(final_rows),
    )
    replacement_receipts = (
        *case.kwargs["replacement_partition_receipts"][:-1],
        divergent_receipt,
    )
    alias_ledger = replace(
        case.alias_state_ledger,
        entries=(
            *case.alias_state_ledger.entries[:-1],
            replace(final_entry, corrected_open_alias=divergent_state),
        ),
    )
    alias_pin = alias_ledger.exact_pin(path="aliases/i4/never-converged-ledger.json")
    artifacts[alias_pin.path] = alias_ledger.canonical_bytes()
    kwargs = _reauthorize_production_case(
        case,
        artifacts=artifacts,
        replacement_receipts=replacement_receipts,
        target_policy=case.target_policy,
        target_bundle_pin=case.target_bundle_pin,
        alias_state_ledger=alias_ledger,
        alias_state_ledger_pin=alias_pin,
        registry_ledger=case.registry_ledger,
        registry_ledger_pin=case.registry_ledger_pin,
        late_source_ledger=None,
        late_source_ledger_pin=None,
        label="never-converged",
    )

    with pytest.raises(ExactGroupExpansionRequired, match="no supplied session"):
        mint_production_i4_correction_capability(**kwargs)


def test_production_factory_rejects_caller_late_source_snapshot_happy_path() -> None:
    case = _late_source_case()

    with pytest.raises(I4CorrectionError, match="S4HistoricalSourceCorrectionReceipt"):
        mint_production_i4_correction_capability(**case.kwargs)


@pytest.mark.parametrize("snapshot_side", ("parent", "corrected"))
def test_late_source_rejects_tampered_old_or_new_exact_pin(snapshot_side: str) -> None:
    case = _late_source_case()
    artifacts = dict(case.artifacts)
    pin = case.parent_snapshot_pin if snapshot_side == "parent" else case.corrected_snapshot_pin
    artifacts[pin.path] += b"tamper"

    with pytest.raises(I4CorrectionError, match="S4HistoricalSourceCorrectionReceipt"):
        mint_production_i4_correction_capability(
            **{**case.kwargs, "artifact_reader": artifacts.__getitem__}
        )


def test_late_source_rejects_snapshot_availability_after_change() -> None:
    case = _late_source_case()
    forged = _replace_late_source_evidence(
        case,
        parent_snapshot=case.parent_snapshot,
        corrected_snapshot=replace(
            case.corrected_snapshot,
            source_available_session=date(2026, 8, 8),
        ),
        label="late-availability",
    )

    with pytest.raises(I4CorrectionError, match="S4HistoricalSourceCorrectionReceipt"):
        mint_production_i4_correction_capability(**forged.kwargs)


def test_late_source_rejects_multi_group_diff() -> None:
    case = _late_source_case()
    corrected_rows = tuple(
        replace(row, source_record_id=_digest("late-source-second-changed-group"))
        if row.ticker == "MSFT"
        else row
        for row in case.corrected_snapshot.rows
    )
    forged = _replace_late_source_evidence(
        case,
        parent_snapshot=case.parent_snapshot,
        corrected_snapshot=replace(case.corrected_snapshot, rows=corrected_rows),
        label="late-multi-group",
    )

    with pytest.raises(I4CorrectionError, match="S4HistoricalSourceCorrectionReceipt"):
        mint_production_i4_correction_capability(**forged.kwargs)


def test_late_source_rejects_foreign_locale_scope() -> None:
    case = _late_source_case()

    def foreign(rows: tuple[SourceIdentityKey, ...]) -> tuple[SourceIdentityKey, ...]:
        return tuple(
            replace(row, provider_locale="ca") if row.ticker == "AAPL" else row for row in rows
        )

    forged = _replace_late_source_evidence(
        case,
        parent_snapshot=replace(case.parent_snapshot, rows=foreign(case.parent_snapshot.rows)),
        corrected_snapshot=replace(
            case.corrected_snapshot,
            rows=foreign(case.corrected_snapshot.rows),
        ),
        label="late-foreign-locale",
    )

    with pytest.raises(I4CorrectionError, match="S4HistoricalSourceCorrectionReceipt"):
        mint_production_i4_correction_capability(**forged.kwargs)


def test_late_source_rejects_empty_exact_diff() -> None:
    case = _late_source_case()
    no_change = replace(
        case.parent_snapshot,
        source_release_id=_digest("late-source-empty-successor-release"),
        source_available_session=PRODUCTION_REPLACEMENT_AVAILABLE,
    )
    forged = _replace_late_source_evidence(
        case,
        parent_snapshot=case.parent_snapshot,
        corrected_snapshot=no_change,
        label="late-empty-diff",
    )

    with pytest.raises(I4CorrectionError, match="S4HistoricalSourceCorrectionReceipt"):
        mint_production_i4_correction_capability(**forged.kwargs)


def test_production_factory_has_no_caller_attested_scope_or_boundary_flags() -> None:
    parameters = inspect.signature(mint_production_i4_correction_capability).parameters

    assert {
        "group",
        "direct_source_rows",
        "exact_group_slots",
        "boundary_proofs",
        "registry_changes",
        "expected_change_set_digest",
        "expected_source_binding_digest",
    }.isdisjoint(parameters)


def test_production_factory_rejects_omitted_registry_source_scope_row() -> None:
    case = _production_case(omitted_source_scope_row=True)

    with pytest.raises(I4CorrectionError, match="omitted a registry source-scope row"):
        mint_production_i4_correction_capability(**case.kwargs)


def test_production_factory_rejects_forged_authorization_digest_and_event() -> None:
    case = _production_case()
    forged_body = replace(
        case.authorization.authorization,
        source_binding_digest=_digest("forged-production-source-binding"),
    )
    forged_authorization = PinnedCorrectionAuthorization.freeze(
        forged_body,
        path="approvals/i4/forged-authorization.json",
    )
    forged_artifacts = dict(case.artifacts)
    forged_artifacts[forged_authorization.artifact.path] = _canonical_bytes(forged_body.to_dict())
    kwargs = {
        **case.kwargs,
        "authorization": forged_authorization,
        "artifact_reader": forged_artifacts.__getitem__,
    }
    with pytest.raises(I4CorrectionError, match="authorization does not reproduce"):
        mint_production_i4_correction_capability(**kwargs)

    forged_event = replace(
        case.approval_event,
        scope_digest=_digest("forged-production-approval-scope"),
    )
    forged_event_pin = forged_event.exact_pin(path="approvals/i4/forged-event.json")
    forged_artifacts[forged_event_pin.path] = forged_event.canonical_bytes()
    kwargs = {
        **case.kwargs,
        "approval_event": forged_event,
        "approval_event_artifact": forged_event_pin,
        "artifact_reader": forged_artifacts.__getitem__,
    }
    with pytest.raises(I4CorrectionError, match="approval event"):
        mint_production_i4_correction_capability(**kwargs)


def test_production_factory_rejects_partition_bytes_behind_a_forged_digest_claim() -> None:
    case = _production_case()
    artifacts = dict(case.artifacts)
    replacement = case.kwargs["replacement_partition_receipts"][0]
    artifacts[replacement.receipt.path] += b"forged"
    kwargs = {**case.kwargs, "artifact_reader": artifacts.__getitem__}

    with pytest.raises(I4CorrectionError, match="stored artifact bytes differ"):
        mint_production_i4_correction_capability(**kwargs)


def test_production_factory_rejects_withdrawal_forged_over_unledgered_successor() -> None:
    case = _production_case()
    with pytest.raises(I4CorrectionError, match="exact reviewed reason artifact"):
        replace(
            case.registry_entry,
            operation=RegistryChangeOperation.WITHDRAWAL,
            change_decision_id=_digest("production-withdrawal-without-reason"),
        )
    reason_content = b"review panel withdrew the predecessor\n"
    reason_pin = _bytes_pin("registries/i4/withdrawal-reason.txt", reason_content)
    withdrawal_entry_seed = replace(
        case.registry_entry,
        operation=RegistryChangeOperation.WITHDRAWAL,
        change_decision_id=_digest("production-withdrawal-control-decision"),
        change_decision_artifact=_artifact("temporary-withdrawal-decision"),
        withdrawal_reason_code="reviewed_withdrawal",
        withdrawal_reason_artifact=reason_pin,
    )
    withdrawal_bytes = withdrawal_entry_seed.withdrawal_decision_bytes()
    withdrawal_pin = _bytes_pin("registries/i4/withdrawal-decision.json", withdrawal_bytes)
    withdrawal_entry = replace(
        withdrawal_entry_seed,
        change_decision_artifact=withdrawal_pin,
    )
    withdrawal_ledger = I4RegistryChangeLedgerRelease(
        release_sequence=1,
        previous_ledger_release_id=None,
        release_available_session=PRODUCTION_APPROVAL_AVAILABLE,
        entries=(withdrawal_entry,),
    )
    withdrawal_ledger_pin = withdrawal_ledger.exact_pin(
        path="registries/i4/forged-withdrawal-ledger.json"
    )
    artifacts = dict(case.artifacts)
    artifacts[reason_pin.path] = reason_content
    artifacts[withdrawal_pin.path] = withdrawal_bytes
    artifacts[withdrawal_ledger_pin.path] = withdrawal_ledger.canonical_bytes()
    kwargs = {
        **case.kwargs,
        "registry_ledger": withdrawal_ledger,
        "registry_ledger_artifact": withdrawal_ledger_pin,
        "artifact_reader": artifacts.__getitem__,
    }

    with pytest.raises(I4CorrectionError, match="target policy changed unrelated"):
        mint_production_i4_correction_capability(**kwargs)


def test_production_factory_rejects_changed_exact_unaffected_projection() -> None:
    case = _production_case(tamper_unaffected_projection=True)

    with pytest.raises(I4CorrectionError, match="unaffected canonical row projection"):
        mint_production_i4_correction_capability(**case.kwargs)
