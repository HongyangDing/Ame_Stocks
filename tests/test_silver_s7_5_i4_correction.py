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
from ame_stocks_api.silver.incremental_contract import (
    ArtifactPin,
    CheckpointReceipt,
    PartitionReceipt,
    PartitionReplacement,
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
    ResolvedPartitionState,
    i3_resolved_state_digest,
)
from ame_stocks_api.silver.incremental_i3_contract import I3_V2_CONTRACTS
from ame_stocks_api.silver.incremental_i3_dispatch import (
    IdentityObservation,
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
    I4ApprovalEvent,
    I4ApprovalEventAttestation,
    I4ApprovalLedgerEntry,
    I4ApprovalLedgerRelease,
    I4CorrectionError,
    I4CorrectionPlan,
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
            else "approved_cross_market_adjudication"
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
    for index, session in enumerate(PRODUCTION_SESSIONS):
        alias_segment_id = _digest(f"production-aapl-alias-{session}")
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
        else:
            parent_aapl = _native_row(
                session,
                ticker="AAPL",
                source_label=f"production-aapl-{session}",
                observed_composite=CANONICAL,
                observed_market="US",
                canonical_composite=CANONICAL,
                policy_bundle=prior_bundle,
                row_available_session=PRODUCTION_PARENT_AVAILABLE,
                alias_segment_id=alias_segment_id,
                alias_resolution_version_id=stable_alias_version,
            )
            replacement_aapl = _native_row(
                session,
                ticker="AAPL",
                source_label=f"production-aapl-{session}",
                observed_composite=CANONICAL,
                observed_market="US",
                canonical_composite=CANONICAL,
                policy_bundle=target_bundle,
                row_available_session=PRODUCTION_REPLACEMENT_AVAILABLE,
                alias_segment_id=alias_segment_id,
                alias_resolution_version_id=stable_alias_version,
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

    frontier_tuple = tuple(frontiers)
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
        open_aliases=base_checkpoint.open_aliases,
        asset_aggregates=base_checkpoint.asset_aggregates,
        issuer_aggregates=base_checkpoint.issuer_aggregates,
        unresolved_subjects=base_checkpoint.unresolved_subjects,
        resolved_partition_map=frontier_tuple,
        terminal_row_versions=base_checkpoint.terminal_row_versions,
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
        resolved_partition_map=frontier_tuple,
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

    replacements = tuple(
        PartitionReplacement(parent, replacement)
        for parent, replacement in zip(parent_receipts, replacement_receipts, strict=True)
    )
    change_set_digest = logical_change_set_digest(
        added_partition_receipts=(),
        partition_replacements=replacements,
        added_row_version_receipts=(),
        superseded_row_version_ids=(),
    )
    source_binding_digest = production_i4_source_binding_digest(
        parent_manifest_pin=parent_manifest_pin,
        parent_run_receipt_artifact=parent_manifest.run_receipt_pin.artifact,
        parent_checkpoint_artifact=checkpoint_pin,
        parent_partition_artifacts=tuple(item.artifact for item in frontier_tuple),
        replacement_partition_receipts=tuple(replacement_receipts),
        prior_policy_bundle_artifact=checkpoint.identity_policy_bundle_artifact,
        target_policy_bundle_artifact=target_bundle_pin,
        registry_ledger_artifact=registry_ledger_pin,
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
        "added_row_version_receipts": (),
        "superseded_row_version_ids": (),
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
        approval_event=approval_event,
        approval_event_pin=approval_event_pin,
        authorization=authorization,
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
            registry_ledger_release_id=capability.registry_ledger_release_id,
            authorization_id=capability.authorization_id,
            approval_attestation=capability.approval_attestation,
            source_binding_digest=capability.source_binding_digest,
            change_set_digest=capability.change_set_digest,
            identity_policy_before_id=capability.identity_policy_before_id,
            identity_policy_after_id=capability.identity_policy_after_id,
            _seal=object(),
        )


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
    with pytest.raises(I4CorrectionError, match="append-only reason artifact"):
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
        release_sequence=2,
        previous_ledger_release_id=case.registry_ledger.ledger_release_id,
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
