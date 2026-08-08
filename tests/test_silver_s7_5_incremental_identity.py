from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import date
from types import SimpleNamespace

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.incremental_identity import (
    ALIAS_RESOLUTION_VERSION_ID_FIELDS,
    ALIAS_RESOLUTION_VERSION_ID_NAMESPACE,
    ALIAS_RESOLUTION_VERSION_ID_RULE_VERSION,
    ALIAS_RESOLUTION_VERSION_ID_SUBJECT_FIELDS,
    ALIAS_SEGMENT_ID_FIELDS,
    ALIAS_SEGMENT_ID_NAMESPACE,
    ALIAS_SEGMENT_ID_RULE_VERSION,
    ALIAS_SEGMENT_ID_SUBJECT_FIELDS,
    AliasResolutionDisposition,
    AliasResolutionMethod,
    AliasResolutionStatus,
    AliasResolutionVersion,
    AliasSegmentIdentity,
    IncrementalIdentityError,
    ShareClassResolutionMethod,
    alias_resolution_version_id,
    alias_segment_id,
    canonical_asset_id,
    canonical_issuer_id,
    canonical_share_class_id,
    successor_alias_resolution_version,
    validate_ticker_alias_clean_delta_root,
    validate_ticker_alias_mechanical_successor,
)


def _digest(label: str) -> str:
    return stable_digest({"fixture": label})


def _segment(**changes: object) -> AliasSegmentIdentity:
    values: dict[str, object] = {
        "provider_id": "massive",
        "provider_market": "stocks",
        "provider_locale": "us",
        "ticker": "AZPN",
        "observed_composite_figi": "BBG000KRLLH9",
        "observed_share_class_figi": "BBG001S87NT0",
        "observed_cik_normalized": "0000929940",
        "valid_from_session": date(2022, 2, 9),
        "segment_origin_source_record_id": _digest("source-2022-02-09"),
    }
    values.update(changes)
    return AliasSegmentIdentity(**values)  # type: ignore[arg-type]


def _resolution(
    segment: AliasSegmentIdentity | None = None,
    **changes: object,
) -> AliasResolutionVersion:
    segment = segment or _segment()
    values: dict[str, object] = {
        "canonical_asset_id": canonical_asset_id("BBG000DFMXT3"),
        "canonical_composite_figi": "BBG000DFMXT3",
        "canonical_share_class_id": canonical_share_class_id("BBG001S87NT0"),
        "canonical_share_class_figi": "BBG001S87NT0",
        "canonical_issuer_id": canonical_issuer_id("0000929940"),
        "canonical_cik_normalized": "0000929940",
        "resolution_method": AliasResolutionMethod.APPROVED_CROSS_MARKET_OVERRIDE,
        "resolution_status": AliasResolutionStatus.RESOLVED,
        "disposition": AliasResolutionDisposition.CONFIRMED_PROVIDER_CONTAMINATION,
        "decision_lineage_ids": (_digest("cross-market-decision"),),
        "share_class_resolution_method": ShareClassResolutionMethod.DIRECT_OBSERVED,
        "share_class_decision_lineage_ids": (),
        "identity_policy_bundle_id": _digest("identity-policy-bundle-v1"),
        "identity_cutoff_session": date(2026, 7, 29),
        "resolution_available_session": date(2026, 7, 28),
        "evidence_cutoff_session": date(2026, 7, 28),
        "evidence_available_session": date(2026, 7, 27),
        "valid_through_session": date(2022, 3, 2),
        "source_record_set_digest": _digest("source-range-2022-02-09--2022-03-02"),
        "predecessor_alias_resolution_version_id": None,
        "is_tombstone": False,
        "tombstone_reason_code": None,
    }
    values.update(changes)
    return AliasResolutionVersion.for_segment(segment, **values)


def _mechanical_successor(
    segment: AliasSegmentIdentity,
    previous: AliasResolutionVersion,
    **changes: object,
) -> AliasResolutionVersion:
    values: dict[str, object] = {
        "evidence_available_session": date(2026, 7, 28),
        "evidence_cutoff_session": date(2026, 7, 29),
        "identity_cutoff_session": date(2026, 7, 30),
        "resolution_available_session": date(2026, 7, 29),
        "source_record_set_digest": _digest("mechanical-source-extension"),
        "valid_through_session": date(2022, 3, 3),
    }
    values.update(changes)
    return successor_alias_resolution_version(previous, segment=segment, **values)


def test_alias_segment_id_is_exactly_the_stable_observation_payload() -> None:
    segment = _segment()

    assert set(segment.logical_payload()) == ALIAS_SEGMENT_ID_FIELDS
    assert (
        set(segment.logical_payload()) - {"namespace", "rule_version"}
        == ALIAS_SEGMENT_ID_SUBJECT_FIELDS
    )
    assert segment.logical_payload()["namespace"] == ALIAS_SEGMENT_ID_NAMESPACE
    assert segment.logical_payload()["rule_version"] == ALIAS_SEGMENT_ID_RULE_VERSION
    assert segment.alias_segment_id == alias_segment_id(segment)
    assert segment.alias_segment_id == stable_digest(segment.logical_payload())
    assert AliasSegmentIdentity.from_dict(segment.to_dict()) == segment
    assert {
        "canonical_asset_id",
        "identity_cutoff_session",
        "identity_policy_bundle_id",
        "release_id",
        "valid_through_session",
    }.isdisjoint(segment.logical_payload())

    mixed_case = _segment(ticker="AANw")
    assert mixed_case.ticker == "AANw"
    assert mixed_case.logical_payload()["ticker"] == "AANw"
    assert mixed_case.alias_segment_id != _segment(ticker="AANW").alias_segment_id

    for field, value in (
        ("namespace", "ame_stocks.identity.wrong_domain"),
        ("rule_version", "ame_stocks_alias_segment_id_v999"),
    ):
        with pytest.raises(IncrementalIdentityError, match="must equal"):
            AliasSegmentIdentity.from_dict({**segment.to_dict(), field: value})


@pytest.mark.parametrize(
    "changes",
    [
        {"identity_cutoff_session": date(2026, 7, 30)},
        {
            "canonical_asset_id": canonical_asset_id("BBG000BG7423"),
            "canonical_composite_figi": "BBG000BG7423",
        },
        {"identity_policy_bundle_id": _digest("identity-policy-bundle-v2")},
        {"evidence_available_session": date(2026, 7, 28)},
        {"evidence_cutoff_session": date(2026, 7, 29)},
        {"decision_lineage_ids": (_digest("revised-decision"),)},
    ],
)
def test_resolution_changes_version_without_changing_segment(
    changes: dict[str, object],
) -> None:
    segment = _segment()
    original = _resolution(segment)
    revised = replace(original, segment=segment, **changes)

    assert revised.alias_segment_id == original.alias_segment_id == segment.alias_segment_id
    assert revised.alias_resolution_version_id != original.alias_resolution_version_id
    assert alias_resolution_version_id(revised) == stable_digest(revised.logical_payload())


def test_resolution_version_payload_is_closed_and_excludes_release_and_wall_time() -> None:
    segment = _segment()
    version = _resolution(segment)

    assert set(version.logical_payload()) == ALIAS_RESOLUTION_VERSION_ID_FIELDS
    assert (
        set(version.logical_payload()) - {"namespace", "rule_version"}
        == ALIAS_RESOLUTION_VERSION_ID_SUBJECT_FIELDS
    )
    assert version.logical_payload()["namespace"] == ALIAS_RESOLUTION_VERSION_ID_NAMESPACE
    assert version.logical_payload()["rule_version"] == ALIAS_RESOLUTION_VERSION_ID_RULE_VERSION
    assert AliasResolutionVersion.from_dict(version.to_dict(), segment=segment) == version
    assert {
        "release_id",
        "current_release_id",
        "created_at_utc",
        "captured_at_utc",
        "runtime_id",
        "run_id",
    }.isdisjoint(version.logical_payload())

    for extra_field in ("current_release_id", "runtime_id", "captured_at_utc"):
        document = {**version.to_dict(), extra_field: _digest(extra_field)}
        with pytest.raises(IncrementalIdentityError, match="fields differ"):
            AliasResolutionVersion.from_dict(document, segment=segment)
        with pytest.raises(IncrementalIdentityError, match="fields differ"):
            successor_alias_resolution_version(
                version,
                segment=segment,
                **{extra_field: "forbidden"},
            )

    for field, value in (
        ("namespace", "ame_stocks.identity.wrong_domain"),
        ("rule_version", "ame_stocks_alias_resolution_version_id_v999"),
    ):
        with pytest.raises(IncrementalIdentityError, match="must equal"):
            AliasResolutionVersion.from_dict(
                {**version.to_dict(), field: value},
                segment=segment,
            )


def test_extension_and_closure_are_successor_versions_of_one_stable_segment() -> None:
    segment = _segment()
    initial = _resolution(segment, valid_through_session=date(2022, 2, 18))
    extension = successor_alias_resolution_version(
        initial,
        segment=segment,
        valid_through_session=date(2022, 3, 2),
        source_record_set_digest=_digest("source-range-extended-through-2022-03-02"),
    )
    closure = successor_alias_resolution_version(
        extension,
        segment=segment,
        valid_through_session=date(2022, 3, 3),
        source_record_set_digest=_digest("source-range-closed-2022-03-03"),
    )

    assert initial.alias_segment_id == extension.alias_segment_id == closure.alias_segment_id
    assert extension.predecessor_alias_resolution_version_id == initial.alias_resolution_version_id
    assert closure.predecessor_alias_resolution_version_id == extension.alias_resolution_version_id
    assert (
        len(
            {
                initial.alias_resolution_version_id,
                extension.alias_resolution_version_id,
                closure.alias_resolution_version_id,
            }
        )
        == 3
    )


def test_gap_or_reopen_creates_a_new_segment_not_a_successor_of_the_old_segment() -> None:
    before_gap = _segment()
    after_gap = _segment(
        valid_from_session=date(2022, 3, 7),
        segment_origin_source_record_id=_digest("source-2022-03-07-reopen"),
    )

    assert before_gap.alias_segment_id != after_gap.alias_segment_id
    assert _resolution(before_gap).alias_segment_id == before_gap.alias_segment_id
    assert (
        _resolution(after_gap, valid_through_session=date(2022, 3, 7)).alias_segment_id
        == after_gap.alias_segment_id
    )


def test_models_are_frozen_and_reject_unknown_fields() -> None:
    segment = _segment()
    version = _resolution(segment)

    with pytest.raises(FrozenInstanceError):
        segment.ticker = "CR"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        version.disposition = "observed_consistent"  # type: ignore[misc]
    with pytest.raises(IncrementalIdentityError, match="fields differ"):
        AliasSegmentIdentity.from_dict({**segment.to_dict(), "release_id": _digest("release")})


def test_invalid_predecessors_and_noop_successors_fail_closed() -> None:
    segment = _segment()
    original = _resolution(segment)

    with pytest.raises(IncrementalIdentityError, match="lowercase SHA-256"):
        replace(
            original,
            segment=segment,
            predecessor_alias_resolution_version_id="not-a-digest",
        )
    with pytest.raises(IncrementalIdentityError, match="cannot be an alias segment ID"):
        replace(
            original,
            segment=segment,
            predecessor_alias_resolution_version_id=original.alias_segment_id,
        )
    with pytest.raises(IncrementalIdentityError, match="must change"):
        successor_alias_resolution_version(original, segment=segment)
    with pytest.raises(IncrementalIdentityError, match="stable segment or predecessor"):
        successor_alias_resolution_version(
            original,
            segment=segment,
            alias_segment_id=_digest("different-segment"),
        )
    with pytest.raises(IncrementalIdentityError, match="does not match previous"):
        successor_alias_resolution_version(
            original,
            segment=_segment(
                valid_from_session=date(2022, 3, 7),
                segment_origin_source_record_id=_digest("different-segment-source"),
            ),
            identity_cutoff_session=date(2026, 7, 30),
        )


def test_tombstone_requires_predecessor_null_canonical_ids_reason_and_lineage() -> None:
    segment = _segment()
    previous = _resolution(segment)
    tombstone = successor_alias_resolution_version(
        previous,
        segment=segment,
        canonical_asset_id=None,
        canonical_composite_figi=None,
        canonical_share_class_id=None,
        canonical_share_class_figi=None,
        canonical_issuer_id=None,
        canonical_cik_normalized=None,
        resolution_method=AliasResolutionMethod.APPROVED_WITHDRAWAL,
        resolution_status=AliasResolutionStatus.TOMBSTONED,
        disposition=AliasResolutionDisposition.WITHDRAWN_SOURCE_CORRECTION,
        decision_lineage_ids=(_digest("withdrawal-decision"),),
        share_class_resolution_method=ShareClassResolutionMethod.NOT_APPLICABLE,
        share_class_decision_lineage_ids=(),
        resolution_available_session=date(2026, 7, 29),
        evidence_available_session=date(2026, 7, 29),
        evidence_cutoff_session=date(2026, 7, 29),
        is_tombstone=True,
        tombstone_reason_code="withdrawn_source_correction",
    )

    assert tombstone.alias_segment_id == previous.alias_segment_id
    assert tombstone.predecessor_alias_resolution_version_id == (
        previous.alias_resolution_version_id
    )
    assert tombstone.alias_resolution_version_id != previous.alias_resolution_version_id
    assert "tombstone_available_session" not in tombstone.logical_payload()
    assert tombstone.resolution_available_session == date(2026, 7, 29)

    with pytest.raises(IncrementalIdentityError, match="must supersede"):
        replace(tombstone, segment=segment, predecessor_alias_resolution_version_id=None)
    with pytest.raises(IncrementalIdentityError, match="null canonical IDs"):
        replace(
            tombstone,
            segment=segment,
            canonical_asset_id=_digest("illegal-tombstone-asset"),
        )
    with pytest.raises(IncrementalIdentityError, match="requires a reason"):
        replace(tombstone, segment=segment, tombstone_reason_code=None)
    with pytest.raises(IncrementalIdentityError, match="requires decision lineage"):
        replace(tombstone, segment=segment, decision_lineage_ids=())
    with pytest.raises(IncrementalIdentityError, match="tombstone-only"):
        replace(
            previous,
            segment=segment,
            tombstone_reason_code="illegal_non_tombstone_reason",
        )
    with pytest.raises(IncrementalIdentityError, match="cannot have a successor"):
        successor_alias_resolution_version(
            tombstone,
            segment=segment,
            identity_cutoff_session=date(2026, 7, 30),
        )


def test_invalid_cutoff_availability_and_resolution_shapes_fail_closed() -> None:
    with pytest.raises(IncrementalIdentityError, match="resolution availability"):
        _resolution(resolution_available_session=date(2026, 7, 30))
    with pytest.raises(IncrementalIdentityError, match="evidence availability"):
        _resolution(evidence_available_session=date(2026, 7, 29))
    with pytest.raises(IncrementalIdentityError, match="evidence cutoff"):
        _resolution(evidence_cutoff_session=date(2026, 7, 30))
    with pytest.raises(IncrementalIdentityError, match="canonical asset"):
        _resolution(canonical_asset_id=None)
    with pytest.raises(IncrementalIdentityError, match="cannot carry canonical IDs"):
        _resolution(
            resolution_method=AliasResolutionMethod.ADJUDICATED_UNRESOLVED,
            resolution_status=AliasResolutionStatus.UNRESOLVED,
            disposition=AliasResolutionDisposition.ADJUDICATED_UNRESOLVED,
        )

    with pytest.raises(IncrementalIdentityError, match="method is invalid"):
        _resolution(resolution_method="arbitrary_token")
    with pytest.raises(IncrementalIdentityError, match="disposition is invalid"):
        _resolution(disposition="arbitrary_token")
    with pytest.raises(IncrementalIdentityError, match="shape is invalid"):
        _resolution(resolution_method=AliasResolutionMethod.DIRECT_OBSERVED)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        (
            {
                "identity_cutoff_session": date(2022, 2, 8),
                "resolution_available_session": date(2022, 2, 8),
                "evidence_cutoff_session": date(2022, 2, 8),
                "evidence_available_session": date(2022, 2, 8),
            },
            "identity cutoff precedes alias segment start",
        ),
        (
            {
                "resolution_available_session": date(2022, 2, 8),
                "evidence_available_session": date(2022, 2, 8),
            },
            "resolution availability precedes alias segment start",
        ),
        (
            {
                "evidence_cutoff_session": date(2022, 2, 8),
                "evidence_available_session": date(2022, 2, 8),
            },
            "evidence cutoff precedes alias segment start",
        ),
        (
            {"evidence_available_session": date(2022, 2, 8)},
            "evidence availability precedes alias segment start",
        ),
    ],
)
def test_knowledge_dates_cannot_precede_segment_start(
    changes: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(IncrementalIdentityError, match=message):
        _resolution(**changes)


@pytest.mark.parametrize(
    ("valid_through", "message"),
    [
        (date(2022, 2, 8), "valid-through session precedes alias segment start"),
        (date(2026, 7, 30), "valid-through session exceeds identity cutoff"),
    ],
)
def test_valid_through_must_stay_inside_segment_and_identity_cutoff(
    valid_through: date,
    message: str,
) -> None:
    with pytest.raises(IncrementalIdentityError, match=message):
        _resolution(valid_through_session=valid_through)


def test_canonical_ids_reproduce_the_frozen_s7_rules_and_reject_tampering() -> None:
    composite = "BBG000DFMXT3"
    share_class = "BBG001S87NT0"
    cik = "0000929940"

    assert canonical_asset_id(composite) == stable_digest(
        {
            "anchor_type": "composite_figi",
            "anchor_value": composite,
            "namespace": "ame_stocks.identity.asset",
            "rule_version": "ame_stocks_asset_id_from_composite_figi_v1",
        }
    )
    assert canonical_share_class_id(share_class) == stable_digest(
        {
            "anchor_type": "share_class_figi",
            "anchor_value": share_class,
            "namespace": "ame_stocks.identity.share_class",
            "rule_version": "ame_stocks_share_class_id_from_share_class_figi_v1",
        }
    )
    assert canonical_issuer_id(cik) == stable_digest(
        {
            "anchor_type": "cik_normalized",
            "anchor_value": cik,
            "namespace": "ame_stocks.identity.issuer",
            "rule_version": "ame_stocks_issuer_id_from_normalized_cik_v1",
        }
    )

    with pytest.raises(IncrementalIdentityError, match="asset ID does not reproduce"):
        _resolution(canonical_asset_id=_digest("tampered-asset"))
    with pytest.raises(IncrementalIdentityError, match="Share Class ID does not reproduce"):
        _resolution(canonical_share_class_id=_digest("tampered-share-class"))
    with pytest.raises(IncrementalIdentityError, match="issuer ID does not reproduce"):
        _resolution(canonical_issuer_id=_digest("tampered-issuer"))


def test_segment_aware_factory_enforces_direct_and_override_decision_shapes() -> None:
    segment = _segment()
    direct = _resolution(
        segment,
        canonical_asset_id=canonical_asset_id("BBG000KRLLH9"),
        canonical_composite_figi="BBG000KRLLH9",
        resolution_method=AliasResolutionMethod.DIRECT_OBSERVED,
        disposition=AliasResolutionDisposition.OBSERVED_CONSISTENT,
        decision_lineage_ids=(),
    )

    assert direct.canonical_composite_figi == segment.observed_composite_figi
    assert direct.decision_lineage_ids == ()

    with pytest.raises(IncrementalIdentityError, match="cannot claim approved decision lineage"):
        _resolution(
            segment,
            canonical_asset_id=canonical_asset_id("BBG000KRLLH9"),
            canonical_composite_figi="BBG000KRLLH9",
            resolution_method=AliasResolutionMethod.DIRECT_OBSERVED,
            disposition=AliasResolutionDisposition.OBSERVED_CONSISTENT,
        )
    with pytest.raises(IncrementalIdentityError, match="requires decision lineage"):
        _resolution(segment, decision_lineage_ids=())
    with pytest.raises(IncrementalIdentityError, match="must equal observed"):
        _resolution(
            segment,
            resolution_method=AliasResolutionMethod.DIRECT_OBSERVED,
            disposition=AliasResolutionDisposition.OBSERVED_CONSISTENT,
            decision_lineage_ids=(),
        )
    with pytest.raises(IncrementalIdentityError, match="direct Share Class resolution"):
        _resolution(
            segment,
            canonical_asset_id=canonical_asset_id("BBG000KRLLH9"),
            canonical_composite_figi="BBG000KRLLH9",
            canonical_share_class_id=canonical_share_class_id("BBG001S5Q3X4"),
            canonical_share_class_figi="BBG001S5Q3X4",
            resolution_method=AliasResolutionMethod.DIRECT_OBSERVED,
            disposition=AliasResolutionDisposition.OBSERVED_CONSISTENT,
            decision_lineage_ids=(),
        )
    with pytest.raises(IncrementalIdentityError, match="must differ"):
        _resolution(
            segment,
            canonical_asset_id=canonical_asset_id("BBG000KRLLH9"),
            canonical_composite_figi="BBG000KRLLH9",
        )

    provider_stale = _resolution(
        segment,
        resolution_method=AliasResolutionMethod.APPROVED_PROVIDER_COMPOSITE_OVERRIDE,
        disposition=(AliasResolutionDisposition.PROVIDER_COMPOSITE_STALE_AFTER_TRANSITION),
    )
    assert provider_stale.canonical_composite_figi != segment.observed_composite_figi
    with pytest.raises(IncrementalIdentityError, match="direct Share Class resolution"):
        _resolution(
            segment,
            canonical_share_class_id=canonical_share_class_id("BBG001S5Q3X4"),
            canonical_share_class_figi="BBG001S5Q3X4",
        )

    share_adjudication = _resolution(
        segment,
        canonical_asset_id=canonical_asset_id("BBG000KRLLH9"),
        canonical_composite_figi="BBG000KRLLH9",
        canonical_share_class_id=canonical_share_class_id("BBG001S5Q3X4"),
        canonical_share_class_figi="BBG001S5Q3X4",
        resolution_method=AliasResolutionMethod.DIRECT_OBSERVED,
        disposition=AliasResolutionDisposition.TRANSIENT_DUPLICATE_SHARE_CLASS,
        decision_lineage_ids=(),
        share_class_resolution_method=(
            ShareClassResolutionMethod.APPROVED_SHARE_CLASS_ADJUDICATION
        ),
        share_class_decision_lineage_ids=(_digest("share-class-decision"),),
    )
    assert share_adjudication.canonical_composite_figi == segment.observed_composite_figi
    assert share_adjudication.canonical_share_class_figi != segment.observed_share_class_figi
    with pytest.raises(IncrementalIdentityError, match="cannot change canonical issuer"):
        _resolution(
            segment,
            canonical_asset_id=canonical_asset_id("BBG000KRLLH9"),
            canonical_composite_figi="BBG000KRLLH9",
            canonical_issuer_id=canonical_issuer_id("0000025445"),
            canonical_cik_normalized="0000025445",
            resolution_method=AliasResolutionMethod.DIRECT_OBSERVED,
            disposition=AliasResolutionDisposition.TRANSIENT_DUPLICATE_SHARE_CLASS,
            decision_lineage_ids=(),
            share_class_resolution_method=(
                ShareClassResolutionMethod.APPROVED_SHARE_CLASS_ADJUDICATION
            ),
            share_class_decision_lineage_ids=(_digest("share-class-decision"),),
        )

    composite_and_share_adjudication = _resolution(
        segment,
        canonical_share_class_id=canonical_share_class_id("BBG001S5Q3X4"),
        canonical_share_class_figi="BBG001S5Q3X4",
        share_class_resolution_method=(
            ShareClassResolutionMethod.APPROVED_SHARE_CLASS_ADJUDICATION
        ),
        share_class_decision_lineage_ids=(_digest("share-class-decision"),),
    )
    assert composite_and_share_adjudication.canonical_composite_figi == "BBG000DFMXT3"
    assert composite_and_share_adjudication.canonical_share_class_figi == "BBG001S5Q3X4"
    assert composite_and_share_adjudication.decision_lineage_ids
    assert composite_and_share_adjudication.share_class_decision_lineage_ids

    with pytest.raises(IncrementalIdentityError, match="requires its own decision lineage"):
        _resolution(
            segment,
            canonical_asset_id=canonical_asset_id("BBG000KRLLH9"),
            canonical_composite_figi="BBG000KRLLH9",
            canonical_share_class_id=canonical_share_class_id("BBG001S5Q3X4"),
            canonical_share_class_figi="BBG001S5Q3X4",
            resolution_method=AliasResolutionMethod.DIRECT_OBSERVED,
            disposition=AliasResolutionDisposition.TRANSIENT_DUPLICATE_SHARE_CLASS,
            decision_lineage_ids=(),
            share_class_resolution_method=(
                ShareClassResolutionMethod.APPROVED_SHARE_CLASS_ADJUDICATION
            ),
            share_class_decision_lineage_ids=(),
        )
    with pytest.raises(IncrementalIdentityError, match="cannot claim decision lineage"):
        _resolution(
            segment,
            share_class_decision_lineage_ids=(_digest("unapproved-share-decision"),),
        )

    mismatched = _segment(
        ticker="CR",
        observed_composite_figi="BBG000BG7423",
        observed_share_class_figi="BBG001S5Q3X4",
        observed_cik_normalized="0000025445",
        segment_origin_source_record_id=_digest("cr-source"),
    )
    with pytest.raises(IncrementalIdentityError, match="does not reproduce"):
        AliasResolutionVersion.from_dict(
            _resolution(segment).to_dict(),
            segment=mismatched,
        )


def test_absent_share_class_has_only_one_not_applicable_representation() -> None:
    segment = _segment(observed_share_class_figi=None)
    absent = _resolution(
        segment,
        canonical_share_class_id=None,
        canonical_share_class_figi=None,
        share_class_resolution_method=ShareClassResolutionMethod.NOT_APPLICABLE,
    )

    assert absent.share_class_resolution_method is ShareClassResolutionMethod.NOT_APPLICABLE
    with pytest.raises(IncrementalIdentityError, match="requires an observed Share Class"):
        replace(
            absent,
            segment=segment,
            share_class_resolution_method=ShareClassResolutionMethod.DIRECT_OBSERVED,
        )


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("identity_cutoff_session", date(2026, 7, 28)),
        ("resolution_available_session", date(2026, 7, 27)),
        ("evidence_cutoff_session", date(2026, 7, 27)),
        ("evidence_available_session", date(2026, 7, 26)),
    ],
)
def test_successor_cutoff_and_availability_cannot_move_backward(
    field: str,
    replacement: date,
) -> None:
    segment = _segment()
    previous = _resolution(segment)

    with pytest.raises(IncrementalIdentityError, match="cannot move backward"):
        successor_alias_resolution_version(
            previous,
            segment=segment,
            **{field: replacement},
        )


def test_clean_delta_root_proof_accepts_direct_and_safe_pending_roots() -> None:
    segment = _segment()
    direct = _resolution(
        segment,
        canonical_asset_id=canonical_asset_id("BBG000KRLLH9"),
        canonical_composite_figi="BBG000KRLLH9",
        resolution_method=AliasResolutionMethod.DIRECT_OBSERVED,
        disposition=AliasResolutionDisposition.OBSERVED_CONSISTENT,
        decision_lineage_ids=(),
    )
    pending = _resolution(
        segment,
        canonical_asset_id=None,
        canonical_composite_figi=None,
        canonical_share_class_id=None,
        canonical_share_class_figi=None,
        canonical_issuer_id=None,
        canonical_cik_normalized=None,
        resolution_method=AliasResolutionMethod.PENDING_REVIEW,
        resolution_status=AliasResolutionStatus.UNRESOLVED,
        disposition=AliasResolutionDisposition.PENDING_UNRESOLVED,
        decision_lineage_ids=(),
        share_class_resolution_method=ShareClassResolutionMethod.NOT_APPLICABLE,
    )

    for version in (direct, pending):
        receipt = SimpleNamespace(
            table_name="ticker_alias",
            stable_row_key=segment.alias_segment_id,
            row_version_id=version.alias_resolution_version_id,
            predecessor_row_version_id=None,
            operation=SimpleNamespace(value="new_root"),
            row_payload_digest=stable_digest(version.to_dict()),
        )
        assert validate_ticker_alias_clean_delta_root(segment, version, receipt) is None


def test_clean_delta_root_rejects_approved_override_relabelled_as_new_root() -> None:
    segment = _segment()
    approved_override = _resolution(segment)
    forged_new_root_receipt = SimpleNamespace(
        table_name="ticker_alias",
        stable_row_key=segment.alias_segment_id,
        row_version_id=approved_override.alias_resolution_version_id,
        predecessor_row_version_id=None,
        operation="new_root",
        row_payload_digest=stable_digest(approved_override.to_dict()),
    )

    with pytest.raises(IncrementalIdentityError, match="direct-observed resolved"):
        validate_ticker_alias_clean_delta_root(
            segment,
            approved_override,
            forged_new_root_receipt,
        )

    approved_share_adjudication = _resolution(
        segment,
        canonical_asset_id=canonical_asset_id("BBG000KRLLH9"),
        canonical_composite_figi="BBG000KRLLH9",
        canonical_share_class_id=canonical_share_class_id("BBG001S5Q3X4"),
        canonical_share_class_figi="BBG001S5Q3X4",
        resolution_method=AliasResolutionMethod.DIRECT_OBSERVED,
        disposition=AliasResolutionDisposition.TRANSIENT_DUPLICATE_SHARE_CLASS,
        decision_lineage_ids=(),
        share_class_resolution_method=(
            ShareClassResolutionMethod.APPROVED_SHARE_CLASS_ADJUDICATION
        ),
        share_class_decision_lineage_ids=(_digest("share-class-decision"),),
    )
    share_receipt = SimpleNamespace(
        table_name="ticker_alias",
        stable_row_key=segment.alias_segment_id,
        row_version_id=approved_share_adjudication.alias_resolution_version_id,
        predecessor_row_version_id=None,
        operation="new_root",
        row_payload_digest=stable_digest(approved_share_adjudication.to_dict()),
    )
    with pytest.raises(IncrementalIdentityError, match="approved decision"):
        validate_ticker_alias_clean_delta_root(
            segment,
            approved_share_adjudication,
            share_receipt,
        )


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("table_name", "asset_master"),
        ("stable_row_key", _digest("wrong-clean-root-stable-key")),
        ("row_version_id", _digest("wrong-clean-root-row-version")),
        ("predecessor_row_version_id", _digest("forged-clean-root-predecessor")),
        ("operation", "mechanical_successor"),
        ("row_payload_digest", _digest("wrong-clean-root-payload")),
    ],
)
def test_clean_delta_root_rejects_non_exact_receipt(
    field: str,
    replacement: object,
) -> None:
    segment = _segment()
    direct = _resolution(
        segment,
        canonical_asset_id=canonical_asset_id("BBG000KRLLH9"),
        canonical_composite_figi="BBG000KRLLH9",
        resolution_method=AliasResolutionMethod.DIRECT_OBSERVED,
        disposition=AliasResolutionDisposition.OBSERVED_CONSISTENT,
        decision_lineage_ids=(),
    )
    values: dict[str, object] = {
        "table_name": "ticker_alias",
        "stable_row_key": segment.alias_segment_id,
        "row_version_id": direct.alias_resolution_version_id,
        "predecessor_row_version_id": None,
        "operation": "new_root",
        "row_payload_digest": stable_digest(direct.to_dict()),
    }
    values[field] = replacement

    with pytest.raises(IncrementalIdentityError, match=f"receipt {field} does not match"):
        validate_ticker_alias_clean_delta_root(
            segment,
            direct,
            SimpleNamespace(**values),
        )


def test_mechanical_successor_proof_accepts_only_exact_extension_and_receipt() -> None:
    segment = _segment()
    previous = _resolution(segment)
    successor = _mechanical_successor(segment, previous)
    receipt = SimpleNamespace(
        table_name="ticker_alias",
        stable_row_key=segment.alias_segment_id,
        row_version_id=successor.alias_resolution_version_id,
        predecessor_row_version_id=previous.alias_resolution_version_id,
        operation=SimpleNamespace(value="mechanical_successor"),
        row_payload_digest=stable_digest(successor.to_dict()),
    )

    assert (
        validate_ticker_alias_mechanical_successor(
            previous,
            successor,
            segment,
            receipt,
        )
        is None
    )


def test_mechanical_successor_helper_is_structural_not_calendar_coverage_proof() -> None:
    segment = _segment()
    previous = _resolution(segment, valid_through_session=date(2022, 2, 18))
    structurally_valid_but_unproven_gap = _mechanical_successor(
        segment,
        previous,
        valid_through_session=date(2026, 7, 29),
    )

    assert (
        validate_ticker_alias_mechanical_successor(
            previous,
            structurally_valid_but_unproven_gap,
            segment,
        )
        is None
    )
    # Gate A therefore keeps every row-bearing release disabled. I3 must load
    # a calendar-aware exact source-coverage receipt before this structural
    # result can be accepted by the module-owned dispatcher.


@pytest.mark.parametrize(
    "changes",
    [
        {
            "canonical_asset_id": canonical_asset_id("BBG000BG7423"),
            "canonical_composite_figi": "BBG000BG7423",
        },
        {
            "resolution_method": AliasResolutionMethod.APPROVED_PROVIDER_COMPOSITE_OVERRIDE,
            "disposition": (AliasResolutionDisposition.PROVIDER_COMPOSITE_STALE_AFTER_TRANSITION),
        },
        {"decision_lineage_ids": (_digest("different-mechanical-decision"),)},
        {"identity_policy_bundle_id": _digest("different-mechanical-policy")},
    ],
)
def test_mechanical_successor_proof_rejects_research_identity_changes(
    changes: dict[str, object],
) -> None:
    segment = _segment()
    previous = _resolution(segment)
    successor = _mechanical_successor(segment, previous, **changes)

    with pytest.raises(IncrementalIdentityError, match="changed protected field"):
        validate_ticker_alias_mechanical_successor(previous, successor, segment)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("identity_cutoff_session", date(2026, 7, 28)),
        ("resolution_available_session", date(2026, 7, 27)),
        ("evidence_cutoff_session", date(2026, 7, 27)),
        ("evidence_available_session", date(2026, 7, 26)),
    ],
)
def test_mechanical_successor_proof_rejects_backward_knowledge_time(
    field: str,
    replacement: date,
) -> None:
    segment = _segment()
    previous = _resolution(segment)
    successor = replace(
        previous,
        segment=segment,
        predecessor_alias_resolution_version_id=previous.alias_resolution_version_id,
        **{field: replacement},
    )

    with pytest.raises(IncrementalIdentityError, match="cannot move backward"):
        validate_ticker_alias_mechanical_successor(previous, successor, segment)


def test_mechanical_successor_proof_rejects_wrong_edges_and_empty_churn() -> None:
    segment = _segment()
    previous = _resolution(segment)
    valid_successor = _mechanical_successor(segment, previous)

    wrong_predecessor = replace(
        valid_successor,
        segment=segment,
        predecessor_alias_resolution_version_id=_digest("wrong-mechanical-predecessor"),
    )
    with pytest.raises(IncrementalIdentityError, match="exact predecessor"):
        validate_ticker_alias_mechanical_successor(previous, wrong_predecessor, segment)

    backward_interval = replace(
        previous,
        segment=segment,
        predecessor_alias_resolution_version_id=previous.alias_resolution_version_id,
        source_record_set_digest=_digest("backward-interval-source"),
        valid_through_session=date(2022, 2, 18),
    )
    with pytest.raises(IncrementalIdentityError, match="valid-through session cannot move"):
        validate_ticker_alias_mechanical_successor(previous, backward_interval, segment)

    empty_churn = replace(
        previous,
        segment=segment,
        predecessor_alias_resolution_version_id=previous.alias_resolution_version_id,
    )
    with pytest.raises(IncrementalIdentityError, match="advance an allowed logical field"):
        validate_ticker_alias_mechanical_successor(previous, empty_churn, segment)

    other_segment = _segment(
        valid_from_session=date(2022, 3, 7),
        segment_origin_source_record_id=_digest("mechanical-other-segment"),
    )
    cross_segment = replace(
        _resolution(other_segment, valid_through_session=date(2022, 3, 7)),
        segment=other_segment,
        predecessor_alias_resolution_version_id=previous.alias_resolution_version_id,
    )
    with pytest.raises(IncrementalIdentityError, match="preserve the exact segment"):
        validate_ticker_alias_mechanical_successor(previous, cross_segment, segment)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("table_name", "asset_master"),
        ("stable_row_key", _digest("wrong-mechanical-stable-key")),
        ("row_version_id", _digest("wrong-mechanical-row-version")),
        ("predecessor_row_version_id", _digest("wrong-receipt-predecessor")),
        ("operation", "reviewed_correction"),
        ("row_payload_digest", _digest("wrong-mechanical-payload")),
    ],
)
def test_mechanical_successor_proof_rejects_mislabeled_receipt(
    field: str,
    replacement: object,
) -> None:
    segment = _segment()
    previous = _resolution(segment)
    successor = _mechanical_successor(segment, previous)
    values: dict[str, object] = {
        "table_name": "ticker_alias",
        "stable_row_key": segment.alias_segment_id,
        "row_version_id": successor.alias_resolution_version_id,
        "predecessor_row_version_id": previous.alias_resolution_version_id,
        "operation": "mechanical_successor",
        "row_payload_digest": stable_digest(successor.to_dict()),
    }
    values[field] = replacement

    with pytest.raises(IncrementalIdentityError, match=f"receipt {field} does not match"):
        validate_ticker_alias_mechanical_successor(
            previous,
            successor,
            segment,
            SimpleNamespace(**values),
        )


def test_segment_requires_strong_observed_composite_and_lineage_is_sorted_unique() -> None:
    with pytest.raises(IncrementalIdentityError, match="observed Composite"):
        _segment(observed_composite_figi=None)

    first = _digest("decision-a")
    second = _digest("decision-b")
    sorted_lineage = tuple(sorted((first, second)))
    assert _resolution(decision_lineage_ids=sorted_lineage).decision_lineage_ids == sorted_lineage
    with pytest.raises(IncrementalIdentityError, match="sorted and unique"):
        _resolution(decision_lineage_ids=tuple(reversed(sorted_lineage)))
    with pytest.raises(IncrementalIdentityError, match="sorted and unique"):
        _resolution(decision_lineage_ids=(first, first))
    with pytest.raises(IncrementalIdentityError, match="sorted and unique"):
        _resolution(
            canonical_share_class_id=canonical_share_class_id("BBG001S5Q3X4"),
            canonical_share_class_figi="BBG001S5Q3X4",
            share_class_resolution_method=(
                ShareClassResolutionMethod.APPROVED_SHARE_CLASS_ADJUDICATION
            ),
            share_class_decision_lineage_ids=tuple(reversed(sorted_lineage)),
        )
