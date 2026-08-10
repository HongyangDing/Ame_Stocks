from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import replace
from datetime import date
from types import MappingProxyType

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    IDENTITY_REGISTRY_ORDER,
    IdentityPolicyBundle,
    IdentityRegistryKind,
    IdentityRegistryReleasePin,
)
from ame_stocks_api.silver.incremental_i3_dispatch import (
    I3_FIXTURE_REGISTRY_DECISION_NAMESPACE,
    I3_FIXTURE_REGISTRY_RELEASE_NAMESPACE,
    I3_FIXTURE_REGISTRY_RELEASE_RULE_VERSION,
    I3_FIXTURE_REGISTRY_RELEASE_SCHEMA_VERSION,
    I3_QA_CATALOG_DIGEST,
    I3_ROW_VALIDATOR_SEMANTICS_DIGEST,
    I3DispatchError,
    IdentityObservation,
    IdentityPolicySnapshot,
    RegistrySourceScopeRow,
    SourceCoverageSlot,
    _dispatch_i3_identity_window_from_verified_batch,
    _verify_i3_identity_policy_snapshot_for_batch,
    bind_alias_source_coverage,
    dispatch_i3_identity_window,
    freeze_exact_trading_calendar,
    load_fixture_identity_policy_snapshot,
    verify_i3_local_staging_attestation,
)
from ame_stocks_api.silver.incremental_identity import canonical_asset_id

_A = "BBG000DFMXT3"
_B = "BBG000KRLLH9"
_C = "BBG000BG7423"
_SHARE = "BBG001S87NT0"
_SESSIONS = (
    date(2026, 7, 7),
    date(2026, 7, 8),
    date(2026, 7, 9),
    date(2026, 7, 10),
)


def _digest(label: str) -> str:
    return stable_digest({"fixture": label})


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _decision_record(
    registry_kind: IdentityRegistryKind,
    *,
    source_record_id: str,
    label: str,
    **overrides: object,
) -> dict[str, object]:
    record: dict[str, object] = {
        "canonical_composite_figi": None,
        "canonical_composite_market_code": None,
        "canonical_share_class_figi": None,
        "composite_scope_figi": None,
        "decision_available_session": "2026-07-30",
        "effective_from_session": _SESSIONS[-1].isoformat(),
        "effective_to_session": _SESSIONS[-1].isoformat(),
        "identity_disposition": None,
        "observed_composite_figi": None,
        "observed_composite_market_code": None,
        "observed_share_class_figi": None,
        "predecessor_asset_id": None,
        "provider_id": "massive",
        "provider_locale": "us",
        "provider_market": "stocks",
        "source_record_id": source_record_id,
        "successor_asset_id": None,
        "ticker": "TEST",
        "transition_relation_id": None,
    }
    if registry_kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION:
        record.update(
            {
                "canonical_composite_market_code": "US",
                "observed_composite_market_code": "GB",
                "observed_share_class_figi": _SHARE,
                "identity_disposition": "confirmed_provider_contamination",
            }
        )
    elif registry_kind is IdentityRegistryKind.IDENTITY_ADJUDICATION:
        record["identity_disposition"] = "confirmed_provider_contamination"
    elif registry_kind is IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE:
        record.update(
            {
                "canonical_composite_market_code": "US",
                "identity_disposition": ("confirmed_provider_composite_stale_after_transition"),
                "observed_composite_market_code": "US",
            }
        )
    elif registry_kind is IdentityRegistryKind.SHARE_CLASS_ADJUDICATION:
        record["identity_disposition"] = "confirmed_share_class_correction"
    elif registry_kind is IdentityRegistryKind.ASSET_TRANSITION:
        record["identity_disposition"] = "confirmed_genuine_transition"
    record.update(overrides)
    logical = {
        "namespace": I3_FIXTURE_REGISTRY_DECISION_NAMESPACE,
        "registry_kind": registry_kind.value,
        **record,
    }
    return {"decision_id": stable_digest(logical), **record, "_fixture_label": label}


def _fixture_bundle_and_contents(
    *records_with_labels: tuple[IdentityRegistryKind, dict[str, object]],
) -> tuple[IdentityPolicyBundle, tuple[bytes, ...]]:
    by_kind: dict[IdentityRegistryKind, list[dict[str, object]]] = {
        kind: [] for kind in IDENTITY_REGISTRY_ORDER
    }
    for kind, raw_record in records_with_labels:
        record = dict(raw_record)
        record.pop("_fixture_label", None)
        by_kind[kind].append(record)
    pins: list[IdentityRegistryReleasePin] = []
    contents: list[bytes] = []
    for index, kind in enumerate(IDENTITY_REGISTRY_ORDER, start=1):
        decisions = sorted(by_kind[kind], key=lambda item: item["decision_id"])
        logical_release = {
            "decision_cutoff_session": "2026-07-29",
            "decisions": decisions,
            "namespace": I3_FIXTURE_REGISTRY_RELEASE_NAMESPACE,
            "registry_kind": kind.value,
            "release_available_session": "2026-07-30",
            "rule_version": I3_FIXTURE_REGISTRY_RELEASE_RULE_VERSION,
            "schema_version": I3_FIXTURE_REGISTRY_RELEASE_SCHEMA_VERSION,
            "scope": "local_fixture_only",
        }
        release_id = stable_digest(logical_release)
        content = _canonical_bytes({"release_id": release_id, **logical_release})
        pins.append(
            IdentityRegistryReleasePin(
                registry_kind=kind,
                release_id=release_id,
                artifact=ArtifactPin(
                    path=f"manifests/fixture/{index:02d}-{kind.value}.json",
                    sha256=hashlib.sha256(content).hexdigest(),
                    bytes=len(content),
                ),
                decision_cutoff_session=date(2026, 7, 29),
                release_available_session=date(2026, 7, 30),
            )
        )
        contents.append(content)
    return (
        IdentityPolicyBundle(
            registry_releases=tuple(pins),
            bundle_available_session=date(2026, 8, 5),
        ),
        tuple(contents),
    )


def _snapshot(
    *records: tuple[IdentityRegistryKind, dict[str, object]],
) -> IdentityPolicySnapshot:
    bundle, contents = _fixture_bundle_and_contents(*records)
    return load_fixture_identity_policy_snapshot(
        bundle,
        registry_release_contents=contents,
    )


def _repin_release_document(
    bundle: IdentityPolicyBundle,
    contents: tuple[bytes, ...],
    *,
    index: int,
    document: dict[str, object],
    reproduce_release_id: bool = True,
) -> tuple[IdentityPolicyBundle, tuple[bytes, ...]]:
    if reproduce_release_id:
        document["release_id"] = stable_digest(
            {key: value for key, value in document.items() if key != "release_id"}
        )
    content = _canonical_bytes(document)
    releases = list(bundle.registry_releases)
    prior = releases[index]
    releases[index] = replace(
        prior,
        release_id=str(document["release_id"]),
        artifact=ArtifactPin(
            path=prior.artifact.path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        ),
    )
    changed_contents = list(contents)
    changed_contents[index] = content
    return (
        IdentityPolicyBundle(
            registry_releases=tuple(releases),
            bundle_available_session=bundle.bundle_available_session,
        ),
        tuple(changed_contents),
    )


def _observation(
    session: date,
    figi: str,
    country: str,
    *,
    source_label: str,
    active_on_date: bool = True,
) -> IdentityObservation:
    return IdentityObservation(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker="TEST",
        session_date=session,
        observed_composite_figi=figi,
        observed_composite_country=country,
        observed_share_class_figi=_SHARE,
        primary_exchange="XNAS",
        source_record_id=_digest(source_label),
        active_on_date=active_on_date,
    )


def test_dispatch_preserves_exact_mixed_case_provider_ticker() -> None:
    observation = replace(
        _observation(_SESSIONS[-1], _A, "US", source_label="mixed-case"),
        ticker="AVKrw",
    )
    scope = RegistrySourceScopeRow(
        session_date=observation.session_date,
        source_record_id=observation.source_record_id,
        source_dataset="asset_observation_daily",
        source_s4_release_set_id=_digest("s4-release"),
        provider_id=observation.provider_id,
        provider_market=observation.provider_market,
        provider_locale=observation.provider_locale,
        ticker=observation.ticker,
        observed_composite_figi=observation.observed_composite_figi,
        observed_share_class_figi=observation.observed_share_class_figi,
        primary_exchange_mic=observation.primary_exchange,
    )

    assert observation.ticker == "AVKrw"
    assert scope.ticker == "AVKrw"


def _coverage(
    observations: tuple[IdentityObservation, ...],
    *,
    target: date = _SESSIONS[-1],
):
    calendar = freeze_exact_trading_calendar(
        _SESSIONS,
        artifact_path="manifests/calendars/xnys-fixture.json",
    )
    window = _SESSIONS[-3:]
    by_session = {item.session_date: item.source_record_id for item in observations}
    slots = tuple(
        SourceCoverageSlot(
            session_date=session,
            partition_receipt_id=_digest(f"partition-{session.isoformat()}"),
            source_record_ids=(by_session[session],) if session in by_session else (),
        )
        for session in window
    )
    return bind_alias_source_coverage(
        calendar,
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker="TEST",
        target_session=target,
        slots=slots,
        coverage_available_session=date(2026, 8, 5),
    )


def _qa(attestation, check_id: str):
    return next(item for item in attestation.qa_receipt.results if item.check_id == check_id)


def _assert_typed_raw_review(result) -> None:
    assert sum(item.count for item in result.reason_counts) == result.observed_count
    assert tuple(sorted({item.source_record_id for item in result.bounded_examples})) == (
        result.bounded_example_ids
    )
    known_reasons = {item.reason_code for item in result.reason_counts}
    assert all(set(item.reason_codes).issubset(known_reasons) for item in result.bounded_examples)


def test_fixture_registry_loader_seals_exact_scope_and_caches_index_and_id() -> None:
    source_record_id = _digest("loader-source")
    record = _decision_record(
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        source_record_id=source_record_id,
        label="loader-cross-market",
        observed_composite_figi=_B,
        canonical_composite_figi=_A,
    )
    snapshot = _snapshot((IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION, record))
    matching = _observation(_SESSIONS[-1], _B, "GB", source_label="loader-source")
    other_source = _observation(_SESSIONS[-1], _B, "GB", source_label="loader-other-source")

    assert len(snapshot.matching_decisions(matching)) == 1
    assert snapshot.matching_decisions(other_source) == ()
    assert snapshot.matching_decisions(replace(matching, provider_market="options")) == ()
    assert snapshot.matching_decisions(replace(matching, observed_composite_country="US")) == ()
    assert snapshot.matching_decisions(replace(matching, observed_share_class_figi=_C)) == ()
    with pytest.raises(I3DispatchError, match="closed to provider locale=us"):
        replace(matching, provider_locale="gb")
    first_id = snapshot.policy_snapshot_id
    assert snapshot.policy_snapshot_id is first_id
    assert snapshot.to_dict()["policy_snapshot_id"] == first_id
    assert snapshot.decisions[0].provider_market == "stocks"
    assert snapshot.decisions[0].source_record_id == source_record_id
    assert snapshot.decisions[0].decision_available_session == date(2026, 7, 30)


def test_snapshot_reverification_rejects_injected_index_and_derived_state_tampering() -> None:
    target_source = _digest("snapshot-injection-target")
    record = _decision_record(
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        source_record_id=target_source,
        label="snapshot-injection",
        observed_composite_figi=_B,
        canonical_composite_figi=_A,
    )
    foreign_snapshot = _snapshot((IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION, record))
    observations = (
        _observation(_SESSIONS[-3], _B, "GB", source_label="snapshot-injection-left"),
        _observation(_SESSIONS[-2], _B, "GB", source_label="snapshot-injection-middle"),
        _observation(_SESSIONS[-1], _B, "GB", source_label="snapshot-injection-target"),
    )

    empty = _snapshot()
    key = ("massive", "stocks", "us", "TEST", target_source)
    object.__setattr__(
        empty,
        "_decision_index",
        MappingProxyType({key: (foreign_snapshot.decisions[0],)}),
    )
    with pytest.raises(I3DispatchError, match="decision index does not reproduce"):
        dispatch_i3_identity_window(
            policy_snapshot=empty,
            coverage=_coverage(observations),
            observations=observations,
        )

    snapshot = _snapshot((IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION, record))
    object.__setattr__(snapshot, "_decision_by_id", MappingProxyType({}))
    with pytest.raises(I3DispatchError, match="decision-ID index does not reproduce"):
        dispatch_i3_identity_window(
            policy_snapshot=snapshot,
            coverage=_coverage(observations),
            observations=observations,
        )

    snapshot = _snapshot((IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION, record))
    object.__setattr__(snapshot.decisions[0], "canonical_composite_figi", _C)
    with pytest.raises(I3DispatchError, match="decision ID does not reproduce"):
        dispatch_i3_identity_window(
            policy_snapshot=snapshot,
            coverage=_coverage(observations),
            observations=observations,
        )

    snapshot = _snapshot((IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION, record))
    object.__setattr__(snapshot, "_policy_snapshot_id", _digest("forged-snapshot-id"))
    with pytest.raises(I3DispatchError, match="snapshot ID does not reproduce"):
        dispatch_i3_identity_window(
            policy_snapshot=snapshot,
            coverage=_coverage(observations),
            observations=observations,
        )


def test_private_verified_batch_rejects_post_mint_index_and_decision_tampering() -> None:
    target_source = _digest("post-mint-target")
    record = _decision_record(
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        source_record_id=target_source,
        label="post-mint",
        observed_composite_figi=_B,
        canonical_composite_figi=_A,
    )
    observations = (
        _observation(_SESSIONS[-3], _B, "GB", source_label="post-mint-left"),
        _observation(_SESSIONS[-2], _B, "GB", source_label="post-mint-middle"),
        _observation(_SESSIONS[-1], _B, "GB", source_label="post-mint-target"),
    )
    handle = _verify_i3_identity_policy_snapshot_for_batch(
        _snapshot((IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION, record))
    )
    original_index = handle._policy_snapshot._decision_index
    object.__setattr__(handle._policy_snapshot, "_decision_index", MappingProxyType({}))
    with pytest.raises(I3DispatchError, match="snapshot identity changed"):
        _dispatch_i3_identity_window_from_verified_batch(
            verified_policy=handle,
            coverage=_coverage(observations),
            observations=observations,
        )

    object.__setattr__(handle._policy_snapshot, "_decision_index", original_index)
    object.__setattr__(handle._policy_snapshot.decisions[0], "canonical_composite_figi", _C)
    with pytest.raises(I3DispatchError, match="decision payload changed after mint"):
        _dispatch_i3_identity_window_from_verified_batch(
            verified_policy=handle,
            coverage=_coverage(observations),
            observations=observations,
        )

    handle = _verify_i3_identity_policy_snapshot_for_batch(
        _snapshot((IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION, record))
    )
    object.__setattr__(
        handle._policy_snapshot.policy_bundle,
        "bundle_available_session",
        date(2026, 8, 4),
    )
    with pytest.raises(I3DispatchError, match="bundle content changed after mint"):
        _dispatch_i3_identity_window_from_verified_batch(
            verified_policy=handle,
            coverage=_coverage(observations),
            observations=observations,
        )


@pytest.mark.parametrize(
    "overrides",
    (
        {"canonical_composite_figi": _B},
        {"canonical_composite_market_code": "GB"},
        {"observed_composite_market_code": "US"},
        {"observed_share_class_figi": None},
        {"identity_disposition": "confirmed_genuine_transition"},
    ),
)
def test_cross_market_fixture_decision_rejects_invalid_direction_or_scope(
    overrides: dict[str, object],
) -> None:
    decision_fields: dict[str, object] = {
        "observed_composite_figi": _B,
        "canonical_composite_figi": _A,
    }
    decision_fields.update(overrides)
    record = _decision_record(
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        source_record_id=_digest("invalid-cross-market-source"),
        label="invalid-cross-market",
        **decision_fields,
    )
    bundle, contents = _fixture_bundle_and_contents(
        (IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION, record)
    )

    with pytest.raises(I3DispatchError, match="cross-market adjudication"):
        load_fixture_identity_policy_snapshot(
            bundle,
            registry_release_contents=contents,
        )


def test_approved_cross_market_dispatch_exposes_decision_availability() -> None:
    target_source = _digest("approved-cross-target")
    record = _decision_record(
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        source_record_id=target_source,
        label="approved-cross-market",
        observed_composite_figi=_B,
        canonical_composite_figi=_A,
    )
    observations = (
        _observation(_SESSIONS[-3], _B, "GB", source_label="approved-cross-left"),
        _observation(_SESSIONS[-2], _B, "GB", source_label="approved-cross-middle"),
        _observation(_SESSIONS[-1], _B, "GB", source_label="approved-cross-target"),
    )
    result = dispatch_i3_identity_window(
        policy_snapshot=_snapshot(
            (IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION, record)
        ),
        coverage=_coverage(observations),
        observations=observations,
    )

    assert result.decision.canonical_composite_figi == _A
    assert result.decision.backtest_identity_eligible is True
    assert result.decision.selected_decision_available_session == date(2026, 7, 30)
    assert target_source == result.decision.source_record_id


def test_genuine_identity_adjudication_derives_closed_method_and_disposition() -> None:
    target_source = _digest("approved-genuine-target")
    record = _decision_record(
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        source_record_id=target_source,
        label="approved-genuine-transition",
        observed_composite_figi=_B,
        canonical_composite_figi=_B,
        identity_disposition="confirmed_genuine_transition",
    )
    observations = (
        _observation(_SESSIONS[-3], _A, "US", source_label="approved-genuine-left"),
        _observation(_SESSIONS[-2], _A, "US", source_label="approved-genuine-middle"),
        _observation(_SESSIONS[-1], _B, "US", source_label="approved-genuine-target"),
    )
    result = dispatch_i3_identity_window(
        policy_snapshot=_snapshot((IdentityRegistryKind.IDENTITY_ADJUDICATION, record)),
        coverage=_coverage(observations),
        observations=observations,
    )

    assert result.decision.canonical_composite_figi == _B
    assert result.decision.identity_resolution_method == "approved_genuine_transition"
    assert result.decision.identity_disposition == "confirmed_genuine_transition"
    assert result.decision.backtest_identity_eligible is True


@pytest.mark.parametrize(
    ("disposition", "observed", "canonical"),
    (
        ("confirmed_genuine_transition", _A, _B),
        ("confirmed_provider_contamination", _A, _A),
    ),
)
def test_identity_adjudication_rejects_wrong_canonical_direction(
    disposition: str,
    observed: str,
    canonical: str,
) -> None:
    record = _decision_record(
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        source_record_id=_digest(f"wrong-direction-{disposition}"),
        label="wrong-direction",
        identity_disposition=disposition,
        observed_composite_figi=observed,
        canonical_composite_figi=canonical,
    )
    bundle, contents = _fixture_bundle_and_contents(
        (IdentityRegistryKind.IDENTITY_ADJUDICATION, record)
    )
    with pytest.raises(I3DispatchError, match="identity adjudication"):
        load_fixture_identity_policy_snapshot(bundle, registry_release_contents=contents)


@pytest.mark.parametrize(
    ("kind", "registry_disposition", "expected_method", "expected_disposition"),
    (
        (
            IdentityRegistryKind.IDENTITY_ADJUDICATION,
            "adjudicated_unresolved",
            "provider_figi_bounce_adjudicated_unresolved",
            "adjudicated_unresolved",
        ),
        (
            IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
            "cross_market_adjudicated_unresolved",
            "cross_market_composite_adjudicated_unresolved",
            "cross_market_adjudicated_unresolved",
        ),
    ),
)
def test_composite_adjudicated_unresolved_retains_lineage_without_canonical_fallback(
    kind: IdentityRegistryKind,
    registry_disposition: str,
    expected_method: str,
    expected_disposition: str,
) -> None:
    target_source = _digest(f"unresolved-{kind.value}")
    record = _decision_record(
        kind,
        source_record_id=target_source,
        label="unresolved-composite",
        identity_disposition=registry_disposition,
        observed_composite_figi=_B,
        canonical_composite_figi=None,
        canonical_composite_market_code=None,
    )
    country = "GB" if kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION else "US"
    observations = tuple(
        _observation(
            session,
            _B,
            country,
            source_label=(f"unresolved-{kind.value}" if session == _SESSIONS[-1] else str(session)),
        )
        for session in _SESSIONS[-3:]
    )
    result = dispatch_i3_identity_window(
        policy_snapshot=_snapshot((kind, record)),
        coverage=_coverage(observations),
        observations=observations,
    )
    assert result.decision.canonical_composite_figi is None
    assert result.decision.backtest_identity_eligible is False
    assert result.decision.identity_resolution_method == expected_method
    assert result.decision.identity_disposition == expected_disposition
    assert [item.decision_id for item in result.decision.decision_lineage] == [
        record["decision_id"]
    ]


def test_provider_and_share_unresolved_mappings_are_explicit_and_ineligible() -> None:
    target_source = _digest("provider-unresolved-target")
    transition_source = _digest("provider-transition-boundary-source")
    transition = _decision_record(
        IdentityRegistryKind.ASSET_TRANSITION,
        source_record_id=transition_source,
        label="provider-transition",
        predecessor_asset_id=canonical_asset_id(_B),
        successor_asset_id=canonical_asset_id(_A),
    )
    provider = _decision_record(
        IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE,
        source_record_id=target_source,
        label="provider-unresolved",
        identity_disposition="provider_composite_override_adjudicated_unresolved",
        observed_composite_figi=_B,
        canonical_composite_figi=None,
        canonical_composite_market_code=None,
        transition_relation_id=transition["decision_id"],
    )
    observations = tuple(
        _observation(
            session,
            _B,
            "US",
            source_label=(
                "provider-unresolved-target" if session == _SESSIONS[-1] else str(session)
            ),
        )
        for session in _SESSIONS[-3:]
    )
    provider_result = dispatch_i3_identity_window(
        policy_snapshot=_snapshot(
            (IdentityRegistryKind.ASSET_TRANSITION, transition),
            (IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE, provider),
        ),
        coverage=_coverage(observations),
        observations=observations,
    )
    assert provider_result.decision.backtest_identity_eligible is False
    assert provider_result.decision.identity_resolution_method == "adjudicated_unresolved"
    assert provider_result.decision.identity_disposition == "adjudicated_unresolved"
    assert provider_result.decision.asset_transition_decision_ids == (transition["decision_id"],)

    share_source = _digest("share-unresolved-target")
    share = _decision_record(
        IdentityRegistryKind.SHARE_CLASS_ADJUDICATION,
        source_record_id=share_source,
        label="share-unresolved",
        identity_disposition="share_class_adjudicated_unresolved",
        observed_composite_figi=_A,
        composite_scope_figi=_A,
        observed_share_class_figi=_SHARE,
        canonical_share_class_figi=None,
    )
    share_observations = tuple(
        _observation(
            session,
            _A,
            "US",
            source_label=("share-unresolved-target" if session == _SESSIONS[-1] else str(session)),
        )
        for session in _SESSIONS[-3:]
    )
    share_result = dispatch_i3_identity_window(
        policy_snapshot=_snapshot((IdentityRegistryKind.SHARE_CLASS_ADJUDICATION, share)),
        coverage=_coverage(share_observations),
        observations=share_observations,
    )
    assert share_result.decision.backtest_identity_eligible is False
    assert share_result.decision.canonical_composite_figi is None
    assert share_result.decision.canonical_share_class_figi is None
    assert share_result.decision.identity_disposition == "adjudicated_unresolved"
    assert share_result.decision.share_class_decision_ids == (share["decision_id"],)


@pytest.mark.parametrize(
    ("disposition", "successor", "eligible"),
    (
        ("confirmed_genuine_transition", canonical_asset_id(_B), True),
        ("asset_transition_adjudicated_unresolved", None, True),
    ),
)
def test_asset_transition_is_lineage_only_for_confirmed_and_unresolved(
    disposition: str,
    successor: str | None,
    eligible: bool,
) -> None:
    target_source = _digest(f"transition-lineage-{disposition}")
    transition = _decision_record(
        IdentityRegistryKind.ASSET_TRANSITION,
        source_record_id=target_source,
        label="transition-lineage",
        identity_disposition=disposition,
        predecessor_asset_id=canonical_asset_id(_A),
        successor_asset_id=successor,
    )
    observations = tuple(
        _observation(
            session,
            _A,
            "US",
            source_label=(
                f"transition-lineage-{disposition}" if session == _SESSIONS[-1] else str(session)
            ),
        )
        for session in _SESSIONS[-3:]
    )
    result = dispatch_i3_identity_window(
        policy_snapshot=_snapshot((IdentityRegistryKind.ASSET_TRANSITION, transition)),
        coverage=_coverage(observations),
        observations=observations,
    )
    assert result.decision.backtest_identity_eligible is eligible
    assert result.decision.canonical_composite_figi == _A
    assert result.decision.identity_disposition == "observed_consistent"
    assert result.decision.asset_transition_decision_ids == (transition["decision_id"],)


def test_fixture_registry_loader_fails_closed_on_exact_pin_mismatch() -> None:
    bundle, contents = _fixture_bundle_and_contents()
    damaged = bytearray(contents[0])
    damaged[-1] = ord("]")

    with pytest.raises(I3DispatchError, match="SHA differs from exact pin"):
        load_fixture_identity_policy_snapshot(
            bundle,
            registry_release_contents=(bytes(damaged), *contents[1:]),
        )


def test_fixture_registry_loader_rejects_noncanonical_json_even_when_exactly_pinned() -> None:
    bundle, contents = _fixture_bundle_and_contents()
    pretty_content = json.dumps(json.loads(contents[0]), indent=2).encode("utf-8")
    releases = list(bundle.registry_releases)
    releases[0] = replace(
        releases[0],
        artifact=ArtifactPin(
            path=releases[0].artifact.path,
            sha256=hashlib.sha256(pretty_content).hexdigest(),
            bytes=len(pretty_content),
        ),
    )
    changed_bundle = IdentityPolicyBundle(
        registry_releases=tuple(releases),
        bundle_available_session=bundle.bundle_available_session,
    )

    with pytest.raises(I3DispatchError, match="strict canonical JSON"):
        load_fixture_identity_policy_snapshot(
            changed_bundle,
            registry_release_contents=(pretty_content, *contents[1:]),
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("schema_version", 3, "schema version differs"),
        (
            "registry_kind",
            IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION.value,
            "registry kind differs",
        ),
    ),
)
def test_fixture_registry_loader_fails_closed_on_schema_or_registry_mismatch(
    field: str,
    value: object,
    message: str,
) -> None:
    bundle, contents = _fixture_bundle_and_contents()
    document = json.loads(contents[0])
    document[field] = value
    if field == "registry_kind":
        document["decisions"] = [{"fixture": "keeps-mismatched-release-id-unique"}]
    changed_bundle, changed_contents = _repin_release_document(
        bundle,
        contents,
        index=0,
        document=document,
    )

    with pytest.raises(I3DispatchError, match=message):
        load_fixture_identity_policy_snapshot(
            changed_bundle,
            registry_release_contents=changed_contents,
        )


def test_fixture_registry_loader_fails_closed_when_release_id_does_not_reproduce() -> None:
    bundle, contents = _fixture_bundle_and_contents()
    document = json.loads(contents[0])
    document["release_id"] = _digest("forged-release-id")
    changed_bundle, changed_contents = _repin_release_document(
        bundle,
        contents,
        index=0,
        document=document,
        reproduce_release_id=False,
    )

    with pytest.raises(I3DispatchError, match="release ID does not reproduce"):
        load_fixture_identity_policy_snapshot(
            changed_bundle,
            registry_release_contents=changed_contents,
        )


def test_fixture_registry_loader_rejects_decision_after_release_availability() -> None:
    record = _decision_record(
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        source_record_id=_digest("late-decision-source"),
        label="late-decision",
        decision_available_session="2026-07-31",
        observed_composite_figi=_B,
        canonical_composite_figi=_A,
    )
    bundle, contents = _fixture_bundle_and_contents(
        (IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION, record)
    )

    with pytest.raises(I3DispatchError, match="exceeds its release availability"):
        load_fixture_identity_policy_snapshot(
            bundle,
            registry_release_contents=contents,
        )


def test_a_b_a_is_reported_without_changing_the_correct_target_identity() -> None:
    observations = (
        _observation(_SESSIONS[-3], _A, "US", source_label="aba-a1"),
        _observation(_SESSIONS[-2], _B, "GB", source_label="aba-b"),
        _observation(_SESSIONS[-1], _A, "US", source_label="aba-a2"),
    )
    snapshot = _snapshot()
    coverage = _coverage(observations)

    result = dispatch_i3_identity_window(
        policy_snapshot=snapshot,
        coverage=coverage,
        observations=observations,
    )

    assert result.decision.canonical_composite_figi == _A
    assert result.decision.backtest_identity_eligible is True
    bounce = _qa(result, "suspected_provider_figi_bounce_rows")
    assert (bounce.observed_count, bounce.failure_count) == (1, 1)
    _assert_typed_raw_review(bounce)
    assert (
        verify_i3_local_staging_attestation(
            result,
            policy_snapshot=snapshot,
            coverage=coverage,
            observations=observations,
        )
        is result
    )


def test_foreign_us_foreign_inverse_is_never_called_a_genuine_transition() -> None:
    observations = (
        _observation(_SESSIONS[-3], _B, "GB", source_label="inverse-b1"),
        _observation(_SESSIONS[-2], _A, "US", source_label="inverse-a"),
        _observation(_SESSIONS[-1], _B, "GB", source_label="inverse-b2"),
    )

    result = dispatch_i3_identity_window(
        policy_snapshot=_snapshot(),
        coverage=_coverage(observations),
        observations=observations,
    )

    assert result.decision.backtest_identity_eligible is False
    assert "inverse_bounce_detected_not_transition" in result.decision.reason_codes
    inverse = _qa(result, "inverse_bounce_misclassified_as_genuine_transition_rows")
    assert (inverse.observed_count, inverse.failure_count) == (1, 0)
    assert "genuine_transition" not in result.decision.identity_resolution_status


def test_inverse_bounce_middle_genuine_decision_is_a_critical_failure() -> None:
    middle_source = _digest("inverse-approved-middle")
    middle_transition = _decision_record(
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        source_record_id=middle_source,
        label="inverse-approved-middle",
        effective_from_session=_SESSIONS[-2].isoformat(),
        effective_to_session=_SESSIONS[-2].isoformat(),
        identity_disposition="confirmed_genuine_transition",
        observed_composite_figi=_A,
        canonical_composite_figi=_A,
    )
    observations = (
        _observation(_SESSIONS[-3], _B, "GB", source_label="inverse-approved-left"),
        _observation(
            _SESSIONS[-2],
            _A,
            "US",
            source_label="inverse-approved-middle",
        ),
        _observation(_SESSIONS[-1], _B, "GB", source_label="inverse-approved-right"),
    )

    with pytest.raises(I3DispatchError, match="Critical QA failure"):
        dispatch_i3_identity_window(
            policy_snapshot=_snapshot(
                (IdentityRegistryKind.IDENTITY_ADJUDICATION, middle_transition)
            ),
            coverage=_coverage(observations),
            observations=observations,
        )


def test_same_market_unapproved_a_b_a_requires_correction_instead_of_clean_append() -> None:
    observations = (
        _observation(_SESSIONS[-3], _A, "US", source_label="same-market-a1"),
        _observation(_SESSIONS[-2], _B, "US", source_label="same-market-b"),
        _observation(_SESSIONS[-1], _A, "US", source_label="same-market-a2"),
    )

    with pytest.raises(I3DispatchError, match="Critical QA failure"):
        dispatch_i3_identity_window(
            policy_snapshot=_snapshot(),
            coverage=_coverage(observations),
            observations=observations,
        )


def test_long_lived_foreign_composite_is_caught_without_a_bounce() -> None:
    observations = tuple(
        _observation(session, _B, "GB", source_label=f"long-foreign-{session}")
        for session in _SESSIONS[-3:]
    )

    result = dispatch_i3_identity_window(
        policy_snapshot=_snapshot(),
        coverage=_coverage(observations),
        observations=observations,
    )

    assert result.decision.active_on_date is True
    assert result.decision.membership_preserved is True
    assert result.decision.backtest_identity_eligible is False
    assert _qa(result, "suspected_provider_figi_bounce_rows").observed_count == 0
    foreign = _qa(result, "us_locale_non_us_composite_figi_rows")
    assert (foreign.observed_count, foreign.failure_count) == (1, 1)
    _assert_typed_raw_review(foreign)


def test_composite_registry_collision_preserves_membership_but_emits_no_alias() -> None:
    source_record_id = _digest(f"collision-{_SESSIONS[-1]}")
    cross_market = _decision_record(
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        source_record_id=source_record_id,
        label="collision-cross-market",
        observed_composite_figi=_B,
        canonical_composite_figi=_A,
    )
    bounce_override = _decision_record(
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        source_record_id=source_record_id,
        label="collision-bounce",
        observed_composite_figi=_B,
        canonical_composite_figi=_C,
    )
    snapshot = _snapshot(
        (IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION, cross_market),
        (IdentityRegistryKind.IDENTITY_ADJUDICATION, bounce_override),
    )
    observations = tuple(
        _observation(session, _B, "GB", source_label=f"collision-{session}")
        for session in _SESSIONS[-3:]
    )

    result = dispatch_i3_identity_window(
        policy_snapshot=snapshot,
        coverage=_coverage(observations),
        observations=observations,
    )

    assert result.decision.active_on_date is True
    assert result.decision.membership_preserved is True
    assert result.decision.composite_registry_collision is True
    assert result.decision.canonical_composite_figi is None
    assert result.decision.backtest_identity_eligible is False
    assert result.decision.alias_permitted is False
    raw = _qa(result, "multi_registry_composite_override_collision_rows")
    assert (raw.observed_count, raw.failure_count) == (1, 0)
    _assert_typed_raw_review(raw)
    for suffix in ("eligible_rows", "resolved_rows", "alias_rows"):
        assert (
            _qa(result, f"multi_registry_composite_override_collision_{suffix}").failure_count == 0
        )


def test_missing_or_gapped_exact_lookback_fails_closed() -> None:
    calendar = freeze_exact_trading_calendar(
        _SESSIONS,
        artifact_path="manifests/calendars/gap-fixture.json",
    )
    two_slots = tuple(
        SourceCoverageSlot(
            session_date=session,
            partition_receipt_id=_digest(f"short-{session}"),
            source_record_ids=(),
        )
        for session in _SESSIONS[-2:]
    )
    with pytest.raises(I3DispatchError, match="exactly three"):
        bind_alias_source_coverage(
            calendar,
            provider_id="massive",
            provider_market="stocks",
            provider_locale="us",
            ticker="TEST",
            target_session=_SESSIONS[-1],
            slots=two_slots,
            coverage_available_session=date(2026, 8, 5),
        )

    gapped_slots = tuple(
        SourceCoverageSlot(
            session_date=session,
            partition_receipt_id=_digest(f"gap-{session}"),
            source_record_ids=(),
        )
        for session in (_SESSIONS[-4], _SESSIONS[-2], _SESSIONS[-1])
    )
    with pytest.raises(I3DispatchError, match="missing or gapped"):
        bind_alias_source_coverage(
            calendar,
            provider_id="massive",
            provider_market="stocks",
            provider_locale="us",
            ticker="TEST",
            target_session=_SESSIONS[-1],
            slots=gapped_slots,
            coverage_available_session=date(2026, 8, 5),
        )

    observations = tuple(
        _observation(session, _A, "US", source_label=f"missing-{session}")
        for session in _SESSIONS[-3:]
    )
    coverage = _coverage(observations)
    with pytest.raises(I3DispatchError, match="differ from exact source coverage"):
        dispatch_i3_identity_window(
            policy_snapshot=_snapshot(),
            coverage=coverage,
            observations=observations[:-1],
        )


def test_forged_proof_or_qa_semantics_is_rejected_and_grants_no_power() -> None:
    observations = tuple(
        _observation(session, _A, "US", source_label=f"forge-{session}")
        for session in _SESSIONS[-3:]
    )
    snapshot = _snapshot()
    coverage = _coverage(observations)
    result = dispatch_i3_identity_window(
        policy_snapshot=snapshot,
        coverage=coverage,
        observations=observations,
    )

    assert result.row_proof.validator_semantics_digest == I3_ROW_VALIDATOR_SEMANTICS_DIGEST
    assert result.qa_receipt.qa_catalog_digest == I3_QA_CATALOG_DIGEST
    assert not any(
        (
            result.allows_publish,
            result.allows_correction,
            result.allows_partition_replacement,
            result.allows_registry_mutation,
            result.allows_base_cutover,
        )
    )
    assert "validator" not in inspect.signature(dispatch_i3_identity_window).parameters
    assert "callback" not in inspect.signature(dispatch_i3_identity_window).parameters

    object.__setattr__(result.row_proof, "validator_semantics_digest", _digest("forged"))
    with pytest.raises(I3DispatchError, match="does not reproduce"):
        verify_i3_local_staging_attestation(
            result,
            policy_snapshot=snapshot,
            coverage=coverage,
            observations=observations,
        )

    fresh = dispatch_i3_identity_window(
        policy_snapshot=snapshot,
        coverage=coverage,
        observations=observations,
    )
    object.__setattr__(fresh.qa_receipt, "qa_catalog_digest", _digest("forged-qa"))
    with pytest.raises(I3DispatchError, match="does not reproduce"):
        verify_i3_local_staging_attestation(
            fresh,
            policy_snapshot=snapshot,
            coverage=coverage,
            observations=observations,
        )
