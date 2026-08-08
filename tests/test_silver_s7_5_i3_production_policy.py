from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.identity_registry_workflow import (
    ExactSourceRow,
    ExactSourceScope,
    LoadedRegistryRelease,
    LoadedRegistryReleaseSet,
    RegistryReleasePin,
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
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    IDENTITY_REGISTRY_ORDER,
    IdentityPolicyBundle,
    IdentityRegistryKind,
    IdentityRegistryReleasePin,
)
from ame_stocks_api.silver.incremental_i3_dispatch import (
    I3_PRODUCTION_POLICY_SOURCE,
    IdentityObservation,
    SourceCoverageSlot,
    bind_alias_source_coverage,
    dispatch_i3_identity_window,
    freeze_exact_trading_calendar,
)
from ame_stocks_api.silver.incremental_i3_production_policy import (
    I3ProductionPolicyError,
    load_production_identity_policy_snapshot,
)
from ame_stocks_api.silver.incremental_i3_runner import (
    _validate_selected_decision_availability,
)

_PREDECESSOR = "BBG000KMY6N2"
_SUCCESSOR = "BBG01RK6N4M5"
_AVAILABLE = date(2026, 7, 20)
_S4_RELEASE = stable_digest({"production-policy-test": "s4-release"})


def _digest(label: str) -> str:
    return stable_digest({"production-policy-test": label})


def _source_row(
    label: str,
    session: date,
    *,
    ticker: str = "SOR",
    composite: str = _PREDECESSOR,
    share_class: str | None = None,
) -> ExactSourceRow:
    return ExactSourceRow(
        session_date=session,
        source_record_id=_digest(label),
        source_dataset="universe_source_daily",
        source_s4_release_set_id=_S4_RELEASE,
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker=ticker,
        observed_composite_figi=composite,
        observed_share_class_figi=share_class,
        primary_exchange_mic="XNYS",
    )


def _common_controls() -> dict[str, object]:
    return {
        "provider_id": "massive",
        "provider_market": "stocks",
        "provider_locale": "us",
        "source_s4_release_set_id": _S4_RELEASE,
        "source_exact_group_candidate_manifest_id": _digest("exact-group-candidate"),
        "source_exact_group_candidate_manifest_sha256": _digest("exact-group-candidate-sha"),
        "candidate_available_session": date(2026, 7, 17),
        "source_external_evidence_manifest_id": _digest("external-evidence"),
        "source_external_evidence_manifest_sha256": _digest("external-evidence-sha"),
        "external_evidence_available_session": date(2026, 7, 17),
        "source_decision_plan_id": _digest("decision-plan"),
        "source_decision_plan_path": "controls/production-policy-test-plan.json",
        "source_decision_plan_sha256": _digest("decision-plan-sha"),
        "approval_request_event_id": _digest("approval-request"),
        "approval_request_event_sha256": _digest("approval-request-sha"),
        "approval_receipt_id": _digest("approval-receipt"),
        "approval_receipt_sha256": _digest("approval-receipt-sha"),
        "approved_by": "production-policy-test-reviewer",
        "approved_at_utc": datetime(2026, 7, 17, 12, tzinfo=UTC),
        "approval_available_session": _AVAILABLE,
        "availability_calendar_id": _digest("calendar"),
        "availability_calendar_sha256": _digest("calendar-sha"),
    }


def _sor_rows() -> tuple[
    dict[str, object],
    ExactSourceScope,
    dict[str, object],
    ExactSourceScope,
]:
    boundary = _source_row("sor-boundary-2025-01-02", date(2025, 1, 2))
    transition_scope = ExactSourceScope(
        rows=(
            _source_row("sor-predecessor-2024-12-31", date(2024, 12, 31)),
            boundary,
        )
    )
    transition = AssetTransitionDecision(
        **_common_controls(),
        observed_ticker="SOR",
        transition_type=AssetTransitionType.CORPORATE_REORGANIZATION_SUCCESSOR_SECURITY,
        legal_effective_date=date(2025, 1, 1),
        predecessor_last_session=date(2024, 12, 31),
        successor_first_session=date(2025, 1, 2),
        predecessor_composite_figi=_PREDECESSOR,
        successor_composite_figi=_SUCCESSOR,
        boundary_source_record_ids=transition_scope.source_record_ids,
        disposition=AssetTransitionDisposition.CONFIRMED_GENUINE_TRANSITION,
        decision_version=1,
        supersedes_asset_transition_id=None,
        reason_code="source_capital_reorganization",
        reason_detail="Official evidence establishes a successor-security boundary.",
    )
    override_scope = ExactSourceScope(
        rows=(
            boundary,
            _source_row("sor-stale-2026-07-09", date(2026, 7, 9)),
        )
    )
    override = ProviderCompositeOverrideDecision(
        **_common_controls(),
        observed_ticker="SOR",
        observed_composite_figi=_PREDECESSOR,
        canonical_composite_figi=_SUCCESSOR,
        observed_composite_market_code="US",
        canonical_composite_market_code="US",
        valid_from_session=date(2025, 1, 2),
        valid_through_session=date(2026, 7, 9),
        scoped_source_record_ids=override_scope.source_record_ids,
        asset_transition_series_id=transition.asset_transition_series_id,
        asset_transition_id=transition.asset_transition_id,
        asset_transition_available_session=transition.transition_available_session,
        disposition=ProviderCompositeOverrideDisposition.CONFIRMED_STALE_AFTER_TRANSITION,
        decision_version=1,
        supersedes_provider_composite_override_id=None,
        reason_code="same_market_stale_after_transition",
        reason_detail="Provider retained the predecessor Composite after the boundary.",
    )
    return (
        transition.to_registry_row(),
        transition_scope,
        override.to_registry_row(),
        override_scope,
    )


def test_production_policy_accepts_parquet_timestamp_subclass() -> None:
    transition_row, transition_scope, override_row, override_scope = _sor_rows()
    override_row = dict(override_row)
    override_row["approved_at_utc"] = pd.Timestamp("2026-07-29T07:40:36.158368Z")
    loaded, bundle = _release_set_and_bundle(
        {
            IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE: ((override_row, override_scope),),
            IdentityRegistryKind.ASSET_TRANSITION: ((transition_row, transition_scope),),
        }
    )

    snapshot = load_production_identity_policy_snapshot(loaded, bundle)

    selected = snapshot.decision_by_id(str(override_row["provider_composite_override_id"]))
    assert selected.production_registry_row is not None
    assert selected.production_registry_row["approved_at_utc"] == (
        "2026-07-29T07:40:36.158368+00:00"
    )


def _loaded_release(
    kind: IdentityRegistryKind,
    decisions: tuple[tuple[dict[str, object], ExactSourceScope], ...] = (),
) -> LoadedRegistryRelease:
    release_id = _digest(f"{kind.value}-release")
    path = (
        "manifests/silver/identity/registry-releases/"
        f"registry={kind.value}/release_id={release_id}/manifest.json"
    )
    content = f"{kind.value}:{release_id}\n".encode()
    pin = RegistryReleasePin(
        registry_name=kind.value,
        release_id=release_id,
        manifest_path=path,
        manifest_sha256=hashlib.sha256(content).hexdigest(),
        manifest_bytes=len(content),
        release_available_session=_AVAILABLE,
    )
    id_column = {
        IdentityRegistryKind.IDENTITY_ADJUDICATION: "identity_adjudication_id",
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION: ("cross_market_adjudication_id"),
        IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE: ("provider_composite_override_id"),
        IdentityRegistryKind.SHARE_CLASS_ADJUDICATION: "share_class_adjudication_id",
        IdentityRegistryKind.ASSET_TRANSITION: "asset_transition_id",
    }[kind]
    rows = {str(row[id_column]): row for row, _scope in decisions}
    scopes = {str(row[id_column]): scope for row, scope in decisions}
    manifest = SimpleNamespace(
        registry_name=kind.value,
        release_id=release_id,
        release_available_session=_AVAILABLE,
        decisions=tuple(SimpleNamespace(decision_id=decision_id) for decision_id in sorted(rows)),
    )
    return LoadedRegistryRelease(
        manifest=manifest,
        manifest_pin=pin,
        candidate=SimpleNamespace(),
        plan=SimpleNamespace(),
        request=SimpleNamespace(),
        approval_receipt=SimpleNamespace(),
        decision_rows=rows,
        source_scopes=scopes,
    )


def _release_set_and_bundle(
    by_kind: dict[
        IdentityRegistryKind,
        tuple[tuple[dict[str, object], ExactSourceScope], ...],
    ],
) -> tuple[LoadedRegistryReleaseSet, IdentityPolicyBundle]:
    releases = tuple(
        _loaded_release(kind, by_kind.get(kind, ())) for kind in IDENTITY_REGISTRY_ORDER
    )
    loaded = LoadedRegistryReleaseSet(releases)
    bundle = IdentityPolicyBundle(
        registry_releases=tuple(
            IdentityRegistryReleasePin(
                registry_kind=kind,
                release_id=release.release_id,
                artifact=ArtifactPin(
                    path=release.manifest_pin.manifest_path,
                    sha256=release.manifest_pin.manifest_sha256,
                    bytes=release.manifest_pin.manifest_bytes,
                ),
                decision_cutoff_session=_AVAILABLE,
                release_available_session=_AVAILABLE,
            )
            for kind, release in zip(IDENTITY_REGISTRY_ORDER, releases, strict=True)
        ),
        bundle_available_session=_AVAILABLE,
    )
    return loaded, bundle


def _coverage_for_ticker(
    observations: tuple[IdentityObservation, ...],
    *,
    ticker: str,
):
    sessions = tuple(item.session_date for item in observations)
    calendar = freeze_exact_trading_calendar(
        sessions,
        artifact_path="manifests/calendars/production-policy-test.json",
    )
    return bind_alias_source_coverage(
        calendar,
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker=ticker,
        target_session=sessions[-1],
        slots=tuple(
            SourceCoverageSlot(
                session_date=item.session_date,
                partition_receipt_id=_digest(f"partition-{item.session_date}"),
                source_record_ids=(item.source_record_id,),
            )
            for item in observations
        ),
        coverage_available_session=_AVAILABLE,
    )


def test_production_adapter_preserves_real_ids_full_scope_and_sor_boundary_semantics() -> None:
    transition_row, transition_scope, override_row, override_scope = _sor_rows()
    loaded, bundle = _release_set_and_bundle(
        {
            IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE: ((override_row, override_scope),),
            IdentityRegistryKind.ASSET_TRANSITION: ((transition_row, transition_scope),),
        }
    )

    snapshot = load_production_identity_policy_snapshot(loaded, bundle)
    override_id = str(override_row["provider_composite_override_id"])
    transition_id = str(transition_row["asset_transition_id"])
    override = snapshot.decision_by_id(override_id)
    transition = snapshot.decision_by_id(transition_id)

    assert snapshot.policy_source == I3_PRODUCTION_POLICY_SOURCE
    assert snapshot.production_release_set_binding_digest is not None
    assert override.decision_id == override_id
    assert transition.decision_id == transition_id
    assert override.source_record_ids == override_scope.source_record_ids
    assert tuple(item.to_dict() for item in override.source_scope) == tuple(
        item.to_dict() for item in override_scope.rows
    )
    assert transition.effective_to_session == date(2025, 1, 2)
    assert override.effective_to_session == date(2026, 7, 9)

    observations = (
        IdentityObservation(
            provider_id="massive",
            provider_market="stocks",
            provider_locale="us",
            ticker="SOR",
            session_date=date(2026, 7, 7),
            observed_composite_figi=_PREDECESSOR,
            observed_composite_country="US",
            observed_share_class_figi=None,
            primary_exchange="XNYS",
            source_record_id=_digest("sor-unscoped-2026-07-07"),
            active_on_date=True,
        ),
        IdentityObservation(
            provider_id="massive",
            provider_market="stocks",
            provider_locale="us",
            ticker="SOR",
            session_date=date(2026, 7, 8),
            observed_composite_figi=_PREDECESSOR,
            observed_composite_country="US",
            observed_share_class_figi=None,
            primary_exchange="XNYS",
            source_record_id=_digest("sor-unscoped-2026-07-08"),
            active_on_date=True,
        ),
        IdentityObservation(
            provider_id="massive",
            provider_market="stocks",
            provider_locale="us",
            ticker="SOR",
            session_date=date(2026, 7, 9),
            observed_composite_figi=_PREDECESSOR,
            observed_composite_country="US",
            observed_share_class_figi=None,
            primary_exchange="XNYS",
            source_record_id=_digest("sor-stale-2026-07-09"),
            active_on_date=True,
        ),
    )
    result = dispatch_i3_identity_window(
        policy_snapshot=snapshot,
        coverage=_coverage_for_ticker(observations, ticker="SOR"),
        observations=observations,
    )
    assert result.decision.canonical_composite_figi == _SUCCESSOR
    assert result.decision.composite_registry_decision_ids == (override_id,)
    assert result.decision.asset_transition_decision_ids == (transition_id,)
    assert result.decision.backtest_identity_eligible is True

    selected_available, lineage = _validate_selected_decision_availability(
        {
            "membership_source_available_session": date(2026, 7, 9),
            "identity_adjudication_id": None,
            "adjudication_available_session": None,
            "cross_market_adjudication_id": None,
            "cross_market_adjudication_available_session": None,
            "provider_composite_override_id": override_id,
            "provider_composite_override_available_session": _AVAILABLE,
            "share_class_adjudication_id": None,
            "share_class_adjudication_available_session": None,
            "asset_transition_ids": [transition_id],
            "session_date": date(2026, 7, 9),
            "ticker": "SOR",
            "selected_source_record_id": _digest("sor-stale-2026-07-09"),
            "identity_evidence_available_session": _AVAILABLE,
            "identity_resolution_method": "approved_provider_composite_override",
            "identity_disposition": "provider_composite_stale_after_transition",
            "backtest_identity_eligible": True,
        },
        policy_snapshot=snapshot,
        output_available_session=_AVAILABLE,
    )
    assert selected_available == _AVAILABLE
    assert tuple(item[1] for item in lineage) == (override_id, transition_id)


def test_production_adapter_rejects_composite_registry_scope_collision() -> None:
    transition_row, transition_scope, override_row, override_scope = _sor_rows()
    target = override_scope.rows[-1]
    collision_scope = ExactSourceScope(rows=(target,))
    identity_id = _digest("colliding-identity-adjudication")
    identity_row: dict[str, object] = {
        "identity_adjudication_id": identity_id,
        "adjudication_series_id": _digest("colliding-identity-series"),
        "decision_version": 1,
        "observed_ticker": "SOR",
        "observed_composite_figi": _PREDECESSOR,
        "canonical_composite_figi": _SUCCESSOR,
        "disposition": "confirmed_provider_contamination",
        "episode_valid_from_session": target.session_date,
        "episode_valid_through_session": target.session_date,
        "episode_source_record_count": 1,
        "episode_source_record_set_digest": collision_scope.source_record_set_digest,
        "source_s4_release_set_id": _S4_RELEASE,
        "adjudication_available_session": _AVAILABLE,
    }
    loaded, bundle = _release_set_and_bundle(
        {
            IdentityRegistryKind.IDENTITY_ADJUDICATION: ((identity_row, collision_scope),),
            IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE: ((override_row, override_scope),),
            IdentityRegistryKind.ASSET_TRANSITION: ((transition_row, transition_scope),),
        }
    )

    with pytest.raises(I3ProductionPolicyError, match="scope collision"):
        load_production_identity_policy_snapshot(loaded, bundle)


def test_production_share_only_correction_keeps_direct_composite_semantics() -> None:
    composite = "BBG01XL8FHT0"
    observed_share = "BBG01XL8FJS7"
    canonical_share = "BBG01227MF17"
    target = _source_row(
        "xzo-share-2025-11-04",
        date(2025, 11, 4),
        ticker="XZO",
        composite=composite,
        share_class=observed_share,
    )
    scope = ExactSourceScope(rows=(target,))
    decision = ShareClassAdjudicationDecision(
        **_common_controls(),
        observed_ticker="XZO",
        observed_composite_figi=composite,
        required_unique_canonical_composite_figi=composite,
        observed_share_class_figi=observed_share,
        canonical_share_class_figi=canonical_share,
        valid_from_session=target.session_date,
        valid_through_session=target.session_date,
        scoped_source_record_ids=scope.source_record_ids,
        disposition=ShareClassAdjudicationDisposition.CONFIRMED_CORRECTION,
        decision_version=1,
        supersedes_share_class_adjudication_id=None,
        reason_code="frozen_share_class_correction",
        reason_detail="Exact hierarchy evidence supports the Share Class correction.",
    )
    row = decision.to_registry_row()
    loaded, bundle = _release_set_and_bundle(
        {IdentityRegistryKind.SHARE_CLASS_ADJUDICATION: ((row, scope),)}
    )
    snapshot = load_production_identity_policy_snapshot(loaded, bundle)
    observations = tuple(
        IdentityObservation(
            provider_id="massive",
            provider_market="stocks",
            provider_locale="us",
            ticker="XZO",
            session_date=session,
            observed_composite_figi=composite,
            observed_composite_country="US",
            observed_share_class_figi=observed_share,
            primary_exchange="XNYS",
            source_record_id=(
                target.source_record_id
                if session == target.session_date
                else _digest(f"xzo-unscoped-{session}")
            ),
            active_on_date=True,
        )
        for session in (date(2025, 10, 31), date(2025, 11, 3), target.session_date)
    )
    result = dispatch_i3_identity_window(
        policy_snapshot=snapshot,
        coverage=_coverage_for_ticker(observations, ticker="XZO"),
        observations=observations,
    )

    assert result.decision.canonical_composite_figi == composite
    assert result.decision.canonical_share_class_figi == canonical_share
    assert result.decision.share_class_decision_ids == (decision.share_class_adjudication_id,)
    assert result.decision.identity_resolution_method == "source_composite_figi_exact"
    assert result.decision.identity_disposition == "observed_consistent"
