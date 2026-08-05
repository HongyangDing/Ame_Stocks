from __future__ import annotations

import hashlib
import json
from datetime import date

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import ArrowType
from ame_stocks_api.silver.identity_resolution_contract import S7_DERIVED_CONTRACTS
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    IDENTITY_REGISTRY_ORDER,
    LEGACY_S7_V1_RELEASE_SET_ID,
    S4_TERMINAL_TABLE_ORDER,
    I3CheckpointError,
    IdentityPolicyBundle,
    IdentityRegistryKind,
    IdentityRegistryReleasePin,
    S4TerminalPartitionPin,
)
from ame_stocks_api.silver.incremental_i3_dispatch import (
    I3_FIXTURE_REGISTRY_DECISION_NAMESPACE,
    I3_FIXTURE_REGISTRY_RELEASE_NAMESPACE,
    I3_FIXTURE_REGISTRY_RELEASE_RULE_VERSION,
    I3_FIXTURE_REGISTRY_RELEASE_SCHEMA_VERSION,
    I3_QA_CATALOG,
    I3_QA_CATALOG_DIGEST,
    IdentityPolicySnapshot,
    load_fixture_identity_policy_snapshot,
)
from ame_stocks_api.silver.incremental_i3_runner import (
    FIXED_BOUNDARY_LOOKBACK_SESSIONS,
    I3FixtureRunnerError,
    I3FixtureS4WindowBinding,
    bootstrap_native_v2_fixture,
    bootstrap_native_v2_fixture_history,
    run_i3_fixture_session,
)
from ame_stocks_api.silver.incremental_identity import (
    canonical_asset_id,
    canonical_issuer_id,
    canonical_share_class_id,
)

CALENDAR = tuple(
    date(2026, 1, day)
    for day in (
        2,
        5,
        6,
        7,
        8,
        9,
    )
)
POLICY_CUTOFF = date(2026, 1, 31)
MEMBER_AVAILABLE = date(2026, 2, 1)
BUNDLE_AVAILABLE = date(2026, 2, 2)
RUN_AVAILABLE = date(2026, 2, 3)
COMPOSITE = "BBG000B9XRY4"
SHARE = "BBG001S5N8V8"
CIK = "0000320193"


def _digest(label: str) -> str:
    return stable_digest({"fixture": label})


def _pin(label: str) -> ArtifactPin:
    content = label.encode()
    return ArtifactPin(
        path=f"fixtures/i3-runner/{label}.json",
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _snapshot(
    *records: tuple[IdentityRegistryKind, dict[str, object]],
) -> IdentityPolicySnapshot:
    by_kind = {kind: [] for kind in IDENTITY_REGISTRY_ORDER}
    for kind, record in records:
        by_kind[kind].append(record)
    pins = []
    contents = []
    for index, kind in enumerate(IDENTITY_REGISTRY_ORDER, start=1):
        logical = {
            "decision_cutoff_session": POLICY_CUTOFF.isoformat(),
            "decisions": sorted(by_kind[kind], key=lambda item: str(item["decision_id"])),
            "namespace": I3_FIXTURE_REGISTRY_RELEASE_NAMESPACE,
            "registry_kind": kind.value,
            "release_available_session": MEMBER_AVAILABLE.isoformat(),
            "rule_version": I3_FIXTURE_REGISTRY_RELEASE_RULE_VERSION,
            "schema_version": I3_FIXTURE_REGISTRY_RELEASE_SCHEMA_VERSION,
            "scope": "local_fixture_only",
        }
        release_id = stable_digest(logical)
        content = _canonical_bytes({"release_id": release_id, **logical})
        pins.append(
            IdentityRegistryReleasePin(
                registry_kind=kind,
                release_id=release_id,
                artifact=ArtifactPin(
                    path=f"fixtures/i3-runner/registry-{index:02d}-{kind.value}.json",
                    sha256=hashlib.sha256(content).hexdigest(),
                    bytes=len(content),
                ),
                decision_cutoff_session=POLICY_CUTOFF,
                release_available_session=MEMBER_AVAILABLE,
            )
        )
        contents.append(content)
    bundle = IdentityPolicyBundle(tuple(pins), bundle_available_session=BUNDLE_AVAILABLE)
    return load_fixture_identity_policy_snapshot(bundle, registry_release_contents=tuple(contents))


def _decision_record(
    kind: IdentityRegistryKind,
    *,
    session: date,
    source_record_id: str,
    observed_composite_figi: str | None = None,
    canonical_composite_figi: str | None = None,
    composite_scope_figi: str | None = None,
    observed_share_class_figi: str | None = None,
    canonical_share_class_figi: str | None = None,
    transition_relation_id: str | None = None,
    predecessor_asset_id: str | None = None,
    successor_asset_id: str | None = None,
    identity_disposition: str | None = None,
) -> dict[str, object]:
    is_cross_market = kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION
    is_provider_override = kind is IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE
    is_share_class = kind is IdentityRegistryKind.SHARE_CLASS_ADJUDICATION
    unresolved_cross_market = identity_disposition == "cross_market_adjudicated_unresolved"
    unresolved_provider_override = (
        identity_disposition == "provider_composite_override_adjudicated_unresolved"
    )
    values = {
        "canonical_composite_figi": canonical_composite_figi,
        "canonical_composite_market_code": (
            "US"
            if (is_cross_market and not unresolved_cross_market)
            or (is_provider_override and not unresolved_provider_override)
            else None
        ),
        "canonical_share_class_figi": canonical_share_class_figi,
        "composite_scope_figi": composite_scope_figi,
        "decision_available_session": MEMBER_AVAILABLE.isoformat(),
        "effective_from_session": session.isoformat(),
        "effective_to_session": session.isoformat(),
        "identity_disposition": (
            identity_disposition
            if identity_disposition is not None
            else "confirmed_provider_contamination"
            if kind
            in {
                IdentityRegistryKind.IDENTITY_ADJUDICATION,
                IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
            }
            else "confirmed_provider_composite_stale_after_transition"
            if is_provider_override
            else "confirmed_share_class_correction"
            if is_share_class
            else "confirmed_genuine_transition"
            if kind is IdentityRegistryKind.ASSET_TRANSITION
            else None
        ),
        "observed_composite_figi": (
            composite_scope_figi
            if is_share_class and observed_composite_figi is None
            else observed_composite_figi
        ),
        "observed_composite_market_code": (
            "GB" if is_cross_market else "US" if is_provider_override else None
        ),
        "observed_share_class_figi": (
            observed_share_class_figi or SHARE if is_cross_market else observed_share_class_figi
        ),
        "predecessor_asset_id": predecessor_asset_id,
        "provider_id": "massive",
        "provider_locale": "us",
        "provider_market": "stocks",
        "source_record_id": source_record_id,
        "successor_asset_id": successor_asset_id,
        "ticker": "AAPL",
        "transition_relation_id": transition_relation_id,
    }
    decision_id = stable_digest(
        {
            "namespace": I3_FIXTURE_REGISTRY_DECISION_NAMESPACE,
            "registry_kind": kind.value,
            **values,
        }
    )
    return {"decision_id": decision_id, **values}


EMPTY_SNAPSHOT = _snapshot()


def _policy() -> IdentityPolicyBundle:
    return EMPTY_SNAPSHOT.policy_bundle


def _release_id(bundle: IdentityPolicyBundle, kind: IdentityRegistryKind) -> str:
    return next(item.release_id for item in bundle.registry_releases if item.registry_kind is kind)


def _s4_binding(*sessions: date) -> I3FixtureS4WindowBinding:
    pins = []
    for session in sessions:
        for table in S4_TERMINAL_TABLE_ORDER:
            label = f"s4-{table}-{session.isoformat()}"
            artifact = _pin(label)
            pins.append(
                S4TerminalPartitionPin(
                    table_name=table,
                    session_date=session,
                    partition_receipt_id=stable_digest(
                        {
                            "artifact": artifact.to_dict(),
                            "session_date": session.isoformat(),
                            "table_name": table,
                        }
                    ),
                    artifact=artifact,
                    availability_session=RUN_AVAILABLE,
                )
            )
    return I3FixtureS4WindowBinding(tuple(sessions), tuple(pins))


def _default_for_type(kind: ArrowType, session: date) -> object:
    if kind in {ArrowType.STRING, ArrowType.JSON_STRING}:
        return "fixture_value"
    if kind is ArrowType.LIST_STRING:
        return []
    if kind is ArrowType.BOOLEAN:
        return False
    if kind is ArrowType.INT64:
        return 0
    if kind is ArrowType.FLOAT64:
        return 0.0
    if kind is ArrowType.DATE32:
        return session
    raise AssertionError(f"unsupported fixture type: {kind}")


def _legacy_row(
    session: date,
    *,
    ticker: str = "AAPL",
    eligible: bool = True,
    active: bool = True,
    observed_composite: str = COMPOSITE,
    observed_market: str = "US",
    observed_share: str = SHARE,
    canonical_composite: str | None = None,
    canonical_share: str | None = SHARE,
    identity_method: str | None = None,
    identity_disposition: str | None = None,
    identity_adjudication_id: str | None = None,
    cross_market_adjudication_id: str | None = None,
    provider_composite_override_id: str | None = None,
    share_class_adjudication_id: str | None = None,
    asset_transition_ids: tuple[str, ...] = (),
    policy_bundle: IdentityPolicyBundle | None = None,
    source_label: str | None = None,
) -> dict[str, object]:
    contract = S7_DERIVED_CONTRACTS["universe_daily"]
    row = {
        column.name: (None if column.nullable else _default_for_type(column.arrow_type, session))
        for column in contract.columns
    }
    source_id = _digest(source_label or f"{ticker}-{session.isoformat()}")
    bundle = policy_bundle or _policy()
    canonical = canonical_composite or observed_composite
    asset_id = canonical_asset_id(canonical) if eligible else None
    decision_selected = any(
        value is not None
        for value in (
            identity_adjudication_id,
            cross_market_adjudication_id,
            provider_composite_override_id,
            share_class_adjudication_id,
        )
    ) or bool(asset_transition_ids)
    evidence_available = max(session, MEMBER_AVAILABLE) if decision_selected else session
    row.update(
        {
            "session_year": session.year,
            "session_date": session,
            "ticker": ticker,
            "active_on_date": active,
            "asset_id": asset_id,
            "share_class_id": (
                canonical_share_class_id(canonical_share)
                if eligible and canonical_share is not None
                else None
            ),
            "canonical_share_class_figi": canonical_share if eligible else None,
            "issuer_id": canonical_issuer_id(CIK),
            "canonical_cik_normalized": CIK,
            "ticker_alias_id": _digest(f"legacy-alias-{ticker}-{session}") if eligible else None,
            "type_code": "CS",
            "primary_exchange_mic": "XNAS",
            "observed_cik_normalized": CIK,
            "observed_composite_figi": observed_composite,
            "observed_composite_market_code": observed_market,
            "observed_asset_id": canonical_asset_id(observed_composite),
            "canonical_composite_figi": canonical if eligible else None,
            "canonical_composite_market_code": "US" if eligible else None,
            "observed_share_class_figi": observed_share,
            "identity_resolution_status": "resolved_strong" if eligible else "unresolved",
            "identity_resolution_method": identity_method
            or (
                "source_composite_figi_exact"
                if eligible
                else "cross_market_composite_pending_unresolved"
            ),
            "identity_disposition": identity_disposition
            or ("observed_consistent" if eligible else "pending_cross_market_review"),
            "identity_adjudication_id": identity_adjudication_id,
            "adjudication_available_session": (
                MEMBER_AVAILABLE if identity_adjudication_id is not None else None
            ),
            "cross_market_adjudication_id": cross_market_adjudication_id,
            "cross_market_adjudication_available_session": (
                MEMBER_AVAILABLE if cross_market_adjudication_id is not None else None
            ),
            "provider_composite_override_id": provider_composite_override_id,
            "provider_composite_override_available_session": (
                MEMBER_AVAILABLE if provider_composite_override_id is not None else None
            ),
            "share_class_adjudication_id": share_class_adjudication_id,
            "share_class_adjudication_available_session": (
                MEMBER_AVAILABLE if share_class_adjudication_id is not None else None
            ),
            "source_identity_case_candidate_manifest_id": _digest("case-candidate"),
            "source_identity_case_candidate_manifest_sha256": _digest("case-manifest"),
            "cross_market_classification_status": "known_us" if eligible else "known_non_us",
            "identity_resolution_cutoff_session": POLICY_CUTOFF,
            "backtest_identity_eligible": eligible,
            "position_continuity_status": "identity_quality_does_not_force_liquidation",
            "identity_quality_liquidation_signal": False,
            "current_reference_factor_eligible": False,
            "security_type_scope": "source_type_code_as_returned_not_historical_dictionary_v1",
            "selected_source_record_id": source_id,
            "source_version_count": 1,
            "source_selection_status": "selected_exact_source_record",
            "membership_time_scope": "point_in_time_membership_v1",
            "membership_source_available_session": session,
            "membership_source_availability_quality": "source_attested",
            "metadata_time_scope": "retrospective_reference_not_signal_v1",
            "identity_mapping_time_scope": "retrospective_identity_reference_not_signal_v1",
            "identity_evidence_available_session": evidence_available,
            "resolution_rule_version": "s7_universe_resolution_v4",
            "source_s4_release_set_id": _digest("s4-release"),
            "source_s5_status_release_id": _digest("s5-status"),
            "source_s5_event_release_id": _digest("s5-event"),
            "source_s6_overview_release_id": _digest("s6-overview"),
            "source_identity_adjudication_release_id": _release_id(
                bundle, IdentityRegistryKind.IDENTITY_ADJUDICATION
            ),
            "source_identity_adjudication_release_available_session": MEMBER_AVAILABLE,
            "source_identity_market_consistency_candidate_manifest_id": _digest("market-candidate"),
            "source_identity_market_consistency_candidate_manifest_sha256": _digest(
                "market-manifest"
            ),
            "source_identity_cross_market_adjudication_release_id": _release_id(
                bundle, IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION
            ),
            "source_identity_cross_market_adjudication_release_available_session": (
                MEMBER_AVAILABLE
            ),
            "asset_transition_ids": list(asset_transition_ids),
            "composite_registry_match_count": 0,
            "composite_registry_collision": False,
            "source_provider_composite_override_release_id": _release_id(
                bundle, IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE
            ),
            "source_provider_composite_override_release_available_session": MEMBER_AVAILABLE,
            "source_share_class_adjudication_release_id": _release_id(
                bundle, IdentityRegistryKind.SHARE_CLASS_ADJUDICATION
            ),
            "source_share_class_adjudication_release_available_session": MEMBER_AVAILABLE,
            "source_asset_transition_release_id": _release_id(
                bundle, IdentityRegistryKind.ASSET_TRANSITION
            ),
            "source_asset_transition_release_available_session": MEMBER_AVAILABLE,
        }
    )
    return row


def _bootstrap(session: date = CALENDAR[2]):
    return bootstrap_native_v2_fixture(
        (_legacy_row(session),),
        session_date=session,
        ordered_calendar_sessions=CALENDAR,
        identity_policy_bundle=_policy(),
        identity_policy_snapshot=EMPTY_SNAPSHOT,
        s4_source_binding=_s4_binding(session),
        availability_session=RUN_AVAILABLE,
        reference_metadata_by_source_id={
            _digest(f"AAPL-{session.isoformat()}"): {
                "reference_name": "Apple Inc.",
                "sic_code": "3571",
            }
        },
        reference_metadata_available_session=MEMBER_AVAILABLE,
    )


class _RecordingReader:
    def __init__(self, by_session: dict[date, tuple[dict[str, object], ...]]) -> None:
        self.by_session = by_session
        self.calls: list[tuple[date, ...]] = []

    def __call__(self, requested: tuple[date, ...]) -> dict[date, tuple[dict[str, object], ...]]:
        self.calls.append(requested)
        return {session: self.by_session[session] for session in requested}


def test_native_v2_bootstrap_projects_exactly_to_v1_oracle() -> None:
    legacy = _legacy_row(CALENDAR[2])
    result = bootstrap_native_v2_fixture(
        (legacy,),
        session_date=CALENDAR[2],
        ordered_calendar_sessions=CALENDAR,
        identity_policy_bundle=_policy(),
        identity_policy_snapshot=EMPTY_SNAPSHOT,
        s4_source_binding=_s4_binding(CALENDAR[2]),
        availability_session=RUN_AVAILABLE,
    )

    assert result.legacy_oracle_projection_digest is not None
    assert result.checkpoint.parent_release.release_id != LEGACY_S7_V1_RELEASE_SET_ID
    manifest_bytes = result.release_manifest.canonical_bytes()
    assert result.checkpoint.parent_release.release_id == result.release_manifest.release_id
    assert result.release_manifest.resolved_state_digest == result.checkpoint.resolved_state_digest
    assert result.checkpoint.parent_release.resolved_state_digest == (
        result.checkpoint.resolved_state_digest
    )
    assert (
        result.checkpoint.parent_release.manifest.sha256
        == hashlib.sha256(manifest_bytes).hexdigest()
    )
    assert result.checkpoint.parent_release.manifest.bytes == len(manifest_bytes)
    assert len(result.release_manifest.output_artifacts) == 4
    assert (
        result.checkpoint.identity_policy_bundle_artifact.sha256
        == hashlib.sha256(result.identity_policy_bundle_content).hexdigest()
    )
    assert result.checkpoint.identity_policy_bundle_artifact.bytes == len(
        result.identity_policy_bundle_content
    )
    assert "ticker_alias_id" not in result.universe_daily_rows[0]
    assert result.qa["qa_catalog_digest"] == I3_QA_CATALOG_DIGEST
    assert all(rule.check_id in result.qa for rule in I3_QA_CATALOG)
    assert "missing_eligible_alias_rows" not in result.qa
    assert result.receipt["publish_authorized"] is False
    receipt_payload = dict(result.receipt)
    receipt_id = receipt_payload.pop("receipt_id")
    assert receipt_id == stable_digest(receipt_payload)
    fixture_binding = result.receipt["fixture_input_binding"]
    assert fixture_binding["authority"] == "local_fixture_oracle_non_authoritative"
    assert fixture_binding["s4_pins_authenticate_resolved_rows"] is False
    assert result.receipt["fixture_input_binding_digest"] == fixture_binding["binding_digest"]


def test_fixture_reference_metadata_is_content_and_availability_bound() -> None:
    session = CALENDAR[2]
    source_id = _digest(f"AAPL-{session.isoformat()}")
    result = bootstrap_native_v2_fixture(
        (_legacy_row(session),),
        session_date=session,
        ordered_calendar_sessions=CALENDAR,
        identity_policy_bundle=_policy(),
        identity_policy_snapshot=EMPTY_SNAPSHOT,
        s4_source_binding=_s4_binding(session),
        availability_session=RUN_AVAILABLE,
        reference_metadata_by_source_id={
            source_id: {"reference_name": "Apple Inc.", "sic_code": "3571"}
        },
        reference_metadata_available_session=RUN_AVAILABLE,
    )

    fixture_binding = result.receipt["fixture_input_binding"]
    assert fixture_binding["reference_metadata_available_session"] == RUN_AVAILABLE.isoformat()
    assert result.checkpoint.issuer_aggregates[0].reference_available_session == RUN_AVAILABLE

    with pytest.raises(I3FixtureRunnerError, match="requires an explicit availability"):
        bootstrap_native_v2_fixture(
            (_legacy_row(session),),
            session_date=session,
            ordered_calendar_sessions=CALENDAR,
            identity_policy_bundle=_policy(),
            identity_policy_snapshot=EMPTY_SNAPSHOT,
            s4_source_binding=_s4_binding(session),
            availability_session=RUN_AVAILABLE,
            reference_metadata_by_source_id={source_id: {"reference_name": "Apple Inc."}},
        )

    with pytest.raises(I3FixtureRunnerError, match="exceeds output availability"):
        bootstrap_native_v2_fixture(
            (_legacy_row(session),),
            session_date=session,
            ordered_calendar_sessions=CALENDAR,
            identity_policy_bundle=_policy(),
            identity_policy_snapshot=EMPTY_SNAPSHOT,
            s4_source_binding=_s4_binding(session),
            availability_session=RUN_AVAILABLE,
            reference_metadata_by_source_id={source_id: {"reference_name": "Apple Inc."}},
            reference_metadata_available_session=date(2026, 2, 4),
        )


def test_fixture_bootstrap_rejects_selected_decisions_before_materialization() -> None:
    session = CALENDAR[2]
    decision_record = _decision_record(
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        session=session,
        source_record_id=_digest(f"AAPL-{session.isoformat()}"),
        observed_composite_figi="BBG000KRLLH9",
        canonical_composite_figi=COMPOSITE,
    )
    snapshot = _snapshot(
        (IdentityRegistryKind.IDENTITY_ADJUDICATION, decision_record),
    )
    row = _legacy_row(
        session,
        observed_composite="BBG000KRLLH9",
        canonical_composite=COMPOSITE,
        identity_method="approved_provider_contamination_override",
        identity_disposition="confirmed_provider_contamination",
        identity_adjudication_id=str(decision_record["decision_id"]),
        policy_bundle=snapshot.policy_bundle,
    )
    row["adjudication_available_session"] = session

    with pytest.raises(I3FixtureRunnerError, match="cannot consume selected registry decisions"):
        bootstrap_native_v2_fixture(
            (row,),
            session_date=session,
            ordered_calendar_sessions=CALENDAR,
            identity_policy_bundle=snapshot.policy_bundle,
            identity_policy_snapshot=snapshot,
            s4_source_binding=_s4_binding(session),
            availability_session=RUN_AVAILABLE,
        )


def test_fixture_bootstrap_rejects_any_applicable_registry_decision_and_snapshot_tamper() -> None:
    session = CALENDAR[2]
    source_record_id = _digest(f"AAPL-{session.isoformat()}")
    other_composite = "BBG000KRLLH9"
    cases = (
        (
            IdentityRegistryKind.IDENTITY_ADJUDICATION,
            _decision_record(
                IdentityRegistryKind.IDENTITY_ADJUDICATION,
                session=session,
                source_record_id=source_record_id,
                observed_composite_figi=COMPOSITE,
                canonical_composite_figi=other_composite,
            ),
        ),
        (
            IdentityRegistryKind.IDENTITY_ADJUDICATION,
            _decision_record(
                IdentityRegistryKind.IDENTITY_ADJUDICATION,
                session=session,
                source_record_id=source_record_id,
                observed_composite_figi=COMPOSITE,
                canonical_composite_figi=None,
                identity_disposition="adjudicated_unresolved",
            ),
        ),
        (
            IdentityRegistryKind.SHARE_CLASS_ADJUDICATION,
            _decision_record(
                IdentityRegistryKind.SHARE_CLASS_ADJUDICATION,
                session=session,
                source_record_id=source_record_id,
                composite_scope_figi=COMPOSITE,
                observed_share_class_figi=SHARE,
                canonical_share_class_figi="BBG001S5Q3X4",
            ),
        ),
        (
            IdentityRegistryKind.ASSET_TRANSITION,
            _decision_record(
                IdentityRegistryKind.ASSET_TRANSITION,
                session=session,
                source_record_id=source_record_id,
                predecessor_asset_id=canonical_asset_id(COMPOSITE),
                successor_asset_id=canonical_asset_id(other_composite),
            ),
        ),
    )
    for kind, record in cases:
        snapshot = _snapshot((kind, record))
        with pytest.raises(I3FixtureRunnerError, match="applicable registry decision"):
            bootstrap_native_v2_fixture(
                (_legacy_row(session, policy_bundle=snapshot.policy_bundle),),
                session_date=session,
                ordered_calendar_sessions=CALENDAR,
                identity_policy_bundle=snapshot.policy_bundle,
                identity_policy_snapshot=snapshot,
                s4_source_binding=_s4_binding(session),
                availability_session=RUN_AVAILABLE,
            )

    snapshot = _snapshot()
    object.__setattr__(snapshot, "_policy_snapshot_id", _digest("tampered-bootstrap-snapshot"))
    with pytest.raises(I3FixtureRunnerError, match="identity policy snapshot is invalid"):
        bootstrap_native_v2_fixture(
            (_legacy_row(session, policy_bundle=snapshot.policy_bundle),),
            session_date=session,
            ordered_calendar_sessions=CALENDAR,
            identity_policy_bundle=snapshot.policy_bundle,
            identity_policy_snapshot=snapshot,
            s4_source_binding=_s4_binding(session),
            availability_session=RUN_AVAILABLE,
        )


def test_clean_append_extends_same_segment_and_reads_fixed_window_only() -> None:
    base = _bootstrap()
    target = CALENDAR[3]
    reader = _RecordingReader(
        {
            CALENDAR[1]: (_legacy_row(CALENDAR[1]),),
            CALENDAR[2]: (_legacy_row(CALENDAR[2]),),
            target: (_legacy_row(target),),
        }
    )

    result = run_i3_fixture_session(
        base.checkpoint,
        target_session=target,
        ordered_calendar_sessions=CALENDAR,
        resolved_row_reader=reader,
        s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
        identity_policy_snapshot=EMPTY_SNAPSHOT,
        availability_session=date(2026, 2, 4),
    )

    expected = (CALENDAR[1], CALENDAR[2], target)
    assert FIXED_BOUNDARY_LOOKBACK_SESSIONS == 2
    assert reader.calls == [expected]
    assert result.requested_sessions == expected
    assert result.checkpoint.open_aliases[0].segment.alias_segment_id == (
        base.checkpoint.open_aliases[0].segment.alias_segment_id
    )
    assert result.ticker_alias_rows[0]["predecessor_alias_resolution_version_id"] == (
        base.checkpoint.open_aliases[0].resolution.alias_resolution_version_id
    )
    assert result.universe_daily_rows[0]["alias_segment_id"] == (
        result.checkpoint.open_aliases[0].segment.alias_segment_id
    )
    assert result.universe_daily_rows[0]["asset_master_version_id"] == (
        result.checkpoint.asset_aggregates[0].terminal_row_version_id
    )
    assert result.qa["critical_failure_count"] == 0
    assert result.release_manifest.parent_release_id == base.checkpoint.parent_release.release_id
    assert result.release_manifest.source_checkpoint_id == base.checkpoint.checkpoint_id
    assert result.checkpoint.parent_release.release_id == result.release_manifest.release_id


def test_history_bootstrap_advances_a_contiguous_fixture_with_exact_windows() -> None:
    rows = {
        session: (_legacy_row(session),)
        for session in (CALENDAR[1], CALENDAR[2], CALENDAR[3], CALENDAR[4])
    }
    result = bootstrap_native_v2_fixture_history(
        rows,
        bootstrap_session=CALENDAR[2],
        terminal_session=CALENDAR[4],
        ordered_calendar_sessions=CALENDAR,
        identity_policy_bundle=_policy(),
        identity_policy_snapshot=EMPTY_SNAPSHOT,
        s4_bindings_by_target={
            CALENDAR[2]: _s4_binding(CALENDAR[2]),
            CALENDAR[3]: _s4_binding(CALENDAR[1], CALENDAR[2], CALENDAR[3]),
            CALENDAR[4]: _s4_binding(CALENDAR[2], CALENDAR[3], CALENDAR[4]),
        },
        availability_by_target={
            CALENDAR[2]: RUN_AVAILABLE,
            CALENDAR[3]: date(2026, 2, 4),
            CALENDAR[4]: date(2026, 2, 5),
        },
    )

    assert result.checkpoint.last_session == CALENDAR[4]
    assert len(result.checkpoint.resolved_partition_map) == 3
    counters = {item.name: item.value for item in result.checkpoint.asset_aggregates[0].counters}
    assert counters["strong_evidence_row_count"] == 3
    assert result.release_manifest.parent_release_id is not None
    assert result.release_manifest.source_checkpoint_id is not None


def test_gap_then_reopen_creates_a_new_stable_segment() -> None:
    base = _bootstrap()
    gap = CALENDAR[3]
    gap_reader = _RecordingReader(
        {
            CALENDAR[1]: (_legacy_row(CALENDAR[1]),),
            CALENDAR[2]: (_legacy_row(CALENDAR[2]),),
            gap: (),
        }
    )
    after_gap = run_i3_fixture_session(
        base.checkpoint,
        target_session=gap,
        ordered_calendar_sessions=CALENDAR,
        resolved_row_reader=gap_reader,
        s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], gap),
        identity_policy_snapshot=EMPTY_SNAPSHOT,
        availability_session=date(2026, 2, 4),
    )
    assert after_gap.checkpoint.open_aliases == ()

    reopened_session = CALENDAR[4]
    reopen_reader = _RecordingReader(
        {
            CALENDAR[2]: (_legacy_row(CALENDAR[2]),),
            gap: (),
            reopened_session: (_legacy_row(reopened_session),),
        }
    )
    reopened = run_i3_fixture_session(
        after_gap.checkpoint,
        target_session=reopened_session,
        ordered_calendar_sessions=CALENDAR,
        resolved_row_reader=reopen_reader,
        s4_window_binding=_s4_binding(CALENDAR[2], gap, reopened_session),
        identity_policy_snapshot=EMPTY_SNAPSHOT,
        availability_session=date(2026, 2, 5),
    )

    assert reopened.checkpoint.open_aliases[0].segment.valid_from_session == reopened_session
    assert reopened.checkpoint.open_aliases[0].segment.alias_segment_id != (
        base.checkpoint.open_aliases[0].segment.alias_segment_id
    )
    assert reopened.ticker_alias_rows[0]["predecessor_alias_resolution_version_id"] is None


def test_unresolved_membership_is_retained_without_alias_or_forced_liquidation() -> None:
    base = _bootstrap()
    target = CALENDAR[3]
    unresolved = _legacy_row(
        target,
        eligible=False,
        active=True,
        observed_composite="BBG000KRLLH9",
        observed_market="GB",
    )
    reader = _RecordingReader(
        {
            CALENDAR[1]: (_legacy_row(CALENDAR[1]),),
            CALENDAR[2]: (_legacy_row(CALENDAR[2]),),
            target: (unresolved,),
        }
    )

    result = run_i3_fixture_session(
        base.checkpoint,
        target_session=target,
        ordered_calendar_sessions=CALENDAR,
        resolved_row_reader=reader,
        s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
        identity_policy_snapshot=EMPTY_SNAPSHOT,
        availability_session=date(2026, 2, 4),
    )

    output = result.universe_daily_rows[0]
    assert output["active_on_date"] is True
    assert output["backtest_identity_eligible"] is False
    assert output["alias_segment_id"] is None
    assert output["alias_resolution_version_id"] is None
    assert output["asset_master_version_id"] is None
    assert output["issuer_master_version_id"] is None
    assert output["identity_quality_liquidation_signal"] is False
    assert result.ticker_alias_rows == ()
    assert result.qa["unresolved_rows"] == 1
    assert result.checkpoint.unresolved_subjects[0].subject_key == "AAPL"
    issuer = result.checkpoint.issuer_aggregates[0]
    assert issuer.first_observed_session == CALENDAR[2]
    assert issuer.last_observed_session == CALENDAR[2]
    assert issuer.reference_names == ("Apple Inc.",)
    assert issuer.sic_codes == ("3571",)
    assert issuer.reference_available_session == BUNDLE_AVAILABLE
    assert {item.name: item.value for item in issuer.counters} == {
        "excluded_contamination_evidence_row_count": 0,
        "excluded_cross_market_contamination_evidence_row_count": 0,
        "source_evidence_row_count": 1,
    }


@pytest.mark.parametrize(
    ("kind", "registry_disposition", "output_method", "output_disposition", "market"),
    (
        (
            IdentityRegistryKind.IDENTITY_ADJUDICATION,
            "adjudicated_unresolved",
            "provider_figi_bounce_adjudicated_unresolved",
            "adjudicated_unresolved",
            "US",
        ),
        (
            IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
            "cross_market_adjudicated_unresolved",
            "cross_market_composite_adjudicated_unresolved",
            "cross_market_adjudicated_unresolved",
            "GB",
        ),
        (
            IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE,
            "provider_composite_override_adjudicated_unresolved",
            "adjudicated_unresolved",
            "adjudicated_unresolved",
            "US",
        ),
        (
            IdentityRegistryKind.SHARE_CLASS_ADJUDICATION,
            "share_class_adjudicated_unresolved",
            "adjudicated_unresolved",
            "adjudicated_unresolved",
            "US",
        ),
    ),
)
def test_runner_accepts_closed_unresolved_registry_disposition_matrix(
    kind: IdentityRegistryKind,
    registry_disposition: str,
    output_method: str,
    output_disposition: str,
    market: str,
) -> None:
    target = CALENDAR[3]
    observed = (
        "BBG000KRLLH9" if kind is not IdentityRegistryKind.SHARE_CLASS_ADJUDICATION else COMPOSITE
    )
    source_record_id = _digest(f"AAPL-{target.isoformat()}")
    records: list[tuple[IdentityRegistryKind, dict[str, object]]] = []
    transition_id: str | None = None
    decision_kwargs: dict[str, object] = {
        "identity_disposition": registry_disposition,
    }
    if kind is IdentityRegistryKind.SHARE_CLASS_ADJUDICATION:
        decision_kwargs.update(
            {
                "composite_scope_figi": COMPOSITE,
                "observed_share_class_figi": SHARE,
                "canonical_share_class_figi": None,
            }
        )
    else:
        decision_kwargs.update(
            {
                "observed_composite_figi": observed,
                "canonical_composite_figi": None,
            }
        )
    if kind is IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE:
        transition = _decision_record(
            IdentityRegistryKind.ASSET_TRANSITION,
            session=target,
            source_record_id=_digest("provider-unresolved-transition-boundary"),
            predecessor_asset_id=canonical_asset_id(observed),
            successor_asset_id=canonical_asset_id(COMPOSITE),
        )
        transition_id = str(transition["decision_id"])
        decision_kwargs["transition_relation_id"] = transition_id
        records.append((IdentityRegistryKind.ASSET_TRANSITION, transition))
    decision = _decision_record(
        kind,
        session=target,
        source_record_id=source_record_id,
        **decision_kwargs,
    )
    records.append((kind, decision))
    snapshot = _snapshot(*records)
    bundle = snapshot.policy_bundle
    base = bootstrap_native_v2_fixture(
        (_legacy_row(CALENDAR[2], policy_bundle=bundle),),
        session_date=CALENDAR[2],
        ordered_calendar_sessions=CALENDAR,
        identity_policy_bundle=bundle,
        identity_policy_snapshot=snapshot,
        s4_source_binding=_s4_binding(CALENDAR[2]),
        availability_session=RUN_AVAILABLE,
    )
    selected = {
        IdentityRegistryKind.IDENTITY_ADJUDICATION: {
            "identity_adjudication_id": decision["decision_id"]
        },
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION: {
            "cross_market_adjudication_id": decision["decision_id"]
        },
        IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE: {
            "provider_composite_override_id": decision["decision_id"],
            "asset_transition_ids": (() if transition_id is None else (transition_id,)),
        },
        IdentityRegistryKind.SHARE_CLASS_ADJUDICATION: {
            "share_class_adjudication_id": decision["decision_id"]
        },
    }[kind]
    target_row = _legacy_row(
        target,
        eligible=False,
        observed_composite=observed,
        observed_market=market,
        identity_method=output_method,
        identity_disposition=output_disposition,
        policy_bundle=bundle,
        **selected,
    )
    source = {
        CALENDAR[1]: (_legacy_row(CALENDAR[1], policy_bundle=bundle),),
        CALENDAR[2]: (_legacy_row(CALENDAR[2], policy_bundle=bundle),),
        target: (target_row,),
    }
    result = run_i3_fixture_session(
        base.checkpoint,
        target_session=target,
        ordered_calendar_sessions=CALENDAR,
        resolved_row_reader=_RecordingReader(source),
        s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
        identity_policy_snapshot=snapshot,
        availability_session=date(2026, 2, 4),
    )

    output = result.universe_daily_rows[0]
    assert output["active_on_date"] is True
    assert output["backtest_identity_eligible"] is False
    assert output["alias_segment_id"] is None
    assert result.ticker_alias_rows == ()


def test_registry_release_ids_and_availability_must_match_policy_exactly() -> None:
    base = _bootstrap()
    target = CALENDAR[3]
    tampered = _legacy_row(target)
    tampered["source_share_class_adjudication_release_id"] = _digest("wrong-release")
    source = {
        CALENDAR[1]: (_legacy_row(CALENDAR[1]),),
        CALENDAR[2]: (_legacy_row(CALENDAR[2]),),
        target: (tampered,),
    }
    with pytest.raises(I3FixtureRunnerError, match="release ID differs"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader(source),
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
            identity_policy_snapshot=EMPTY_SNAPSHOT,
            availability_session=date(2026, 2, 4),
        )

    tampered = _legacy_row(target)
    tampered["source_share_class_adjudication_release_available_session"] = date(2026, 2, 2)
    source[target] = (tampered,)
    with pytest.raises(I3FixtureRunnerError, match="availability differs"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader(source),
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
            identity_policy_snapshot=EMPTY_SNAPSHOT,
            availability_session=date(2026, 2, 4),
        )


def test_selected_decision_availability_must_reproduce_from_sealed_snapshot() -> None:
    target = CALENDAR[3]
    decision_record = _decision_record(
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        session=target,
        source_record_id=_digest(f"AAPL-{target.isoformat()}"),
        observed_composite_figi="BBG000KRLLH9",
        canonical_composite_figi=COMPOSITE,
    )
    snapshot = _snapshot(
        (IdentityRegistryKind.IDENTITY_ADJUDICATION, decision_record),
    )
    bundle = snapshot.policy_bundle
    base = bootstrap_native_v2_fixture(
        (_legacy_row(CALENDAR[2], policy_bundle=bundle),),
        session_date=CALENDAR[2],
        ordered_calendar_sessions=CALENDAR,
        identity_policy_bundle=bundle,
        identity_policy_snapshot=snapshot,
        s4_source_binding=_s4_binding(CALENDAR[2]),
        availability_session=RUN_AVAILABLE,
    )
    target_row = _legacy_row(
        target,
        observed_composite="BBG000KRLLH9",
        canonical_composite=COMPOSITE,
        identity_method="approved_provider_contamination_override",
        identity_disposition="confirmed_provider_contamination",
        identity_adjudication_id=str(decision_record["decision_id"]),
        policy_bundle=bundle,
    )
    target_row["adjudication_available_session"] = target
    source = {
        CALENDAR[1]: (_legacy_row(CALENDAR[1], policy_bundle=bundle),),
        CALENDAR[2]: (_legacy_row(CALENDAR[2], policy_bundle=bundle),),
        target: (target_row,),
    }
    with pytest.raises(I3FixtureRunnerError, match="availability differs from the sealed"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader(source),
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
            identity_policy_snapshot=snapshot,
            availability_session=date(2026, 2, 4),
        )

    target_row = dict(target_row)
    target_row["adjudication_available_session"] = MEMBER_AVAILABLE
    target_row["identity_evidence_available_session"] = target
    source[target] = (target_row,)
    with pytest.raises(I3FixtureRunnerError, match="evidence availability precedes"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader(source),
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
            identity_policy_snapshot=snapshot,
            availability_session=date(2026, 2, 4),
        )

    relabeled = dict(target_row)
    relabeled["identity_evidence_available_session"] = MEMBER_AVAILABLE
    relabeled["identity_resolution_method"] = "approved_genuine_transition"
    relabeled["identity_disposition"] = "confirmed_genuine_transition"
    source[target] = (relabeled,)
    with pytest.raises(I3FixtureRunnerError, match="closed identity dispatch differs"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader(source),
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
            identity_policy_snapshot=snapshot,
            availability_session=date(2026, 2, 4),
        )


def test_asset_transition_availability_and_missing_counterpart_fail_closed() -> None:
    target = CALENDAR[3]
    successor_composite = "BBG000KRLLH9"
    transition_record = _decision_record(
        IdentityRegistryKind.ASSET_TRANSITION,
        session=target,
        source_record_id=_digest(f"AAPL-{target.isoformat()}"),
        predecessor_asset_id=canonical_asset_id(COMPOSITE),
        successor_asset_id=canonical_asset_id(successor_composite),
    )
    snapshot = _snapshot(
        (IdentityRegistryKind.ASSET_TRANSITION, transition_record),
    )
    bundle = snapshot.policy_bundle
    base = bootstrap_native_v2_fixture(
        (_legacy_row(CALENDAR[2], policy_bundle=bundle),),
        session_date=CALENDAR[2],
        ordered_calendar_sessions=CALENDAR,
        identity_policy_bundle=bundle,
        identity_policy_snapshot=snapshot,
        s4_source_binding=_s4_binding(CALENDAR[2]),
        availability_session=RUN_AVAILABLE,
    )
    target_row = _legacy_row(
        target,
        asset_transition_ids=(str(transition_record["decision_id"]),),
        policy_bundle=bundle,
    )
    target_row["identity_evidence_available_session"] = target
    source = {
        CALENDAR[1]: (_legacy_row(CALENDAR[1], policy_bundle=bundle),),
        CALENDAR[2]: (_legacy_row(CALENDAR[2], policy_bundle=bundle),),
        target: (target_row,),
    }
    with pytest.raises(I3FixtureRunnerError, match="evidence availability precedes"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader(source),
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
            identity_policy_snapshot=snapshot,
            availability_session=date(2026, 2, 4),
        )

    target_row = dict(target_row)
    target_row["identity_evidence_available_session"] = MEMBER_AVAILABLE
    source[target] = (target_row,)
    with pytest.raises(I3FixtureRunnerError, match="endpoint is absent"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader(source),
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
            identity_policy_snapshot=snapshot,
            availability_session=date(2026, 2, 4),
        )


def test_confirmed_asset_transition_rejects_an_unrelated_current_asset() -> None:
    target = CALENDAR[3]
    predecessor_composite = "BBG000F2XXP2"
    successor_composite = "BBG000KRLLH9"
    transition = _decision_record(
        IdentityRegistryKind.ASSET_TRANSITION,
        session=target,
        source_record_id=_digest(f"AAPL-{target.isoformat()}"),
        predecessor_asset_id=canonical_asset_id(predecessor_composite),
        successor_asset_id=canonical_asset_id(successor_composite),
    )
    snapshot = _snapshot((IdentityRegistryKind.ASSET_TRANSITION, transition))
    bundle = snapshot.policy_bundle
    base = bootstrap_native_v2_fixture(
        (
            _legacy_row(CALENDAR[2], policy_bundle=bundle),
            _legacy_row(
                CALENDAR[2],
                ticker="OLD1",
                observed_composite=predecessor_composite,
                policy_bundle=bundle,
            ),
            _legacy_row(
                CALENDAR[2],
                ticker="OLD2",
                observed_composite=successor_composite,
                policy_bundle=bundle,
            ),
        ),
        session_date=CALENDAR[2],
        ordered_calendar_sessions=CALENDAR,
        identity_policy_bundle=bundle,
        identity_policy_snapshot=snapshot,
        s4_source_binding=_s4_binding(CALENDAR[2]),
        availability_session=RUN_AVAILABLE,
    )
    source = {
        CALENDAR[1]: (_legacy_row(CALENDAR[1], policy_bundle=bundle),),
        CALENDAR[2]: (_legacy_row(CALENDAR[2], policy_bundle=bundle),),
        target: (
            _legacy_row(
                target,
                asset_transition_ids=(str(transition["decision_id"]),),
                policy_bundle=bundle,
            ),
        ),
    }
    with pytest.raises(I3FixtureRunnerError, match="non-endpoint canonical asset"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader(source),
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
            identity_policy_snapshot=snapshot,
            availability_session=date(2026, 2, 4),
        )


def test_resolved_v1_identity_claim_cannot_bypass_the_closed_dispatcher() -> None:
    base = _bootstrap()
    target = CALENDAR[3]
    lied = _legacy_row(
        target,
        observed_composite="BBG000KRLLH9",
        canonical_composite=COMPOSITE,
        identity_method="approved_provider_contamination_override",
        identity_disposition="confirmed_provider_contamination",
        identity_adjudication_id=_digest("unregistered-decision"),
    )
    source = {
        CALENDAR[1]: (_legacy_row(CALENDAR[1]),),
        CALENDAR[2]: (_legacy_row(CALENDAR[2]),),
        target: (lied,),
    }
    with pytest.raises(I3FixtureRunnerError, match="closed identity dispatch"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader(source),
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
            identity_policy_snapshot=EMPTY_SNAPSHOT,
            availability_session=date(2026, 2, 4),
        )


def test_s4_binding_and_exact_two_session_lookback_fail_closed_before_read() -> None:
    base = _bootstrap()
    target = CALENDAR[3]
    reader = _RecordingReader({})
    with pytest.raises(I3FixtureRunnerError, match="differs from the requested window"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=reader,
            s4_window_binding=_s4_binding(CALENDAR[0], CALENDAR[2], target),
            identity_policy_snapshot=EMPTY_SNAPSHOT,
            availability_session=date(2026, 2, 4),
        )
    assert reader.calls == []

    early = _bootstrap(CALENDAR[0])
    with pytest.raises(I3FixtureRunnerError, match="two-session boundary lookback"):
        run_i3_fixture_session(
            early.checkpoint,
            target_session=CALENDAR[1],
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=reader,
            s4_window_binding=_s4_binding(CALENDAR[0], CALENDAR[1], CALENDAR[2]),
            identity_policy_snapshot=EMPTY_SNAPSHOT,
            availability_session=date(2026, 2, 4),
        )
    assert reader.calls == []


def test_unapproved_same_market_bounce_fails_before_checkpoint_or_receipt() -> None:
    middle_composite = "BBG000KRLLH9"
    base = bootstrap_native_v2_fixture(
        (_legacy_row(CALENDAR[2], observed_composite=middle_composite),),
        session_date=CALENDAR[2],
        ordered_calendar_sessions=CALENDAR,
        identity_policy_bundle=_policy(),
        identity_policy_snapshot=EMPTY_SNAPSHOT,
        s4_source_binding=_s4_binding(CALENDAR[2]),
        availability_session=RUN_AVAILABLE,
    )
    target = CALENDAR[3]
    source = {
        CALENDAR[1]: (_legacy_row(CALENDAR[1]),),
        CALENDAR[2]: (_legacy_row(CALENDAR[2], observed_composite=middle_composite),),
        target: (_legacy_row(target),),
    }
    with pytest.raises(I3FixtureRunnerError, match="closed identity dispatch"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader(source),
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
            identity_policy_snapshot=EMPTY_SNAPSHOT,
            availability_session=date(2026, 2, 4),
        )


def test_asset_checkpoint_accumulates_complete_decision_and_share_class_sets() -> None:
    predecessor_composite = "BBG000F2XXP2"
    cross_record = _decision_record(
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        session=CALENDAR[3],
        source_record_id=_digest(f"AAPL-{CALENDAR[3].isoformat()}"),
        observed_composite_figi="BBG000KRLLH9",
        canonical_composite_figi=COMPOSITE,
    )
    transition_record = _decision_record(
        IdentityRegistryKind.ASSET_TRANSITION,
        session=CALENDAR[4],
        source_record_id=_digest("provider-transition-boundary-source"),
        predecessor_asset_id=canonical_asset_id(predecessor_composite),
        successor_asset_id=canonical_asset_id(COMPOSITE),
    )
    provider_record = _decision_record(
        IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE,
        session=CALENDAR[4],
        source_record_id=_digest(f"AAPL-{CALENDAR[4].isoformat()}"),
        observed_composite_figi=predecessor_composite,
        canonical_composite_figi=COMPOSITE,
        transition_relation_id=str(transition_record["decision_id"]),
    )
    share_record = _decision_record(
        IdentityRegistryKind.SHARE_CLASS_ADJUDICATION,
        session=CALENDAR[4],
        source_record_id=_digest(f"AAPL-{CALENDAR[4].isoformat()}"),
        observed_composite_figi=predecessor_composite,
        composite_scope_figi=COMPOSITE,
        observed_share_class_figi="BBG000C3K505",
        canonical_share_class_figi="BBG001S7W602",
    )
    episode_record = _decision_record(
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        session=CALENDAR[5],
        source_record_id=_digest(f"AAPL-{CALENDAR[5].isoformat()}"),
        observed_composite_figi="BBG000DY6735",
        canonical_composite_figi=COMPOSITE,
    )
    snapshot = _snapshot(
        (IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION, cross_record),
        (IdentityRegistryKind.ASSET_TRANSITION, transition_record),
        (IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE, provider_record),
        (IdentityRegistryKind.SHARE_CLASS_ADJUDICATION, share_record),
        (IdentityRegistryKind.IDENTITY_ADJUDICATION, episode_record),
    )
    bundle = snapshot.policy_bundle
    base_row = _legacy_row(CALENDAR[2], policy_bundle=bundle)
    predecessor_base_row = _legacy_row(
        CALENDAR[2],
        ticker="OLD",
        observed_composite=predecessor_composite,
        canonical_composite=predecessor_composite,
        policy_bundle=bundle,
    )
    base = bootstrap_native_v2_fixture(
        (base_row, predecessor_base_row),
        session_date=CALENDAR[2],
        ordered_calendar_sessions=CALENDAR,
        identity_policy_bundle=bundle,
        identity_policy_snapshot=snapshot,
        s4_source_binding=_s4_binding(CALENDAR[2]),
        availability_session=RUN_AVAILABLE,
    )
    rows_by_session = {
        CALENDAR[1]: (_legacy_row(CALENDAR[1], policy_bundle=bundle),),
        CALENDAR[2]: (base_row,),
        CALENDAR[3]: (
            _legacy_row(
                CALENDAR[3],
                observed_composite="BBG000KRLLH9",
                observed_market="GB",
                canonical_composite=COMPOSITE,
                identity_method="approved_cross_market_provider_contamination_override",
                identity_disposition="confirmed_provider_contamination",
                cross_market_adjudication_id=str(cross_record["decision_id"]),
                policy_bundle=bundle,
            ),
        ),
        CALENDAR[4]: (
            _legacy_row(
                CALENDAR[4],
                observed_composite=predecessor_composite,
                canonical_composite=COMPOSITE,
                observed_share="BBG000C3K505",
                canonical_share="BBG001S7W602",
                identity_method="approved_provider_composite_override",
                identity_disposition="provider_composite_stale_after_transition",
                provider_composite_override_id=str(provider_record["decision_id"]),
                share_class_adjudication_id=str(share_record["decision_id"]),
                asset_transition_ids=(str(transition_record["decision_id"]),),
                policy_bundle=bundle,
            ),
        ),
        CALENDAR[5]: (
            _legacy_row(
                CALENDAR[5],
                observed_composite="BBG000DY6735",
                canonical_composite=COMPOSITE,
                identity_method="approved_provider_contamination_override",
                identity_disposition="confirmed_provider_contamination",
                identity_adjudication_id=str(episode_record["decision_id"]),
                policy_bundle=bundle,
            ),
        ),
    }
    current = base
    cross_market_result = None
    for target, available in zip(
        CALENDAR[3:],
        (date(2026, 2, 4), date(2026, 2, 5), date(2026, 2, 6)),
        strict=True,
    ):
        index = CALENDAR.index(target)
        requested = CALENDAR[index - 2 : index + 1]
        current = run_i3_fixture_session(
            current.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader(
                {session: rows_by_session[session] for session in requested}
            ),
            s4_window_binding=_s4_binding(*requested),
            identity_policy_snapshot=snapshot,
            availability_session=available,
        )
        if target == CALENDAR[3]:
            cross_market_result = current

    assert cross_market_result is not None
    raw_foreign = cross_market_result.qa["raw_review_results"][
        "us_locale_non_us_composite_figi_rows"
    ]
    assert sum(item["count"] for item in raw_foreign["reason_counts"]) == 1
    assert [item["source_record_id"] for item in raw_foreign["bounded_examples"]] == [
        _digest(f"AAPL-{CALENDAR[3].isoformat()}")
    ]
    assert (
        cross_market_result.receipt["qa_result_digest"]
        == cross_market_result.qa["qa_result_digest"]
    )
    assert (
        cross_market_result.receipt["dispatcher_qa_receipt_id"]
        == cross_market_result.qa["dispatcher_qa_receipt_id"]
    )

    state = next(
        item
        for item in current.checkpoint.asset_aggregates
        if item.asset_id == canonical_asset_id(COMPOSITE)
    )
    assert state.identity_adjudication_ids == (episode_record["decision_id"],)
    assert state.cross_market_adjudication_ids == (cross_record["decision_id"],)
    assert state.provider_composite_override_ids == (provider_record["decision_id"],)
    assert state.share_class_adjudication_ids == (share_record["decision_id"],)
    assert state.asset_transition_ids == (transition_record["decision_id"],)
    assert state.predecessor_asset_ids == (canonical_asset_id(predecessor_composite),)
    assert state.successor_asset_ids == ()
    assert state.canonical_share_class_figis == tuple(sorted({SHARE, "BBG001S7W602"}))
    assert state.canonical_share_class_figi is None
    assert state.genuine_transition_identity_adjudication_ids == ()
    assert state.provider_contamination_identity_adjudication_ids == (
        episode_record["decision_id"],
    )
    counters = {item.name: item.value for item in state.counters}
    assert counters["strong_evidence_row_count"] == 4
    assert counters["candidate_evidence_row_count"] == 0
    assert counters["direct_observed_evidence_row_count"] == 1
    assert counters["adjudicated_override_evidence_row_count"] == 1
    assert counters["cross_market_override_evidence_row_count"] == 1
    assert counters["identity_adjudication_count"] == 1
    assert counters["cross_market_adjudication_count"] == 1
    assert counters["provider_composite_override_count"] == 1
    assert counters["share_class_adjudication_count"] == 1
    assert counters["genuine_transition_adjudication_count"] == 0
    assert counters["provider_contamination_adjudication_count"] == 1
    predecessor_state = next(
        item
        for item in current.checkpoint.asset_aggregates
        if item.asset_id == canonical_asset_id(predecessor_composite)
    )
    assert predecessor_state.asset_transition_ids == (transition_record["decision_id"],)
    assert predecessor_state.predecessor_asset_ids == ()
    assert predecessor_state.successor_asset_ids == (canonical_asset_id(COMPOSITE),)
    assert predecessor_state.last_canonical_membership_session == CALENDAR[2]
    issuer = current.checkpoint.issuer_aggregates[0]
    issuer_counters = {item.name: item.value for item in issuer.counters}
    assert issuer.last_observed_session == CALENDAR[4]
    assert issuer_counters == {
        "excluded_contamination_evidence_row_count": 2,
        "excluded_cross_market_contamination_evidence_row_count": 1,
        "source_evidence_row_count": 3,
    }


def test_same_checkpoint_and_input_are_idempotent() -> None:
    base = _bootstrap()
    target = CALENDAR[3]
    source = {
        CALENDAR[1]: (_legacy_row(CALENDAR[1]),),
        CALENDAR[2]: (_legacy_row(CALENDAR[2]),),
        target: (_legacy_row(target),),
    }
    first = run_i3_fixture_session(
        base.checkpoint,
        target_session=target,
        ordered_calendar_sessions=CALENDAR,
        resolved_row_reader=_RecordingReader(source),
        s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
        identity_policy_snapshot=EMPTY_SNAPSHOT,
        availability_session=date(2026, 2, 4),
    )
    second = run_i3_fixture_session(
        base.checkpoint,
        target_session=target,
        ordered_calendar_sessions=CALENDAR,
        resolved_row_reader=_RecordingReader(source),
        s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
        identity_policy_snapshot=EMPTY_SNAPSHOT,
        availability_session=date(2026, 2, 4),
    )

    assert first.result_digest == second.result_digest
    assert first.checkpoint.checkpoint_id == second.checkpoint.checkpoint_id


def test_corrupt_reader_window_and_checkpoint_are_rejected() -> None:
    base = _bootstrap()
    target = CALENDAR[3]

    def extra_session_reader(
        requested: tuple[date, ...],
    ) -> dict[date, tuple[dict[str, object], ...]]:
        return {
            **{session: (_legacy_row(session),) for session in requested},
            CALENDAR[0]: (_legacy_row(CALENDAR[0]),),
        }

    with pytest.raises(I3FixtureRunnerError, match="outside the fixed window"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=extra_session_reader,
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
            identity_policy_snapshot=EMPTY_SNAPSHOT,
            availability_session=date(2026, 2, 4),
        )

    object.__setattr__(base.checkpoint, "schema_digest", _digest("corrupt-schema"))
    with pytest.raises((I3CheckpointError, I3FixtureRunnerError)):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=target,
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader({}),
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], target),
            identity_policy_snapshot=EMPTY_SNAPSHOT,
            availability_session=date(2026, 2, 4),
        )


def test_legacy_v1_parent_cannot_masquerade_as_native_v2() -> None:
    base = _bootstrap()
    object.__setattr__(
        base.checkpoint.parent_release,
        "release_id",
        LEGACY_S7_V1_RELEASE_SET_ID,
    )

    with pytest.raises((I3CheckpointError, I3FixtureRunnerError), match="legacy S7 v1"):
        run_i3_fixture_session(
            base.checkpoint,
            target_session=CALENDAR[3],
            ordered_calendar_sessions=CALENDAR,
            resolved_row_reader=_RecordingReader({}),
            s4_window_binding=_s4_binding(CALENDAR[1], CALENDAR[2], CALENDAR[3]),
            identity_policy_snapshot=EMPTY_SNAPSHOT,
            availability_session=date(2026, 2, 4),
        )
