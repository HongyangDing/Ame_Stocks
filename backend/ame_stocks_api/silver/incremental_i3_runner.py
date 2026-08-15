"""Deterministic, fixture-only native-v2 I3 single-session runner.

This module is intentionally not a production entry point.  It accepts one
already-authenticated native-v2 checkpoint, an explicit ordered market
calendar, and a reader that is called exactly once for the target session and
the fixed two-session boundary lookback.  It performs no path discovery,
Parquet access, publication, correction, policy change, or remote mutation.

The implementation is useful for freezing the I3 state transition before a
separate staging package is authorised.  Physical artifacts are represented by
deterministic in-memory rows and content pins only.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from typing import Final

import pyarrow as pa

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.identity_resolution_contract import S7_DERIVED_CONTRACTS
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    ASSET_COUNTER_NAMES,
    ISSUER_COUNTER_NAMES,
    LEGACY_S7_V1_RELEASE_SET_ID,
    NATIVE_V2_FIXTURE_RELEASE_FAMILY,
    S4_TERMINAL_TABLE_ORDER,
    AggregateCount,
    AssetAggregateState,
    I3CheckpointState,
    IdentityPolicyBundle,
    IdentityRegistryKind,
    IssuerAggregateState,
    NativeV2OutputArtifact,
    NativeV2ParentReleasePin,
    NativeV2ReleaseManifest,
    OpenAliasState,
    ResolvedPartitionState,
    S4TerminalPartitionPin,
    TerminalRowVersionState,
    UnresolvedSubjectState,
    i3_resolved_state_digest,
)
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_CONTRACTS,
    I3_V2_SCHEMA_BUNDLE_DIGEST,
    I3_V2_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_i3_dispatch import (
    I3_QA_CATALOG,
    I3_QA_CATALOG_DIGEST,
    I3_RAW_REVIEW_CHECK_IDS,
    I3_ROW_VALIDATOR_SEMANTICS_DIGEST,
    ExactTradingCalendar,
    I3DispatchError,
    I3QaBoundedExample,
    I3QaReasonCount,
    I3QaReceipt,
    I3QaResult,
    IdentityObservation,
    IdentityPolicySnapshot,
    RegistryDecision,
    SourceCoverageSlot,
    _dispatch_i3_identity_window_from_verified_batch,
    _verify_i3_identity_policy_snapshot_for_batch,
    bind_alias_source_coverage,
    freeze_exact_trading_calendar,
)
from ame_stocks_api.silver.incremental_identity import (
    ALIAS_RESOLUTION_VERSION_ID_RULE_VERSION,
    ALIAS_SEGMENT_ID_RULE_VERSION,
    AliasResolutionDisposition,
    AliasResolutionMethod,
    AliasResolutionStatus,
    AliasResolutionVersion,
    AliasSegmentIdentity,
    ShareClassResolutionMethod,
    canonical_asset_id,
    canonical_issuer_id,
    canonical_share_class_id,
    successor_alias_resolution_version,
    validate_ticker_alias_mechanical_successor,
)

I3_FIXTURE_RUNNER_SEMANTICS: Final = "s7_5_i3_fixture_single_session_runner_v3"
I3_FIXTURE_INPUT_BINDING_RULE_VERSION: Final = "s7_5_i3_fixture_input_binding_v1"
I3_FIXTURE_RUNNER_SEMANTICS_DIGEST: Final = stable_digest(
    {
        "fixed_boundary_lookback_sessions": 2,
        "fixture_input_binding_rule_version": I3_FIXTURE_INPUT_BINDING_RULE_VERSION,
        "runner": I3_FIXTURE_RUNNER_SEMANTICS,
        "schema_bundle_digest": I3_V2_SCHEMA_BUNDLE_DIGEST,
        "row_validator_semantics_digest": I3_ROW_VALIDATOR_SEMANTICS_DIGEST,
    }
)
FIXED_BOUNDARY_LOOKBACK_SESSIONS: Final = 2
_SOURCE_TERMINAL_TABLES: Final = (
    "asset_observation_daily",
    "asset_observation_version",
    "universe_source_daily",
)
if _SOURCE_TERMINAL_TABLES != S4_TERMINAL_TABLE_ORDER:  # pragma: no cover - import-time guard
    raise RuntimeError("runner S4 table order differs from the checkpoint contract")

_REGISTRY_SOURCE_FIELDS: Final = {
    IdentityRegistryKind.IDENTITY_ADJUDICATION: (
        "source_identity_adjudication_release_id",
        "source_identity_adjudication_release_available_session",
    ),
    IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION: (
        "source_identity_cross_market_adjudication_release_id",
        "source_identity_cross_market_adjudication_release_available_session",
    ),
    IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE: (
        "source_provider_composite_override_release_id",
        "source_provider_composite_override_release_available_session",
    ),
    IdentityRegistryKind.SHARE_CLASS_ADJUDICATION: (
        "source_share_class_adjudication_release_id",
        "source_share_class_adjudication_release_available_session",
    ),
    IdentityRegistryKind.ASSET_TRANSITION: (
        "source_asset_transition_release_id",
        "source_asset_transition_release_available_session",
    ),
}
_SELECTED_DECISION_FIELDS: Final = (
    (
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        "identity_adjudication_id",
        "adjudication_available_session",
    ),
    (
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        "cross_market_adjudication_id",
        "cross_market_adjudication_available_session",
    ),
    (
        IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE,
        "provider_composite_override_id",
        "provider_composite_override_available_session",
    ),
    (
        IdentityRegistryKind.SHARE_CLASS_ADJUDICATION,
        "share_class_adjudication_id",
        "share_class_adjudication_available_session",
    ),
)
_COMPOSITE_SELECTED_DECISION_KINDS: Final = frozenset(
    {
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE,
    }
)
_SOURCE_BINDING_FIELDS: Final = (
    "source_s4_release_set_id",
    "source_s5_status_release_id",
    "source_s5_event_release_id",
    "source_s6_overview_release_id",
    "source_identity_case_candidate_manifest_id",
    "source_identity_case_candidate_manifest_sha256",
    "source_identity_adjudication_release_id",
    "source_identity_adjudication_release_available_session",
    "source_identity_market_consistency_candidate_manifest_id",
    "source_identity_market_consistency_candidate_manifest_sha256",
    "source_identity_cross_market_adjudication_release_id",
    "source_identity_cross_market_adjudication_release_available_session",
    "source_provider_composite_override_release_id",
    "source_provider_composite_override_release_available_session",
    "source_share_class_adjudication_release_id",
    "source_share_class_adjudication_release_available_session",
    "source_asset_transition_release_id",
    "source_asset_transition_release_available_session",
)


class I3FixtureRunnerError(ValueError):
    """Raised when a fixture transition cannot be proven from its exact inputs."""


ResolvedRowReader = Callable[[tuple[date, ...]], Mapping[date, Sequence[Mapping[str, object]]]]


@dataclass(frozen=True, slots=True)
class I3FixtureS4WindowBinding:
    """Exact fixture-only S4 pins for the target and two lookback sessions.

    The binding is supplied by the caller rather than inferred from resolved
    universe rows.  It therefore preserves the distinction between the three
    upstream S4 tables and makes every lookback partition independently
    content-addressed.
    """

    requested_sessions: tuple[date, ...]
    partition_pins: tuple[S4TerminalPartitionPin, ...]

    def __post_init__(self) -> None:
        if len(self.requested_sessions) not in {1, 3}:
            raise I3FixtureRunnerError(
                "S4 fixture binding must contain one bootstrap or three run sessions"
            )
        if self.requested_sessions != tuple(sorted(set(self.requested_sessions))):
            raise I3FixtureRunnerError("S4 fixture binding sessions must be sorted and unique")
        expected = tuple(
            (session, table)
            for session in self.requested_sessions
            for table in _SOURCE_TERMINAL_TABLES
        )
        actual = tuple((pin.session_date, pin.table_name) for pin in self.partition_pins)
        if actual != expected:
            raise I3FixtureRunnerError(
                "S4 fixture pins must use exact session-major terminal-table order"
            )
        paths = tuple(pin.artifact.path for pin in self.partition_pins)
        if len(paths) != len(set(paths)):
            raise I3FixtureRunnerError("S4 fixture partition artifact paths must be unique")

    @property
    def binding_digest(self) -> str:
        return stable_digest(
            {
                "partition_pins": [pin.to_dict() for pin in self.partition_pins],
                "requested_sessions": [item.isoformat() for item in self.requested_sessions],
            }
        )

    def pins_for_session(self, session: date) -> tuple[S4TerminalPartitionPin, ...]:
        result = tuple(pin for pin in self.partition_pins if pin.session_date == session)
        if len(result) != len(_SOURCE_TERMINAL_TABLES):  # pragma: no cover - constructor proves
            raise I3FixtureRunnerError("S4 fixture binding is missing a terminal table")
        return result


@dataclass(frozen=True, slots=True)
class I3FixtureRunResult:
    """Deterministic in-memory output of one fixture-only I3 transition."""

    target_session: date
    requested_sessions: tuple[date, ...]
    universe_daily_rows: tuple[dict[str, object], ...]
    ticker_alias_rows: tuple[dict[str, object], ...]
    asset_master_rows: tuple[dict[str, object], ...]
    issuer_master_rows: tuple[dict[str, object], ...]
    qa: dict[str, object]
    receipt: dict[str, object]
    checkpoint: I3CheckpointState
    release_manifest: NativeV2ReleaseManifest
    legacy_oracle_projection_digest: str | None = None

    @property
    def identity_policy_bundle_content(self) -> bytes:
        """Canonical bytes authenticated by the checkpoint's wrapper pin."""

        return self.checkpoint.identity_policy_bundle.canonical_bytes()

    @property
    def result_digest(self) -> str:
        return stable_digest(
            {
                "asset_master_rows": _jsonable(self.asset_master_rows),
                "checkpoint_id": self.checkpoint.checkpoint_id,
                "issuer_master_rows": _jsonable(self.issuer_master_rows),
                "identity_policy_bundle_content_sha256": hashlib.sha256(
                    self.identity_policy_bundle_content
                ).hexdigest(),
                "legacy_oracle_projection_digest": self.legacy_oracle_projection_digest,
                "qa": _jsonable(self.qa),
                "receipt": _jsonable(self.receipt),
                "release_manifest": self.release_manifest.to_dict(),
                "requested_sessions": [item.isoformat() for item in self.requested_sessions],
                "target_session": self.target_session.isoformat(),
                "ticker_alias_rows": _jsonable(self.ticker_alias_rows),
                "universe_daily_rows": _jsonable(self.universe_daily_rows),
            }
        )


@dataclass(frozen=True, slots=True)
class _DayMaterialization:
    universe_rows: tuple[dict[str, object], ...]
    alias_rows: tuple[dict[str, object], ...]
    asset_rows: tuple[dict[str, object], ...]
    issuer_rows: tuple[dict[str, object], ...]
    open_aliases: tuple[OpenAliasState, ...]
    asset_aggregates: tuple[AssetAggregateState, ...]
    issuer_aggregates: tuple[IssuerAggregateState, ...]
    unresolved_subjects: tuple[UnresolvedSubjectState, ...]
    terminal_rows: tuple[TerminalRowVersionState, ...]
    qa: dict[str, object]


@dataclass(frozen=True, slots=True)
class _DispatchBatchValidation:
    attestation_ids: tuple[str, ...]
    qa_receipt: I3QaReceipt


def run_i3_fixture_session(
    checkpoint: I3CheckpointState,
    *,
    target_session: date,
    ordered_calendar_sessions: Sequence[date],
    resolved_row_reader: ResolvedRowReader,
    s4_window_binding: I3FixtureS4WindowBinding,
    identity_policy_snapshot: IdentityPolicySnapshot,
    availability_session: date,
    reference_metadata_by_source_id: Mapping[str, Mapping[str, object]] | None = None,
    reference_metadata_available_session: date | None = None,
) -> I3FixtureRunResult:
    """Advance one native-v2 fixture checkpoint by exactly one market session.

    ``resolved_row_reader`` is invoked exactly once and receives only the target
    plus the two preceding calendar sessions.  Returned data for any other
    session is rejected.  Only target-session rows affect output state; the
    lookback is used for bounded boundary QA.
    """

    _require_native_checkpoint(checkpoint)
    if availability_session < checkpoint.availability_cutoff_session:
        raise I3FixtureRunnerError("run availability regressed behind the checkpoint")
    calendar = _calendar(ordered_calendar_sessions)
    exact_calendar = _fixture_dispatch_calendar(calendar)
    if exact_calendar.calendar_digest != checkpoint.calendar_digest:
        raise I3FixtureRunnerError("calendar digest differs from the native-v2 checkpoint")
    _require_policy_snapshot(
        identity_policy_snapshot, expected_bundle=checkpoint.identity_policy_bundle
    )
    try:
        previous_index = calendar.index(checkpoint.last_session)
    except ValueError as exc:
        raise I3FixtureRunnerError("checkpoint last_session is absent from the calendar") from exc
    if previous_index + 1 >= len(calendar) or calendar[previous_index + 1] != target_session:
        raise I3FixtureRunnerError("target is not the exact next calendar session")
    if previous_index < FIXED_BOUNDARY_LOOKBACK_SESSIONS - 1:
        raise I3FixtureRunnerError("exact two-session boundary lookback is unavailable")
    start = previous_index + 1 - FIXED_BOUNDARY_LOOKBACK_SESSIONS
    requested = tuple(calendar[start : previous_index + 2])
    _require_s4_window_binding(
        s4_window_binding,
        requested=requested,
        availability_session=availability_session,
    )
    window = _read_exact_window(resolved_row_reader, requested)
    normalized_metadata, fixture_input_binding = _freeze_fixture_input_binding(
        window=window,
        requested=requested,
        target_session=target_session,
        s4_window_binding=s4_window_binding,
        reference_metadata_by_source_id=reference_metadata_by_source_id or {},
        reference_metadata_available_session=reference_metadata_available_session,
        output_available_session=availability_session,
    )
    for session in requested:
        for row in window[session]:
            _require_policy_source_bindings(row, checkpoint.identity_policy_bundle)
    dispatch_validation = _dispatch_and_validate_target_rows(
        window=window,
        requested=requested,
        target_session=target_session,
        availability_session=availability_session,
        s4_window_binding=s4_window_binding,
        policy_snapshot=identity_policy_snapshot,
        exact_calendar=exact_calendar,
    )
    materialized = _materialize_day(
        target_rows=window[target_session],
        lookback_rows=tuple(row for session in requested for row in window[session]),
        target_session=target_session,
        calendar=calendar,
        availability_session=availability_session,
        policy=checkpoint.identity_policy_bundle,
        policy_snapshot=identity_policy_snapshot,
        prior_open_aliases=checkpoint.open_aliases,
        prior_assets=checkpoint.asset_aggregates,
        prior_issuers=checkpoint.issuer_aggregates,
        prior_unresolved=checkpoint.unresolved_subjects,
        prior_terminal_rows=checkpoint.terminal_row_versions,
        reference_metadata_by_source_id=normalized_metadata,
        reference_metadata_available_session=reference_metadata_available_session,
    )
    materialized = replace(
        materialized,
        qa=_finalize_qa(materialized.qa, dispatch_validation.qa_receipt),
    )
    _raise_for_critical_qa(materialized.qa)
    partition_pin = _content_pin(
        f"universe/session_date={target_session.isoformat()}",
        materialized.universe_rows,
    )
    partition_receipt_id = stable_digest(
        {
            "artifact": partition_pin.to_dict(),
            "row_count": len(materialized.universe_rows),
            "schema_digest": I3_V2_CONTRACTS["universe_daily"].schema_digest,
            "session_date": target_session.isoformat(),
        }
    )
    resolved_partitions = (
        *checkpoint.resolved_partition_map,
        ResolvedPartitionState(
            session_date=target_session,
            partition_receipt_id=partition_receipt_id,
            artifact=partition_pin,
            row_count=len(materialized.universe_rows),
            availability_session=availability_session,
        ),
    )
    source_pins = s4_window_binding.pins_for_session(target_session)
    source_cutoff_session = max(checkpoint.source_cutoff_session, target_session)
    resolved_state_digest = i3_resolved_state_digest(
        last_session=target_session,
        source_cutoff_session=source_cutoff_session,
        availability_cutoff_session=availability_session,
        s4_terminal_pins=source_pins,
        calendar_digest=checkpoint.calendar_digest,
        schema_digest=I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=I3_FIXTURE_RUNNER_SEMANTICS_DIGEST,
        identity_policy_bundle=checkpoint.identity_policy_bundle,
        identity_policy_bundle_artifact=checkpoint.identity_policy_bundle_artifact,
        open_aliases=materialized.open_aliases,
        asset_aggregates=materialized.asset_aggregates,
        issuer_aggregates=materialized.issuer_aggregates,
        unresolved_subjects=materialized.unresolved_subjects,
        resolved_partition_map=resolved_partitions,
        terminal_row_versions=materialized.terminal_rows,
    )
    release_manifest = _result_release_manifest(
        prior_release=checkpoint.parent_release,
        source_checkpoint_id=checkpoint.checkpoint_id,
        target_session=target_session,
        availability_session=availability_session,
        resolved_state_digest=resolved_state_digest,
        materialized=materialized,
        universe_partition_pin=partition_pin,
    )
    parent = NativeV2ParentReleasePin.from_manifest(
        release_manifest,
        path=(
            "fixtures/i3/releases/"
            f"session_date={target_session.isoformat()}/"
            f"partition_receipt_id={partition_receipt_id}/manifest.json"
        ),
    )
    next_checkpoint = I3CheckpointState(
        parent_release=parent,
        last_session=target_session,
        source_cutoff_session=source_cutoff_session,
        availability_cutoff_session=availability_session,
        s4_terminal_pins=source_pins,
        calendar_digest=checkpoint.calendar_digest,
        schema_digest=I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=I3_FIXTURE_RUNNER_SEMANTICS_DIGEST,
        identity_policy_bundle=checkpoint.identity_policy_bundle,
        identity_policy_bundle_artifact=checkpoint.identity_policy_bundle_artifact,
        open_aliases=materialized.open_aliases,
        asset_aggregates=materialized.asset_aggregates,
        issuer_aggregates=materialized.issuer_aggregates,
        unresolved_subjects=materialized.unresolved_subjects,
        resolved_partition_map=resolved_partitions,
        terminal_row_versions=materialized.terminal_rows,
    )
    receipt = _receipt(
        checkpoint=checkpoint,
        next_checkpoint=next_checkpoint,
        requested=requested,
        target_session=target_session,
        materialized=materialized,
        s4_window_binding=s4_window_binding,
        dispatch_attestation_ids=dispatch_validation.attestation_ids,
        fixture_input_binding=fixture_input_binding,
    )
    return I3FixtureRunResult(
        target_session=target_session,
        requested_sessions=requested,
        universe_daily_rows=materialized.universe_rows,
        ticker_alias_rows=materialized.alias_rows,
        asset_master_rows=materialized.asset_rows,
        issuer_master_rows=materialized.issuer_rows,
        qa=materialized.qa,
        receipt=receipt,
        checkpoint=next_checkpoint,
        release_manifest=release_manifest,
    )


def bootstrap_native_v2_fixture(
    legacy_universe_rows: Sequence[Mapping[str, object]],
    *,
    session_date: date,
    ordered_calendar_sessions: Sequence[date],
    identity_policy_bundle: IdentityPolicyBundle,
    identity_policy_snapshot: IdentityPolicySnapshot,
    s4_source_binding: I3FixtureS4WindowBinding,
    availability_session: date,
    legacy_oracle_release_set_id: str = LEGACY_S7_V1_RELEASE_SET_ID,
    reference_metadata_by_source_id: Mapping[str, Mapping[str, object]] | None = None,
    reference_metadata_available_session: date | None = None,
) -> I3FixtureRunResult:
    """Create a one-session native-v2 fixture base and its v1 oracle proof.

    This helper cannot adopt a v1 release as its parent.  It constructs a new
    native-v2 migration and release identity, while binding the immutable v1
    release set only as an equivalence oracle.
    """

    if legacy_oracle_release_set_id != LEGACY_S7_V1_RELEASE_SET_ID:
        raise I3FixtureRunnerError("fixture bootstrap binds the wrong immutable v1 oracle")
    calendar = _calendar(ordered_calendar_sessions)
    exact_calendar = _fixture_dispatch_calendar(calendar)
    _require_policy_snapshot(identity_policy_snapshot, expected_bundle=identity_policy_bundle)
    try:
        verified_policy = _verify_i3_identity_policy_snapshot_for_batch(identity_policy_snapshot)
    except I3DispatchError as exc:
        raise I3FixtureRunnerError("fixture bootstrap identity policy snapshot is invalid") from exc
    if session_date not in calendar:
        raise I3FixtureRunnerError("bootstrap session is absent from the calendar")
    _require_s4_window_binding(
        s4_source_binding,
        requested=(session_date,),
        availability_session=availability_session,
    )
    rows = tuple(dict(row) for row in legacy_universe_rows)
    normalized_metadata, fixture_input_binding = _freeze_fixture_input_binding(
        window={session_date: rows},
        requested=(session_date,),
        target_session=session_date,
        s4_window_binding=s4_source_binding,
        reference_metadata_by_source_id=reference_metadata_by_source_id or {},
        reference_metadata_available_session=reference_metadata_available_session,
        output_available_session=availability_session,
    )
    for row in rows:
        _require_policy_source_bindings(row, identity_policy_bundle)
        _require_fixture_bootstrap_root_row(row)
        try:
            applicable_decisions = verified_policy.matching_decisions(_identity_observation(row))
        except I3DispatchError as exc:
            raise I3FixtureRunnerError(
                "fixture bootstrap identity policy snapshot changed during validation"
            ) from exc
        if applicable_decisions:
            raise I3FixtureRunnerError(
                "fixture bootstrap root row has an applicable registry decision"
            )
        _validate_selected_decision_availability(
            row,
            policy_snapshot=identity_policy_snapshot,
            output_available_session=availability_session,
        )
    materialized = _materialize_day(
        target_rows=rows,
        lookback_rows=rows,
        target_session=session_date,
        calendar=calendar,
        availability_session=availability_session,
        policy=identity_policy_bundle,
        policy_snapshot=identity_policy_snapshot,
        prior_open_aliases=(),
        prior_assets=(),
        prior_issuers=(),
        prior_unresolved=(),
        prior_terminal_rows=(),
        reference_metadata_by_source_id=normalized_metadata,
        reference_metadata_available_session=reference_metadata_available_session,
    )
    materialized = replace(
        materialized,
        qa=_finalize_qa(materialized.qa, _empty_dispatch_qa_receipt()),
    )
    _raise_for_critical_qa(materialized.qa)
    oracle_rows = tuple(
        legacy_oracle_universe_projection(v2, legacy)
        for v2, legacy in zip(
            materialized.universe_rows,
            sorted(rows, key=lambda item: str(item["ticker"])),
            strict=True,
        )
    )
    expected_oracle = tuple(
        _normalise_row_for_digest(row) for row in sorted(rows, key=lambda item: str(item["ticker"]))
    )
    if tuple(_normalise_row_for_digest(row) for row in oracle_rows) != expected_oracle:
        raise I3FixtureRunnerError("native-v2 fixture does not project to its v1 oracle")
    oracle_digest = stable_digest(_jsonable(oracle_rows))
    migration_id = stable_digest(
        {
            "legacy_oracle_projection_digest": oracle_digest,
            "legacy_oracle_release_set_id": legacy_oracle_release_set_id,
            "schema_bundle_digest": I3_V2_SCHEMA_BUNDLE_DIGEST,
            "session_date": session_date.isoformat(),
        }
    )
    partition_pin = _content_pin(
        f"bootstrap/universe/session_date={session_date.isoformat()}",
        materialized.universe_rows,
    )
    partition_receipt_id = stable_digest(
        {
            "artifact": partition_pin.to_dict(),
            "oracle_digest": oracle_digest,
            "row_count": len(materialized.universe_rows),
        }
    )
    source_cutoff_session = max(session_date, identity_policy_bundle.policy_cutoff_session)
    source_pins = s4_source_binding.pins_for_session(session_date)
    policy_artifact = identity_policy_bundle.exact_pin(
        path="fixtures/i3/identity-policy-bundle.json"
    )
    resolved_partitions = (
        ResolvedPartitionState(
            session_date=session_date,
            partition_receipt_id=partition_receipt_id,
            artifact=partition_pin,
            row_count=len(materialized.universe_rows),
            availability_session=availability_session,
        ),
    )
    resolved_state_digest = i3_resolved_state_digest(
        last_session=session_date,
        source_cutoff_session=source_cutoff_session,
        availability_cutoff_session=availability_session,
        s4_terminal_pins=source_pins,
        calendar_digest=exact_calendar.calendar_digest,
        schema_digest=I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=I3_FIXTURE_RUNNER_SEMANTICS_DIGEST,
        identity_policy_bundle=identity_policy_bundle,
        identity_policy_bundle_artifact=policy_artifact,
        open_aliases=materialized.open_aliases,
        asset_aggregates=materialized.asset_aggregates,
        issuer_aggregates=materialized.issuer_aggregates,
        unresolved_subjects=materialized.unresolved_subjects,
        resolved_partition_map=resolved_partitions,
        terminal_row_versions=materialized.terminal_rows,
    )
    release_manifest = NativeV2ReleaseManifest(
        release_family=NATIVE_V2_FIXTURE_RELEASE_FAMILY,
        terminal_session=session_date,
        release_available_session=availability_session,
        native_v2_migration_id=migration_id,
        identity_policy_bundle_id=identity_policy_bundle.identity_policy_bundle_id,
        transform_semantics_digest=I3_FIXTURE_RUNNER_SEMANTICS_DIGEST,
        resolved_state_digest=resolved_state_digest,
        output_artifacts=_materialized_output_pins(
            session_date=session_date,
            universe_partition_pin=partition_pin,
            materialized=materialized,
        ),
        parent_release_id=None,
        source_checkpoint_id=None,
        legacy_oracle_release_set_id=legacy_oracle_release_set_id,
    )
    parent = NativeV2ParentReleasePin.from_manifest(
        release_manifest,
        path="fixtures/i3/bootstrap/native-v2-parent-manifest.json",
    )
    release_id = parent.release_id
    if release_id == legacy_oracle_release_set_id:  # pragma: no cover - hash-domain guard
        raise I3FixtureRunnerError("legacy v1 release masqueraded as native v2")
    checkpoint = I3CheckpointState(
        parent_release=parent,
        last_session=session_date,
        source_cutoff_session=source_cutoff_session,
        availability_cutoff_session=availability_session,
        s4_terminal_pins=source_pins,
        calendar_digest=exact_calendar.calendar_digest,
        schema_digest=I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=I3_FIXTURE_RUNNER_SEMANTICS_DIGEST,
        identity_policy_bundle=identity_policy_bundle,
        identity_policy_bundle_artifact=policy_artifact,
        open_aliases=materialized.open_aliases,
        asset_aggregates=materialized.asset_aggregates,
        issuer_aggregates=materialized.issuer_aggregates,
        unresolved_subjects=materialized.unresolved_subjects,
        resolved_partition_map=resolved_partitions,
        terminal_row_versions=materialized.terminal_rows,
    )
    receipt = _freeze_fixture_receipt(
        {
            "artifact_type": "s7_5_i3_fixture_native_v2_bootstrap_receipt",
            "checkpoint_id": checkpoint.checkpoint_id,
            "dispatcher_qa_receipt_id": materialized.qa["dispatcher_qa_receipt_id"],
            "legacy_oracle_projection_digest": oracle_digest,
            "legacy_oracle_release_set_id": legacy_oracle_release_set_id,
            "native_v2_migration_id": migration_id,
            "native_v2_release_id": release_id,
            "publish_authorized": False,
            "qa_catalog_digest": materialized.qa["qa_catalog_digest"],
            "qa_result_digest": materialized.qa["qa_result_digest"],
            "runner_semantics_digest": I3_FIXTURE_RUNNER_SEMANTICS_DIGEST,
            "s4_source_binding_digest": s4_source_binding.binding_digest,
            "fixture_input_binding": fixture_input_binding,
            "fixture_input_binding_digest": fixture_input_binding["binding_digest"],
        }
    )
    return I3FixtureRunResult(
        target_session=session_date,
        requested_sessions=(session_date,),
        universe_daily_rows=materialized.universe_rows,
        ticker_alias_rows=materialized.alias_rows,
        asset_master_rows=materialized.asset_rows,
        issuer_master_rows=materialized.issuer_rows,
        qa=materialized.qa,
        receipt=receipt,
        checkpoint=checkpoint,
        release_manifest=release_manifest,
        legacy_oracle_projection_digest=oracle_digest,
    )


def bootstrap_native_v2_fixture_history(
    resolved_rows_by_session: Mapping[date, Sequence[Mapping[str, object]]],
    *,
    bootstrap_session: date,
    terminal_session: date,
    ordered_calendar_sessions: Sequence[date],
    identity_policy_bundle: IdentityPolicyBundle,
    identity_policy_snapshot: IdentityPolicySnapshot,
    s4_bindings_by_target: Mapping[date, I3FixtureS4WindowBinding],
    availability_by_target: Mapping[date, date],
    legacy_oracle_release_set_id: str = LEGACY_S7_V1_RELEASE_SET_ID,
    reference_metadata_by_source_id: Mapping[str, Mapping[str, object]] | None = None,
    reference_metadata_available_session: date | None = None,
) -> I3FixtureRunResult:
    """Build a fixture base and advance a contiguous, explicitly pinned history.

    The caller supplies every boundary source session, per-target S4 binding,
    and availability date.  This is a deterministic test convenience only; it
    does not discover files or relax the single-session runner's three-session
    window.
    """

    calendar = _calendar(ordered_calendar_sessions)
    try:
        bootstrap_index = calendar.index(bootstrap_session)
        terminal_index = calendar.index(terminal_session)
    except ValueError as exc:
        raise I3FixtureRunnerError("fixture-history boundary is absent from the calendar") from exc
    if bootstrap_index > terminal_index:
        raise I3FixtureRunnerError("fixture-history terminal precedes bootstrap")
    targets = calendar[bootstrap_index : terminal_index + 1]
    expected_binding_targets = set(targets)
    if set(s4_bindings_by_target) != expected_binding_targets:
        raise I3FixtureRunnerError("fixture-history S4 target bindings are incomplete or extra")
    if set(availability_by_target) != expected_binding_targets:
        raise I3FixtureRunnerError("fixture-history availability map is incomplete or extra")
    required_source_sessions: set[date] = {bootstrap_session}
    for target_index in range(bootstrap_index + 1, terminal_index + 1):
        if target_index < FIXED_BOUNDARY_LOOKBACK_SESSIONS:
            raise I3FixtureRunnerError("fixture history lacks the exact boundary lookback")
        required_source_sessions.update(calendar[target_index - 2 : target_index + 1])
    if set(resolved_rows_by_session) != required_source_sessions:
        raise I3FixtureRunnerError("fixture-history source sessions are incomplete or extra")

    current = bootstrap_native_v2_fixture(
        resolved_rows_by_session[bootstrap_session],
        session_date=bootstrap_session,
        ordered_calendar_sessions=calendar,
        identity_policy_bundle=identity_policy_bundle,
        identity_policy_snapshot=identity_policy_snapshot,
        s4_source_binding=s4_bindings_by_target[bootstrap_session],
        availability_session=availability_by_target[bootstrap_session],
        legacy_oracle_release_set_id=legacy_oracle_release_set_id,
        reference_metadata_by_source_id=reference_metadata_by_source_id,
        reference_metadata_available_session=reference_metadata_available_session,
    )
    for target_index in range(bootstrap_index + 1, terminal_index + 1):
        target = calendar[target_index]
        requested = tuple(calendar[target_index - 2 : target_index + 1])

        def reader(
            received: tuple[date, ...],
            *,
            expected: tuple[date, ...] = requested,
        ) -> Mapping[date, Sequence[Mapping[str, object]]]:
            if received != expected:  # pragma: no cover - runner supplies exact value
                raise I3FixtureRunnerError("fixture-history reader received another window")
            return {session: resolved_rows_by_session[session] for session in expected}

        current = run_i3_fixture_session(
            current.checkpoint,
            target_session=target,
            ordered_calendar_sessions=calendar,
            resolved_row_reader=reader,
            s4_window_binding=s4_bindings_by_target[target],
            identity_policy_snapshot=identity_policy_snapshot,
            availability_session=availability_by_target[target],
            reference_metadata_by_source_id=reference_metadata_by_source_id,
            reference_metadata_available_session=reference_metadata_available_session,
        )
    return current


def legacy_oracle_universe_projection(
    native_v2_row: Mapping[str, object],
    legacy_v1_row: Mapping[str, object],
) -> dict[str, object]:
    """Project a v2 membership back to the exact supplied immutable v1 oracle."""

    expected_v2 = {item.name for item in I3_V2_CONTRACTS["universe_daily"].columns}
    if set(native_v2_row) != expected_v2:
        raise I3FixtureRunnerError("native-v2 universe row fields differ from its contract")
    expected_v1 = {item.name for item in S7_DERIVED_CONTRACTS["universe_daily"].columns}
    if set(legacy_v1_row) != expected_v1:
        raise I3FixtureRunnerError("legacy universe oracle row fields differ from v1")
    added = {
        "alias_segment_id",
        "alias_resolution_version_id",
        "asset_master_version_id",
        "issuer_master_version_id",
        "identity_policy_bundle_id",
        "row_available_session",
    }
    projected = {key: value for key, value in native_v2_row.items() if key not in added}
    projected["ticker_alias_id"] = legacy_v1_row["ticker_alias_id"]
    return projected


def _require_fixture_bootstrap_root_row(row: Mapping[str, object]) -> None:
    """Keep the one-session fixture root narrower than a production base scan."""

    selected_fields = tuple(item[1] for item in _SELECTED_DECISION_FIELDS)
    selected_availability_fields = tuple(item[2] for item in _SELECTED_DECISION_FIELDS)
    if any(row.get(name) is not None for name in selected_fields):
        raise I3FixtureRunnerError("fixture bootstrap cannot consume selected registry decisions")
    if any(row.get(name) is not None for name in selected_availability_fields):
        raise I3FixtureRunnerError(
            "fixture bootstrap cannot consume selected decision availability"
        )
    if tuple(row.get("asset_transition_ids") or ()):
        raise I3FixtureRunnerError("fixture bootstrap cannot consume asset transitions")
    if (
        not bool(row.get("backtest_identity_eligible"))
        or bool(row.get("composite_registry_collision"))
        or row.get("composite_registry_match_count") != 0
        or row.get("observed_composite_market_code") not in {"US", "USA"}
        or row.get("canonical_composite_market_code") not in {"US", "USA"}
        or row.get("observed_composite_figi") != row.get("canonical_composite_figi")
        or row.get("observed_asset_id") != row.get("asset_id")
        or row.get("observed_share_class_figi") != row.get("canonical_share_class_figi")
        or row.get("observed_cik_normalized") != row.get("canonical_cik_normalized")
        or row.get("identity_resolution_method") != "source_composite_figi_exact"
        or row.get("identity_disposition") != "observed_consistent"
    ):
        raise I3FixtureRunnerError(
            "fixture bootstrap requires direct-observed known-US identity roots"
        )


def _materialize_day(
    *,
    target_rows: Sequence[Mapping[str, object]],
    lookback_rows: Sequence[Mapping[str, object]],
    target_session: date,
    calendar: tuple[date, ...],
    availability_session: date,
    policy: IdentityPolicyBundle,
    policy_snapshot: IdentityPolicySnapshot,
    prior_open_aliases: Sequence[OpenAliasState],
    prior_assets: Sequence[AssetAggregateState],
    prior_issuers: Sequence[IssuerAggregateState],
    prior_unresolved: Sequence[UnresolvedSubjectState],
    prior_terminal_rows: Sequence[TerminalRowVersionState],
    reference_metadata_by_source_id: Mapping[str, Mapping[str, object]],
    reference_metadata_available_session: date | None,
) -> _DayMaterialization:
    if availability_session < max(target_session, policy.policy_available_session):
        raise I3FixtureRunnerError("run availability precedes source or policy availability")
    rows = tuple(_validate_target_row(row, target_session, policy) for row in target_rows)
    tickers = [str(row["ticker"]) for row in rows]
    if tickers != sorted(tickers) or len(tickers) != len(set(tickers)):
        raise I3FixtureRunnerError("target rows must be sorted and unique by ticker")
    calendar_index = {session: index for index, session in enumerate(calendar)}
    prior_open_by_ticker: dict[str, OpenAliasState] = {}
    for item in prior_open_aliases:
        if item.segment.ticker in prior_open_by_ticker:
            raise I3FixtureRunnerError("checkpoint contains multiple open aliases for one ticker")
        prior_open_by_ticker[item.segment.ticker] = item
    terminal = {item.map_key: item for item in prior_terminal_rows}
    assets = {item.asset_id: item for item in prior_assets}
    issuers = {item.issuer_id: item for item in prior_issuers}
    prior_asset_ids = frozenset(assets)
    prior_issuer_ids = frozenset(issuers)
    unresolved = {(item.subject_kind, item.subject_key): item for item in prior_unresolved}
    if len(unresolved) != len(prior_unresolved):
        raise I3FixtureRunnerError("checkpoint has duplicate unresolved logical subjects")

    open_aliases: list[OpenAliasState] = []
    alias_rows: list[dict[str, object]] = []
    row_aliases: dict[str, OpenAliasState] = {}
    unresolved_target_rows = 0
    for row in rows:
        ticker = str(row["ticker"])
        eligible = bool(row["backtest_identity_eligible"])
        collision = bool(row["composite_registry_collision"])
        if not eligible or collision:
            unresolved_target_rows += 1
            reason = (
                "registry_collision"
                if collision
                else _reason_code(str(row["identity_disposition"]))
            )
            unresolved[("ticker_identity", ticker)] = _advance_unresolved(
                unresolved.get(("ticker_identity", ticker)),
                ticker=ticker,
                target_session=target_session,
                reason=reason,
                source_record_id=str(row["selected_source_record_id"]),
                availability_session=availability_session,
            )
            continue
        segment, resolution, operation = _alias_for_target(
            row,
            prior=prior_open_by_ticker.get(ticker),
            target_session=target_session,
            policy=policy,
        )
        state = OpenAliasState(segment=segment, resolution=resolution)
        open_aliases.append(state)
        row_aliases[ticker] = state
        alias_row = _alias_physical_row(
            row,
            segment=segment,
            resolution=resolution,
            availability_session=availability_session,
            calendar_index=calendar_index,
        )
        alias_rows.append(alias_row)
        terminal["ticker_alias", segment.alias_segment_id] = _terminal_state(
            "ticker_alias",
            segment.alias_segment_id,
            resolution.alias_resolution_version_id,
            resolution.predecessor_alias_resolution_version_id,
            alias_row,
            availability_session,
            operation=operation,
        )
        unresolved.pop(("ticker_identity", ticker), None)

    touched_assets: set[str] = set()
    touched_issuers: set[str] = set()
    first_asset_rows: dict[str, Mapping[str, object]] = {}
    first_issuer_rows: dict[str, Mapping[str, object]] = {}
    for row in rows:
        asset_id = row.get("asset_id")
        if isinstance(asset_id, str):
            assets[asset_id] = _advance_asset(
                assets.get(asset_id),
                row,
                availability_session=availability_session,
            )
            touched_assets.add(asset_id)
            first_asset_rows.setdefault(asset_id, row)
        issuer_id = row.get("issuer_id")
        cik = row.get("canonical_cik_normalized")
        if isinstance(issuer_id, str) and isinstance(cik, str):
            metadata = reference_metadata_by_source_id.get(
                str(row["selected_source_record_id"]), {}
            )
            advanced_issuer = _advance_issuer(
                issuers.get(issuer_id),
                row,
                availability_session=availability_session,
                policy=policy,
                reference_name=metadata.get("reference_name"),
                sic_code=metadata.get("sic_code"),
                reference_metadata_available_session=(
                    reference_metadata_available_session if metadata else None
                ),
            )
            if advanced_issuer is not None:
                issuers[issuer_id] = advanced_issuer
                touched_issuers.add(issuer_id)
                first_issuer_rows.setdefault(issuer_id, row)

    _apply_symmetric_asset_transition_lineage(
        rows=rows,
        assets=assets,
        touched_assets=touched_assets,
        first_asset_rows=first_asset_rows,
        policy_snapshot=policy_snapshot,
        availability_session=availability_session,
    )

    asset_rows: list[dict[str, object]] = []
    for asset_id in sorted(touched_assets):
        state = assets[asset_id]
        predecessor = state.terminal_row_version_id if asset_id in prior_asset_ids else None
        row, state = _asset_master_row(
            state,
            predecessor=predecessor,
            source_row=first_asset_rows[asset_id],
            availability_session=availability_session,
        )
        assets[asset_id] = state
        asset_rows.append(row)
        terminal["asset_master", asset_id] = _terminal_state(
            "asset_master",
            asset_id,
            state.terminal_row_version_id,
            predecessor,
            row,
            availability_session,
            operation="new_root" if predecessor is None else "mechanical_successor",
        )
    issuer_rows: list[dict[str, object]] = []
    for issuer_id in sorted(touched_issuers):
        state = issuers[issuer_id]
        predecessor = state.terminal_row_version_id if issuer_id in prior_issuer_ids else None
        row, state = _issuer_master_row(
            state,
            predecessor=predecessor,
            source_row=first_issuer_rows[issuer_id],
            availability_session=availability_session,
        )
        issuers[issuer_id] = state
        issuer_rows.append(row)
        terminal["issuer_master", issuer_id] = _terminal_state(
            "issuer_master",
            issuer_id,
            state.terminal_row_version_id,
            predecessor,
            row,
            availability_session,
            operation="new_root" if predecessor is None else "mechanical_successor",
        )

    universe_rows: list[dict[str, object]] = []
    for source in rows:
        row = dict(source)
        legacy_alias = row.pop("ticker_alias_id")
        ticker = str(row["ticker"])
        alias = row_aliases.get(ticker)
        if alias is None:
            if legacy_alias is not None and not bool(row["backtest_identity_eligible"]):
                raise I3FixtureRunnerError("ineligible legacy membership unexpectedly has an alias")
            alias_segment = alias_version = None
        else:
            alias_segment = alias.segment.alias_segment_id
            alias_version = alias.resolution.alias_resolution_version_id
        asset_id = row.get("asset_id")
        issuer_id = row.get("issuer_id")
        eligible = bool(row["backtest_identity_eligible"])
        issuer_state = (
            issuers.get(str(issuer_id)) if eligible and isinstance(issuer_id, str) else None
        )
        row.update(
            {
                "alias_segment_id": alias_segment,
                "alias_resolution_version_id": alias_version,
                "asset_master_version_id": (
                    assets[str(asset_id)].terminal_row_version_id
                    if eligible and isinstance(asset_id, str)
                    else None
                ),
                "issuer_master_version_id": (
                    issuer_state.terminal_row_version_id if issuer_state is not None else None
                ),
                "identity_policy_bundle_id": policy.identity_policy_bundle_id,
                "row_available_session": availability_session,
            }
        )
        if row["identity_quality_liquidation_signal"] is not False:
            raise I3FixtureRunnerError("identity quality attempted to force liquidation")
        universe_rows.append(row)

    _validate_v2_rows("universe_daily", universe_rows)
    _validate_v2_rows("ticker_alias", alias_rows)
    _validate_v2_rows("asset_master", asset_rows)
    _validate_v2_rows("issuer_master", issuer_rows)
    qa = _build_qa(
        rows=rows,
        lookback_rows=lookback_rows,
        universe_rows=universe_rows,
        alias_rows=alias_rows,
        asset_rows=asset_rows,
        issuer_rows=issuer_rows,
        unresolved_target_rows=unresolved_target_rows,
    )
    return _DayMaterialization(
        universe_rows=tuple(universe_rows),
        alias_rows=tuple(alias_rows),
        asset_rows=tuple(asset_rows),
        issuer_rows=tuple(issuer_rows),
        open_aliases=tuple(sorted(open_aliases, key=lambda item: item.segment.alias_segment_id)),
        asset_aggregates=tuple(sorted(assets.values(), key=lambda item: item.asset_id)),
        issuer_aggregates=tuple(sorted(issuers.values(), key=lambda item: item.issuer_id)),
        unresolved_subjects=tuple(
            sorted(unresolved.values(), key=lambda item: item.unresolved_subject_id)
        ),
        terminal_rows=tuple(sorted(terminal.values(), key=lambda item: item.map_key)),
        qa=qa,
    )


def _alias_for_target(
    row: Mapping[str, object],
    *,
    prior: OpenAliasState | None,
    target_session: date,
    policy: IdentityPolicyBundle,
) -> tuple[AliasSegmentIdentity, AliasResolutionVersion, str]:
    if prior is not None and _same_segment_subject(prior.segment, row):
        target_evidence_available = _native_date(
            row["identity_evidence_available_session"],
            "identity evidence availability",
        )
        evidence_available = max(
            prior.resolution.evidence_available_session,
            target_evidence_available,
        )
        resolution_available = max(
            prior.resolution.resolution_available_session,
            policy.bundle_available_session,
            evidence_available,
        )
        identity_cutoff = max(
            prior.resolution.identity_cutoff_session,
            policy.policy_cutoff_session,
            resolution_available,
        )
        source_digest = stable_digest(
            {
                "append_source_record_id": row["selected_source_record_id"],
                "previous_source_record_set_digest": prior.resolution.source_record_set_digest,
            }
        )
        successor = successor_alias_resolution_version(
            prior.resolution,
            segment=prior.segment,
            identity_cutoff_session=identity_cutoff,
            resolution_available_session=resolution_available,
            evidence_cutoff_session=identity_cutoff,
            evidence_available_session=evidence_available,
            valid_through_session=target_session,
            source_record_set_digest=source_digest,
        )
        _require_resolution_matches_row(successor, row, policy)
        validate_ticker_alias_mechanical_successor(
            prior.resolution,
            successor,
            prior.segment,
        )
        return prior.segment, successor, "mechanical_successor"
    segment = AliasSegmentIdentity(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker=str(row["ticker"]),
        observed_composite_figi=_optional_string(row.get("observed_composite_figi")),
        observed_share_class_figi=_optional_string(row.get("observed_share_class_figi")),
        observed_cik_normalized=_optional_string(row.get("observed_cik_normalized")),
        valid_from_session=target_session,
        segment_origin_source_record_id=str(row["selected_source_record_id"]),
    )
    version = _resolution_root(segment, row=row, policy=policy)
    return segment, version, "new_root"


def _resolution_root(
    segment: AliasSegmentIdentity,
    *,
    row: Mapping[str, object],
    policy: IdentityPolicyBundle,
) -> AliasResolutionVersion:
    method, disposition, decision_ids = _resolution_shape(row)
    share_method = (
        ShareClassResolutionMethod.APPROVED_SHARE_CLASS_ADJUDICATION
        if row.get("share_class_adjudication_id") is not None
        else ShareClassResolutionMethod.DIRECT_OBSERVED
        if row.get("observed_share_class_figi") is not None
        else ShareClassResolutionMethod.NOT_APPLICABLE
    )
    share_ids = (
        ()
        if row.get("share_class_adjudication_id") is None
        else (str(row["share_class_adjudication_id"]),)
    )
    evidence_available = _native_date(
        row["identity_evidence_available_session"], "identity evidence availability"
    )
    resolution_available = max(policy.bundle_available_session, evidence_available)
    identity_cutoff = max(policy.policy_cutoff_session, resolution_available)
    return AliasResolutionVersion.for_segment(
        segment,
        canonical_asset_id=str(row["asset_id"]),
        canonical_composite_figi=str(row["canonical_composite_figi"]),
        canonical_share_class_id=_optional_string(row.get("share_class_id")),
        canonical_share_class_figi=_optional_string(row.get("canonical_share_class_figi")),
        canonical_issuer_id=_optional_string(row.get("issuer_id")),
        canonical_cik_normalized=_optional_string(row.get("canonical_cik_normalized")),
        resolution_method=method,
        resolution_status=AliasResolutionStatus.RESOLVED,
        disposition=disposition,
        decision_lineage_ids=decision_ids,
        share_class_resolution_method=share_method,
        share_class_decision_lineage_ids=share_ids,
        identity_policy_bundle_id=policy.identity_policy_bundle_id,
        identity_cutoff_session=identity_cutoff,
        # Wrapper availability is when this project may first consume the
        # resolution; evidence availability remains the source-era timeline.
        resolution_available_session=resolution_available,
        evidence_cutoff_session=identity_cutoff,
        evidence_available_session=evidence_available,
        valid_through_session=segment.valid_from_session,
        source_record_set_digest=stable_digest(
            {"source_record_ids": [segment.segment_origin_source_record_id]}
        ),
        predecessor_alias_resolution_version_id=None,
        is_tombstone=False,
        tombstone_reason_code=None,
    )


def _resolution_shape(
    row: Mapping[str, object],
) -> tuple[AliasResolutionMethod, AliasResolutionDisposition, tuple[str, ...]]:
    method = str(row["identity_resolution_method"])
    raw_disposition = str(row["identity_disposition"])
    identities = tuple(
        sorted(
            str(value)
            for value in (
                row.get("identity_adjudication_id"),
                row.get("cross_market_adjudication_id"),
                row.get("provider_composite_override_id"),
                *tuple(row.get("asset_transition_ids") or ()),
            )
            if value is not None
        )
    )
    if method == "source_composite_figi_exact":
        disposition = (
            AliasResolutionDisposition.TRANSIENT_DUPLICATE_SHARE_CLASS
            if row.get("share_class_adjudication_id") is not None
            else AliasResolutionDisposition.OBSERVED_CONSISTENT
        )
        return (
            AliasResolutionMethod.DIRECT_OBSERVED,
            disposition,
            (),
        )
    if method == "approved_genuine_transition":
        return (
            AliasResolutionMethod.APPROVED_GENUINE_TRANSITION,
            AliasResolutionDisposition.CONFIRMED_GENUINE_TRANSITION,
            identities,
        )
    if row.get("provider_composite_override_id") is not None:
        return (
            AliasResolutionMethod.APPROVED_PROVIDER_COMPOSITE_OVERRIDE,
            AliasResolutionDisposition.PROVIDER_COMPOSITE_STALE_AFTER_TRANSITION,
            identities,
        )
    if method == "approved_cross_market_provider_contamination_override":
        return (
            AliasResolutionMethod.APPROVED_CROSS_MARKET_OVERRIDE,
            AliasResolutionDisposition.CONFIRMED_PROVIDER_CONTAMINATION,
            identities,
        )
    if method == "approved_provider_contamination_override":
        disposition = (
            AliasResolutionDisposition.CONFIRMED_GENUINE_TRANSITION
            if raw_disposition == "confirmed_genuine_transition"
            else AliasResolutionDisposition.CONFIRMED_PROVIDER_CONTAMINATION
        )
        mapped = (
            AliasResolutionMethod.APPROVED_GENUINE_TRANSITION
            if disposition is AliasResolutionDisposition.CONFIRMED_GENUINE_TRANSITION
            else AliasResolutionMethod.APPROVED_PROVIDER_CONTAMINATION_OVERRIDE
        )
        return mapped, disposition, identities
    raise I3FixtureRunnerError("eligible row uses an unsupported identity resolution shape")


def _require_resolution_matches_row(
    resolution: AliasResolutionVersion,
    row: Mapping[str, object],
    policy: IdentityPolicyBundle,
) -> None:
    method, disposition, decisions = _resolution_shape(row)
    share_method = (
        ShareClassResolutionMethod.APPROVED_SHARE_CLASS_ADJUDICATION
        if row.get("share_class_adjudication_id") is not None
        else ShareClassResolutionMethod.DIRECT_OBSERVED
        if row.get("observed_share_class_figi") is not None
        else ShareClassResolutionMethod.NOT_APPLICABLE
    )
    share_ids = (
        ()
        if row.get("share_class_adjudication_id") is None
        else (str(row["share_class_adjudication_id"]),)
    )
    expected = {
        "canonical_asset_id": row.get("asset_id"),
        "canonical_cik_normalized": row.get("canonical_cik_normalized"),
        "canonical_composite_figi": row.get("canonical_composite_figi"),
        "canonical_issuer_id": row.get("issuer_id"),
        "canonical_share_class_figi": row.get("canonical_share_class_figi"),
        "canonical_share_class_id": row.get("share_class_id"),
        "decision_lineage_ids": decisions,
        "disposition": disposition,
        "identity_policy_bundle_id": policy.identity_policy_bundle_id,
        "resolution_method": method,
        "share_class_decision_lineage_ids": share_ids,
        "share_class_resolution_method": share_method,
    }
    for name, value in expected.items():
        if getattr(resolution, name) != value:
            raise I3FixtureRunnerError(
                f"same observed alias subject changed protected resolution field {name}"
            )
    row_evidence_available = _native_date(
        row["identity_evidence_available_session"], "identity evidence availability"
    )
    if resolution.evidence_available_session < row_evidence_available:
        raise I3FixtureRunnerError("alias resolution omitted newer target evidence availability")


def _same_segment_subject(segment: AliasSegmentIdentity, row: Mapping[str, object]) -> bool:
    return (
        segment.provider_id == "massive"
        and segment.provider_market == "stocks"
        and segment.provider_locale == "us"
        and segment.ticker == row.get("ticker")
        and segment.observed_composite_figi == row.get("observed_composite_figi")
        and segment.observed_share_class_figi == row.get("observed_share_class_figi")
        and segment.observed_cik_normalized == row.get("observed_cik_normalized")
    )


def _alias_physical_row(
    source: Mapping[str, object],
    *,
    segment: AliasSegmentIdentity,
    resolution: AliasResolutionVersion,
    availability_session: date,
    calendar_index: Mapping[date, int],
) -> dict[str, object]:
    try:
        count = (
            calendar_index[resolution.valid_through_session]
            - calendar_index[segment.valid_from_session]
            + 1
        )
    except KeyError as exc:  # pragma: no cover - target/calendar validation proves
        raise I3FixtureRunnerError("alias interval escaped the exact calendar") from exc
    base_fields = {item.name for item in S7_DERIVED_CONTRACTS["ticker_alias"].columns} - {
        "ticker_alias_id",
        "ticker_alias_id_rule_version",
    }
    row: dict[str, object] = {}
    for name in base_fields:
        if name in source:
            row[name] = source[name]
    row.update(
        {
            "asset_id": resolution.canonical_asset_id,
            "ticker": segment.ticker,
            "valid_from_session": segment.valid_from_session,
            "valid_through_session": resolution.valid_through_session,
            "valid_to_session_exclusive": None,
            "interval_end_status": "right_censored_at_source_end",
            "interval_session_count": count,
            "observed_composite_figi": segment.observed_composite_figi,
            "observed_share_class_figi": segment.observed_share_class_figi,
            "observed_cik_normalized": segment.observed_cik_normalized,
            "canonical_composite_figi": resolution.canonical_composite_figi,
            "canonical_share_class_figi": resolution.canonical_share_class_figi,
            "canonical_cik_normalized": resolution.canonical_cik_normalized,
            "share_class_id": resolution.canonical_share_class_id,
            "issuer_id": resolution.canonical_issuer_id,
            "alias_resolution_method": str(source["identity_resolution_method"]),
            "alias_resolution_status": str(source["identity_resolution_status"]),
            "ticker_event_corroborated": False,
            "ticker_event_count": 0,
            "source_row_count": count,
            "first_source_record_id": segment.segment_origin_source_record_id,
            "last_source_record_id": source["selected_source_record_id"],
            "backtest_identity_eligible": True,
            "resolution_rule_version": (
                "s7_ticker_alias_resolution_with_mutually_exclusive_composite_share_and_"
                "transition_registries_v4"
            ),
            "alias_segment_id": segment.alias_segment_id,
            "alias_resolution_version_id": resolution.alias_resolution_version_id,
            "predecessor_alias_resolution_version_id": (
                resolution.predecessor_alias_resolution_version_id
            ),
            "alias_segment_id_rule_version": ALIAS_SEGMENT_ID_RULE_VERSION,
            "alias_resolution_version_id_rule_version": (ALIAS_RESOLUTION_VERSION_ID_RULE_VERSION),
            "provider_id": segment.provider_id,
            "provider_market": segment.provider_market,
            "provider_locale": segment.provider_locale,
            "segment_origin_source_record_id": segment.segment_origin_source_record_id,
            "source_record_set_digest": resolution.source_record_set_digest,
            "decision_lineage_ids": list(resolution.decision_lineage_ids),
            "share_class_decision_lineage_ids": list(resolution.share_class_decision_lineage_ids),
            "identity_policy_bundle_id": resolution.identity_policy_bundle_id,
            "resolution_available_session": resolution.resolution_available_session,
            "evidence_cutoff_session": resolution.evidence_cutoff_session,
            "alias_version_available_session": availability_session,
            "alias_is_tombstone": False,
            "alias_tombstone_reason_code": None,
        }
    )
    missing = base_fields - set(row)
    if missing:
        raise I3FixtureRunnerError(f"cannot construct ticker_alias fields: {sorted(missing)}")
    return row


def _advance_asset(
    prior: AssetAggregateState | None,
    row: Mapping[str, object],
    *,
    availability_session: date,
) -> AssetAggregateState:
    asset_id = str(row["asset_id"])
    canonical = str(row["canonical_composite_figi"])
    session = _native_date(row["session_date"], "asset membership session")
    observed = _optional_string(row.get("observed_composite_figi"))
    share = _optional_string(row.get("canonical_share_class_figi"))
    issuer = _optional_string(row.get("issuer_id"))
    evidence_available = _native_date(
        row["identity_evidence_available_session"], "asset identity evidence availability"
    )
    counters = Counter({name: 0 for name in ASSET_COUNTER_NAMES})
    if prior is not None:
        if prior.canonical_composite_figi != canonical:
            raise I3FixtureRunnerError("asset aggregate changed canonical Composite")
        counters.update({item.name: item.value for item in prior.counters})
    eligible = bool(row["backtest_identity_eligible"])
    counters["strong_evidence_row_count"] += int(eligible)
    direct = observed == canonical
    direct_admitted = eligible and direct
    counters["direct_observed_evidence_row_count"] += int(direct_admitted)
    counters["conflict_evidence_row_count"] += int(bool(row["composite_registry_collision"]))

    canonical_shares = _union(prior.canonical_share_class_figis if prior else (), share)
    identity_ids = _union(
        prior.identity_adjudication_ids if prior else (),
        _optional_string(row.get("identity_adjudication_id")),
    )
    genuine_identity_ids = prior.genuine_transition_identity_adjudication_ids if prior else ()
    contamination_identity_ids = (
        prior.provider_contamination_identity_adjudication_ids if prior else ()
    )
    identity_id = _optional_string(row.get("identity_adjudication_id"))
    if identity_id is not None:
        disposition = row.get("identity_disposition")
        if disposition == "confirmed_genuine_transition":
            genuine_identity_ids = _union(genuine_identity_ids, identity_id)
        elif disposition == "confirmed_provider_contamination":
            contamination_identity_ids = _union(contamination_identity_ids, identity_id)
        else:
            raise I3FixtureRunnerError(
                "identity adjudication lacks a closed genuine/contamination disposition"
            )
    cross_market_ids = _union(
        prior.cross_market_adjudication_ids if prior else (),
        _optional_string(row.get("cross_market_adjudication_id")),
    )
    provider_override_ids = _union(
        prior.provider_composite_override_ids if prior else (),
        _optional_string(row.get("provider_composite_override_id")),
    )
    share_adjudication_ids = _union(
        prior.share_class_adjudication_ids if prior else (),
        _optional_string(row.get("share_class_adjudication_id")),
    )
    # Confirmed relation IDs are attached symmetrically in the dedicated
    # transition pass; unresolved relation lineage never creates an edge.
    transition_ids = prior.asset_transition_ids if prior else ()
    counters["adjudicated_override_evidence_row_count"] += int(
        eligible
        and identity_id is not None
        and row.get("identity_disposition") == "confirmed_provider_contamination"
    )
    counters["cross_market_override_evidence_row_count"] += int(
        eligible and row.get("cross_market_adjudication_id") is not None
    )
    distinct_counts = {
        "cross_market_adjudication_count": len(cross_market_ids),
        "genuine_transition_adjudication_count": len(genuine_identity_ids),
        "identity_adjudication_count": len(identity_ids),
        "provider_composite_override_count": len(provider_override_ids),
        "provider_contamination_adjudication_count": len(contamination_identity_ids),
        "share_class_adjudication_count": len(share_adjudication_ids),
    }
    for name, value in distinct_counts.items():
        counters[name] = value
    terminal_placeholder = (
        prior.terminal_row_version_id
        if prior is not None
        else stable_digest({"asset_root_placeholder": asset_id})
    )
    return AssetAggregateState(
        asset_id=asset_id,
        canonical_composite_figi=canonical,
        canonical_share_class_figi=(canonical_shares[0] if len(canonical_shares) == 1 else None),
        canonical_share_class_figis=canonical_shares,
        terminal_row_version_id=terminal_placeholder,
        first_direct_observed_session=(
            min(filter(None, (prior.first_direct_observed_session if prior else None, session)))
            if direct_admitted
            else prior.first_direct_observed_session
            if prior
            else None
        ),
        last_direct_observed_session=(
            session if direct_admitted else prior.last_direct_observed_session if prior else None
        ),
        first_canonical_membership_session=(
            prior.first_canonical_membership_session if prior else session
        ),
        last_canonical_membership_session=session,
        observed_tickers=_union(prior.observed_tickers if prior else (), str(row["ticker"])),
        observed_composite_figis=_union(prior.observed_composite_figis if prior else (), observed),
        observed_share_class_figis=_union(
            prior.observed_share_class_figis if prior else (),
            _optional_string(row.get("observed_share_class_figi")),
        ),
        observed_issuer_ids=_union(prior.observed_issuer_ids if prior else (), issuer),
        identity_adjudication_ids=identity_ids,
        genuine_transition_identity_adjudication_ids=genuine_identity_ids,
        provider_contamination_identity_adjudication_ids=contamination_identity_ids,
        cross_market_adjudication_ids=cross_market_ids,
        provider_composite_override_ids=provider_override_ids,
        share_class_adjudication_ids=share_adjudication_ids,
        asset_transition_ids=transition_ids,
        predecessor_asset_ids=prior.predecessor_asset_ids if prior else (),
        successor_asset_ids=prior.successor_asset_ids if prior else (),
        counters=tuple(AggregateCount(name, counters[name]) for name in ASSET_COUNTER_NAMES),
        source_record_set_digest=_advance_digest(
            prior.source_record_set_digest if prior else None,
            str(row["selected_source_record_id"]),
        ),
        identity_evidence_available_session=max(
            prior.identity_evidence_available_session if prior else evidence_available,
            evidence_available,
        ),
        state_available_session=availability_session,
    )


def _advance_issuer(
    prior: IssuerAggregateState | None,
    row: Mapping[str, object],
    *,
    availability_session: date,
    policy: IdentityPolicyBundle,
    reference_name: object,
    sic_code: object,
    reference_metadata_available_session: date | None,
) -> IssuerAggregateState | None:
    issuer_id = str(row["issuer_id"])
    cik = str(row["canonical_cik_normalized"])
    session = _native_date(row["session_date"], "issuer membership session")
    if prior is not None and prior.cik_normalized != cik:
        raise I3FixtureRunnerError("issuer aggregate changed normalized CIK")
    counters = Counter({name: 0 for name in ISSUER_COUNTER_NAMES})
    if prior is not None:
        counters.update({item.name: item.value for item in prior.counters})
    confirmed_cross_market_contamination = row.get("cross_market_adjudication_id") is not None
    confirmed_contamination = (
        row.get("identity_disposition") == "confirmed_provider_contamination"
        or confirmed_cross_market_contamination
    )
    excluded_from_consensus = confirmed_contamination or not bool(row["backtest_identity_eligible"])
    if excluded_from_consensus and prior is None:
        # Untrusted identity evidence cannot create an issuer master root.
        return None
    counters["source_evidence_row_count"] += int(not excluded_from_consensus)
    counters["excluded_contamination_evidence_row_count"] += int(confirmed_contamination)
    counters["excluded_cross_market_contamination_evidence_row_count"] += int(
        confirmed_cross_market_contamination
    )
    placeholder = (
        prior.terminal_row_version_id
        if prior
        else stable_digest({"issuer_root_placeholder": issuer_id})
    )
    reference_available = max(
        _native_date(
            row["membership_source_available_session"],
            "issuer membership reference availability",
        ),
        _native_date(
            row["identity_evidence_available_session"],
            "issuer identity-control availability",
        ),
        policy.bundle_available_session,
        *(item.release_available_session for item in policy.registry_releases),
        *(
            [reference_metadata_available_session]
            if reference_metadata_available_session is not None
            else []
        ),
    )
    if excluded_from_consensus:
        if prior is None:  # pragma: no cover - returned above
            raise I3FixtureRunnerError("untrusted issuer evidence escaped exclusion")
        return IssuerAggregateState(
            issuer_id=issuer_id,
            cik_normalized=cik,
            terminal_row_version_id=placeholder,
            first_observed_session=prior.first_observed_session,
            last_observed_session=prior.last_observed_session,
            observed_asset_ids=prior.observed_asset_ids,
            observed_tickers=prior.observed_tickers,
            reference_names=prior.reference_names,
            sic_codes=prior.sic_codes,
            counters=tuple(AggregateCount(name, counters[name]) for name in ISSUER_COUNTER_NAMES),
            source_record_set_digest=_advance_digest(
                prior.source_record_set_digest,
                str(row["selected_source_record_id"]),
            ),
            reference_available_session=max(
                prior.reference_available_session,
                reference_available,
            ),
            state_available_session=availability_session,
        )
    return IssuerAggregateState(
        issuer_id=issuer_id,
        cik_normalized=cik,
        terminal_row_version_id=placeholder,
        first_observed_session=prior.first_observed_session if prior else session,
        last_observed_session=session,
        observed_asset_ids=_union(
            prior.observed_asset_ids if prior else (), _optional_string(row.get("asset_id"))
        ),
        observed_tickers=_union(prior.observed_tickers if prior else (), str(row["ticker"])),
        reference_names=_union(
            prior.reference_names if prior else (), _clean_optional_text(reference_name)
        ),
        sic_codes=_union(prior.sic_codes if prior else (), _clean_optional_text(sic_code)),
        counters=tuple(AggregateCount(name, counters[name]) for name in ISSUER_COUNTER_NAMES),
        source_record_set_digest=_advance_digest(
            prior.source_record_set_digest if prior else None,
            str(row["selected_source_record_id"]),
        ),
        reference_available_session=max(
            prior.reference_available_session if prior else reference_available,
            reference_available,
        ),
        state_available_session=availability_session,
    )


def _apply_symmetric_asset_transition_lineage(
    *,
    rows: Sequence[Mapping[str, object]],
    assets: dict[str, AssetAggregateState],
    touched_assets: set[str],
    first_asset_rows: dict[str, Mapping[str, object]],
    policy_snapshot: IdentityPolicySnapshot,
    availability_session: date,
) -> None:
    transition_release = next(
        item
        for item in policy_snapshot.policy_bundle.registry_releases
        if item.registry_kind is IdentityRegistryKind.ASSET_TRANSITION
    )
    for row in rows:
        for raw_transition_id in tuple(row.get("asset_transition_ids") or ()):
            transition_id = str(raw_transition_id)
            try:
                transition = policy_snapshot.decision_by_id(transition_id)
            except I3DispatchError as exc:  # pragma: no cover - dispatch validates first
                raise I3FixtureRunnerError("asset transition is absent from policy") from exc
            if transition.registry_kind is not IdentityRegistryKind.ASSET_TRANSITION:
                raise I3FixtureRunnerError("asset transition lineage names another registry")
            evidence_floor = max(
                transition.decision_available_session,
                transition_release.release_available_session,
                _native_date(
                    row["identity_evidence_available_session"],
                    "asset transition evidence availability",
                ),
            )
            if transition.identity_disposition == "asset_transition_adjudicated_unresolved":
                if (
                    transition.predecessor_asset_id is None
                    or transition.successor_asset_id is not None
                ):  # pragma: no cover - policy proves
                    raise I3FixtureRunnerError(
                        "unresolved asset transition has an invalid endpoint shape"
                    )
                if row.get("asset_id") != transition.predecessor_asset_id:
                    raise I3FixtureRunnerError(
                        "unresolved asset transition is attached to another canonical asset"
                    )
                if transition.predecessor_asset_id not in assets:
                    raise I3FixtureRunnerError(
                        "unresolved asset transition predecessor is absent from membership history"
                    )
                predecessor = assets[transition.predecessor_asset_id]
                assets[transition.predecessor_asset_id] = replace(
                    predecessor,
                    asset_transition_ids=_union(predecessor.asset_transition_ids, transition_id),
                    identity_evidence_available_session=max(
                        predecessor.identity_evidence_available_session,
                        evidence_floor,
                    ),
                    state_available_session=availability_session,
                )
                touched_assets.add(transition.predecessor_asset_id)
                first_asset_rows.setdefault(transition.predecessor_asset_id, row)
                continue
            if (
                transition.identity_disposition != "confirmed_genuine_transition"
                or transition.predecessor_asset_id is None
                or transition.successor_asset_id is None
            ):
                raise I3FixtureRunnerError("asset transition relation is not confirmed")
            endpoints = (transition.predecessor_asset_id, transition.successor_asset_id)
            provider_decision_id = row.get("provider_composite_override_id")
            if provider_decision_id is not None:
                try:
                    provider_decision = policy_snapshot.decision_by_id(str(provider_decision_id))
                except I3DispatchError as exc:  # pragma: no cover - dispatch validates first
                    raise I3FixtureRunnerError(
                        "provider Composite override is absent from policy"
                    ) from exc
                if (
                    provider_decision.registry_kind
                    is IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE
                    and provider_decision.identity_disposition
                    == "provider_composite_override_adjudicated_unresolved"
                    and provider_decision.transition_relation_id == transition_id
                ):
                    # The confirmed relation remains exact decision lineage for
                    # the unresolved provider row, but without a unique
                    # canonical asset that row cannot materialize either edge.
                    continue
            if row.get("asset_id") not in endpoints:
                raise I3FixtureRunnerError(
                    "confirmed asset transition is attached to a non-endpoint canonical asset"
                )
            if any(asset_id not in assets for asset_id in endpoints):
                raise I3FixtureRunnerError(
                    "asset transition endpoint is absent from checkpoint membership history"
                )
            for asset_id, related_id, relation in (
                (
                    transition.predecessor_asset_id,
                    transition.successor_asset_id,
                    "successor",
                ),
                (
                    transition.successor_asset_id,
                    transition.predecessor_asset_id,
                    "predecessor",
                ),
            ):
                state = assets[asset_id]
                assets[asset_id] = replace(
                    state,
                    asset_transition_ids=_union(state.asset_transition_ids, transition_id),
                    predecessor_asset_ids=(
                        _union(state.predecessor_asset_ids, related_id)
                        if relation == "predecessor"
                        else state.predecessor_asset_ids
                    ),
                    successor_asset_ids=(
                        _union(state.successor_asset_ids, related_id)
                        if relation == "successor"
                        else state.successor_asset_ids
                    ),
                    identity_evidence_available_session=max(
                        state.identity_evidence_available_session,
                        evidence_floor,
                    ),
                    state_available_session=availability_session,
                )
                touched_assets.add(asset_id)
                first_asset_rows.setdefault(asset_id, row)


def _asset_master_row(
    state: AssetAggregateState,
    *,
    predecessor: str | None,
    source_row: Mapping[str, object],
    availability_session: date,
) -> tuple[dict[str, object], AssetAggregateState]:
    counters = {item.name: item.value for item in state.counters}
    aggregate_digest = _aggregate_state_digest(state)
    share = state.canonical_share_class_figi
    basis = (
        "approved_cross_market_external_anchor"
        if counters["cross_market_adjudication_count"]
        else "approved_episode_adjudication"
        if counters["identity_adjudication_count"] or counters["provider_composite_override_count"]
        else "direct_observed_composite"
    )
    base = {
        "asset_id": state.asset_id,
        "canonical_composite_figi": state.canonical_composite_figi,
        "canonical_identity_basis": basis,
        "asset_id_rule_version": "ame_stocks_asset_id_from_composite_figi_v1",
        "share_class_id": canonical_share_class_id(share) if share else None,
        "share_class_id_rule_version": "ame_stocks_share_class_id_from_share_class_figi_v1",
        "canonical_share_class_figi": share,
        "share_class_resolution_status": (
            "unique_share_class"
            if share
            else "missing_share_class"
            if not state.canonical_share_class_figis
            else "multiple_share_classes_unresolved"
        ),
        "identity_resolution_status": "resolved_identity",
        "asset_status": "active_identity",
        "superseded_by_asset_id": None,
        "first_direct_observed_session": state.first_direct_observed_session,
        "last_direct_observed_session": state.last_direct_observed_session,
        "first_canonical_membership_session": state.first_canonical_membership_session,
        "last_canonical_membership_session": state.last_canonical_membership_session,
        "observed_ticker_count": len(state.observed_tickers),
        "observed_composite_figi_count": len(state.observed_composite_figis),
        "observed_share_class_figi_count": len(state.observed_share_class_figis),
        "observed_issuer_count": len(state.observed_issuer_ids),
        **counters,
        "backtest_identity_eligible": True,
        "identity_mapping_time_scope": "retrospective_identity_reference_not_signal_v1",
        "identity_evidence_available_session": state.identity_evidence_available_session,
        "identity_resolution_cutoff_session": source_row["identity_resolution_cutoff_session"],
        "resolution_rule_version": (
            "s7_asset_master_resolution_with_mutually_exclusive_composite_share_and_"
            "transition_registries_v4"
        ),
        **_source_bindings(source_row),
        "predecessor_asset_ids": list(state.predecessor_asset_ids),
        "successor_asset_ids": list(state.successor_asset_ids),
    }
    version_id = stable_digest(
        {
            "aggregate_state_digest": aggregate_digest,
            "availability_session": availability_session.isoformat(),
            "namespace": "ame_stocks.silver.asset_master_version",
            "predecessor_asset_master_version_id": predecessor,
            "stable_row_key": state.asset_id,
            "v1_payload": _jsonable(base),
        }
    )
    row = {
        **base,
        "asset_master_version_id": version_id,
        "predecessor_asset_master_version_id": predecessor,
        "version_available_session": availability_session,
        "aggregate_state_digest": aggregate_digest,
    }
    return row, replace(state, terminal_row_version_id=version_id)


def _issuer_master_row(
    state: IssuerAggregateState,
    *,
    predecessor: str | None,
    source_row: Mapping[str, object],
    availability_session: date,
) -> tuple[dict[str, object], IssuerAggregateState]:
    counters = {item.name: item.value for item in state.counters}
    names = state.reference_names
    sics = state.sic_codes
    aggregate_digest = _aggregate_state_digest(state)
    base = {
        "issuer_id": state.issuer_id,
        "cik_normalized": state.cik_normalized,
        "issuer_id_rule_version": "ame_stocks_issuer_id_from_normalized_cik_v1",
        "issuer_status": "active_reference",
        "superseded_by_issuer_id": None,
        "reference_name": names[0] if len(names) == 1 else None,
        "reference_name_variant_count": len(names),
        "reference_name_resolution_status": (
            "unique_reference_name"
            if len(names) == 1
            else "missing_reference_name"
            if not names
            else "multiple_reference_names"
        ),
        "sic_code_current_reference": sics[0] if len(sics) == 1 else None,
        "sic_code_variant_count": len(sics),
        "sic_resolution_status": (
            "unique_reference_sic"
            if len(sics) == 1
            else "missing_reference_sic"
            if not sics
            else "multiple_reference_sics"
        ),
        "first_observed_session": state.first_observed_session,
        "last_observed_session": state.last_observed_session,
        "observed_asset_count": len(state.observed_asset_ids),
        "observed_ticker_count": len(state.observed_tickers),
        **counters,
        "backtest_classification_eligible": False,
        "reference_time_scope": "retrospective_issuer_reference_not_pit_classification_v1",
        "reference_available_session": state.reference_available_session,
        "identity_resolution_cutoff_session": source_row["identity_resolution_cutoff_session"],
        "resolution_rule_version": (
            "s7_issuer_master_reference_consensus_with_registry_isolation_v4"
        ),
        **_source_bindings(source_row),
    }
    version_id = stable_digest(
        {
            "aggregate_state_digest": aggregate_digest,
            "availability_session": availability_session.isoformat(),
            "namespace": "ame_stocks.silver.issuer_master_version",
            "predecessor_issuer_master_version_id": predecessor,
            "stable_row_key": state.issuer_id,
            "v1_payload": _jsonable(base),
        }
    )
    row = {
        **base,
        "issuer_master_version_id": version_id,
        "predecessor_issuer_master_version_id": predecessor,
        "version_available_session": availability_session,
        "aggregate_state_digest": aggregate_digest,
    }
    return row, replace(state, terminal_row_version_id=version_id)


def _build_qa(
    *,
    rows: Sequence[Mapping[str, object]],
    lookback_rows: Sequence[Mapping[str, object]],
    universe_rows: Sequence[Mapping[str, object]],
    alias_rows: Sequence[Mapping[str, object]],
    asset_rows: Sequence[Mapping[str, object]],
    issuer_rows: Sequence[Mapping[str, object]],
    unresolved_target_rows: int,
) -> dict[str, object]:
    eligible_missing_alias = sum(
        bool(row["backtest_identity_eligible"]) and row["alias_segment_id"] is None
        for row in universe_rows
    )
    ineligible_alias = sum(
        not bool(row["backtest_identity_eligible"]) and row["alias_segment_id"] is not None
        for row in universe_rows
    )
    ineligible_master_versions = sum(
        not bool(row["backtest_identity_eligible"])
        and any(
            row[name] is not None
            for name in ("asset_master_version_id", "issuer_master_version_id")
        )
        for row in universe_rows
    )
    aliases_by_segment = {
        str(row["alias_segment_id"]): row
        for row in alias_rows
        if row.get("alias_segment_id") is not None
    }
    asset_versions_by_id = {
        str(row["asset_id"]): row["asset_master_version_id"] for row in asset_rows
    }
    issuer_versions_by_id = {
        str(row["issuer_id"]): row["issuer_master_version_id"] for row in issuer_rows
    }
    row_version_fk_mismatch = 0
    for row in universe_rows:
        alias_segment_id = row.get("alias_segment_id")
        alias_version_id = row.get("alias_resolution_version_id")
        if alias_segment_id is not None:
            alias = aliases_by_segment.get(str(alias_segment_id))
            row_version_fk_mismatch += int(
                alias is None
                or alias.get("alias_resolution_version_id") != alias_version_id
                or alias.get("asset_id") != row.get("asset_id")
            )
        elif alias_version_id is not None:
            row_version_fk_mismatch += 1
        asset_version_id = row.get("asset_master_version_id")
        asset_presence_mismatch = (asset_version_id is None) != (
            not bool(row["backtest_identity_eligible"])
        )
        asset_target_mismatch = (
            asset_version_id is not None
            and asset_versions_by_id.get(str(row["asset_id"])) != asset_version_id
        )
        if asset_presence_mismatch or asset_target_mismatch:
            row_version_fk_mismatch += 1
        issuer_version_id = row.get("issuer_master_version_id")
        if issuer_version_id is not None and (
            row.get("issuer_id") is None
            or issuer_versions_by_id.get(str(row["issuer_id"])) != issuer_version_id
        ):
            row_version_fk_mismatch += 1
    bounce = _bounce_count(lookback_rows)
    foreign = sum(
        row.get("observed_composite_market_code") not in {None, "US", "USA"}
        for row in rows
        if row.get("observed_composite_figi") is not None
    )
    unapproved_cross_market = sum(
        bool(row["backtest_identity_eligible"])
        and row.get("observed_composite_market_code") not in {None, "US", "USA"}
        and row.get("cross_market_adjudication_id") is None
        for row in rows
    )
    suspected_contamination_eligible = _same_market_unapproved_bounce_eligible_rows(lookback_rows)
    collision_rows = sum(bool(row["composite_registry_collision"]) for row in rows)
    collision_alias_rows = sum(
        bool(source["composite_registry_collision"]) and output["alias_segment_id"] is not None
        for source, output in zip(rows, universe_rows, strict=True)
    )
    collision_eligible_rows = sum(
        bool(row["composite_registry_collision"]) and bool(row["backtest_identity_eligible"])
        for row in rows
    )
    collision_resolved_rows = sum(
        bool(row["composite_registry_collision"])
        and (
            row.get("canonical_composite_figi") is not None
            or row.get("asset_id") is not None
            or row.get("identity_resolution_status") != "unresolved_registry_collision"
        )
        for row in rows
    )
    changed_active = sum(
        source["active_on_date"] != output["active_on_date"]
        for source, output in zip(rows, universe_rows, strict=True)
    )
    inferred_inactive = sum(
        bool(source["active_on_date"]) and not bool(output["active_on_date"])
        for source, output in zip(rows, universe_rows, strict=True)
    )
    forced_liquidation = sum(
        bool(row["identity_quality_liquidation_signal"]) for row in universe_rows
    )
    share_before_composite = sum(
        row.get("share_class_adjudication_id") is not None
        and (
            row.get("canonical_composite_figi") is None or bool(row["composite_registry_collision"])
        )
        and not (
            row.get("identity_resolution_method") == "adjudicated_unresolved"
            and row.get("identity_disposition") == "adjudicated_unresolved"
        )
        for row in rows
    )
    unapproved_override = sum(
        bool(row["backtest_identity_eligible"])
        and row.get("observed_composite_figi") != row.get("canonical_composite_figi")
        and not any(
            row.get(name) is not None
            for name in (
                "identity_adjudication_id",
                "cross_market_adjudication_id",
                "provider_composite_override_id",
            )
        )
        for row in rows
    )
    metrics = {
        "availability_mismatch_rows": 0,
        "boundary_coverage_mismatch_rows": 0,
        "eligible_membership_missing_alias_rows": eligible_missing_alias,
        "identity_quality_changed_active_rows": changed_active,
        "identity_quality_forced_liquidation_rows": forced_liquidation,
        "inactive_or_delisted_inferred_from_identity_quality_rows": inferred_inactive,
        "ineligible_membership_with_alias_rows": ineligible_alias,
        "ineligible_membership_with_master_version_rows": ineligible_master_versions,
        "inverse_bounce_misclassified_as_genuine_transition_rows": (
            _inverse_bounce_misclassified_as_genuine_transition_rows(lookback_rows)
        ),
        "multi_registry_composite_override_collision_alias_rows": collision_alias_rows,
        "multi_registry_composite_override_collision_eligible_rows": collision_eligible_rows,
        "multi_registry_composite_override_collision_resolved_rows": collision_resolved_rows,
        "multi_registry_composite_override_collision_rows": collision_rows,
        "row_semantic_proof_mismatch_rows": 0,
        "row_version_fk_mismatch_rows": row_version_fk_mismatch,
        "share_class_applied_before_unique_composite_rows": share_before_composite,
        "source_membership_omission_or_duplication_rows": abs(len(rows) - len(universe_rows)),
        "suspected_provider_contamination_eligible_rows": suspected_contamination_eligible,
        "suspected_provider_figi_bounce_rows": bounce,
        "target_market_consistency_unchecked_rows": 0,
        "unapproved_canonical_identity_override_rows": unapproved_override,
        "unapproved_cross_market_composite_eligible_rows": unapproved_cross_market,
        "us_locale_non_us_composite_figi_rows": foreign,
    }
    expected_metrics = {item.check_id for item in I3_QA_CATALOG}
    if set(metrics) != expected_metrics:  # pragma: no cover - import-time-owned catalog
        raise I3FixtureRunnerError("runner QA metrics differ from the closed dispatcher catalog")
    critical_ids = {item.check_id for item in I3_QA_CATALOG if item.severity.value == "critical"}
    critical = sum(metrics[check_id] for check_id in critical_ids)
    return {
        "artifact_type": "s7_5_i3_fixture_single_session_qa",
        "critical_failure_count": critical,
        **metrics,
        "output_membership_rows": len(universe_rows),
        "publish_authorized": False,
        "qa_catalog_digest": I3_QA_CATALOG_DIGEST,
        "source_membership_rows": len(rows),
        "unresolved_rows": unresolved_target_rows,
        "written_alias_rows": len(alias_rows),
    }


def _inverse_bounce_misclassified_as_genuine_transition_rows(
    rows: Sequence[Mapping[str, object]],
) -> int:
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["ticker"])].append(row)
    result = 0
    for values in grouped.values():
        ordered = sorted(
            values,
            key=lambda row: _native_date(row["session_date"], "lookback session"),
        )
        for left, middle, right in zip(ordered, ordered[1:], ordered[2:], strict=False):
            outer_figi = left.get("observed_composite_figi")
            if (
                outer_figi is None
                or outer_figi != right.get("observed_composite_figi")
                or outer_figi == middle.get("observed_composite_figi")
                or left.get("observed_composite_market_code") in {None, "US", "USA"}
                or right.get("observed_composite_market_code") in {None, "US", "USA"}
                or middle.get("observed_composite_market_code") not in {"US", "USA"}
            ):
                continue
            result += int(
                middle.get("identity_disposition") == "confirmed_genuine_transition"
                or middle.get("identity_resolution_method") == "approved_genuine_transition"
            )
    return result


def _same_market_unapproved_bounce_eligible_rows(
    rows: Sequence[Mapping[str, object]],
) -> int:
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["ticker"])].append(row)
    result = 0
    for values in grouped.values():
        ordered = sorted(
            values,
            key=lambda row: _native_date(row["session_date"], "lookback session"),
        )
        for left, middle, right in zip(ordered, ordered[1:], ordered[2:], strict=False):
            left_figi = left.get("observed_composite_figi")
            middle_figi = middle.get("observed_composite_figi")
            right_figi = right.get("observed_composite_figi")
            if left_figi is None or left_figi != right_figi or left_figi == middle_figi:
                continue
            if any(
                row.get("observed_composite_market_code") not in {"US", "USA"}
                for row in (left, middle, right)
            ):
                continue
            if not bool(middle["backtest_identity_eligible"]):
                continue
            approved = any(
                middle.get(name) is not None
                for name in (
                    "identity_adjudication_id",
                    "cross_market_adjudication_id",
                    "provider_composite_override_id",
                )
            ) or bool(tuple(middle.get("asset_transition_ids") or ()))
            result += int(not approved)
    return result


def _raise_for_critical_qa(qa: Mapping[str, object]) -> None:
    if qa.get("qa_catalog_digest") != I3_QA_CATALOG_DIGEST:
        raise I3FixtureRunnerError("fixture QA does not bind the closed dispatcher catalog")
    for rule in I3_QA_CATALOG:
        value = qa.get(rule.check_id)
        if type(value) is not int or value < 0:
            raise I3FixtureRunnerError(f"fixture QA metric {rule.check_id} is invalid")
    digest_payload = {
        "dispatcher_qa_receipt": qa.get("dispatcher_qa_receipt"),
        "materialization_failure_metrics": {
            rule.check_id: qa[rule.check_id] for rule in I3_QA_CATALOG
        },
        "qa_catalog_digest": I3_QA_CATALOG_DIGEST,
        "raw_review_results": qa.get("raw_review_results"),
    }
    if qa.get("qa_result_digest") != stable_digest(digest_payload):
        raise I3FixtureRunnerError("fixture QA result digest does not reproduce")
    dispatcher_receipt = qa.get("dispatcher_qa_receipt")
    if not isinstance(dispatcher_receipt, Mapping):
        raise I3FixtureRunnerError("fixture QA dispatcher receipt is invalid")
    if qa.get("dispatcher_qa_receipt_id") != dispatcher_receipt.get("qa_receipt_id"):
        raise I3FixtureRunnerError("fixture QA dispatcher receipt ID differs")
    count = qa.get("critical_failure_count")
    if type(count) is not int or count < 0:
        raise I3FixtureRunnerError("fixture QA critical count is invalid")
    expected_critical = sum(
        int(qa[rule.check_id]) for rule in I3_QA_CATALOG if rule.severity.value == "critical"
    )
    if count != expected_critical:
        raise I3FixtureRunnerError("fixture QA critical count does not reproduce")
    if count:
        raise I3FixtureRunnerError(
            f"fixture QA failed closed before checkpoint construction: {count} critical rows"
        )


def _bounce_count(rows: Sequence[Mapping[str, object]]) -> int:
    grouped: dict[str, list[tuple[date, object]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["ticker"])].append(
            (
                _native_date(row["session_date"], "lookback session"),
                row.get("observed_composite_figi"),
            )
        )
    count = 0
    for values in grouped.values():
        ordered = [value for _, value in sorted(values)]
        if len(ordered) >= 3 and ordered[-3] == ordered[-1] and ordered[-2] != ordered[-1]:
            count += 1
    return count


def _validate_target_row(
    row: Mapping[str, object], target_session: date, policy: IdentityPolicyBundle
) -> dict[str, object]:
    expected = {item.name for item in S7_DERIVED_CONTRACTS["universe_daily"].columns}
    if set(row) != expected:
        raise I3FixtureRunnerError("resolved target row fields differ from immutable S7 v1")
    result = dict(row)
    if _native_date(result["session_date"], "target row session") != target_session:
        raise I3FixtureRunnerError("resolved target row belongs to another session")
    if result["session_year"] != target_session.year:
        raise I3FixtureRunnerError("resolved target row session_year differs")
    source_id = result["selected_source_record_id"]
    if not isinstance(source_id, str) or len(source_id) != 64:
        raise I3FixtureRunnerError("resolved target source-record ID is invalid")
    cutoff = _native_date(result["identity_resolution_cutoff_session"], "identity cutoff")
    if cutoff != policy.decision_cutoff_session:
        raise I3FixtureRunnerError("target identity cutoff differs from registry decision cutoff")
    if bool(result["composite_registry_collision"]) and bool(result["backtest_identity_eligible"]):
        raise I3FixtureRunnerError("registry collision remained backtest eligible")
    eligible = bool(result["backtest_identity_eligible"])
    if (result.get("ticker_alias_id") is None) == eligible:
        raise I3FixtureRunnerError("legacy alias presence differs from identity eligibility")
    if result["identity_quality_liquidation_signal"] is not False:
        raise I3FixtureRunnerError("identity quality attempted to force liquidation")
    if eligible:
        canonical = result.get("canonical_composite_figi")
        asset_id = result.get("asset_id")
        if not isinstance(canonical, str) or asset_id != canonical_asset_id(canonical):
            raise I3FixtureRunnerError("eligible target canonical asset does not reproduce")
        cik = result.get("canonical_cik_normalized")
        issuer = result.get("issuer_id")
        if isinstance(cik, str) and issuer != canonical_issuer_id(cik):
            raise I3FixtureRunnerError("target issuer does not reproduce from CIK")
    elif any(
        result.get(name) is not None
        for name in (
            "asset_id",
            "canonical_composite_figi",
            "canonical_composite_market_code",
            "canonical_share_class_figi",
            "share_class_id",
        )
    ):
        raise I3FixtureRunnerError("ineligible target retained canonical research identity")
    return result


def _require_policy_source_bindings(
    row: Mapping[str, object], policy: IdentityPolicyBundle
) -> None:
    release_by_kind = {item.registry_kind: item for item in policy.registry_releases}
    if set(release_by_kind) != set(_REGISTRY_SOURCE_FIELDS):  # pragma: no cover - policy proves
        raise I3FixtureRunnerError("identity policy registry set differs from runner contract")
    for kind, (release_field, availability_field) in _REGISTRY_SOURCE_FIELDS.items():
        pin = release_by_kind[kind]
        if row.get(release_field) != pin.release_id:
            raise I3FixtureRunnerError(
                f"resolved row {kind.value} release ID differs from the policy bundle"
            )
        available = _native_date(
            row.get(availability_field), f"resolved row {kind.value} release availability"
        )
        if available != pin.release_available_session:
            raise I3FixtureRunnerError(
                f"resolved row {kind.value} availability differs from the policy bundle"
            )


def _fixture_dispatch_calendar(calendar: tuple[date, ...]) -> ExactTradingCalendar:
    return freeze_exact_trading_calendar(
        calendar,
        artifact_path="fixtures/i3/exact-trading-calendar.json",
    )


def _require_policy_snapshot(
    snapshot: IdentityPolicySnapshot, *, expected_bundle: IdentityPolicyBundle
) -> None:
    if type(snapshot) is not IdentityPolicySnapshot:
        raise I3FixtureRunnerError("runner requires a sealed identity policy snapshot")
    if snapshot.policy_bundle.to_dict() != expected_bundle.to_dict():
        raise I3FixtureRunnerError("identity policy snapshot differs from the checkpoint bundle")


def _dispatch_and_validate_target_rows(
    *,
    window: Mapping[date, Sequence[Mapping[str, object]]],
    requested: tuple[date, ...],
    target_session: date,
    availability_session: date,
    s4_window_binding: I3FixtureS4WindowBinding,
    policy_snapshot: IdentityPolicySnapshot,
    exact_calendar: ExactTradingCalendar,
) -> _DispatchBatchValidation:
    try:
        verified_policy = _verify_i3_identity_policy_snapshot_for_batch(policy_snapshot)
    except I3DispatchError as exc:
        raise I3FixtureRunnerError("closed identity policy snapshot is invalid") from exc
    rows_by_session_ticker = {
        (session, str(row["ticker"])): row for session in requested for row in window[session]
    }
    universe_receipts = {
        pin.session_date: pin.partition_receipt_id
        for pin in s4_window_binding.partition_pins
        if pin.table_name == "universe_source_daily"
    }
    attestation_ids: list[str] = []
    qa_receipts: list[I3QaReceipt] = []
    for target in window[target_session]:
        ticker = str(target["ticker"])
        observations = tuple(
            _identity_observation(rows_by_session_ticker[(session, ticker)])
            for session in requested
            if (session, ticker) in rows_by_session_ticker
        )
        slots = tuple(
            SourceCoverageSlot(
                session_date=session,
                partition_receipt_id=universe_receipts[session],
                source_record_ids=(
                    (str(rows_by_session_ticker[(session, ticker)]["selected_source_record_id"]),)
                    if (session, ticker) in rows_by_session_ticker
                    else ()
                ),
            )
            for session in requested
        )
        try:
            coverage = bind_alias_source_coverage(
                exact_calendar,
                provider_id="massive",
                provider_market="stocks",
                provider_locale="us",
                ticker=ticker,
                target_session=target_session,
                slots=slots,
                coverage_available_session=availability_session,
            )
            attestation = _dispatch_i3_identity_window_from_verified_batch(
                verified_policy=verified_policy,
                coverage=coverage,
                observations=observations,
            )
        except I3DispatchError as exc:
            raise I3FixtureRunnerError("closed identity dispatch rejected the target row") from exc
        decision = attestation.decision
        expected = {
            "active_on_date": bool(target["active_on_date"]),
            "backtest_identity_eligible": bool(target["backtest_identity_eligible"]),
            "canonical_composite_figi": target.get("canonical_composite_figi"),
            "canonical_share_class_figi": target.get("canonical_share_class_figi"),
            "composite_registry_collision": bool(target["composite_registry_collision"]),
            "identity_disposition": target["identity_disposition"],
            "identity_resolution_method": target["identity_resolution_method"],
            "observed_composite_figi": target.get("observed_composite_figi"),
            "source_record_id": target["selected_source_record_id"],
        }
        for name, value in expected.items():
            if getattr(decision, name) != value:
                raise I3FixtureRunnerError(
                    f"closed identity dispatch differs from resolved row field {name}"
                )
        expected_composite_ids = tuple(
            sorted(
                str(value)
                for value in (
                    target.get("identity_adjudication_id"),
                    target.get("cross_market_adjudication_id"),
                    target.get("provider_composite_override_id"),
                )
                if value is not None
            )
        )
        expected_share_ids = (
            ()
            if target.get("share_class_adjudication_id") is None
            else (str(target["share_class_adjudication_id"]),)
        )
        if decision.composite_registry_decision_ids != expected_composite_ids:
            raise I3FixtureRunnerError(
                "closed identity dispatch differs from Composite decision lineage"
            )
        if decision.share_class_decision_ids != expected_share_ids:
            raise I3FixtureRunnerError(
                "closed identity dispatch differs from Share Class decision lineage"
            )
        expected_transition_ids = tuple(
            sorted(str(value) for value in tuple(target.get("asset_transition_ids") or ()))
        )
        if decision.asset_transition_decision_ids != expected_transition_ids:
            raise I3FixtureRunnerError(
                "closed identity dispatch differs from asset-transition decision lineage"
            )
        selected_available, selected_lineage = _validate_selected_decision_availability(
            target,
            policy_snapshot=policy_snapshot,
            output_available_session=availability_session,
        )
        if decision.selected_decision_available_session != selected_available:
            raise I3FixtureRunnerError(
                "closed identity dispatch differs from selected decision availability"
            )
        observed_lineage = tuple(
            (
                item.registry_kind,
                item.decision_id,
                item.decision_available_session,
            )
            for item in decision.decision_lineage
        )
        if observed_lineage != selected_lineage:
            raise I3FixtureRunnerError(
                "closed identity dispatch differs from typed decision availability lineage"
            )
        if not decision.market_consistency_checked:
            raise I3FixtureRunnerError("closed identity dispatch omitted market consistency")
        if decision.alias_permitted != bool(target["backtest_identity_eligible"]):
            raise I3FixtureRunnerError("closed identity dispatch alias permission differs")
        attestation_ids.append(attestation.attestation_id)
        qa_receipts.append(attestation.qa_receipt)
    aggregate = _aggregate_dispatch_qa(tuple(qa_receipts))
    if aggregate.critical_failure_count:
        raise I3FixtureRunnerError("closed dispatcher aggregate contains a Critical failure")
    return _DispatchBatchValidation(tuple(attestation_ids), aggregate)


def _empty_dispatch_qa_receipt() -> I3QaReceipt:
    return _aggregate_dispatch_qa(())


def _aggregate_dispatch_qa(receipts: tuple[I3QaReceipt, ...]) -> I3QaReceipt:
    for receipt in receipts:
        if type(receipt) is not I3QaReceipt:
            raise I3FixtureRunnerError("dispatcher QA aggregate received an invalid receipt")
    results: list[I3QaResult] = []
    for index, rule in enumerate(I3_QA_CATALOG):
        members = tuple(receipt.results[index] for receipt in receipts)
        expected_status = (
            "evaluated" if rule.owner == "dispatcher" else "deferred_to_materialization"
        )
        if any(
            item.check_id != rule.check_id
            or item.semantics_digest != rule.semantics_digest
            or item.evaluation_status != expected_status
            for item in members
        ):
            raise I3FixtureRunnerError("dispatcher QA receipt differs from the exact catalog")
        reason_counts: Counter[str] = Counter()
        examples: dict[tuple[str, tuple[str, ...]], I3QaBoundedExample] = {}
        for item in members:
            reason_counts.update(
                {reason.reason_code: reason.count for reason in item.reason_counts}
            )
            examples.update({example.sort_key: example for example in item.bounded_examples})
        bounded_examples = tuple(examples[key] for key in sorted(examples)[:20])
        bounded_ids = tuple(
            sorted(
                {
                    *(source_id for item in members for source_id in item.bounded_example_ids),
                    *(item.source_record_id for item in bounded_examples),
                }
            )[:20]
        )
        if rule.check_id in I3_RAW_REVIEW_CHECK_IDS:
            bounded_ids = tuple(sorted({item.source_record_id for item in bounded_examples}))
        results.append(
            I3QaResult(
                check_id=rule.check_id,
                semantics_digest=rule.semantics_digest,
                observed_count=sum(item.observed_count for item in members),
                failure_count=sum(item.failure_count for item in members),
                evaluation_status=expected_status,
                bounded_example_ids=bounded_ids,
                reason_counts=tuple(
                    I3QaReasonCount(reason_code=reason, count=count)
                    for reason, count in sorted(reason_counts.items())
                ),
                bounded_examples=bounded_examples,
            )
        )
    return I3QaReceipt(
        qa_catalog_digest=I3_QA_CATALOG_DIGEST,
        results=tuple(results),
    )


def _finalize_qa(
    materialization_qa: Mapping[str, object],
    dispatcher_qa: I3QaReceipt,
) -> dict[str, object]:
    if dispatcher_qa.qa_catalog_digest != I3_QA_CATALOG_DIGEST:
        raise I3FixtureRunnerError("dispatcher QA catalog differs from the runner")
    if dispatcher_qa.critical_failure_count:
        raise I3FixtureRunnerError("dispatcher QA Critical failure cannot enter final QA")
    metrics = {rule.check_id: int(materialization_qa[rule.check_id]) for rule in I3_QA_CATALOG}
    dispatcher_by_id = {item.check_id: item for item in dispatcher_qa.results}
    if any(
        dispatcher_by_id[check_id].observed_count != metrics[check_id]
        for check_id in I3_RAW_REVIEW_CHECK_IDS
    ):
        raise I3FixtureRunnerError("dispatcher raw-review counts differ from materialization QA")
    dispatcher_document = dispatcher_qa.to_dict()
    raw_review = {
        check_id: {
            "bounded_examples": [item.to_dict() for item in result.bounded_examples],
            "reason_counts": [item.to_dict() for item in result.reason_counts],
        }
        for check_id in I3_RAW_REVIEW_CHECK_IDS
        for result in dispatcher_qa.results
        if result.check_id == check_id
    }
    digest_payload = {
        "dispatcher_qa_receipt": dispatcher_document,
        "materialization_failure_metrics": metrics,
        "qa_catalog_digest": I3_QA_CATALOG_DIGEST,
        "raw_review_results": raw_review,
    }
    return {
        **dict(materialization_qa),
        "dispatcher_qa_receipt": dispatcher_document,
        "dispatcher_qa_receipt_id": dispatcher_qa.qa_receipt_id,
        "qa_catalog_digest": I3_QA_CATALOG_DIGEST,
        "qa_result_digest": stable_digest(digest_payload),
        "raw_review_results": raw_review,
    }


def _validate_selected_decision_availability(
    row: Mapping[str, object],
    *,
    policy_snapshot: IdentityPolicySnapshot,
    output_available_session: date,
) -> tuple[date | None, tuple[tuple[IdentityRegistryKind, str, date], ...]]:
    """Reproduce selected decision timing from the sealed O(1) policy index."""

    release_by_kind = {
        item.registry_kind: item for item in policy_snapshot.policy_bundle.registry_releases
    }
    selected_decisions = []
    evidence_floor = _native_date(
        row["membership_source_available_session"], "membership source availability"
    )
    for kind, decision_field, availability_field in _SELECTED_DECISION_FIELDS:
        raw_decision_id = row.get(decision_field)
        raw_available = row.get(availability_field)
        if raw_decision_id is None:
            if raw_available is not None:
                raise I3FixtureRunnerError(
                    f"{availability_field} exists without its selected decision"
                )
            continue
        decision_id = _optional_string(raw_decision_id)
        if decision_id is None:  # pragma: no cover - raw value was non-null
            raise I3FixtureRunnerError(f"{decision_field} is empty")
        try:
            decision = policy_snapshot.decision_by_id(decision_id)
        except I3DispatchError as exc:
            raise I3FixtureRunnerError(
                f"{decision_field} is absent from the sealed policy snapshot"
            ) from exc
        if decision.registry_kind is not kind:
            raise I3FixtureRunnerError(f"{decision_field} belongs to another registry")
        row_available = _native_date(raw_available, f"{decision_field} availability")
        if row_available != decision.decision_available_session:
            raise I3FixtureRunnerError(
                f"{decision_field} availability differs from the sealed decision"
            )
        selected_decisions.append(decision)
        evidence_floor = max(
            evidence_floor,
            decision.decision_available_session,
            release_by_kind[kind].release_available_session,
        )

    raw_transition_ids = row.get("asset_transition_ids")
    if not isinstance(raw_transition_ids, list) or any(
        not isinstance(value, str) for value in raw_transition_ids
    ):
        raise I3FixtureRunnerError("asset transition IDs must be a list of digests")
    transition_ids = tuple(raw_transition_ids)
    if transition_ids != tuple(sorted(set(transition_ids))):
        raise I3FixtureRunnerError("asset transition IDs must be sorted and unique")
    transitions = []
    related_transition_ids = {
        decision.transition_relation_id
        for decision in selected_decisions
        if decision.registry_kind is IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE
        and decision.transition_relation_id is not None
    }
    row_session = _native_date(row["session_date"], "identity row session")
    for decision_id in transition_ids:
        try:
            transition = policy_snapshot.decision_by_id(decision_id)
        except I3DispatchError as exc:
            raise I3FixtureRunnerError(
                "asset transition ID is absent from the sealed policy snapshot"
            ) from exc
        if transition.registry_kind is not IdentityRegistryKind.ASSET_TRANSITION:
            raise I3FixtureRunnerError("asset transition ID belongs to another registry")
        evidence_floor = max(
            evidence_floor,
            transition.decision_available_session,
            release_by_kind[IdentityRegistryKind.ASSET_TRANSITION].release_available_session,
        )
        is_related_transition = transition.decision_id in related_transition_ids
        is_production_related_transition = (
            is_related_transition and transition.is_production_registry_decision
        )
        if (
            transition.provider_id != "massive"
            or transition.provider_market != "stocks"
            or transition.provider_locale != "us"
            or transition.ticker != row.get("ticker")
            or (
                not is_production_related_transition
                and (
                    (
                        not is_related_transition
                        and row.get("selected_source_record_id") not in transition.source_record_ids
                    )
                    or row_session < transition.effective_from_session
                    or (
                        transition.effective_to_session is not None
                        and row_session > transition.effective_to_session
                    )
                )
            )
        ):
            raise I3FixtureRunnerError("asset transition crossed its exact source-row scope")
        transitions.append(transition)
    transition_id_set = {item.decision_id for item in transitions}
    for decision in selected_decisions:
        if (
            decision.registry_kind is IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE
            and decision.transition_relation_id not in transition_id_set
        ):
            raise I3FixtureRunnerError(
                "provider Composite override omitted its exact asset-transition lineage"
            )

    identity_evidence_available = _native_date(
        row["identity_evidence_available_session"], "identity evidence availability"
    )
    _require_selected_disposition_shape(row, tuple(selected_decisions))
    if identity_evidence_available < evidence_floor:
        raise I3FixtureRunnerError(
            "identity evidence availability precedes selected decision or member release"
        )
    output_floor = max(
        identity_evidence_available,
        policy_snapshot.policy_bundle.bundle_available_session,
        *(
            [
                decision.decision_available_session
                for decision in (*selected_decisions, *transitions)
            ]
            or [identity_evidence_available]
        ),
    )
    if output_available_session < output_floor:
        raise I3FixtureRunnerError(
            "output availability precedes evidence, decision, or policy wrapper"
        )
    all_decisions = (*selected_decisions, *transitions)
    if not all_decisions:
        return None, ()
    ordered_decisions = tuple(
        sorted(
            all_decisions,
            key=lambda item: (
                tuple(IdentityRegistryKind).index(item.registry_kind),
                item.decision_id,
            ),
        )
    )
    return (
        max(decision.decision_available_session for decision in ordered_decisions),
        tuple(
            (
                decision.registry_kind,
                decision.decision_id,
                decision.decision_available_session,
            )
            for decision in ordered_decisions
        ),
    )


def _require_selected_disposition_shape(
    row: Mapping[str, object],
    selected_decisions: tuple[RegistryDecision, ...],
) -> None:
    """Apply the frozen five-registry resolved/unresolved output matrix."""

    decisions = tuple(selected_decisions)
    composite = tuple(
        item for item in decisions if item.registry_kind in _COMPOSITE_SELECTED_DECISION_KINDS
    )
    share = tuple(
        item
        for item in decisions
        if item.registry_kind is IdentityRegistryKind.SHARE_CLASS_ADJUDICATION
    )
    expected: tuple[str, str, bool] | None = None
    if len(composite) > 1:
        expected = (
            "registry_collision_unresolved",
            "pending_registry_collision_review",
            False,
        )
    elif composite:
        selected = composite[0]
        matrix = {
            (
                IdentityRegistryKind.IDENTITY_ADJUDICATION,
                "confirmed_genuine_transition",
            ): (
                "approved_genuine_transition",
                "confirmed_genuine_transition",
                True,
            ),
            (
                IdentityRegistryKind.IDENTITY_ADJUDICATION,
                "confirmed_provider_contamination",
            ): (
                "approved_provider_contamination_override",
                "confirmed_provider_contamination",
                True,
            ),
            (
                IdentityRegistryKind.IDENTITY_ADJUDICATION,
                "adjudicated_unresolved",
            ): (
                "provider_figi_bounce_adjudicated_unresolved",
                "adjudicated_unresolved",
                False,
            ),
            (
                IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
                "confirmed_provider_contamination",
            ): (
                "approved_cross_market_provider_contamination_override",
                "confirmed_provider_contamination",
                True,
            ),
            (
                IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
                "cross_market_adjudicated_unresolved",
            ): (
                "cross_market_composite_adjudicated_unresolved",
                "cross_market_adjudicated_unresolved",
                False,
            ),
            (
                IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE,
                "confirmed_provider_composite_stale_after_transition",
            ): (
                "approved_provider_composite_override",
                "provider_composite_stale_after_transition",
                True,
            ),
            (
                IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE,
                "provider_composite_override_adjudicated_unresolved",
            ): (
                "adjudicated_unresolved",
                "adjudicated_unresolved",
                False,
            ),
        }
        expected = matrix.get((selected.registry_kind, selected.identity_disposition))
        if expected is None:  # pragma: no cover - RegistryDecision proves the closed domain
            raise I3FixtureRunnerError("Composite decision has no closed output disposition")
    if len(share) > 1:  # pragma: no cover - one selected field exists in the row contract
        raise I3FixtureRunnerError(
            "multiple Share Class decisions escaped dispatch collision checks"
        )
    if share and share[0].identity_disposition == "share_class_adjudicated_unresolved":
        expected = ("adjudicated_unresolved", "adjudicated_unresolved", False)
    if expected is None:
        return
    actual = (
        str(row.get("identity_resolution_method")),
        str(row.get("identity_disposition")),
        bool(row.get("backtest_identity_eligible")),
    )
    if actual != expected:
        raise I3FixtureRunnerError("selected registry decision row uses another resolution shape")


def _identity_observation(row: Mapping[str, object]) -> IdentityObservation:
    return IdentityObservation(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker=str(row["ticker"]),
        session_date=_native_date(row["session_date"], "dispatch observation session"),
        observed_composite_figi=_optional_string(row.get("observed_composite_figi")),
        observed_composite_country=_optional_string(row.get("observed_composite_market_code")),
        observed_share_class_figi=_optional_string(row.get("observed_share_class_figi")),
        primary_exchange=_optional_string(row.get("primary_exchange_mic")),
        source_record_id=str(row["selected_source_record_id"]),
        active_on_date=bool(row["active_on_date"]),
    )


def _validate_v2_rows(table_name: str, rows: Sequence[Mapping[str, object]]) -> None:
    contract = I3_V2_CONTRACTS[table_name]
    expected = {item.name for item in contract.columns}
    for row in rows:
        if set(row) != expected:
            raise I3FixtureRunnerError(
                f"{table_name} fields differ: missing={sorted(expected - set(row))}, "
                f"extra={sorted(set(row) - expected)}"
            )
    try:
        table = pa.Table.from_pylist([dict(row) for row in rows], schema=contract.arrow_schema)
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise I3FixtureRunnerError(f"cannot construct exact {table_name} v2 rows") from exc
    for field, column in zip(table.schema, table.columns, strict=True):
        if not field.nullable and column.null_count:
            raise I3FixtureRunnerError(f"{table_name}.{field.name} contains forbidden nulls")
    keys = list(zip(*(table[name].to_pylist() for name in contract.primary_key), strict=True))
    if len(keys) != len(set(keys)):
        raise I3FixtureRunnerError(f"{table_name} primary key is duplicated")


def _read_exact_window(
    reader: ResolvedRowReader, requested: tuple[date, ...]
) -> dict[date, tuple[dict[str, object], ...]]:
    raw = reader(requested)
    if not isinstance(raw, Mapping) or set(raw) != set(requested):
        raise I3FixtureRunnerError("resolved reader returned sessions outside the fixed window")
    result: dict[date, tuple[dict[str, object], ...]] = {}
    expected_fields = {item.name for item in S7_DERIVED_CONTRACTS["universe_daily"].columns}
    for session in requested:
        values = raw[session]
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
            raise I3FixtureRunnerError("resolved reader returned a non-row sequence")
        copied = tuple(dict(value) for value in values)
        tickers: list[str] = []
        for row in copied:
            if set(row) != expected_fields:
                raise I3FixtureRunnerError("resolved reader row fields differ from immutable S7 v1")
            if _native_date(row.get("session_date"), "resolved reader session") != session:
                raise I3FixtureRunnerError("resolved reader row escaped its requested session")
            ticker = row.get("ticker")
            if not isinstance(ticker, str) or not ticker:
                raise I3FixtureRunnerError("resolved reader ticker is invalid")
            tickers.append(ticker)
        if tickers != sorted(tickers) or len(tickers) != len(set(tickers)):
            raise I3FixtureRunnerError(
                "resolved reader rows must be sorted and unique by ticker within session"
            )
        result[session] = copied
    return result


def _require_native_checkpoint(checkpoint: I3CheckpointState) -> None:
    if not isinstance(checkpoint, I3CheckpointState):
        raise I3FixtureRunnerError("runner requires an authenticated I3 checkpoint")
    # Exact closed roundtrip detects forged instances and replays every invariant.
    if I3CheckpointState.from_dict(checkpoint.to_dict()) != checkpoint:
        raise I3FixtureRunnerError("checkpoint exact roundtrip differs")
    if checkpoint.parent_release.release_id == LEGACY_S7_V1_RELEASE_SET_ID:
        raise I3FixtureRunnerError("legacy S7 v1 cannot masquerade as native v2")
    if checkpoint.parent_release.release_family != NATIVE_V2_FIXTURE_RELEASE_FAMILY:
        raise I3FixtureRunnerError("fixture runner requires the fixture release family")
    if checkpoint.schema_digest != I3_V2_SCHEMA_BUNDLE_DIGEST:
        raise I3FixtureRunnerError("checkpoint does not bind the native-v2 schema bundle")
    if checkpoint.transform_semantics_digest != I3_FIXTURE_RUNNER_SEMANTICS_DIGEST:
        raise I3FixtureRunnerError("checkpoint transform semantics differ from this runner")


def _require_s4_window_binding(
    binding: I3FixtureS4WindowBinding,
    *,
    requested: tuple[date, ...],
    availability_session: date,
) -> None:
    if not isinstance(binding, I3FixtureS4WindowBinding):
        raise I3FixtureRunnerError("runner requires an exact typed S4 window binding")
    if binding.requested_sessions != requested:
        raise I3FixtureRunnerError("S4 fixture binding differs from the requested window")
    if any(pin.availability_session > availability_session for pin in binding.partition_pins):
        raise I3FixtureRunnerError("run availability precedes an S4 partition pin")


def _result_release_manifest(
    *,
    prior_release: NativeV2ParentReleasePin,
    source_checkpoint_id: str,
    target_session: date,
    availability_session: date,
    resolved_state_digest: str,
    materialized: _DayMaterialization,
    universe_partition_pin: ArtifactPin,
) -> NativeV2ReleaseManifest:
    if prior_release.release_family != NATIVE_V2_FIXTURE_RELEASE_FAMILY:
        raise I3FixtureRunnerError("fixture runner cannot advance a production release family")
    return NativeV2ReleaseManifest(
        release_family=NATIVE_V2_FIXTURE_RELEASE_FAMILY,
        terminal_session=target_session,
        release_available_session=availability_session,
        native_v2_migration_id=prior_release.native_v2_migration_id,
        identity_policy_bundle_id=prior_release.identity_policy_bundle_id,
        transform_semantics_digest=I3_FIXTURE_RUNNER_SEMANTICS_DIGEST,
        resolved_state_digest=resolved_state_digest,
        output_artifacts=_materialized_output_pins(
            session_date=target_session,
            universe_partition_pin=universe_partition_pin,
            materialized=materialized,
        ),
        parent_release_id=prior_release.release_id,
        source_checkpoint_id=source_checkpoint_id,
    )


def _materialized_output_pins(
    *,
    session_date: date,
    universe_partition_pin: ArtifactPin,
    materialized: _DayMaterialization,
) -> tuple[NativeV2OutputArtifact, ...]:
    prefix = f"outputs/session_date={session_date.isoformat()}"
    artifacts = {
        "asset_master": _content_pin(f"{prefix}/asset_master", materialized.asset_rows),
        "ticker_alias": _content_pin(f"{prefix}/ticker_alias", materialized.alias_rows),
        "issuer_master": _content_pin(f"{prefix}/issuer_master", materialized.issuer_rows),
        "universe_daily": universe_partition_pin,
    }
    rows = {
        "asset_master": materialized.asset_rows,
        "ticker_alias": materialized.alias_rows,
        "issuer_master": materialized.issuer_rows,
        "universe_daily": materialized.universe_rows,
    }
    return tuple(
        NativeV2OutputArtifact(
            table_name=table_name,
            session_date=session_date,
            row_count=len(rows[table_name]),
            contract_id=I3_V2_CONTRACTS[table_name].contract_id,
            schema_digest=I3_V2_CONTRACTS[table_name].schema_digest,
            artifact=artifacts[table_name],
        )
        for table_name in I3_V2_TABLE_ORDER
    )


def _freeze_fixture_input_binding(
    *,
    window: Mapping[date, Sequence[Mapping[str, object]]],
    requested: tuple[date, ...],
    target_session: date,
    s4_window_binding: I3FixtureS4WindowBinding,
    reference_metadata_by_source_id: Mapping[str, Mapping[str, object]],
    reference_metadata_available_session: date | None,
    output_available_session: date,
) -> tuple[dict[str, dict[str, str | None]], dict[str, object]]:
    """Bind the exact caller-supplied fixture oracle without granting authority.

    The S4 pins are deliberately treated as declarative lineage in this local
    fixture boundary.  Combining them with the canonical supplied row and
    metadata projections prevents accidental mixing, but it does not prove
    that the rows came from the pinned Parquet.  A production loader must do
    that content authentication separately.
    """

    if tuple(window) != requested:
        raise I3FixtureRunnerError("fixture input window order differs from its request")
    if s4_window_binding.requested_sessions != requested:
        raise I3FixtureRunnerError("fixture input window differs from its S4 lineage binding")
    if target_session not in window:
        raise I3FixtureRunnerError("fixture input binding lacks its target session")
    source_rows = tuple(
        {
            "selected_source_record_id": str(row.get("selected_source_record_id")),
            "session_date": session.isoformat(),
            "ticker": str(row.get("ticker")),
        }
        for session in requested
        for row in window[session]
    )
    source_record_set_digest = stable_digest(
        {
            "rule_version": I3_FIXTURE_INPUT_BINDING_RULE_VERSION,
            "source_rows": sorted(
                source_rows,
                key=lambda item: (
                    item["session_date"],
                    item["ticker"],
                    item["selected_source_record_id"],
                ),
            ),
        }
    )
    resolved_row_window_digest = stable_digest(
        {
            "requested_sessions": [item.isoformat() for item in requested],
            "resolved_rows": {
                session.isoformat(): [_normalise_row_for_digest(row) for row in window[session]]
                for session in requested
            },
            "rule_version": I3_FIXTURE_INPUT_BINDING_RULE_VERSION,
            "s4_window_binding_digest": s4_window_binding.binding_digest,
            "selected_source_record_set_digest": source_record_set_digest,
        }
    )

    target_source_ids = {
        str(row.get("selected_source_record_id")) for row in window[target_session]
    }
    normalized_metadata: dict[str, dict[str, str | None]] = {}
    metadata_records: list[dict[str, object]] = []
    for source_record_id in sorted(target_source_ids):
        raw = reference_metadata_by_source_id.get(source_record_id)
        if raw is None:
            continue
        if not isinstance(raw, Mapping):
            raise I3FixtureRunnerError("fixture reference metadata record must be a mapping")
        if set(raw) - {"reference_name", "sic_code"}:
            raise I3FixtureRunnerError("fixture reference metadata contains unknown fields")
        reference_name = _clean_optional_text(raw.get("reference_name"))
        sic_code = _clean_optional_text(raw.get("sic_code"))
        if reference_name is None and sic_code is None:
            continue
        normalized_metadata[source_record_id] = {
            "reference_name": reference_name,
            "sic_code": sic_code,
        }
        metadata_records.append(
            {
                "reference_name": reference_name,
                "selected_source_record_id": source_record_id,
                "sic_code": sic_code,
            }
        )
    if normalized_metadata and reference_metadata_available_session is None:
        raise I3FixtureRunnerError(
            "fixture reference metadata requires an explicit availability session"
        )
    if reference_metadata_available_session is not None:
        metadata_available = _native_date(
            reference_metadata_available_session,
            "fixture reference metadata availability",
        )
        if metadata_available > output_available_session:
            raise I3FixtureRunnerError(
                "fixture reference metadata availability exceeds output availability"
            )
    reference_metadata_digest = stable_digest(
        {
            "records": metadata_records,
            "rule_version": I3_FIXTURE_INPUT_BINDING_RULE_VERSION,
        }
    )
    logical = {
        "authority": "local_fixture_oracle_non_authoritative",
        "reference_metadata_available_session": (
            reference_metadata_available_session.isoformat()
            if reference_metadata_available_session is not None
            else None
        ),
        "reference_metadata_digest": reference_metadata_digest,
        "requested_sessions": [item.isoformat() for item in requested],
        "resolved_row_window_digest": resolved_row_window_digest,
        "rule_version": I3_FIXTURE_INPUT_BINDING_RULE_VERSION,
        "s4_pins_authenticate_resolved_rows": False,
        "s4_window_binding_digest": s4_window_binding.binding_digest,
        "selected_source_record_set_digest": source_record_set_digest,
        "target_session": target_session.isoformat(),
    }
    return normalized_metadata, {**logical, "binding_digest": stable_digest(logical)}


def _freeze_fixture_receipt(payload: Mapping[str, object]) -> dict[str, object]:
    if "receipt_id" in payload:
        raise I3FixtureRunnerError("fixture receipt payload contains a self hash")
    logical = {
        **dict(payload),
        "receipt_scope": "local_fixture_diagnostic_non_authoritative",
    }
    return {**logical, "receipt_id": stable_digest(_jsonable(logical))}


def _receipt(
    *,
    checkpoint: I3CheckpointState,
    next_checkpoint: I3CheckpointState,
    requested: tuple[date, ...],
    target_session: date,
    materialized: _DayMaterialization,
    s4_window_binding: I3FixtureS4WindowBinding,
    dispatch_attestation_ids: tuple[str, ...],
    fixture_input_binding: dict[str, object],
) -> dict[str, object]:
    return _freeze_fixture_receipt(
        {
            "artifact_type": "s7_5_i3_fixture_single_session_receipt",
            "dispatcher_qa_receipt_id": materialized.qa["dispatcher_qa_receipt_id"],
            "input_checkpoint_id": checkpoint.checkpoint_id,
            "input_window_digest": fixture_input_binding["resolved_row_window_digest"],
            "fixture_input_binding": fixture_input_binding,
            "fixture_input_binding_digest": fixture_input_binding["binding_digest"],
            "dispatch_attestation_ids": list(dispatch_attestation_ids),
            "output_checkpoint_id": next_checkpoint.checkpoint_id,
            "output_row_counts": {
                "asset_master": len(materialized.asset_rows),
                "issuer_master": len(materialized.issuer_rows),
                "ticker_alias": len(materialized.alias_rows),
                "universe_daily": len(materialized.universe_rows),
            },
            "publish_authorized": False,
            "qa_catalog_digest": materialized.qa["qa_catalog_digest"],
            "qa_result_digest": materialized.qa["qa_result_digest"],
            "requested_sessions": [item.isoformat() for item in requested],
            "runner_semantics_digest": I3_FIXTURE_RUNNER_SEMANTICS_DIGEST,
            "s4_window_binding_digest": s4_window_binding.binding_digest,
            "target_session": target_session.isoformat(),
            "native_v2_release_id": next_checkpoint.parent_release.release_id,
        }
    )


def _terminal_state(
    table: str,
    stable_key: str,
    version_id: str,
    predecessor: str | None,
    row: Mapping[str, object],
    availability: date,
    *,
    operation: str,
) -> TerminalRowVersionState:
    payload_digest = stable_digest(_jsonable(row))
    index = _content_pin(
        f"indexes/{table}/{stable_key}/{version_id}",
        {
            "operation": operation,
            "row_payload_digest": payload_digest,
            "row_version_id": version_id,
            "stable_row_key": stable_key,
            "table": table,
        },
    )
    return TerminalRowVersionState(
        table_name=table,
        stable_row_key=stable_key,
        row_version_id=version_id,
        predecessor_row_version_id=predecessor,
        row_payload_digest=payload_digest,
        index_artifact=index,
        availability_session=availability,
    )


def _advance_unresolved(
    prior: UnresolvedSubjectState | None,
    *,
    ticker: str,
    target_session: date,
    reason: str,
    source_record_id: str,
    availability_session: date,
) -> UnresolvedSubjectState:
    return UnresolvedSubjectState(
        subject_kind="ticker_identity",
        subject_key=ticker,
        first_observed_session=prior.first_observed_session if prior else target_session,
        last_observed_session=target_session,
        reason_codes=tuple(sorted({*(prior.reason_codes if prior else ()), reason})),
        source_record_set_digest=_advance_digest(
            prior.source_record_set_digest if prior else None, source_record_id
        ),
        state_available_session=availability_session,
    )


def _source_bindings(row: Mapping[str, object]) -> dict[str, object]:
    try:
        return {name: row[name] for name in _SOURCE_BINDING_FIELDS}
    except KeyError as exc:  # pragma: no cover - target contract validation catches this first
        raise I3FixtureRunnerError(f"source binding field is missing: {exc.args[0]}") from exc


def _aggregate_state_digest(state: AssetAggregateState | IssuerAggregateState) -> str:
    payload = state.to_dict()
    payload.pop("terminal_row_version_id")
    return stable_digest(payload)


def _advance_digest(previous: str | None, source_record_id: str) -> str:
    return stable_digest(
        {
            "previous_source_record_set_digest": previous,
            "source_record_id": source_record_id,
        }
    )


def _union(values: Sequence[str], value: str | None) -> tuple[str, ...]:
    return tuple(sorted({*values, *(() if value is None else (value,))}))


def _reason_code(value: str) -> str:
    cleaned = "".join(character if character.isalnum() else "_" for character in value.lower())
    cleaned = cleaned.strip("_") or "pending_review"
    return cleaned if cleaned[0].isalpha() else f"identity_{cleaned}"


def _calendar(values: Sequence[date]) -> tuple[date, ...]:
    result = tuple(values)
    if not result or any(type(item) is not date for item in result):
        raise I3FixtureRunnerError("calendar must contain native dates")
    if result != tuple(sorted(set(result))):
        raise I3FixtureRunnerError("calendar must be sorted and unique")
    return result


def _native_date(value: object, label: str) -> date:
    if type(value) is not date:
        raise I3FixtureRunnerError(f"{label} must be a native date")
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise I3FixtureRunnerError("optional identity field must be a nonempty string")
    return value


def _clean_optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise I3FixtureRunnerError("reference metadata must be nonempty text")
    return value.strip()


def _normalise_row_for_digest(row: Mapping[str, object]) -> dict[str, object]:
    return {str(key): _jsonable(value) for key, value in sorted(row.items())}


def _jsonable(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in sorted(value.items())}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


def _content_pin(label: str, payload: object) -> ArtifactPin:
    content = json.dumps(
        _jsonable(payload), allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    slug = label.strip("/")
    return ArtifactPin(
        path=f"fixtures/i3/{slug}.json",
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


__all__ = [
    "FIXED_BOUNDARY_LOOKBACK_SESSIONS",
    "I3_FIXTURE_INPUT_BINDING_RULE_VERSION",
    "I3_FIXTURE_RUNNER_SEMANTICS",
    "I3_FIXTURE_RUNNER_SEMANTICS_DIGEST",
    "I3FixtureRunResult",
    "I3FixtureRunnerError",
    "I3FixtureS4WindowBinding",
    "ResolvedRowReader",
    "bootstrap_native_v2_fixture",
    "bootstrap_native_v2_fixture_history",
    "legacy_oracle_universe_projection",
    "run_i3_fixture_session",
]
