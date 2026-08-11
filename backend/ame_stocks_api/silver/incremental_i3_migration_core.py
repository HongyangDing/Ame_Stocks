"""Pure compact migration primitives for the production S7.5 I3 base.

The immutable S7 v1 release remains the canonical oracle for every historical
row.  A production native-v2 base therefore does not replay one master-table
version per historical session.  Instead it creates:

* one v2 alias root for each immutable v1 alias interval;
* one terminal root for each v1 asset and issuer master row; and
* one v2 universe row per v1 membership row with exact version foreign keys.

The caller is responsible for authenticating the v1/S4 inputs and for deriving
the complete aggregate set projections.  This module validates those
projections against the legacy master counts, mints stable v2 identities, and
proves that removing the v2 envelope reproduces the supplied v1 rows exactly.
It performs no filesystem discovery, IO, publication, or cutover.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from typing import Final

import pyarrow as pa

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.identity_resolution_contract import S7_DERIVED_CONTRACTS
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    ASSET_COUNTER_NAMES,
    ISSUER_COUNTER_NAMES,
    AggregateCount,
    AssetAggregateState,
    IdentityPolicyBundle,
    IssuerAggregateState,
    OpenAliasState,
)
from ame_stocks_api.silver.incremental_i3_contract import I3_V2_CONTRACTS
from ame_stocks_api.silver.incremental_identity import (
    ALIAS_RESOLUTION_VERSION_ID_RULE_VERSION,
    ALIAS_SEGMENT_ID_RULE_VERSION,
    AliasResolutionDisposition,
    AliasResolutionMethod,
    AliasResolutionStatus,
    AliasResolutionVersion,
    AliasSegmentIdentity,
    ShareClassResolutionMethod,
)

MIGRATION_RULE_VERSION: Final = "s7_5_i3_compact_v1_base_migration_v1"
MIGRATION_SOURCE_SEED_RULE_VERSION: Final = "s7_5_i3_v1_source_seed_digest_v1"
ASSET_VERSION_NAMESPACE: Final = "ame_stocks.silver.asset_master_version"
ISSUER_VERSION_NAMESPACE: Final = "ame_stocks.silver.issuer_master_version"

_ASSET_V2_FIELDS = frozenset(
    {
        "aggregate_state_digest",
        "asset_master_version_id",
        "predecessor_asset_master_version_id",
        "version_available_session",
    }
)
_ISSUER_V2_FIELDS = frozenset(
    {
        "aggregate_state_digest",
        "issuer_master_version_id",
        "predecessor_issuer_master_version_id",
        "version_available_session",
    }
)
_ALIAS_V2_FIELDS = frozenset(
    {
        "alias_is_tombstone",
        "alias_resolution_version_id",
        "alias_resolution_version_id_rule_version",
        "alias_segment_id",
        "alias_segment_id_rule_version",
        "alias_tombstone_reason_code",
        "alias_version_available_session",
        "decision_lineage_ids",
        "evidence_cutoff_session",
        "identity_policy_bundle_id",
        "predecessor_alias_resolution_version_id",
        "provider_id",
        "provider_locale",
        "provider_market",
        "resolution_available_session",
        "segment_origin_source_record_id",
        "share_class_decision_lineage_ids",
        "source_record_set_digest",
    }
)
_UNIVERSE_V2_FIELDS = frozenset(
    {
        "alias_resolution_version_id",
        "alias_segment_id",
        "asset_master_version_id",
        "identity_policy_bundle_id",
        "issuer_master_version_id",
        "row_available_session",
    }
)


class I3MigrationError(ValueError):
    """Raised when a legacy row cannot be migrated without semantic drift."""


@dataclass(frozen=True, slots=True)
class LegacyAssetAggregateProjection:
    """Complete distinct-set projection omitted from the compact v1 master."""

    observed_tickers: tuple[str, ...]
    observed_composite_figis: tuple[str, ...]
    observed_share_class_figis: tuple[str, ...]
    observed_issuer_ids: tuple[str, ...]
    canonical_share_class_figis: tuple[str, ...]
    identity_adjudication_ids: tuple[str, ...]
    genuine_transition_identity_adjudication_ids: tuple[str, ...]
    provider_contamination_identity_adjudication_ids: tuple[str, ...]
    cross_market_adjudication_ids: tuple[str, ...]
    provider_composite_override_ids: tuple[str, ...]
    share_class_adjudication_ids: tuple[str, ...]
    asset_transition_ids: tuple[str, ...]
    source_record_seed_digest: str

    def __post_init__(self) -> None:
        for values, label in (
            (self.observed_tickers, "observed tickers"),
            (self.observed_composite_figis, "observed Composite FIGIs"),
            (self.observed_share_class_figis, "observed ShareClass FIGIs"),
            (self.observed_issuer_ids, "observed issuer IDs"),
            (self.canonical_share_class_figis, "canonical ShareClass FIGIs"),
            (self.identity_adjudication_ids, "identity adjudication IDs"),
            (
                self.genuine_transition_identity_adjudication_ids,
                "genuine-transition identity adjudication IDs",
            ),
            (
                self.provider_contamination_identity_adjudication_ids,
                "provider-contamination identity adjudication IDs",
            ),
            (self.cross_market_adjudication_ids, "cross-market adjudication IDs"),
            (self.provider_composite_override_ids, "provider Composite override IDs"),
            (self.share_class_adjudication_ids, "ShareClass adjudication IDs"),
            (self.asset_transition_ids, "asset transition IDs"),
        ):
            _sorted_unique(values, label)
        _digest(self.source_record_seed_digest, "asset source-record seed digest")


@dataclass(frozen=True, slots=True)
class LegacyIssuerAggregateProjection:
    """Complete issuer sets needed for safe future incremental updates."""

    observed_asset_ids: tuple[str, ...]
    observed_tickers: tuple[str, ...]
    reference_names: tuple[str, ...]
    sic_codes: tuple[str, ...]
    source_record_seed_digest: str

    def __post_init__(self) -> None:
        for values, label in (
            (self.observed_asset_ids, "issuer observed asset IDs"),
            (self.observed_tickers, "issuer observed tickers"),
            (self.reference_names, "issuer reference names"),
            (self.sic_codes, "issuer SIC codes"),
        ):
            _sorted_unique(values, label)
        _digest(self.source_record_seed_digest, "issuer source-record seed digest")


@dataclass(frozen=True, slots=True)
class MigratedAliasRoot:
    """One compact v1 alias interval represented as one native-v2 root."""

    legacy_ticker_alias_id: str
    row: Mapping[str, object]
    state: OpenAliasState

    def __post_init__(self) -> None:
        _digest(self.legacy_ticker_alias_id, "legacy ticker-alias ID")
        _require_fields(self.row, "ticker_alias", v2=True)
        if self.row["alias_segment_id"] != self.state.segment.alias_segment_id:
            raise I3MigrationError("alias row segment ID differs from migrated state")
        if (
            self.row["alias_resolution_version_id"]
            != self.state.resolution.alias_resolution_version_id
        ):
            raise I3MigrationError("alias row version ID differs from migrated state")


def migration_source_seed_digest(
    *,
    table_name: str,
    stable_row_key: str,
    legacy_row: Mapping[str, object],
    legacy_release_set_id: str,
    legacy_partition_set_digest: str,
) -> str:
    """Bind an aggregate seed to the immutable v1 base instead of 69M Python hashes."""

    if table_name not in {"asset_master", "issuer_master", "ticker_alias"}:
        raise I3MigrationError("migration source seed table is invalid")
    _digest(stable_row_key, "migration stable row key")
    _digest(legacy_release_set_id, "legacy release-set ID")
    _digest(legacy_partition_set_digest, "legacy partition-set digest")
    return stable_digest(
        {
            "legacy_partition_set_digest": legacy_partition_set_digest,
            "legacy_release_set_id": legacy_release_set_id,
            "legacy_row": _jsonable(dict(legacy_row)),
            "rule_version": MIGRATION_SOURCE_SEED_RULE_VERSION,
            "stable_row_key": stable_row_key,
            "table_name": table_name,
        }
    )


def migrate_alias_root(
    legacy_row: Mapping[str, object],
    *,
    policy: IdentityPolicyBundle,
    migration_available_session: date,
    source_record_seed_digest: str,
) -> MigratedAliasRoot:
    """Migrate one exact eligible v1 alias interval without changing its v1 projection."""

    _require_fields(legacy_row, "ticker_alias", v2=False)
    if not bool(legacy_row["backtest_identity_eligible"]):
        raise I3MigrationError("v1 ticker_alias roots must be identity eligible")
    if migration_available_session < policy.policy_available_session:
        raise I3MigrationError("alias migration availability precedes policy availability")
    _digest(source_record_seed_digest, "alias source-record seed digest")
    legacy_alias_id = _text(legacy_row["ticker_alias_id"], "legacy ticker-alias ID")
    _digest(legacy_alias_id, "legacy ticker-alias ID")
    segment = AliasSegmentIdentity(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker=_text(legacy_row["ticker"], "ticker"),
        observed_composite_figi=_optional_text(legacy_row["observed_composite_figi"]),
        observed_share_class_figi=_optional_text(legacy_row["observed_share_class_figi"]),
        observed_cik_normalized=_optional_text(legacy_row["observed_cik_normalized"]),
        valid_from_session=_date(legacy_row["valid_from_session"], "alias valid-from"),
        segment_origin_source_record_id=_text(
            legacy_row["first_source_record_id"], "alias origin source record"
        ),
    )
    method, disposition, decision_ids = _resolution_shape(legacy_row)
    share_id = _optional_text(legacy_row["share_class_adjudication_id"])
    if share_id is not None:
        _digest(share_id, "ShareClass adjudication ID")
    share_method = (
        ShareClassResolutionMethod.APPROVED_SHARE_CLASS_ADJUDICATION
        if share_id is not None
        else ShareClassResolutionMethod.DIRECT_OBSERVED
        if legacy_row["observed_share_class_figi"] is not None
        else ShareClassResolutionMethod.NOT_APPLICABLE
    )
    evidence_available = _date(
        legacy_row["identity_evidence_available_session"],
        "alias identity evidence availability",
    )
    resolution_available = max(policy.policy_available_session, evidence_available)
    identity_cutoff = max(policy.policy_cutoff_session, resolution_available)
    resolution = AliasResolutionVersion.for_segment(
        segment,
        canonical_asset_id=_optional_text(legacy_row["asset_id"]),
        canonical_composite_figi=_optional_text(legacy_row["canonical_composite_figi"]),
        canonical_share_class_id=_optional_text(legacy_row["share_class_id"]),
        canonical_share_class_figi=_optional_text(legacy_row["canonical_share_class_figi"]),
        canonical_issuer_id=_optional_text(legacy_row["issuer_id"]),
        canonical_cik_normalized=_optional_text(legacy_row["canonical_cik_normalized"]),
        resolution_method=method,
        resolution_status=AliasResolutionStatus.RESOLVED,
        disposition=disposition,
        decision_lineage_ids=decision_ids,
        share_class_resolution_method=share_method,
        share_class_decision_lineage_ids=(() if share_id is None else (share_id,)),
        identity_policy_bundle_id=policy.identity_policy_bundle_id,
        identity_cutoff_session=identity_cutoff,
        resolution_available_session=resolution_available,
        evidence_cutoff_session=identity_cutoff,
        evidence_available_session=evidence_available,
        valid_through_session=_date(legacy_row["valid_through_session"], "alias valid-through"),
        source_record_set_digest=source_record_seed_digest,
        predecessor_alias_resolution_version_id=None,
        is_tombstone=False,
        tombstone_reason_code=None,
    )
    row = {
        key: value
        for key, value in legacy_row.items()
        if key not in {"ticker_alias_id", "ticker_alias_id_rule_version"}
    }
    row.update(
        {
            "alias_is_tombstone": False,
            "alias_resolution_version_id": resolution.alias_resolution_version_id,
            "alias_resolution_version_id_rule_version": (ALIAS_RESOLUTION_VERSION_ID_RULE_VERSION),
            "alias_segment_id": segment.alias_segment_id,
            "alias_segment_id_rule_version": ALIAS_SEGMENT_ID_RULE_VERSION,
            "alias_tombstone_reason_code": None,
            "alias_version_available_session": migration_available_session,
            "decision_lineage_ids": list(decision_ids),
            "evidence_cutoff_session": identity_cutoff,
            "identity_policy_bundle_id": policy.identity_policy_bundle_id,
            "predecessor_alias_resolution_version_id": None,
            "provider_id": "massive",
            "provider_locale": "us",
            "provider_market": "stocks",
            "resolution_available_session": resolution_available,
            "segment_origin_source_record_id": segment.segment_origin_source_record_id,
            "share_class_decision_lineage_ids": [] if share_id is None else [share_id],
            "source_record_set_digest": source_record_seed_digest,
        }
    )
    _validate_row("ticker_alias", row)
    _assert_legacy_projection("ticker_alias", row, legacy_row, legacy_alias_id=legacy_alias_id)
    return MigratedAliasRoot(
        legacy_ticker_alias_id=legacy_alias_id,
        row=row,
        state=OpenAliasState(segment=segment, resolution=resolution),
    )


def build_asset_aggregate_state(
    legacy_row: Mapping[str, object],
    projection: LegacyAssetAggregateProjection,
    *,
    migration_available_session: date,
) -> AssetAggregateState:
    """Reconstruct the complete checkpoint state behind one compact v1 asset row."""

    _require_fields(legacy_row, "asset_master", v2=False)
    _validate_asset_projection(legacy_row, projection)
    counter_values = {
        name: _integer(legacy_row[name], f"asset counter {name}") for name in ASSET_COUNTER_NAMES
    }
    # The v1 compatibility row deliberately counted cross-market decisions in
    # both its provider-contamination total and its dedicated cross-market
    # total.  Native-v2 checkpoint state stores disjoint complete decision
    # sets, so only its internal distinct counters are normalized here.  The
    # materialized v1 projection remains byte-for-byte compatible.
    counter_values.update(
        {
            "cross_market_adjudication_count": len(projection.cross_market_adjudication_ids),
            "genuine_transition_adjudication_count": len(
                projection.genuine_transition_identity_adjudication_ids
            ),
            "identity_adjudication_count": len(projection.identity_adjudication_ids),
            "provider_composite_override_count": len(projection.provider_composite_override_ids),
            "provider_contamination_adjudication_count": len(
                projection.provider_contamination_identity_adjudication_ids
            ),
            "share_class_adjudication_count": len(projection.share_class_adjudication_ids),
        }
    )
    counters = tuple(AggregateCount(name, counter_values[name]) for name in ASSET_COUNTER_NAMES)
    canonical_shares = projection.canonical_share_class_figis
    state = AssetAggregateState(
        asset_id=_text(legacy_row["asset_id"], "asset ID"),
        canonical_composite_figi=_text(
            legacy_row["canonical_composite_figi"], "canonical Composite FIGI"
        ),
        canonical_share_class_figi=(canonical_shares[0] if len(canonical_shares) == 1 else None),
        canonical_share_class_figis=canonical_shares,
        terminal_row_version_id=stable_digest(
            {"migration_placeholder": legacy_row["asset_id"], "table": "asset_master"}
        ),
        first_direct_observed_session=_optional_date(
            legacy_row["first_direct_observed_session"], "first direct observation"
        ),
        last_direct_observed_session=_optional_date(
            legacy_row["last_direct_observed_session"], "last direct observation"
        ),
        first_canonical_membership_session=_optional_date(
            legacy_row["first_canonical_membership_session"], "first membership"
        ),
        last_canonical_membership_session=_optional_date(
            legacy_row["last_canonical_membership_session"], "last membership"
        ),
        observed_tickers=projection.observed_tickers,
        observed_composite_figis=projection.observed_composite_figis,
        observed_share_class_figis=projection.observed_share_class_figis,
        observed_issuer_ids=projection.observed_issuer_ids,
        identity_adjudication_ids=projection.identity_adjudication_ids,
        genuine_transition_identity_adjudication_ids=(
            projection.genuine_transition_identity_adjudication_ids
        ),
        provider_contamination_identity_adjudication_ids=(
            projection.provider_contamination_identity_adjudication_ids
        ),
        cross_market_adjudication_ids=projection.cross_market_adjudication_ids,
        provider_composite_override_ids=projection.provider_composite_override_ids,
        share_class_adjudication_ids=projection.share_class_adjudication_ids,
        asset_transition_ids=projection.asset_transition_ids,
        predecessor_asset_ids=tuple(legacy_row["predecessor_asset_ids"]),
        successor_asset_ids=tuple(legacy_row["successor_asset_ids"]),
        counters=counters,
        source_record_set_digest=projection.source_record_seed_digest,
        identity_evidence_available_session=_date(
            legacy_row["identity_evidence_available_session"], "asset evidence availability"
        ),
        state_available_session=migration_available_session,
    )
    return state


def materialize_asset_root(
    legacy_row: Mapping[str, object], state: AssetAggregateState, *, available_session: date
) -> tuple[dict[str, object], AssetAggregateState]:
    """Attach one native-v2 root envelope to an exact v1 asset row."""

    _require_fields(legacy_row, "asset_master", v2=False)
    if legacy_row["asset_id"] != state.asset_id:
        raise I3MigrationError("asset state belongs to another legacy row")
    aggregate_digest = _aggregate_digest(state)
    version_id = stable_digest(
        {
            "aggregate_state_digest": aggregate_digest,
            "availability_session": available_session.isoformat(),
            "namespace": ASSET_VERSION_NAMESPACE,
            "predecessor_asset_master_version_id": None,
            "stable_row_key": state.asset_id,
            "v1_payload": _jsonable(dict(legacy_row)),
        }
    )
    row = {
        **legacy_row,
        "asset_master_version_id": version_id,
        "predecessor_asset_master_version_id": None,
        "version_available_session": available_session,
        "aggregate_state_digest": aggregate_digest,
    }
    _validate_row("asset_master", row)
    _assert_legacy_projection("asset_master", row, legacy_row)
    return row, replace(state, terminal_row_version_id=version_id)


def build_issuer_aggregate_state(
    legacy_row: Mapping[str, object],
    projection: LegacyIssuerAggregateProjection,
    *,
    migration_available_session: date,
) -> IssuerAggregateState:
    """Reconstruct the complete checkpoint state behind one compact v1 issuer row."""

    _require_fields(legacy_row, "issuer_master", v2=False)
    _validate_issuer_projection(legacy_row, projection)
    return IssuerAggregateState(
        issuer_id=_text(legacy_row["issuer_id"], "issuer ID"),
        cik_normalized=_text(legacy_row["cik_normalized"], "normalized CIK"),
        terminal_row_version_id=stable_digest(
            {"migration_placeholder": legacy_row["issuer_id"], "table": "issuer_master"}
        ),
        first_observed_session=_date(
            legacy_row["first_observed_session"], "issuer first observation"
        ),
        last_observed_session=_date(legacy_row["last_observed_session"], "issuer last observation"),
        observed_asset_ids=projection.observed_asset_ids,
        observed_tickers=projection.observed_tickers,
        reference_names=projection.reference_names,
        sic_codes=projection.sic_codes,
        counters=tuple(
            AggregateCount(name, _integer(legacy_row[name], f"issuer counter {name}"))
            for name in ISSUER_COUNTER_NAMES
        ),
        source_record_set_digest=projection.source_record_seed_digest,
        reference_available_session=_date(
            legacy_row["reference_available_session"], "issuer reference availability"
        ),
        state_available_session=migration_available_session,
    )


def materialize_issuer_root(
    legacy_row: Mapping[str, object], state: IssuerAggregateState, *, available_session: date
) -> tuple[dict[str, object], IssuerAggregateState]:
    """Attach one native-v2 root envelope to an exact v1 issuer row."""

    _require_fields(legacy_row, "issuer_master", v2=False)
    if legacy_row["issuer_id"] != state.issuer_id:
        raise I3MigrationError("issuer state belongs to another legacy row")
    aggregate_digest = _aggregate_digest(state)
    version_id = stable_digest(
        {
            "aggregate_state_digest": aggregate_digest,
            "availability_session": available_session.isoformat(),
            "namespace": ISSUER_VERSION_NAMESPACE,
            "predecessor_issuer_master_version_id": None,
            "stable_row_key": state.issuer_id,
            "v1_payload": _jsonable(dict(legacy_row)),
        }
    )
    row = {
        **legacy_row,
        "issuer_master_version_id": version_id,
        "predecessor_issuer_master_version_id": None,
        "version_available_session": available_session,
        "aggregate_state_digest": aggregate_digest,
    }
    _validate_row("issuer_master", row)
    _assert_legacy_projection("issuer_master", row, legacy_row)
    return row, replace(state, terminal_row_version_id=version_id)


def migrate_universe_rows(
    legacy_rows: Sequence[Mapping[str, object]],
    *,
    aliases_by_legacy_id: Mapping[str, MigratedAliasRoot],
    asset_version_by_id: Mapping[str, str],
    issuer_version_by_id: Mapping[str, str],
    identity_policy_bundle_id: str,
    row_available_session: date,
) -> tuple[dict[str, object], ...]:
    """Add exact v2 FKs while retaining every immutable v1 membership fact."""

    _digest(identity_policy_bundle_id, "identity-policy bundle ID")
    migrated: list[dict[str, object]] = []
    for legacy_row in legacy_rows:
        _require_fields(legacy_row, "universe_daily", v2=False)
        eligible = bool(legacy_row["backtest_identity_eligible"])
        legacy_alias_id = _optional_text(legacy_row["ticker_alias_id"])
        if eligible and legacy_alias_id is None:
            raise I3MigrationError("eligible legacy membership lacks ticker alias")
        if not eligible and legacy_alias_id is not None:
            raise I3MigrationError("ineligible legacy membership carries ticker alias")
        alias = aliases_by_legacy_id.get(legacy_alias_id) if legacy_alias_id else None
        if eligible and alias is None:
            raise I3MigrationError("legacy ticker alias is absent from compact migration map")
        asset_id = _optional_text(legacy_row["asset_id"])
        issuer_id = _optional_text(legacy_row["issuer_id"])
        asset_version = asset_version_by_id.get(asset_id) if eligible and asset_id else None
        issuer_version = issuer_version_by_id.get(issuer_id) if eligible and issuer_id else None
        if eligible and asset_version is None:
            raise I3MigrationError("eligible membership lacks asset root version")
        if eligible and issuer_id is not None and issuer_version is None:
            raise I3MigrationError("eligible membership lacks issuer root version")
        row = {key: value for key, value in legacy_row.items() if key != "ticker_alias_id"}
        row.update(
            {
                "alias_segment_id": alias.state.segment.alias_segment_id if alias else None,
                "alias_resolution_version_id": (
                    alias.state.resolution.alias_resolution_version_id if alias else None
                ),
                "asset_master_version_id": asset_version,
                "issuer_master_version_id": issuer_version,
                "identity_policy_bundle_id": identity_policy_bundle_id,
                "row_available_session": row_available_session,
            }
        )
        _validate_row("universe_daily", row)
        _assert_legacy_projection(
            "universe_daily", row, legacy_row, legacy_alias_id=legacy_alias_id
        )
        migrated.append(row)
    return tuple(migrated)


def _resolution_shape(
    row: Mapping[str, object],
) -> tuple[AliasResolutionMethod, AliasResolutionDisposition, tuple[str, ...]]:
    raw_method = _text(row["alias_resolution_method"], "legacy alias resolution method")
    raw_disposition = _text(row["identity_disposition"], "legacy identity disposition")
    share_decision = row["share_class_adjudication_id"]
    decisions = tuple(
        sorted(
            str(value)
            for value in (
                row["identity_adjudication_id"],
                row["cross_market_adjudication_id"],
                row["provider_composite_override_id"],
                *tuple(row["asset_transition_ids"]),
            )
            if value is not None
        )
    )
    if row["provider_composite_override_id"] is not None:
        return (
            AliasResolutionMethod.APPROVED_PROVIDER_COMPOSITE_OVERRIDE,
            AliasResolutionDisposition.PROVIDER_COMPOSITE_STALE_AFTER_TRANSITION,
            decisions,
        )
    if raw_method == "approved_cross_market_provider_contamination_override":
        return (
            AliasResolutionMethod.APPROVED_CROSS_MARKET_OVERRIDE,
            AliasResolutionDisposition.CONFIRMED_PROVIDER_CONTAMINATION,
            decisions,
        )
    if raw_method == "approved_provider_contamination_override":
        if raw_disposition == "confirmed_genuine_transition":
            return (
                AliasResolutionMethod.APPROVED_GENUINE_TRANSITION,
                AliasResolutionDisposition.CONFIRMED_GENUINE_TRANSITION,
                decisions,
            )
        return (
            AliasResolutionMethod.APPROVED_PROVIDER_CONTAMINATION_OVERRIDE,
            AliasResolutionDisposition.CONFIRMED_PROVIDER_CONTAMINATION,
            decisions,
        )
    if raw_method == "approved_genuine_transition":
        return (
            AliasResolutionMethod.APPROVED_GENUINE_TRANSITION,
            AliasResolutionDisposition.CONFIRMED_GENUINE_TRANSITION,
            decisions,
        )
    if raw_method == "source_composite_figi_exact":
        if share_decision is not None:
            # S7 v1 preserved the Composite-level observed_consistent label for
            # share-only corrections.  The v2 envelope records the narrower,
            # non-asset-changing interpretation without altering v1 columns.
            return (
                AliasResolutionMethod.DIRECT_OBSERVED,
                AliasResolutionDisposition.TRANSIENT_DUPLICATE_SHARE_CLASS,
                (),
            )
        return (
            AliasResolutionMethod.DIRECT_OBSERVED,
            AliasResolutionDisposition.OBSERVED_CONSISTENT,
            (),
        )
    raise I3MigrationError(f"unsupported eligible legacy alias method: {raw_method}")


def _validate_asset_projection(
    row: Mapping[str, object], projection: LegacyAssetAggregateProjection
) -> None:
    # The frozen v1 materializer populated the historically named
    # ``genuine_transition_adjudication_count`` from ``asset_transition_ids``
    # (lineage-only relation decisions), not from identity-adjudication
    # dispositions. Native-v2 separates those two domains: its internal
    # counter is normalized from genuine identity decisions in
    # ``build_asset_aggregate_state``, while this guard validates the exact v1
    # projection against the lineage relation set that produced it.
    expected = {
        "observed_ticker_count": len(projection.observed_tickers),
        "observed_composite_figi_count": len(projection.observed_composite_figis),
        "observed_share_class_figi_count": len(projection.observed_share_class_figis),
        "observed_issuer_count": len(projection.observed_issuer_ids),
        "identity_adjudication_count": len(projection.identity_adjudication_ids),
        "genuine_transition_adjudication_count": len(projection.asset_transition_ids),
        "provider_contamination_adjudication_count": len(
            {
                *projection.provider_contamination_identity_adjudication_ids,
                *projection.cross_market_adjudication_ids,
            }
        ),
        "cross_market_adjudication_count": len(projection.cross_market_adjudication_ids),
        "provider_composite_override_count": len(projection.provider_composite_override_ids),
        "share_class_adjudication_count": len(projection.share_class_adjudication_ids),
    }
    mismatch = {name: (row[name], value) for name, value in expected.items() if row[name] != value}
    if mismatch:
        raise I3MigrationError(f"asset distinct-set projection differs from v1 counts: {mismatch}")
    selected_share = (
        projection.canonical_share_class_figis[0]
        if len(projection.canonical_share_class_figis) == 1
        else None
    )
    if row["canonical_share_class_figi"] != selected_share:
        raise I3MigrationError("asset canonical ShareClass set differs from v1 selection")
    if set(projection.identity_adjudication_ids) != {
        *projection.genuine_transition_identity_adjudication_ids,
        *projection.provider_contamination_identity_adjudication_ids,
    }:
        raise I3MigrationError("asset identity adjudication disposition sets are incomplete")


def _validate_issuer_projection(
    row: Mapping[str, object], projection: LegacyIssuerAggregateProjection
) -> None:
    expected = {
        "observed_asset_count": len(projection.observed_asset_ids),
        "observed_ticker_count": len(projection.observed_tickers),
        "reference_name_variant_count": len(projection.reference_names),
        "sic_code_variant_count": len(projection.sic_codes),
    }
    mismatch = {name: (row[name], value) for name, value in expected.items() if row[name] != value}
    if mismatch:
        raise I3MigrationError(f"issuer distinct-set projection differs from v1 counts: {mismatch}")
    expected_name = projection.reference_names[0] if len(projection.reference_names) == 1 else None
    expected_sic = projection.sic_codes[0] if len(projection.sic_codes) == 1 else None
    if row["reference_name"] != expected_name or row["sic_code_current_reference"] != expected_sic:
        raise I3MigrationError("issuer selected reference value differs from complete sets")


def _assert_legacy_projection(
    table_name: str,
    row: Mapping[str, object],
    legacy_row: Mapping[str, object],
    *,
    legacy_alias_id: str | None = None,
) -> None:
    if table_name == "asset_master":
        projected = {key: value for key, value in row.items() if key not in _ASSET_V2_FIELDS}
    elif table_name == "issuer_master":
        projected = {key: value for key, value in row.items() if key not in _ISSUER_V2_FIELDS}
    elif table_name == "ticker_alias":
        projected = {key: value for key, value in row.items() if key not in _ALIAS_V2_FIELDS}
        projected["ticker_alias_id"] = legacy_alias_id
        projected["ticker_alias_id_rule_version"] = legacy_row["ticker_alias_id_rule_version"]
    elif table_name == "universe_daily":
        projected = {key: value for key, value in row.items() if key not in _UNIVERSE_V2_FIELDS}
        projected["ticker_alias_id"] = legacy_alias_id
    else:  # pragma: no cover - private closed caller set
        raise I3MigrationError("legacy projection table is invalid")
    if projected != dict(legacy_row):
        differing = sorted(
            key
            for key in set(projected) | set(legacy_row)
            if projected.get(key) != legacy_row.get(key)
        )
        raise I3MigrationError(
            f"native-v2 {table_name} does not reproduce exact v1 projection: {differing}"
        )


def _aggregate_digest(state: AssetAggregateState | IssuerAggregateState) -> str:
    body = state.to_dict()
    body.pop("terminal_row_version_id")
    return stable_digest(body)


def _require_fields(row: Mapping[str, object], table_name: str, *, v2: bool) -> None:
    contract = I3_V2_CONTRACTS[table_name] if v2 else S7_DERIVED_CONTRACTS[table_name]
    expected = {item.name for item in contract.columns}
    if set(row) != expected:
        missing = sorted(expected - set(row))
        extra = sorted(set(row) - expected)
        raise I3MigrationError(
            f"{table_name} fields differ from {'v2' if v2 else 'v1'} contract; "
            f"missing={missing}, extra={extra}"
        )


def _validate_row(table_name: str, row: Mapping[str, object]) -> None:
    _require_fields(row, table_name, v2=True)
    try:
        table = pa.Table.from_pylist([dict(row)], schema=I3_V2_CONTRACTS[table_name].arrow_schema)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError) as exc:
        raise I3MigrationError(f"{table_name} row violates native-v2 schema") from exc
    if table.num_rows != 1:  # pragma: no cover - Arrow invariant
        raise I3MigrationError(f"{table_name} row did not materialize exactly once")


def _jsonable(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _sorted_unique(values: tuple[str, ...], label: str) -> None:
    if type(values) is not tuple or values != tuple(sorted(set(values))):
        raise I3MigrationError(f"{label} must be a sorted unique tuple")
    if any(not isinstance(value, str) or not value for value in values):
        raise I3MigrationError(f"{label} contains an invalid value")


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise I3MigrationError(f"{label} must be a non-empty string")
    return value


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    return _text(value, "optional text")


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise I3MigrationError(f"{label} must be a SHA-256 digest")
    return text


def _integer(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise I3MigrationError(f"{label} must be a nonnegative integer")
    return value


def _date(value: object, label: str) -> date:
    if not isinstance(value, date):
        raise I3MigrationError(f"{label} must be a date")
    return value


def _optional_date(value: object, label: str) -> date | None:
    if value is None:
        return None
    return _date(value, label)


__all__ = [
    "MIGRATION_RULE_VERSION",
    "I3MigrationError",
    "LegacyAssetAggregateProjection",
    "LegacyIssuerAggregateProjection",
    "MigratedAliasRoot",
    "build_asset_aggregate_state",
    "build_issuer_aggregate_state",
    "materialize_asset_root",
    "materialize_issuer_root",
    "migrate_alias_root",
    "migrate_universe_rows",
    "migration_source_seed_digest",
]
