from __future__ import annotations

from dataclasses import replace

import pytest
from test_silver_s7_5_i3_runner import (
    RUN_AVAILABLE,
    _bootstrap,
    _digest,
    _legacy_row,
    _policy,
)

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.incremental_i3_migration_core import (
    I3MigrationError,
    LegacyAssetAggregateProjection,
    LegacyIssuerAggregateProjection,
    build_asset_aggregate_state,
    build_issuer_aggregate_state,
    materialize_asset_root,
    materialize_issuer_root,
    migrate_alias_root,
    migrate_universe_rows,
    migration_source_seed_digest,
)
from ame_stocks_api.silver.incremental_identity import AliasResolutionDisposition

_ALIAS_ADDED = {
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
_ASSET_ADDED = {
    "aggregate_state_digest",
    "asset_master_version_id",
    "predecessor_asset_master_version_id",
    "version_available_session",
}
_ISSUER_ADDED = {
    "aggregate_state_digest",
    "issuer_master_version_id",
    "predecessor_issuer_master_version_id",
    "version_available_session",
}


def _legacy_alias_and_masters():
    result = _bootstrap()
    legacy_universe = _legacy_row(result.target_session)
    legacy_alias = {
        key: value for key, value in result.ticker_alias_rows[0].items() if key not in _ALIAS_ADDED
    }
    legacy_alias.update(
        {
            "ticker_alias_id": legacy_universe["ticker_alias_id"],
            "ticker_alias_id_rule_version": (
                "ame_stocks_ticker_alias_id_from_observed_and_canonical_interval_v3"
            ),
        }
    )
    legacy_asset = {
        key: value for key, value in result.asset_master_rows[0].items() if key not in _ASSET_ADDED
    }
    legacy_issuer = {
        key: value
        for key, value in result.issuer_master_rows[0].items()
        if key not in _ISSUER_ADDED
    }
    return result, legacy_universe, legacy_alias, legacy_asset, legacy_issuer


def _asset_projection(result) -> LegacyAssetAggregateProjection:
    state = result.checkpoint.asset_aggregates[0]
    return LegacyAssetAggregateProjection(
        observed_tickers=state.observed_tickers,
        observed_composite_figis=state.observed_composite_figis,
        observed_share_class_figis=state.observed_share_class_figis,
        observed_issuer_ids=state.observed_issuer_ids,
        canonical_share_class_figis=state.canonical_share_class_figis,
        identity_adjudication_ids=state.identity_adjudication_ids,
        genuine_transition_identity_adjudication_ids=(
            state.genuine_transition_identity_adjudication_ids
        ),
        provider_contamination_identity_adjudication_ids=(
            state.provider_contamination_identity_adjudication_ids
        ),
        cross_market_adjudication_ids=state.cross_market_adjudication_ids,
        provider_composite_override_ids=state.provider_composite_override_ids,
        share_class_adjudication_ids=state.share_class_adjudication_ids,
        asset_transition_ids=state.asset_transition_ids,
        source_record_seed_digest=_digest("migration-asset-seed"),
    )


def _issuer_projection(result) -> LegacyIssuerAggregateProjection:
    state = result.checkpoint.issuer_aggregates[0]
    return LegacyIssuerAggregateProjection(
        observed_asset_ids=state.observed_asset_ids,
        observed_tickers=state.observed_tickers,
        reference_names=state.reference_names,
        sic_codes=state.sic_codes,
        source_record_seed_digest=_digest("migration-issuer-seed"),
    )


def test_compact_root_migration_roundtrips_every_v1_projection() -> None:
    result, legacy_universe, legacy_alias, legacy_asset, legacy_issuer = _legacy_alias_and_masters()
    alias = migrate_alias_root(
        legacy_alias,
        policy=_policy(),
        migration_available_session=RUN_AVAILABLE,
        source_record_seed_digest=_digest("migration-alias-seed"),
    )
    asset_state = build_asset_aggregate_state(
        legacy_asset,
        _asset_projection(result),
        migration_available_session=RUN_AVAILABLE,
    )
    asset_row, asset_state = materialize_asset_root(
        legacy_asset, asset_state, available_session=RUN_AVAILABLE
    )
    issuer_state = build_issuer_aggregate_state(
        legacy_issuer,
        _issuer_projection(result),
        migration_available_session=RUN_AVAILABLE,
    )
    issuer_row, issuer_state = materialize_issuer_root(
        legacy_issuer, issuer_state, available_session=RUN_AVAILABLE
    )
    universe = migrate_universe_rows(
        (legacy_universe,),
        aliases_by_legacy_id={alias.legacy_ticker_alias_id: alias},
        asset_version_by_id={asset_state.asset_id: asset_state.terminal_row_version_id},
        issuer_version_by_id={issuer_state.issuer_id: issuer_state.terminal_row_version_id},
        identity_policy_bundle_id=_policy().identity_policy_bundle_id,
        row_available_session=RUN_AVAILABLE,
    )

    assert asset_row["asset_master_version_id"] == asset_state.terminal_row_version_id
    assert issuer_row["issuer_master_version_id"] == issuer_state.terminal_row_version_id
    assert universe[0]["alias_segment_id"] == alias.state.segment.alias_segment_id
    assert universe[0]["asset_master_version_id"] == asset_state.terminal_row_version_id
    assert universe[0]["issuer_master_version_id"] == issuer_state.terminal_row_version_id
    assert (
        migrate_alias_root(
            legacy_alias,
            policy=_policy(),
            migration_available_session=RUN_AVAILABLE,
            source_record_seed_digest=_digest("migration-alias-seed"),
        )
        == alias
    )


def test_compact_alias_migration_preserves_exact_mixed_case_provider_ticker() -> None:
    _, _, legacy_alias, _, _ = _legacy_alias_and_masters()
    legacy_alias = dict(legacy_alias)
    legacy_alias["ticker"] = "AANw"

    migrated = migrate_alias_root(
        legacy_alias,
        policy=_policy(),
        migration_available_session=RUN_AVAILABLE,
        source_record_seed_digest=_digest("mixed-case-alias-seed"),
    )

    assert migrated.state.segment.ticker == "AANw"
    assert migrated.row["ticker"] == "AANw"


def test_share_only_v1_observed_consistent_becomes_narrow_v2_disposition() -> None:
    _, _, legacy_alias, _, _ = _legacy_alias_and_masters()
    share_decision_id = _digest("share-only-decision")
    legacy_alias = dict(legacy_alias)
    legacy_alias.update(
        {
            "share_class_adjudication_id": share_decision_id,
            "share_class_adjudication_available_session": RUN_AVAILABLE,
            "canonical_share_class_figi": "BBG001S5N8V8",
            "share_class_id": stable_digest(
                {
                    "anchor_type": "share_class_figi",
                    "anchor_value": "BBG001S5N8V8",
                    "namespace": "ame_stocks.identity.share_class",
                    "rule_version": "ame_stocks_share_class_id_from_share_class_figi_v1",
                }
            ),
            "observed_share_class_figi": "BBG000C3K505",
            "identity_disposition": "observed_consistent",
            "alias_resolution_method": "source_composite_figi_exact",
        }
    )
    migrated = migrate_alias_root(
        legacy_alias,
        policy=_policy(),
        migration_available_session=RUN_AVAILABLE,
        source_record_seed_digest=_digest("share-only-seed"),
    )
    assert (
        migrated.state.resolution.disposition
        is AliasResolutionDisposition.TRANSIENT_DUPLICATE_SHARE_CLASS
    )
    assert migrated.row["identity_disposition"] == "observed_consistent"
    assert migrated.row["decision_lineage_ids"] == []
    assert migrated.row["share_class_decision_lineage_ids"] == [share_decision_id]


def test_aggregate_projection_count_mismatch_fails_closed() -> None:
    result, _, _, legacy_asset, legacy_issuer = _legacy_alias_and_masters()
    asset_projection = replace(_asset_projection(result), observed_tickers=("AAPL", "FORGED"))
    with pytest.raises(I3MigrationError, match="differs from v1 counts"):
        build_asset_aggregate_state(
            legacy_asset,
            asset_projection,
            migration_available_session=RUN_AVAILABLE,
        )
    issuer_projection = replace(
        _issuer_projection(result), reference_names=("Apple Inc.", "Forged Inc.")
    )
    with pytest.raises(I3MigrationError, match="differs from v1 counts"):
        build_issuer_aggregate_state(
            legacy_issuer,
            issuer_projection,
            migration_available_session=RUN_AVAILABLE,
        )


def test_universe_missing_alias_or_master_version_fails_closed() -> None:
    _, legacy_universe, _, _, _ = _legacy_alias_and_masters()
    with pytest.raises(I3MigrationError, match="alias is absent"):
        migrate_universe_rows(
            (legacy_universe,),
            aliases_by_legacy_id={},
            asset_version_by_id={},
            issuer_version_by_id={},
            identity_policy_bundle_id=_policy().identity_policy_bundle_id,
            row_available_session=RUN_AVAILABLE,
        )


def test_migration_source_seed_binds_legacy_row_and_partition_set() -> None:
    _, _, legacy_alias, _, _ = _legacy_alias_and_masters()
    first = migration_source_seed_digest(
        table_name="ticker_alias",
        stable_row_key=legacy_alias["ticker_alias_id"],
        legacy_row=legacy_alias,
        legacy_release_set_id=_digest("legacy-release"),
        legacy_partition_set_digest=_digest("partitions"),
    )
    assert first == migration_source_seed_digest(
        table_name="ticker_alias",
        stable_row_key=legacy_alias["ticker_alias_id"],
        legacy_row=legacy_alias,
        legacy_release_set_id=_digest("legacy-release"),
        legacy_partition_set_digest=_digest("partitions"),
    )
    assert first != migration_source_seed_digest(
        table_name="ticker_alias",
        stable_row_key=legacy_alias["ticker_alias_id"],
        legacy_row=legacy_alias,
        legacy_release_set_id=_digest("legacy-release"),
        legacy_partition_set_digest=_digest("other-partitions"),
    )
