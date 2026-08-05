from __future__ import annotations

import hashlib
import json
from pathlib import Path

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.identity_resolution_contract import S7_DERIVED_CONTRACTS
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_CONTRACT_ID_BY_TABLE,
    I3_V2_CONTRACTS,
    I3_V2_RESOURCE_SHA256_BY_TABLE,
    I3_V2_SCHEMA_BUNDLE_DIGEST,
    I3_V2_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_i3_dispatch import (
    I3_QA_CATALOG,
    I3_QA_CATALOG_DIGEST,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
CANDIDATE = (
    REPO_ROOT / "docs/silver/contracts/control/"
    "s7_5_identity_session_incremental_bundle-v1.candidate.json"
)
CANDIDATE_SHA256 = "22bb9d2eb5b01f618e824ee437127937de4434e033dcb8519bd1396ae0228898"
CONTRACT_ID = "4ac6fdb83ef5d0c080c997841406a5bf4614818269ec2a7cc81d833cf4ce4605"


def test_v2_overlay_contracts_are_exact_and_content_addressed() -> None:
    assert tuple(I3_V2_CONTRACTS) == I3_V2_TABLE_ORDER
    assert {table: contract.contract_id for table, contract in I3_V2_CONTRACTS.items()} == dict(
        I3_V2_CONTRACT_ID_BY_TABLE
    )
    assert (
        stable_digest(
            [
                {
                    "contract_id": I3_V2_CONTRACTS[table].contract_id,
                    "resource_sha256": I3_V2_RESOURCE_SHA256_BY_TABLE[table],
                    "schema_digest": I3_V2_CONTRACTS[table].schema_digest,
                    "table": table,
                }
                for table in I3_V2_TABLE_ORDER
            ]
        )
        == I3_V2_SCHEMA_BUNDLE_DIGEST
    )


def test_v2_overlays_preserve_v1_columns_except_explicit_identity_replacement() -> None:
    removed = {
        "asset_master": set(),
        "ticker_alias": {"ticker_alias_id", "ticker_alias_id_rule_version"},
        "issuer_master": set(),
        "universe_daily": {"ticker_alias_id"},
    }
    for table in I3_V2_TABLE_ORDER:
        v1 = {column.name for column in S7_DERIVED_CONTRACTS[table].columns}
        v2 = {column.name for column in I3_V2_CONTRACTS[table].columns}
        assert (v1 - removed[table]).issubset(v2)
        assert removed[table].isdisjoint(v2)

    alias_columns = {column.name for column in I3_V2_CONTRACTS["ticker_alias"].columns}
    assert {
        "alias_segment_id",
        "alias_resolution_version_id",
        "predecessor_alias_resolution_version_id",
        "identity_policy_bundle_id",
        "source_record_set_digest",
    }.issubset(alias_columns)
    universe_columns = {column.name for column in I3_V2_CONTRACTS["universe_daily"].columns}
    assert {
        "alias_segment_id",
        "alias_resolution_version_id",
        "asset_master_version_id",
        "issuer_master_version_id",
        "identity_policy_bundle_id",
        "row_available_session",
    }.issubset(universe_columns)


def test_i3_candidate_hashes_and_local_only_boundary_are_frozen() -> None:
    content = CANDIDATE.read_bytes()
    document = json.loads(content)
    assert hashlib.sha256(content).hexdigest() == CANDIDATE_SHA256
    assert document["contract_id"] == CONTRACT_ID
    assert document["contract_id"] == stable_digest(document["logical_contract"])

    authorization = document["logical_contract"]["authorization_state"]
    assert authorization == {
        "local_fixture_bootstrap_and_execution": True,
        "production_or_remote_parquet_read": False,
        "production_or_remote_staging_write": False,
        "publish": False,
        "registry_mutation": False,
        "replacement_or_correction": False,
        "s8_execution": False,
        "top_release_or_base_cutover": False,
    }
    runner = document["logical_contract"]["runner"]
    assert runner["filesystem_discovery"] is False
    assert runner["legacy_full_runner_wrapping"] is False


def test_candidate_binds_exact_contract_schema_and_resource_digests() -> None:
    document = json.loads(CANDIDATE.read_text(encoding="utf-8"))
    tables = document["logical_contract"]["native_v2_tables"]
    assert tables["combined_schema_bundle_digest"] == I3_V2_SCHEMA_BUNDLE_DIGEST
    for table in I3_V2_TABLE_ORDER:
        assert tables[table] == {
            "contract_id": I3_V2_CONTRACTS[table].contract_id,
            "resource_sha256": I3_V2_RESOURCE_SHA256_BY_TABLE[table],
            "schema_digest": I3_V2_CONTRACTS[table].schema_digest,
        }


def test_candidate_collision_and_boundary_semantics_are_fail_closed() -> None:
    document = json.loads(CANDIDATE.read_text(encoding="utf-8"))
    logical = document["logical_contract"]
    critical = set(logical["qa_policy"]["critical_zero_metrics"])
    assert {
        "multi_registry_composite_override_collision_alias_rows",
        "multi_registry_composite_override_collision_eligible_rows",
        "multi_registry_composite_override_collision_resolved_rows",
    }.issubset(critical)
    assert logical["boundary_dispatcher"]["coverage"] == (
        "target_session_plus_exact_two_preceding_xnys_sessions"
    )
    assert logical["failure_semantics"]["identity_uncertainty"] == (
        "retain_membership_without_alias_and_without_forced_inactive_delist_or_liquidation"
    )


def test_candidate_binds_the_exact_shared_qa_catalog_and_stage_owners() -> None:
    logical = json.loads(CANDIDATE.read_text(encoding="utf-8"))["logical_contract"]
    policy = logical["qa_policy"]
    catalog = [item.to_dict() for item in I3_QA_CATALOG]

    assert policy["catalog_digest"] == I3_QA_CATALOG_DIGEST
    assert policy["catalog"] == catalog
    assert set(policy["critical_zero_metrics"]) == {
        item["check_id"] for item in catalog if item["severity"] == "critical"
    }
    assert set(policy["raw_review_metrics"]) == {
        item["check_id"] for item in catalog if item["severity"] in {"high", "info"}
    }


def test_v2_overlay_qa_severities_match_the_shared_catalog() -> None:
    catalog_severity = {item.check_id: item.severity.value for item in I3_QA_CATALOG}
    for contract in I3_V2_CONTRACTS.values():
        for rule in contract.qa_rules:
            if rule.check_id in catalog_severity:
                assert rule.severity.value == catalog_severity[rule.check_id]


def test_candidate_separates_fixture_and_production_authority_and_exact_parent() -> None:
    logical = json.loads(CANDIDATE.read_text(encoding="utf-8"))["logical_contract"]
    manifest = logical["native_v2_release_manifest"]
    assert manifest["fixture_family"] == "s7_5_native_v2_fixture"
    assert manifest["production_family"] == "s7_5_native_v2"
    assert manifest["family_cross_use"] == "forbidden"
    assert logical["checkpoint"]["exact_loader"].startswith("verify_checkpoint_and_parent_manifest")
    dispatcher = logical["boundary_dispatcher"]
    assert dispatcher["policy_snapshot_input"].startswith("sealed_snapshot_derived_only")
    assert "load_registry_release_set" in dispatcher["production_registry_trust_boundary"]
    assert logical["runner"]["pre_checkpoint_gate"].startswith("any_critical_qa_failure")


def test_candidate_freezes_fixture_input_and_registry_responsibility_boundaries() -> None:
    logical = json.loads(CANDIDATE.read_text(encoding="utf-8"))["logical_contract"]
    fixture = logical["fixture_input_binding"]
    assert fixture["authority"] == "local_fixture_oracle_non_authoritative"
    assert fixture["s4_pin_boundary"].endswith(
        "do_not_authenticate_the_supplied_rows_or_pinned_parquet_content"
    )
    assert "authenticated_i2_run_receipts" in fixture["production_requirement"]
    assert "output_byte_verification" in fixture["production_requirement"]

    responsibilities = logical["identity_policy_bundle"]["responsibility_separation"]
    assert "no_eligibility_change" in responsibilities["asset_transition"]
    assert "no_asset_or_issuer_change" in responsibilities["share_class_adjudication"]
    assert logical["identity_policy_bundle"]["provider_transition_binding"].endswith(
        "confirmed_genuine_asset_transition_relation"
    )
    assert (
        "not_output_file_bytes"
        in logical["native_v2_release_manifest"]["output_verification_boundary"]
    )

    matrix = logical["identity_policy_bundle"]["exact_disposition_matrix"]
    assert matrix == {
        "asset_transition": {
            "asset_transition_adjudicated_unresolved": {
                "canonical_composite": "unchanged_by_registry",
                "canonical_share_class": "unchanged_by_registry",
                "eligibility": "unchanged_by_registry",
                "row_disposition": "unchanged_by_registry",
                "row_method": "unchanged_by_registry",
                "row_status": "unchanged_by_registry",
                "transition_edge": "none_lineage_only",
            },
            "confirmed_genuine_transition": {
                "canonical_composite": "unchanged_by_registry",
                "canonical_share_class": "unchanged_by_registry",
                "eligibility": "unchanged_by_registry",
                "row_disposition": "unchanged_by_registry",
                "row_method": "unchanged_by_registry",
                "row_status": "unchanged_by_registry",
                "transition_edge": "symmetric_predecessor_successor",
            },
        },
        "identity_adjudication": {
            "adjudicated_unresolved": {
                "canonical_composite": None,
                "canonical_share_class": None,
                "eligibility": False,
                "row_disposition": "adjudicated_unresolved",
                "row_method": "provider_figi_bounce_adjudicated_unresolved",
                "row_status": "unresolved",
                "transition_edge": "none",
            },
            "confirmed_genuine_transition": {
                "canonical_composite": "observed_composite",
                "canonical_share_class": "observed_or_exact_share_adjudication",
                "eligibility": True,
                "row_disposition": "confirmed_genuine_transition",
                "row_method": "approved_genuine_transition",
                "row_status": "resolved_approved_override",
                "transition_edge": "none",
            },
            "confirmed_provider_contamination": {
                "canonical_composite": "distinct_registry_target",
                "canonical_share_class": "observed_or_exact_share_adjudication",
                "eligibility": True,
                "row_disposition": "confirmed_provider_contamination",
                "row_method": "approved_provider_contamination_override",
                "row_status": "resolved_approved_override",
                "transition_edge": "none",
            },
        },
        "identity_cross_market_adjudication": {
            "confirmed_provider_contamination": {
                "canonical_composite": "distinct_us_registry_target",
                "canonical_share_class": "observed_or_exact_share_adjudication",
                "eligibility": True,
                "row_disposition": "confirmed_provider_contamination",
                "row_method": "approved_cross_market_provider_contamination_override",
                "row_status": "resolved_approved_override",
                "transition_edge": "none",
            },
            "cross_market_adjudicated_unresolved": {
                "canonical_composite": None,
                "canonical_share_class": None,
                "eligibility": False,
                "row_disposition": "cross_market_adjudicated_unresolved",
                "row_method": "cross_market_composite_adjudicated_unresolved",
                "row_status": "unresolved",
                "transition_edge": "none",
            },
        },
        "provider_composite_override": {
            "confirmed_provider_composite_stale_after_transition": {
                "canonical_composite": "same_market_registry_target",
                "canonical_share_class": "observed_or_exact_share_adjudication",
                "eligibility": True,
                "row_disposition": "provider_composite_stale_after_transition",
                "row_method": "approved_provider_composite_override",
                "row_status": "resolved_approved_override",
                "transition_edge": "confirmed_relation_lineage_only",
            },
            "provider_composite_override_adjudicated_unresolved": {
                "canonical_composite": None,
                "canonical_share_class": None,
                "eligibility": False,
                "row_disposition": "adjudicated_unresolved",
                "row_method": "adjudicated_unresolved",
                "row_status": "unresolved",
                "transition_edge": "confirmed_relation_lineage_only",
            },
        },
        "share_class_adjudication": {
            "confirmed_share_class_correction": {
                "canonical_composite": "preserve_unique_composite_result",
                "canonical_share_class": "exact_registry_target",
                "eligibility": "preserve_unique_composite_result",
                "row_disposition": "preserve_composite_result",
                "row_method": "preserve_composite_result",
                "row_status": "preserve_composite_result",
                "transition_edge": "none",
            },
            "share_class_adjudicated_unresolved": {
                "canonical_composite": None,
                "canonical_share_class": None,
                "eligibility": False,
                "row_disposition": "adjudicated_unresolved",
                "row_method": "adjudicated_unresolved",
                "row_status": "unresolved",
                "transition_edge": "none",
            },
        },
    }


def test_candidate_does_not_claim_overlay_only_qa_was_run() -> None:
    logical = json.loads(CANDIDATE.read_text(encoding="utf-8"))["logical_contract"]
    qa_policy = logical["qa_policy"]
    assert qa_policy["overlay_only_checks"].endswith(
        "deferred_to_the_future_production_content_validator"
    )
    assert "typed_reason_counts_sum_to_observed_count" in qa_policy["raw_review_output"]
