from __future__ import annotations

import hashlib
import json
from pathlib import Path

from ame_stocks_api.silver.asset_contract import (
    ASSET_CONTRACTS,
    ASSET_OBSERVATION_DAILY_CONTRACT,
    ASSET_OBSERVATION_DAILY_CONTRACT_ID,
    ASSET_OBSERVATION_VERSION_CONTRACT,
    ASSET_OBSERVATION_VERSION_CONTRACT_ID,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT_ID,
)
from ame_stocks_api.silver.contracts import QASeverity, QAStatus, TableContract

_ROOT = Path(__file__).resolve().parents[1]
_CANDIDATES = {
    "asset_observation_daily": (
        _ROOT / "docs/silver/contracts/identity/asset_observation_daily.schema-v1.candidate.json"
    ),
    "asset_observation_version": (
        _ROOT / "docs/silver/contracts/identity/asset_observation_version.schema-v1.candidate.json"
    ),
    "universe_source_daily": (
        _ROOT / "docs/silver/contracts/reference/universe_source_daily.schema-v1.candidate.json"
    ),
}
_EXPECTED_IDS = {
    "asset_observation_daily": ("dd916b8528b9ce1a341e6b8ad897ae80e40d5df118b8e102e4ea1f1ea6e9c045"),
    "asset_observation_version": (
        "14ce114f5911f7e4d1c15e58f0f42a8307066d6517e859d6233fa23c199616fc"
    ),
    "universe_source_daily": ("9711320ee9227df347224b7cd17a41fe10a352fddf089cd72b758bde7a7f0c58"),
}
_EXPECTED_SCHEMA_DIGESTS = {
    "asset_observation_daily": ("402d0ea624dc26e43ea63974572ede5a46ae20e0741e97a3d01d07075a71bc1e"),
    "asset_observation_version": (
        "4c797ca373d697078b2061b9a76696dc036a1d2db0a5f8e1fe3ce2dac4b6bb4b"
    ),
    "universe_source_daily": ("78b799cd5a2621b5a78e4ed8c23c090f6aea686fcd786366e5c258e81ad278a5"),
}
_PROFILE_PATH = _ROOT / "docs/silver/source-profiles/assets-full-2026-07-13.json"
_PROFILE_SHA256 = "5d813c13d6e79c8da43d230b223b19e3d6aebb9846f865be1236e4299e6e48a6"


def _contract(name: str) -> TableContract:
    return TableContract.from_dict(json.loads(_CANDIDATES[name].read_text(encoding="utf-8")))


def test_s4_full_source_profile_summary_is_machine_readable_and_reconciled() -> None:
    content = _PROFILE_PATH.read_bytes()
    profile = json.loads(content)

    assert hashlib.sha256(content).hexdigest() == _PROFILE_SHA256
    assert profile["profile_summary_schema_version"] == 1
    assert profile["total_rows"] == 69_381_182
    assert profile["total_pages"] == 72_038
    assert profile["authoritative_inputs"]["manifest_count"] == 5_026
    assert profile["authoritative_inputs"]["session_count"] == 2_513
    assert profile["manifest_profile"]["active_rows"] == 25_630_067
    assert profile["manifest_profile"]["inactive_rows"] == 43_751_115
    assert all(value == 0 for value in profile["hard_gate_numerators"].values())

    for field in profile["field_profile"].values():
        assert field["present"] + field["missing"] == profile["total_rows"]
        assert field["explicit_null"] == 0
        assert field["empty"] == 0
        assert field["wrong_native_type"] == 0

    duplicates = profile["duplicate_versions"]
    assert duplicates["group_count"] == 4_853
    assert duplicates["exact_canonical_object_group_count"] == 2
    assert duplicates["last_updated_only_group_count"] == 2_115
    assert duplicates["delisted_and_last_updated_only_group_count"] == 2_736
    assert duplicates["non_exact_unique_max_last_updated_groups"] == 4_851
    assert duplicates["selected_unresolved_groups"] == 0
    assert (
        sum(
            duplicates[name]
            for name in (
                "exact_canonical_object_group_count",
                "last_updated_only_group_count",
                "delisted_and_last_updated_only_group_count",
            )
        )
        == duplicates["group_count"]
    )
    assert profile["row_funnel"]["expected_universe_rows"] == (
        profile["total_rows"] - duplicates["duplicate_excess_rows"]
    )
    assert profile["distinct_counts"] == {"primary_exchange": 7, "type": 15}


def test_s4_candidates_are_valid_and_packaged_byte_for_byte_as_approved() -> None:
    contracts = {name: _contract(name) for name in _CANDIDATES}

    assert {name: contract.contract_id for name, contract in contracts.items()} == _EXPECTED_IDS
    assert {
        name: contract.schema_digest for name, contract in contracts.items()
    } == _EXPECTED_SCHEMA_DIGESTS
    assert all(
        TableContract.from_dict(contract.to_dict()) == contract for contract in contracts.values()
    )
    assert {
        name: (contract.domain, contract.table, contract.schema_version)
        for name, contract in contracts.items()
    } == {
        "asset_observation_daily": ("identity", "asset_observation_daily", 1),
        "asset_observation_version": ("identity", "asset_observation_version", 1),
        "universe_source_daily": ("reference", "universe_source_daily", 1),
    }
    assert all(contract.source_datasets == ("assets",) for contract in contracts.values())

    approved_root = _ROOT / "backend/ame_stocks_api/silver/schema_resources"
    resources = {
        "asset_observation_daily": (approved_root / "asset_observation_daily.schema-v1.json"),
        "asset_observation_version": (approved_root / "asset_observation_version.schema-v1.json"),
        "universe_source_daily": approved_root / "universe_source_daily.schema-v1.json",
    }
    assert all(
        resources[name].read_bytes() == path.read_bytes() for name, path in _CANDIDATES.items()
    )
    assert dict(ASSET_CONTRACTS) == contracts
    assert contracts["asset_observation_daily"] == ASSET_OBSERVATION_DAILY_CONTRACT
    assert contracts["asset_observation_version"] == ASSET_OBSERVATION_VERSION_CONTRACT
    assert contracts["universe_source_daily"] == UNIVERSE_SOURCE_DAILY_CONTRACT
    assert _EXPECTED_IDS["asset_observation_daily"] == ASSET_OBSERVATION_DAILY_CONTRACT_ID
    assert _EXPECTED_IDS["asset_observation_version"] == ASSET_OBSERVATION_VERSION_CONTRACT_ID
    assert _EXPECTED_IDS["universe_source_daily"] == UNIVERSE_SOURCE_DAILY_CONTRACT_ID


def test_asset_observation_daily_freezes_full_source_evidence_without_deduplication() -> None:
    contract = _contract("asset_observation_daily")
    columns = {column.name: column for column in contract.columns}

    assert tuple(columns) == (
        "session_year",
        "session_date",
        "requested_active",
        "provider_active",
        "ticker",
        "type_code",
        "name",
        "market",
        "locale",
        "primary_exchange_mic",
        "currency_name",
        "cik",
        "composite_figi",
        "share_class_figi",
        "delisted_utc_raw",
        "delisted_at_utc",
        "last_updated_utc_raw",
        "last_updated_at_utc",
        "reference_time_scope",
        "metadata_time_scope",
        "source_capture_at_utc",
        "source_available_session",
        "source_available_at_utc",
        "source_availability_rule",
        "source_availability_quality",
        "source_record_id",
        "source_request_id",
        "source_provider_request_id",
        "source_artifact_sha256",
        "source_page_sequence",
        "source_row_ordinal",
        "source_row_hash",
    )
    assert {name for name, spec in columns.items() if spec.nullable} == {
        "type_code",
        "name",
        "market",
        "locale",
        "primary_exchange_mic",
        "currency_name",
        "cik",
        "composite_figi",
        "share_class_figi",
        "delisted_utc_raw",
        "delisted_at_utc",
        "last_updated_utc_raw",
        "last_updated_at_utc",
    }
    assert contract.primary_key == ("session_date", "source_record_id")
    assert contract.partition_by == ("session_year", "session_date")
    assert contract.sort_by == (
        "session_date",
        "ticker",
        "requested_active",
        "source_page_sequence",
        "source_row_ordinal",
    )
    assert "duplicate ticker versions remain separate rows" in contract.grain
    assert "without trim" in columns["ticker"].description
    assert "never research availability" in columns["last_updated_at_utc"].description


def test_asset_observation_version_is_a_bounded_duplicate_projection() -> None:
    contract = _contract("asset_observation_version")
    columns = {column.name: column for column in contract.columns}

    assert tuple(columns) == (
        "session_year",
        "session_date",
        "requested_active",
        "ticker",
        "version_group_id",
        "version_count",
        "source_record_id",
        "identity_signature",
        "difference_fields_json",
        "last_updated_at_utc",
        "delisted_at_utc",
        "selection_rank",
        "is_selected",
        "selection_status",
        "selection_reason",
        "selection_rule_version",
        "selected_source_record_id",
        "source_capture_at_utc",
        "source_request_id",
        "source_provider_request_id",
        "source_artifact_sha256",
        "source_page_sequence",
        "source_row_ordinal",
        "source_row_hash",
    )
    assert {name for name, spec in columns.items() if spec.nullable} == {
        "last_updated_at_utc",
        "delisted_at_utc",
        "selection_rank",
        "selected_source_record_id",
    }
    assert contract.primary_key == (
        "session_date",
        "version_group_id",
        "source_record_id",
    )
    assert contract.partition_by == ("session_year", "session_date")
    assert "more than one row" in contract.grain
    assert "singleton observations are intentionally excluded" in contract.description
    assert (
        "active,ticker,type,name,market,locale,primary_exchange"
        in columns["identity_signature"].description
    )
    assert "[delisted_utc,last_updated_utc]" in columns["difference_fields_json"].description
    assert "resolved_exact_duplicate" in columns["selection_status"].description
    assert "unresolved_timestamp_tie" in columns["selection_status"].description
    assert "canonical-JSON-equivalent" in columns["selection_rule_version"].description
    assert "unique maximum last_updated wins" in columns["selection_rule_version"].description

    rules = {rule.check_id: rule for rule in contract.qa_rules}
    assert rules["singleton_version_rows"].severity is QASeverity.CRITICAL
    assert rules["identity_conflict_selected_groups"].severity is QASeverity.CRITICAL
    assert rules["hash_only_semantic_selection_groups"].severity is QASeverity.CRITICAL
    assert rules["unresolved_version_groups"].severity is QASeverity.HIGH
    assert rules["delisted_changed_groups"].failure_status is QAStatus.WARNING


def test_universe_source_daily_is_source_membership_not_final_identity() -> None:
    contract = _contract("universe_source_daily")
    columns = {column.name: column for column in contract.columns}

    assert tuple(columns) == (
        "session_year",
        "session_date",
        "ticker",
        "active_on_date",
        "type_code",
        "name",
        "market",
        "locale",
        "primary_exchange_mic",
        "currency_name",
        "cik",
        "composite_figi",
        "share_class_figi",
        "delisted_at_utc",
        "last_updated_at_utc",
        "identity_link_status",
        "selected_source_record_id",
        "version_group_id",
        "source_version_count",
        "selection_status",
        "selection_rule_version",
        "reference_time_scope",
        "metadata_time_scope",
        "active_source_request_id",
        "inactive_source_request_id",
        "source_pair_id",
        "selected_source_capture_at_utc",
        "universe_capture_completed_at_utc",
        "source_available_session",
        "source_available_at_utc",
        "source_availability_rule",
        "source_availability_quality",
        "source_request_id",
        "source_provider_request_id",
        "source_artifact_sha256",
        "source_page_sequence",
        "source_row_ordinal",
        "source_row_hash",
    )
    assert contract.primary_key == ("session_date", "ticker")
    assert contract.partition_by == ("session_year", "session_date")
    assert contract.sort_by == ("session_date", "ticker")
    assert {name for name, spec in columns.items() if spec.nullable} == {
        "type_code",
        "name",
        "market",
        "locale",
        "primary_exchange_mic",
        "currency_name",
        "cik",
        "composite_figi",
        "share_class_figi",
        "delisted_at_utc",
        "last_updated_at_utc",
        "version_group_id",
    }
    assert (
        "complete active=true and active=false request pair"
        in columns["universe_capture_completed_at_utc"].description
    )
    assert "No value is an asset ID" in columns["identity_link_status"].description
    assert "unresolved groups cannot enter" in columns["selection_status"].description
    assert "resolved_unique_latest_last_updated" in columns["selection_status"].description
    assert "active_source_request_id" in columns["source_pair_id"].description


def test_s4_contracts_freeze_fail_closed_qa_and_forbid_future_semantics() -> None:
    contracts = {name: _contract(name) for name in _CANDIDATES}

    for contract in contracts.values():
        assert all(type(rule.limit) is float and rule.limit == 0.0 for rule in contract.qa_rules)
        for rule in contract.qa_rules:
            if rule.severity in {QASeverity.CRITICAL, QASeverity.HIGH}:
                assert rule.failure_status is QAStatus.FAILED

    observation_rules = {
        rule.check_id: rule for rule in contracts["asset_observation_daily"].qa_rules
    }
    universe_rules = {rule.check_id: rule for rule in contracts["universe_source_daily"].qa_rules}
    for check_id in {
        "active_inactive_overlap_rows",
        "provider_active_mismatch_rows",
        "source_integrity_invalid",
        "source_session_pair_cardinality_invalid",
    }:
        assert observation_rules[check_id].severity is QASeverity.CRITICAL
    assert universe_rules["current_dictionary_backfill_rows"].severity is QASeverity.CRITICAL
    assert universe_rules["identity_link_status_invalid_rows"].severity is QASeverity.CRITICAL
    assert universe_rules["source_pair_lineage_invalid_rows"].severity is QASeverity.CRITICAL
    assert universe_rules["identity_evidence_missing_rows"].failure_status is QAStatus.WARNING
    assert observation_rules["optional_field_type_invalid_rows"].severity is QASeverity.HIGH
    assert observation_rules["provider_scope_invalid_rows"].severity is QASeverity.HIGH

    forbidden = {
        "asset_id",
        "issuer_id",
        "ticker_alias",
        "research_eligibility",
        "is_research_eligible",
        "coarse_type",
        "normalized_ticker",
        "normalized_type_code",
        "candidate_asset_id",
    }
    for contract in contracts.values():
        assert forbidden.isdisjoint(column.name for column in contract.columns)

    assert (
        "current-only" in observation_rules["current_type_dictionary_unmatched_values"].description
    )
    assert (
        "diagnostic only"
        in universe_rules["current_exchange_dictionary_unmatched_values"].description
    )
