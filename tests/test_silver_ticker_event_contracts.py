from __future__ import annotations

import json
from pathlib import Path

from ame_stocks_api.silver.contracts import QASeverity, QAStatus, TableContract
from ame_stocks_api.silver.ticker_event_contract import (
    TICKER_CHANGE_EVENT_CONTRACT,
    TICKER_CHANGE_EVENT_CONTRACT_ID,
    TICKER_EVENT_CONTRACTS,
    TICKER_EVENT_REQUEST_STATUS_CONTRACT,
    TICKER_EVENT_REQUEST_STATUS_CONTRACT_ID,
)

_ROOT = Path(__file__).resolve().parents[1]
_CANDIDATE_ROOT = _ROOT / "docs/silver/contracts/identity"
_RESOURCE_ROOT = _ROOT / "backend/ame_stocks_api/silver/schema_resources"
_PATHS = {
    "ticker_change_event": (
        _CANDIDATE_ROOT / "ticker_change_event.schema-v1.candidate.json",
        _RESOURCE_ROOT / "ticker_change_event.schema-v1.json",
    ),
    "ticker_event_request_status": (
        _CANDIDATE_ROOT / "ticker_event_request_status.schema-v1.candidate.json",
        _RESOURCE_ROOT / "ticker_event_request_status.schema-v1.json",
    ),
}
_EXPECTED_IDS = {
    "ticker_change_event": "48a46dfd810b95137125b336917c23343da2aace5a6a71d99129b4d10f2e59b1",
    "ticker_event_request_status": (
        "5890117915e8ffc585c2faa1b9f4a9909a75f068bdad50a5e6bd64f78cf1df02"
    ),
}
_EXPECTED_SCHEMA_DIGESTS = {
    "ticker_change_event": "b643a7381e3fd704800aa703aa6c621173b951c40c80ce806ae491f578715fb2",
    "ticker_event_request_status": (
        "8c80dbd8c56508c1c03a5a7a8ecd08c2de9e6afefdf7d4edef027c9c0bea7c88"
    ),
}


def _candidate(name: str) -> TableContract:
    path, _ = _PATHS[name]
    return TableContract.from_dict(json.loads(path.read_text(encoding="utf-8")))


def test_s5_candidates_are_valid_packaged_and_frozen() -> None:
    candidates = {name: _candidate(name) for name in _PATHS}

    assert {name: item.contract_id for name, item in candidates.items()} == _EXPECTED_IDS
    assert {
        name: item.schema_digest for name, item in candidates.items()
    } == _EXPECTED_SCHEMA_DIGESTS
    assert all(TableContract.from_dict(item.to_dict()) == item for item in candidates.values())
    assert all(
        candidate_path.read_bytes() == resource_path.read_bytes()
        for candidate_path, resource_path in _PATHS.values()
    )
    assert dict(TICKER_EVENT_CONTRACTS) == candidates
    assert candidates["ticker_change_event"] == TICKER_CHANGE_EVENT_CONTRACT
    assert candidates["ticker_event_request_status"] == TICKER_EVENT_REQUEST_STATUS_CONTRACT
    assert _EXPECTED_IDS["ticker_change_event"] == TICKER_CHANGE_EVENT_CONTRACT_ID
    assert _EXPECTED_IDS["ticker_event_request_status"] == TICKER_EVENT_REQUEST_STATUS_CONTRACT_ID
    assert all(item.source_datasets == ("ticker_events",) for item in candidates.values())


def test_ticker_change_event_freezes_occurrence_grain_and_fields() -> None:
    contract = _candidate("ticker_change_event")
    columns = {item.name: item for item in contract.columns}

    assert (contract.domain, contract.table, contract.schema_version) == (
        "identity",
        "ticker_change_event",
        1,
    )
    assert tuple(columns) == (
        "source_capture_date",
        "event_date_raw",
        "event_date",
        "event_date_quality",
        "event_date_is_weekend",
        "event_date_is_known_cluster",
        "same_figi_date_multiple_tickers",
        "event_type",
        "effective_ticker",
        "requested_identifier_type",
        "requested_identifier",
        "response_name",
        "response_cik",
        "response_composite_figi",
        "identity_evidence_scope",
        "backtest_identity_eligible",
        "source_capture_at_utc",
        "source_available_session",
        "source_available_at_utc",
        "source_availability_rule",
        "source_availability_quality",
        "source_record_id",
        "source_request_id",
        "source_provider_request_id",
        "source_manifest_sha256",
        "source_artifact_sha256",
        "source_page_sequence",
        "source_event_ordinal",
        "source_event_hash",
        "source_result_hash",
        "source_pointer",
    )
    assert len(columns) == 31
    assert {name for name, item in columns.items() if item.nullable} == {"response_cik"}
    assert contract.primary_key == ("source_record_id",)
    assert contract.partition_by == ("source_capture_date",)
    assert contract.sort_by == (
        "source_capture_date",
        "event_date",
        "response_composite_figi",
        "effective_ticker",
        "source_record_id",
    )
    assert "manifest-bound occurrence" in contract.grain
    assert "high-severity quarantine" in contract.grain
    assert "without trim" in columns["effective_ticker"].description
    assert "never an announcement" in columns["event_date"].description
    assert "evidence_only_pending_s7" in columns["identity_evidence_scope"].description
    assert "Always false" in columns["backtest_identity_eligible"].description


def test_ticker_change_event_freezes_exact_qa_policy() -> None:
    contract = _candidate("ticker_change_event")
    rules = {item.check_id: item for item in contract.qa_rules}
    failed_critical = {
        "availability_invalid_rows",
        "backtest_identity_eligible_rows",
        "date_quality_invalid_rows",
        "diagnostic_flag_invalid_rows",
        "event_date_parse_invalid_rows",
        "event_structure_invalid_rows",
        "identity_evidence_scope_invalid_rows",
        "lineage_invalid_rows",
        "pilot_output_rows",
        "primary_key_duplicate_excess",
        "request_status_parent_missing_rows",
        "response_identity_mismatch_rows",
        "row_funnel_unreconciled",
        "schema_exact",
        "source_envelope_invalid",
        "source_integrity_invalid",
        "source_plan_invalid",
        "source_request_contract_invalid_rows",
        "valid_sibling_event_loss_rows",
    }
    failed_high = {
        "blank_target_entered_data_rows",
        "target_ticker_format_invalid_rows",
    }
    warning_medium = {
        "blank_target_placeholder_rows",
        "event_after_request_end_rows",
        "event_after_source_capture_rows",
        "event_before_s4_window_rows",
        "figi_multiple_ticker_groups",
        "non_descending_multi_event_responses",
        "provider_cluster_2023_11_18_rows",
        "request_boundary_2003_09_10_rows",
        "response_cik_missing_requests",
        "same_figi_date_multiple_ticker_groups",
        "semantic_event_key_duplicate_excess",
        "sentinel_1969_12_31_rows",
        "ticker_reuse_multiple_figi_groups",
        "unexpected_source_field_rows",
        "weekend_event_rows",
    }

    assert len(rules) == 36
    assert set(rules) == failed_critical | failed_high | warning_medium
    assert {
        key
        for key, rule in rules.items()
        if rule.severity is QASeverity.CRITICAL and rule.failure_status is QAStatus.FAILED
    } == failed_critical
    assert {
        key
        for key, rule in rules.items()
        if rule.severity is QASeverity.HIGH and rule.failure_status is QAStatus.FAILED
    } == failed_high
    assert {
        key
        for key, rule in rules.items()
        if rule.severity is QASeverity.MEDIUM and rule.failure_status is QAStatus.WARNING
    } == warning_medium
    assert all(type(rule.limit) is float and rule.limit == 0.0 for rule in rules.values())
    assert "high-severity quarantine" in rules["blank_target_entered_data_rows"].description
    assert "valid event" in rules["valid_sibling_event_loss_rows"].description


def test_request_status_freezes_formal_complete_and_404_fields() -> None:
    contract = _candidate("ticker_event_request_status")
    columns = {item.name: item for item in contract.columns}

    assert (contract.domain, contract.table, contract.schema_version) == (
        "identity",
        "ticker_event_request_status",
        1,
    )
    assert tuple(columns) == (
        "source_observed_date",
        "requested_identifier_type",
        "requested_identifier",
        "requested_event_type",
        "request_start_label",
        "request_end_label",
        "request_window_semantics",
        "source_manifest_status",
        "request_outcome",
        "provider_status_code",
        "response_name",
        "response_cik",
        "response_composite_figi",
        "raw_event_count",
        "accepted_event_count",
        "quarantined_event_count",
        "source_manifest_created_at_utc",
        "source_status_observed_at_utc",
        "source_available_session",
        "source_available_at_utc",
        "source_availability_rule",
        "source_availability_quality",
        "coverage_interpretation",
        "identity_evidence_scope",
        "backtest_identity_eligible",
        "source_request_id",
        "source_provider_request_id",
        "source_manifest_sha256",
        "source_artifact_sha256",
        "source_page_count",
    )
    assert len(columns) == 30
    assert {name for name, item in columns.items() if item.nullable} == {
        "provider_status_code",
        "response_name",
        "response_cik",
        "response_composite_figi",
        "source_provider_request_id",
        "source_artifact_sha256",
    }
    assert contract.primary_key == ("source_request_id",)
    assert contract.partition_by == ("source_observed_date",)
    assert contract.sort_by == (
        "source_observed_date",
        "requested_identifier",
        "source_request_id",
    )
    assert "complete_timeline or not_found_404" in columns["request_outcome"].description
    assert "never asset absence" in columns["request_outcome"].description
    assert "not sent as a server-side" in columns["request_start_label"].description
    assert "Always false" in columns["backtest_identity_eligible"].description


def test_request_status_freezes_exact_qa_policy() -> None:
    contract = _candidate("ticker_event_request_status")
    rules = {item.check_id: item for item in contract.qa_rules}
    failed_critical = {
        "availability_invalid_rows",
        "backtest_identity_eligible_rows",
        "complete_response_contract_invalid_rows",
        "coverage_interpretation_invalid_rows",
        "event_child_coverage_invalid_rows",
        "event_count_reconciliation_invalid_rows",
        "formal_request_cardinality_invalid",
        "identity_evidence_scope_invalid_rows",
        "lineage_invalid_rows",
        "not_found_404_contract_invalid_rows",
        "outcome_field_consistency_invalid_rows",
        "pilot_output_rows",
        "primary_key_duplicate_excess",
        "request_outcome_invalid_rows",
        "request_window_semantics_invalid_rows",
        "response_identity_mismatch_rows",
        "row_funnel_unreconciled",
        "schema_exact",
        "source_integrity_invalid",
        "source_plan_invalid",
        "source_request_contract_invalid_rows",
        "status_count_formula_invalid",
    }
    warning_medium = {
        "excluded_pilot_manifests",
        "identifier_not_found_404_requests",
        "request_outcome_changed_since_prior_capture",
        "response_cik_missing_complete_requests",
        "unexpected_source_field_rows",
    }

    assert len(rules) == 27
    assert set(rules) == failed_critical | warning_medium
    assert {
        key
        for key, rule in rules.items()
        if rule.severity is QASeverity.CRITICAL and rule.failure_status is QAStatus.FAILED
    } == failed_critical
    assert {
        key
        for key, rule in rules.items()
        if rule.severity is QASeverity.MEDIUM and rule.failure_status is QAStatus.WARNING
    } == warning_medium
    assert all(type(rule.limit) is float and rule.limit == 0.0 for rule in rules.values())
    assert "15,173" in rules["formal_request_cardinality_invalid"].description
    assert "no 404 is dropped or quarantined" in rules["row_funnel_unreconciled"].description
