from __future__ import annotations

import json
from pathlib import Path

from ame_stocks_api.silver.contracts import (
    QASeverity,
    QAStatus,
    TableContract,
)

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_CANDIDATE_CONTRACT_PATH = (
    _REPOSITORY_ROOT
    / "docs"
    / "silver"
    / "contracts"
    / "reference"
    / "ticker_type_dim.schema-v1.candidate.json"
)
_EXPECTED_CONTRACT_ID = (
    "b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d"
)


def _candidate_contract() -> TableContract:
    return TableContract.from_dict(
        json.loads(_CANDIDATE_CONTRACT_PATH.read_text(encoding="utf-8"))
    )


def test_ticker_type_dim_candidate_is_valid_and_deterministic() -> None:
    contract = _candidate_contract()

    assert contract.contract_id == _EXPECTED_CONTRACT_ID
    assert TableContract.from_dict(contract.to_dict()) == contract
    assert (contract.domain, contract.table, contract.schema_version) == (
        "reference",
        "ticker_type_dim",
        1,
    )
    assert contract.primary_key == (
        "capture_date",
        "asset_class",
        "locale",
        "type_code",
    )
    assert contract.partition_by == ("capture_date",)
    assert contract.sort_by == (
        "capture_date",
        "asset_class",
        "locale",
        "type_code",
    )
    assert contract.source_datasets == ("ticker_types",)


def test_ticker_type_dim_candidate_freezes_fields_and_nullability() -> None:
    contract = _candidate_contract()
    columns = {column.name: column for column in contract.columns}

    assert tuple(columns) == (
        "capture_date",
        "asset_class",
        "locale",
        "type_code",
        "description",
        "snapshot_scope",
        "source_capture_at_utc",
        "available_session",
        "available_at_utc",
        "availability_rule",
        "source_record_id",
        "source_request_id",
        "source_provider_request_id",
        "source_artifact_sha256",
        "source_page_sequence",
        "source_row_ordinal",
        "source_row_hash",
    )
    assert len(columns) == 17
    assert {name for name, column in columns.items() if column.nullable} == {
        "description"
    }

    # S2 is a provider-code dictionary, not a requested-date, surrogate-ID,
    # coarse-bucket, or research-eligibility mapping.
    assert {
        "requested_snapshot_date",
        "ticker_type_id",
        "normalized_type_code",
        "coarse_type",
        "research_eligibility",
        "is_research_eligible",
    }.isdisjoint(columns)
    assert "copied verbatim" in columns["type_code"].description
    assert "not normalized or mapped" in columns["type_code"].description


def test_ticker_type_dim_candidate_freezes_exact_qa_policy() -> None:
    contract = _candidate_contract()
    rules = {rule.check_id: rule for rule in contract.qa_rules}
    failed_critical = {
        "availability_invalid_rows",
        "lineage_invalid_rows",
        "primary_key_conflict_rows",
        "primary_key_duplicate_excess",
        "required_field_invalid_rows",
        "row_funnel_unreconciled",
        "schema_exact",
        "snapshot_scope_invalid_rows",
        "source_envelope_invalid",
        "source_integrity_invalid",
        "source_snapshot_cardinality_invalid",
    }
    failed_high = {
        "asset_class_domain_invalid_rows",
        "locale_domain_invalid_rows",
    }
    warning_medium = {
        "description_changed_rows_since_prior_capture",
        "description_missing_or_blank_rows",
        "disappeared_type_code_rows_since_prior_capture",
        "exact_duplicate_excess_rows",
        "new_type_code_rows_since_prior_capture",
        "type_code_format_unreviewed_rows",
        "unexpected_source_field_rows",
    }

    assert len(rules) == 20
    assert set(rules) == failed_critical | failed_high | warning_medium
    assert {
        check_id
        for check_id, rule in rules.items()
        if rule.failure_status is QAStatus.FAILED
        and rule.severity is QASeverity.CRITICAL
    } == failed_critical
    assert {
        check_id
        for check_id, rule in rules.items()
        if rule.failure_status is QAStatus.FAILED
        and rule.severity is QASeverity.HIGH
    } == failed_high
    assert {
        check_id
        for check_id, rule in rules.items()
        if rule.failure_status is QAStatus.WARNING
        and rule.severity is QASeverity.MEDIUM
    } == warning_medium
    assert all(type(rule.limit) is float and rule.limit == 0.0 for rule in rules.values())


def test_ticker_type_dim_candidate_keeps_temporal_drift_review_only() -> None:
    contract = _candidate_contract()
    rules = {rule.check_id: rule for rule in contract.qa_rules}
    temporal_checks = {
        "new_type_code_rows_since_prior_capture",
        "disappeared_type_code_rows_since_prior_capture",
        "description_changed_rows_since_prior_capture",
    }

    assert temporal_checks.issubset(rules)
    for check_id in temporal_checks:
        rule = rules[check_id]
        assert rule.severity is QASeverity.MEDIUM
        assert rule.failure_status is QAStatus.WARNING
        assert "prior capture" in rule.description

    assert "earliest capture is excluded" in rules[
        "new_type_code_rows_since_prior_capture"
    ].description
    assert "earliest capture is excluded" in rules[
        "disappeared_type_code_rows_since_prior_capture"
    ].description

    # Coverage against assets.type belongs to the later S4 point-in-time join,
    # not to this current-only S2 dictionary contract.
    assert not any("coverage" in check_id for check_id in rules)
    assert not any(check_id.startswith("assets_") for check_id in rules)
