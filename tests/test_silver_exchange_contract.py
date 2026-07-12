from __future__ import annotations

import json
from pathlib import Path

from ame_stocks_api.silver.contracts import (
    QASeverity,
    QAStatus,
    TableContract,
)

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_CANDIDATE_PATH = (
    _REPOSITORY_ROOT
    / "docs"
    / "silver"
    / "contracts"
    / "reference"
    / "exchange_dim.schema-v1.candidate.json"
)


def _candidate() -> TableContract:
    return TableContract.from_dict(json.loads(_CANDIDATE_PATH.read_text(encoding="utf-8")))


def test_exchange_dim_candidate_is_a_valid_deterministic_table_contract() -> None:
    contract = _candidate()

    assert contract.contract_id == (
        "1803d28f2b4b6088e32d27d06c7102111e4f141b6645a1059829732442f0e479"
    )
    assert TableContract.from_dict(contract.to_dict()) == contract
    assert (contract.domain, contract.table, contract.schema_version) == (
        "reference",
        "exchange_dim",
        1,
    )
    assert contract.primary_key == ("capture_date", "exchange_id")
    assert contract.partition_by == ("capture_date",)
    assert contract.sort_by == ("capture_date", "exchange_id")
    assert contract.source_datasets == ("exchanges",)


def test_exchange_dim_candidate_freezes_reviewed_fields_and_nullability() -> None:
    contract = _candidate()
    columns = {column.name: column for column in contract.columns}

    assert tuple(columns) == (
        "capture_date",
        "exchange_id",
        "name",
        "acronym",
        "mic",
        "operating_mic",
        "participant_id",
        "exchange_type",
        "asset_class",
        "locale",
        "url",
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
    assert {name for name, column in columns.items() if column.nullable} == {
        "acronym",
        "mic",
        "operating_mic",
        "participant_id",
        "url",
    }
    assert "requested_snapshot_date" not in columns
    assert "provider-internal" in columns["exchange_id"].description
    assert "not substituted" in columns["operating_mic"].description


def test_exchange_dim_candidate_has_fail_closed_controls_and_reviewable_drift() -> None:
    contract = _candidate()
    rules = {rule.check_id: rule for rule in contract.qa_rules}

    assert set(rules) == {
        "asset_class_domain_invalid_rows",
        "availability_invalid_rows",
        "empty_optional_string_rows",
        "exact_duplicate_excess_rows",
        "lineage_invalid_rows",
        "locale_domain_invalid_rows",
        "mic_conflict_rows",
        "mic_format_invalid_values",
        "primary_key_conflict_rows",
        "primary_key_duplicate_excess",
        "required_field_invalid_rows",
        "row_funnel_unreconciled",
        "schema_exact",
        "snapshot_scope_invalid_rows",
        "source_envelope_invalid",
        "source_integrity_invalid",
        "source_snapshot_cardinality_invalid",
        "unexpected_source_field_rows",
        "unreviewed_exchange_type_rows",
        "url_invalid_rows",
    }
    assert all(type(rule.limit) is float and rule.limit == 0.0 for rule in rules.values())
    assert rules["source_integrity_invalid"].severity is QASeverity.CRITICAL
    assert rules["source_integrity_invalid"].failure_status is QAStatus.FAILED
    assert rules["mic_conflict_rows"].severity is QASeverity.CRITICAL
    assert rules["snapshot_scope_invalid_rows"].severity is QASeverity.CRITICAL
    assert rules["unreviewed_exchange_type_rows"].severity is QASeverity.MEDIUM
    assert rules["unreviewed_exchange_type_rows"].failure_status is QAStatus.WARNING
    assert "ORF" in rules["unreviewed_exchange_type_rows"].description
