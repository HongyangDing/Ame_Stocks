from __future__ import annotations

from copy import deepcopy

import pytest

from ame_stocks_api.silver.contracts import QASeverity, QAStatus
from ame_stocks_api.silver.ticker_overview_contract import TICKER_OVERVIEW_SAFE_CONTRACT
from ame_stocks_api.silver.ticker_overviews import (
    TickerOverviewSafeTransformError,
    transform_ticker_overview_safe,
)

BUILD_ID = "f" * 64


def _row(token: str, *, identity_match: bool = True) -> dict[str, object]:
    suffix = 0 if token == "a" else 1
    source_request = ("c", "d")[suffix] * 64
    return {
        "lifecycle_id": token * 64,
        "source_request_id": source_request,
        "query_ticker": f"TICK{token.upper()}",
        "query_date": "2024-01-31",
        "first_active_date": "2024-01-02",
        "last_active_date": "2024-01-31",
        "identity_type": "share_class_figi",
        "identity_value": f"BBG00{token.upper()}",
        "identity_match": identity_match,
        "identity_match_basis": "share_class_figi" if identity_match else None,
        "identity_evidence_status": (
            "matched" if identity_match else "no_comparable_identity"
        ),
        "ticker": f"TICK{token.upper()}",
        "name": f"Issuer {token.upper()}",
        "type": "CS",
        "market": "stocks",
        "locale": "us",
        "active": True,
        "primary_exchange": "XNYS",
        "currency_name": "usd",
        "cik": f"000000000{ord(token) - 96}",
        "composite_figi": f"BBG0{token.upper()}COMPOSITE",
        "share_class_figi": f"BBG00{token.upper()}",
        "sic_code": None if not identity_match else "3571",
        "sic_description": None if not identity_match else "Electronic Computers",
        "list_date": None if not identity_match else "2020-01-02",
        "delisted_utc": None,
        "ticker_root": f"TICK{token.upper()}",
        "ticker_suffix": None,
        "source_manifest_created_at_utc": "2026-07-11T11:00:00+00:00",
        "source_capture_at_utc": "2026-07-11T12:00:00+00:00",
        "source_manifest_path": f"manifests/massive/ticker_overview/{token * 64}.json",
        "source_manifest_sha256": ("e", "f")[suffix] * 64,
        "source_artifact_path": (
            f"bronze/massive/ticker_overview/request_id={source_request}/page-00000.json.gz"
        ),
        "source_artifact_sha256": ("1", "2")[suffix] * 64,
        "source_artifact_raw_sha256": ("3", "4")[suffix] * 64,
        "source_page_sequence": 0,
        "source_row_ordinal": 0,
        "source_provider_request_id": f"provider-{token}",
        "source_result_hash": ("5", "6")[suffix] * 64,
    }


def test_transform_admits_matching_evidence_and_quarantines_unresolved_identity() -> None:
    accepted = _row("a")
    unresolved = _row("b", identity_match=False)

    result = transform_ticker_overview_safe([accepted, unresolved], build_id=BUILD_ID)
    rows = result.table.to_pylist()

    assert result.table.schema == TICKER_OVERVIEW_SAFE_CONTRACT.arrow_schema
    assert len(rows) == 1
    assert rows[0]["lifecycle_id"] == "a" * 64
    assert rows[0]["identity_match"] is True
    assert rows[0]["identity_evidence_scope"] == "evidence_only_pending_s7"
    assert rows[0]["backtest_identity_eligible"] is False
    assert rows[0]["source_capture_date"].isoformat() == "2026-07-11"
    assert rows[0]["source_available_session"].isoformat() == "2026-07-13"
    assert rows[0]["source_available_at_utc"].isoformat() == "2026-07-13T13:30:00+00:00"
    assert rows[0]["source_pointer"] == (f"{accepted['source_artifact_path']}#page=0&row=0")
    assert rows[0]["source_manifest_path"] == accepted["source_manifest_path"]
    assert rows[0]["source_artifact_raw_sha256"] == accepted["source_artifact_raw_sha256"]
    assert not {
        "market_cap",
        "share_class_shares_outstanding",
        "weighted_shares_outstanding",
    }.intersection(rows[0])

    assert len(result.quarantine_records) == 1
    issue = result.quarantine_records[0]
    assert issue.issue_code == "identity_evidence_unresolved"
    assert issue.severity is QASeverity.HIGH
    assert issue.field_name == "identity_match"
    assert issue.review_status.value == "pending"
    assert result.row_funnel.to_dict() == {
        "accepted_source_rows": 1,
        "exact_duplicate_excess": 0,
        "input_rows": 2,
        "output_rows_by_table": {"ticker_overview_safe": 1},
        "quarantined_source_rows": 1,
        "unmapped_source_rows": 0,
        "version_preserved_rows": 0,
    }


def test_transform_qa_is_recomputable_and_preserves_temporal_warning_boundary() -> None:
    result = transform_ticker_overview_safe(
        [_row("a"), _row("b", identity_match=False)],
        build_id=BUILD_ID,
    )

    assert result.qa_by_id("unresolved_identity_rows").numerator == 1
    assert result.qa_by_id("unresolved_identity_rows").status is QAStatus.WARNING
    assert result.qa_by_id("sic_code_missing_rows").numerator == 1
    assert result.qa_by_id("list_date_missing_rows").numerator == 1
    retrospective = result.qa_by_id("retrospective_query_without_archived_vintage_rows")
    assert (retrospective.numerator, retrospective.status) == (1, QAStatus.WARNING)
    assert result.qa_by_id("list_date_after_query_date_rows").numerator == 0
    assert result.qa_by_id("identity_match_false_output_rows").numerator == 0
    assert result.qa_by_id("unsafe_output_columns").numerator == 0
    assert result.qa_by_id("lineage_invalid_rows").numerator == 0
    assert result.qa_by_id("availability_invalid_rows").numerator == 0
    assert result.qa_by_id("row_funnel_unreconciled").numerator == 0
    assert result.qa_by_id("formal_lifecycle_cardinality_invalid").numerator == 30_737
    assert result.qa_by_id("unresolved_identity_count_drift").numerator == 168


def test_transform_is_deterministic_under_input_order() -> None:
    records = [_row("a"), _row("b", identity_match=False)]
    first = transform_ticker_overview_safe(records, build_id=BUILD_ID)
    second = transform_ticker_overview_safe(
        list(reversed(deepcopy(records))),
        build_id=BUILD_ID,
    )

    assert first.table.equals(second.table)
    assert first.qa_checks == second.qa_checks
    assert first.quarantine_records == second.quarantine_records


def test_transform_fails_closed_if_unsafe_market_or_share_fields_enter_input() -> None:
    row = _row("a")
    row["market_cap"] = 1_000_000

    with pytest.raises(TickerOverviewSafeTransformError, match="unsafe Ticker Overview fields"):
        transform_ticker_overview_safe([row], build_id=BUILD_ID)


def test_transform_rejects_comparable_identity_conflict_as_unreviewed() -> None:
    row = _row("b", identity_match=False)
    row["identity_evidence_status"] = "comparable_identity_conflict"
    row["share_class_figi"] = "BBG-CONFLICT"

    with pytest.raises(TickerOverviewSafeTransformError, match="unreviewed identity"):
        transform_ticker_overview_safe([row], build_id=BUILD_ID)


@pytest.mark.parametrize("build_id", ["", "not-a-sha", "A" * 64])
def test_transform_rejects_invalid_build_id(build_id: str) -> None:
    with pytest.raises(TickerOverviewSafeTransformError, match="build_id"):
        transform_ticker_overview_safe([_row("a")], build_id=build_id)
