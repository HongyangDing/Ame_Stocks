from __future__ import annotations

from copy import deepcopy

import pytest

from ame_stocks_api.silver.contracts import QASeverity, QAStatus
from ame_stocks_api.silver.ticker_event_contract import (
    TICKER_CHANGE_EVENT_CONTRACT,
    TICKER_EVENT_REQUEST_STATUS_CONTRACT,
)
from ame_stocks_api.silver.ticker_events import (
    TickerEventTransformError,
    transform_ticker_events,
)

BUILD_ID = "f" * 64
CAPTURE = "2026-07-11T12:00:00+00:00"


def _request(
    token: str,
    figi: str,
    *,
    event_count: int,
    cik: str | None = "1234",
    not_found: bool = False,
) -> dict[str, object]:
    return {
        "event_count": event_count,
        "outcome": "not_found_404" if not_found else "complete",
        "provider_status_code": 404 if not_found else None,
        "requested_identifier": figi,
        "result_cik": None if not_found else cik,
        "result_composite_figi": None if not_found else figi,
        "result_name": None if not_found else f"Issuer {token}",
        "source_artifact_sha256": None if not_found else token * 64,
        "source_capture_at_utc": None if not_found else CAPTURE,
        "source_created_at_utc": "2026-07-11T11:00:00+00:00",
        "source_manifest_path": f"bronze/ticker_events/{token}/manifest.json",
        "source_manifest_sha256": token * 64,
        "source_page_count": 0 if not_found else 1,
        "source_provider_request_id": None if not_found else f"provider-{token}",
        "source_request_id": token * 64,
        "source_updated_at_utc": CAPTURE,
    }


def _event(
    request: dict[str, object],
    ordinal: int,
    ticker: str,
    event_date: str,
) -> dict[str, object]:
    token = str(request["source_request_id"])[0]
    return {
        "date_quality": "source-profile-label-is-not-an-output-contract",
        "event_date_raw": event_date,
        "event_type": "ticker_change",
        "requested_identifier": request["requested_identifier"],
        "result_cik": request["result_cik"],
        "result_composite_figi": request["result_composite_figi"],
        "result_name": request["result_name"],
        "source_artifact_path": f"bronze/ticker_events/{token}/page-00000.json.gz",
        "source_artifact_sha256": request["source_artifact_sha256"],
        "source_capture_at_utc": request["source_capture_at_utc"],
        "source_manifest_path": request["source_manifest_path"],
        "source_manifest_sha256": request["source_manifest_sha256"],
        "source_page_sequence": 0,
        "source_provider_request_id": request["source_provider_request_id"],
        "source_request_id": request["source_request_id"],
        "source_row_ordinal": ordinal,
        "source_event_hash": f"{ordinal + 1:x}" * 64,
        "source_result_hash": token * 64,
        "target_ticker_raw": ticker,
    }


def _fixture() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    first = _request("a", "BBG000000001", event_count=3, cik=None)
    second = _request("b", "BBG000000002", event_count=1)
    missing = _request("c", "BBG000000003", event_count=0, not_found=True)
    events = [
        _event(first, 0, "AAA", "2023-11-18"),
        _event(first, 1, "AAB", "2023-11-18"),
        _event(first, 2, "", "2023-11-18"),
        _event(second, 0, "AAA", "2003-09-10"),
    ]
    return [first, second, missing], events


def _transform(*, excluded_pilot_manifests: int = 0):
    requests, events = _fixture()
    return transform_ticker_events(
        requests,
        events,
        build_id=BUILD_ID,
        excluded_pilot_manifests=excluded_pilot_manifests,
    )


def test_two_table_transform_matches_frozen_schemas_and_parent_child_counts() -> None:
    result = _transform()

    assert result.request_status.table.schema == TICKER_EVENT_REQUEST_STATUS_CONTRACT.arrow_schema
    assert result.ticker_change.table.schema == TICKER_CHANGE_EVENT_CONTRACT.arrow_schema
    assert result.by_table(TICKER_EVENT_REQUEST_STATUS_CONTRACT.table) is result.request_status
    assert result.by_table(TICKER_CHANGE_EVENT_CONTRACT.table) is result.ticker_change
    with pytest.raises(KeyError):
        result.by_table("unknown")

    status = {row["requested_identifier"]: row for row in result.request_status.table.to_pylist()}
    assert (
        status["BBG000000001"]["raw_event_count"],
        status["BBG000000001"]["accepted_event_count"],
        status["BBG000000001"]["quarantined_event_count"],
    ) == (3, 2, 1)
    assert status["BBG000000002"]["accepted_event_count"] == 1
    assert status["BBG000000003"]["request_outcome"] == "not_found_404"
    assert status["BBG000000003"]["provider_status_code"] == 404
    assert status["BBG000000003"]["source_page_count"] == 0
    assert status["BBG000000001"]["provider_status_code"] is None
    assert all(row["backtest_identity_eligible"] is False for row in status.values())
    assert result.request_status.row_funnel.input_rows == 3
    assert result.request_status.row_funnel.accepted_source_rows == 3
    assert result.ticker_change.row_funnel.to_dict() == {
        "accepted_source_rows": 3,
        "exact_duplicate_excess": 0,
        "input_rows": 4,
        "output_rows_by_table": {"ticker_change_event": 3},
        "quarantined_source_rows": 1,
        "unmapped_source_rows": 0,
        "version_preserved_rows": 0,
    }


def test_blank_target_is_high_quarantine_and_valid_siblings_are_preserved() -> None:
    result = _transform()
    records = result.ticker_change.quarantine_records

    assert len(records) == 1
    assert records[0].issue_code == "blank_target_ticker"
    assert records[0].severity is QASeverity.HIGH
    assert records[0].field_name == "effective_ticker"
    assert records[0].observed_value == ""
    assert records[0].detected_build_id == BUILD_ID
    rows = result.ticker_change.table.to_pylist()
    siblings = [row for row in rows if row["requested_identifier"] == "BBG000000001"]
    assert {row["effective_ticker"] for row in siblings} == {"AAA", "AAB"}
    assert all(row["same_figi_date_multiple_tickers"] is True for row in siblings)
    assert all(row["backtest_identity_eligible"] is False for row in rows)
    assert result.ticker_change.qa_by_id("blank_target_placeholder_rows").numerator == 1
    assert result.ticker_change.qa_by_id("blank_target_entered_data_rows").numerator == 0


def test_dates_are_not_shifted_and_diagnostic_groups_use_reviewed_scope() -> None:
    result = _transform()
    rows = result.ticker_change.table.to_pylist()
    cluster = [row for row in rows if row["event_date_raw"] == "2023-11-18"]
    boundary = next(row for row in rows if row["event_date_raw"] == "2003-09-10")

    assert all(row["event_date"].isoformat() == "2023-11-18" for row in cluster)
    assert all(row["event_date_is_weekend"] is True for row in cluster)
    assert all(row["event_date_is_known_cluster"] is True for row in cluster)
    assert all(
        row["event_date_quality"] == "provider_cluster_candidate_2023_11_18" for row in cluster
    )
    assert boundary["event_date_quality"] == "source_boundary_candidate_2003_09_10"
    assert result.ticker_change.qa_by_id("provider_cluster_2023_11_18_rows").numerator == 3
    assert result.ticker_change.qa_by_id("weekend_event_rows").numerator == 3
    assert result.ticker_change.qa_by_id("same_figi_date_multiple_ticker_groups").numerator == 1
    assert result.ticker_change.qa_by_id("ticker_reuse_multiple_figi_groups").numerator == 1
    assert result.ticker_change.qa_by_id("figi_multiple_ticker_groups").numerator == 1
    assert result.ticker_change.qa_by_id("non_descending_multi_event_responses").numerator == 0


def test_multi_event_order_allows_same_day_but_rejects_later_dates() -> None:
    request = _request("d", "BBG000000004", event_count=2)
    descending = [
        _event(request, 0, "DNEW", "2024-02-01"),
        _event(request, 1, "DOLD", "2024-01-01"),
    ]
    accepted = transform_ticker_events([request], descending, build_id=BUILD_ID)
    assert accepted.ticker_change.qa_by_id("non_descending_multi_event_responses").numerator == 0

    ascending = deepcopy(descending)
    ascending[0]["event_date_raw"] = "2024-01-01"
    ascending[1]["event_date_raw"] = "2024-02-01"
    rejected = transform_ticker_events([request], ascending, build_id=BUILD_ID)
    assert rejected.ticker_change.qa_by_id("non_descending_multi_event_responses").numerator == 1


def test_qa_partition_and_source_context_warning_are_deterministic() -> None:
    first = _transform(excluded_pilot_manifests=100)
    requests, events = _fixture()
    second = transform_ticker_events(
        list(reversed(deepcopy(requests))),
        list(reversed(deepcopy(events))),
        build_id=BUILD_ID,
        excluded_pilot_manifests=100,
    )

    assert first.request_status.table.equals(second.request_status.table)
    assert first.ticker_change.table.equals(second.ticker_change.table)
    assert first.request_status.qa_checks == second.request_status.qa_checks
    assert first.ticker_change.qa_checks == second.ticker_change.qa_checks
    assert first.ticker_change.quarantine_records == second.ticker_change.quarantine_records
    assert {item.partition_key for item in first.request_status.qa_checks} == {
        "source_observed_date=2026-07-11"
    }
    assert {item.partition_key for item in first.ticker_change.qa_checks} == {
        "source_capture_date=2026-07-11"
    }
    excluded = first.request_status.qa_by_id("excluded_pilot_manifests")
    assert (excluded.numerator, excluded.status) == (100, QAStatus.WARNING)
    assert first.request_status.qa_by_id("response_cik_missing_complete_requests").numerator == 1
    assert first.ticker_change.qa_by_id("response_cik_missing_requests").numerator == 1


@pytest.mark.parametrize("build_id", ["", "not-a-sha", "A" * 64])
def test_transform_rejects_invalid_build_id(build_id: str) -> None:
    requests, events = _fixture()
    with pytest.raises(TickerEventTransformError, match="build_id"):
        transform_ticker_events(requests, events, build_id=build_id)


def test_transform_rejects_duplicate_request_primary_key() -> None:
    requests, events = _fixture()
    requests.append(deepcopy(requests[0]))
    with pytest.raises(TickerEventTransformError, match="duplicate source_request_id"):
        transform_ticker_events(requests, events, build_id=BUILD_ID)
