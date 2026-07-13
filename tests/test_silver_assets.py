from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pandas as pd
import pytest

from ame_stocks_api.silver import assets as asset_transform
from ame_stocks_api.silver.asset_source import (
    AssetSourcePage,
    AssetSourceRecord,
    AssetSourceRequest,
    AssetSourceSession,
)
from ame_stocks_api.silver.assets import (
    ASSET_SOURCE_AVAILABILITY_QUALITY,
    ASSET_VERSION_SELECTION_RULE,
    AssetTransformError,
    transform_asset_session,
)

BUILD_ID = "a" * 64
SESSION_DATE = date(2026, 5, 11)
ACTIVE_CAPTURE = datetime(2026, 7, 11, 14, 3, 15, tzinfo=UTC)
INACTIVE_CAPTURE = datetime(2026, 7, 11, 14, 4, 15, tzinfo=UTC)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _row(
    ticker: object,
    *,
    active: bool,
    updated: object = "2026-07-01T12:00:00Z",
    delisted: object | None = None,
    name: object | None = None,
    **extra: object,
) -> dict[str, object]:
    row: dict[str, object] = {
        "active": active,
        "cik": "0000123456",
        "composite_figi": f"BBG-{ticker}",
        "currency_name": "usd",
        "last_updated_utc": updated,
        "locale": "us",
        "market": "stocks",
        "name": str(ticker) if name is None else name,
        "primary_exchange": "XNAS",
        "share_class_figi": f"BBGS-{ticker}",
        "ticker": ticker,
        "type": "CS",
    }
    if delisted is not None:
        row["delisted_utc"] = delisted
    row.update(extra)
    return row


def _request(*, active: bool, count: int) -> AssetSourceRequest:
    label = "active" if active else "inactive"
    request_id = _sha(f"request-{label}")
    capture = ACTIVE_CAPTURE if active else INACTIVE_CAPTURE
    return AssetSourceRequest(
        session_date=SESSION_DATE,
        requested_active=active,
        source_request_id=request_id,
        source_manifest_path=f"manifests/massive/assets/{request_id}.json",
        source_manifest_sha256=_sha(f"manifest-{label}"),
        source_created_at_utc=capture - timedelta(seconds=2),
        source_capture_at_utc=capture,
        source_updated_at_utc=capture + timedelta(seconds=1),
        pages=(
            AssetSourcePage(
                source_path=(f"bronze/massive/assets/request_id={request_id}/page-00000.json.gz"),
                source_artifact_sha256=_sha(f"artifact-{label}"),
                raw_sha256=_sha(f"raw-{label}"),
                sequence=0,
                compressed_bytes=100,
                raw_bytes=200,
                record_count=count,
            ),
        ),
    )


def _fixture(
    active_rows: list[dict[str, object]],
    inactive_rows: list[dict[str, object]],
) -> tuple[AssetSourceSession, tuple[AssetSourceRecord, ...]]:
    active_request = _request(active=True, count=len(active_rows))
    inactive_request = _request(active=False, count=len(inactive_rows))
    session = AssetSourceSession(
        session_date=SESSION_DATE,
        active_request=active_request,
        inactive_request=inactive_request,
    )
    records: list[AssetSourceRecord] = []
    for request, rows in (
        (active_request, active_rows),
        (inactive_request, inactive_rows),
    ):
        page = request.pages[0]
        for ordinal, row in enumerate(rows):
            records.append(
                AssetSourceRecord(
                    session_date=SESSION_DATE,
                    requested_active=request.requested_active,
                    source_request_id=request.source_request_id,
                    source_manifest_path=request.source_manifest_path,
                    source_manifest_sha256=request.source_manifest_sha256,
                    source_created_at_utc=request.source_created_at_utc,
                    source_capture_at_utc=request.source_capture_at_utc,
                    source_updated_at_utc=request.source_updated_at_utc,
                    source_artifact_path=page.source_path,
                    source_artifact_sha256=page.source_artifact_sha256,
                    source_page_sequence=0,
                    source_row_ordinal=ordinal,
                    source_provider_request_id=f"provider-{request.source_request_id[:12]}",
                    row=row,
                )
            )
    return session, tuple(records)


def _success_fixture() -> tuple[AssetSourceSession, tuple[AssetSourceRecord, ...]]:
    exact = _row("DUP", active=True)
    active_rows = [
        _row("A", active=True),
        _row("a", active=True),
        _row("ONE", active=True),
        _row("TWO", active=True),
        exact,
        dict(exact),
        _row("LAST", active=True, updated="2026-06-01T12:00:00Z"),
        _row("LAST", active=True, updated="2026-07-01T12:00:00Z"),
        _row(
            "DEL",
            active=True,
            updated="2026-06-01T12:00:00Z",
            delisted="2026-05-01T00:00:00Z",
        ),
        _row(
            "DEL",
            active=True,
            updated="2026-07-01T12:00:00Z",
            delisted="2026-04-01T00:00:00Z",
        ),
    ]
    return _fixture(active_rows, [_row("OLD", active=False)])


def _run(
    session: AssetSourceSession,
    records: tuple[AssetSourceRecord, ...],
):
    return transform_asset_session(
        session,
        records,
        build_id=BUILD_ID,
        current_ticker_types={"CS"},
        current_exchange_mics={"XNAS"},
    )


def test_successful_fixture_preserves_occurrences_and_builds_three_tables() -> None:
    session, records = _success_fixture()
    result = _run(session, records)

    assert result.observation.table.num_rows == 11
    assert result.version.table.num_rows == 6
    assert result.universe.table.num_rows == 8
    assert not result.quarantine_records
    assert not result.blocks_publish
    assert result.observation.row_funnel.to_dict() == {
        "accepted_source_rows": 11,
        "exact_duplicate_excess": 0,
        "input_rows": 11,
        "output_rows_by_table": {"asset_observation_daily": 11},
        "quarantined_source_rows": 0,
        "unmapped_source_rows": 0,
        "version_preserved_rows": 6,
    }
    assert result.version.row_funnel.unmapped_source_rows == 5
    assert result.universe.row_funnel.unmapped_source_rows == 3

    observations = result.observation.table.to_pylist()
    versions = result.version.table.to_pylist()
    universe = result.universe.table.to_pylist()
    assert {row["ticker"] for row in universe} >= {"A", "a", "DUP", "LAST", "DEL"}
    assert sum(row["is_selected"] for row in versions) == 3
    assert Counter(row["selection_status"] for row in versions) == {
        "resolved_exact_duplicate": 2,
        "resolved_unique_latest_last_updated": 4,
    }
    assert {row["selection_status"] for row in universe} == {
        "singleton",
        "resolved_exact_duplicate",
        "resolved_unique_latest_last_updated",
    }
    assert all(row["selection_rule_version"] == ASSET_VERSION_SELECTION_RULE for row in universe)
    assert all(
        row["source_availability_quality"] == ASSET_SOURCE_AVAILABILITY_QUALITY
        for row in observations + universe
    )
    assert all("asset_id" not in row and "candidate_asset_id" not in row for row in universe)

    exact_rows = [row for row in versions if row["ticker"] == "DUP"]
    assert [row["selection_rank"] for row in exact_rows] == [1, 2]
    assert exact_rows[0]["is_selected"] is True
    assert exact_rows[1]["is_selected"] is False
    for ticker in ("DUP", "LAST", "DEL"):
        group = [row for row in versions if row["ticker"] == ticker]
        assert [row["selection_rank"] for row in group] == [1, 2]
        assert group[0]["is_selected"] is True
    assert result.observation.qa_by_id("exact_duplicate_excess_rows").numerator == 1
    assert result.version.qa_by_id("exact_duplicate_groups").numerator == 1
    assert result.version.qa_by_id("delisted_changed_groups").numerator == 1


def test_semantic_winner_uses_last_updated_not_delisted_date() -> None:
    session, records = _fixture(
        [
            _row(
                "RANK",
                active=True,
                updated="2026-07-01T00:00:00Z",
                delisted="2026-01-01T00:00:00Z",
            ),
            _row(
                "RANK",
                active=True,
                updated="2026-06-01T00:00:00Z",
                delisted="2026-05-01T00:00:00Z",
            ),
        ],
        [_row("OLD", active=False)],
    )
    result = _run(session, records)
    versions = [row for row in result.version.table.to_pylist() if row["ticker"] == "RANK"]
    selected = next(row for row in versions if row["is_selected"])
    assert selected["last_updated_at_utc"] == datetime(2026, 7, 1, tzinfo=UTC)
    assert selected["delisted_at_utc"] == datetime(2026, 1, 1, tzinfo=UTC)


def test_identity_conflict_is_unresolved_and_cannot_enter_universe() -> None:
    session, records = _fixture(
        [
            _row("CONFLICT", active=True, name="first"),
            _row("CONFLICT", active=True, name="second", updated="2026-07-02T00:00:00Z"),
        ],
        [_row("OLD", active=False)],
    )
    result = _run(session, records)
    versions = [row for row in result.version.table.to_pylist() if row["ticker"] == "CONFLICT"]
    assert {row["selection_status"] for row in versions} == {"unresolved_identity_conflict"}
    assert all(row["selection_rank"] is None and not row["is_selected"] for row in versions)
    assert "CONFLICT" not in {row["ticker"] for row in result.universe.table.to_pylist()}
    assert result.version.qa_by_id("unresolved_version_groups").blocks_publish
    assert result.blocks_publish


def test_invalid_timestamp_and_unreviewed_difference_fail_closed() -> None:
    session, records = _fixture(
        [
            _row("BADTIME", active=True, updated="not-a-time"),
            _row("BADTIME", active=True, updated="2026-07-01T00:00:00Z"),
            _row("DELONLY", active=True, delisted="2026-01-01T00:00:00Z"),
            _row("DELONLY", active=True, delisted="2026-02-01T00:00:00Z"),
        ],
        [_row("OLD", active=False)],
    )
    result = _run(session, records)
    statuses = {row["ticker"]: row["selection_status"] for row in result.version.table.to_pylist()}
    assert statuses["BADTIME"] == "unresolved_timestamp_missing_or_invalid"
    assert statuses["DELONLY"] == "unresolved_difference_set"
    assert result.observation.qa_by_id("timestamp_parse_invalid_rows").numerator == 1
    assert result.version.qa_by_id("unexpected_difference_field_groups").numerator == 1


def test_non_exact_max_timestamp_tie_is_unresolved() -> None:
    session, records = _fixture(
        [
            _row(
                "TIE",
                active=True,
                updated="2026-06-01T00:00:00Z",
                delisted="2026-01-01T00:00:00Z",
            ),
            _row(
                "TIE",
                active=True,
                updated="2026-07-01T00:00:00Z",
                delisted="2026-02-01T00:00:00Z",
            ),
            _row(
                "TIE",
                active=True,
                updated="2026-07-01T00:00:00Z",
                delisted="2026-03-01T00:00:00Z",
            ),
        ],
        [_row("OLD", active=False)],
    )
    result = _run(session, records)
    assert {
        row["selection_status"]
        for row in result.version.table.to_pylist()
        if row["ticker"] == "TIE"
    } == {"unresolved_timestamp_tie"}
    assert result.version.qa_by_id("semantic_tie_groups").numerator == 1


def test_active_inactive_overlap_and_ticker_whitespace_do_not_enter_universe() -> None:
    session, records = _fixture(
        [_row("OVER", active=True), _row(" SPACE ", active=True)],
        [_row("OVER", active=False), _row("OLD", active=False)],
    )
    result = _run(session, records)
    tickers = {row["ticker"] for row in result.universe.table.to_pylist()}
    assert "OVER" not in tickers
    assert " SPACE " not in tickers
    assert result.observation.table.num_rows == 4
    assert result.observation.qa_by_id("active_inactive_overlap_rows").blocks_publish
    assert result.observation.qa_by_id("ticker_whitespace_rows").blocks_publish
    assert result.universe.qa_by_id("universe_row_formula_invalid").blocks_publish


def test_source_drift_is_preserved_as_hash_evidence_and_blocks_publish() -> None:
    session, records = _fixture(
        [
            _row(
                "DRIFT",
                active=True,
                primary_exchange={"unexpected": "object"},
                provider_new_field="new",
            )
        ],
        [_row("OLD", active=False)],
    )
    result = _run(session, records)
    row = next(item for item in result.observation.table.to_pylist() if item["ticker"] == "DRIFT")
    assert row["primary_exchange_mic"] is None
    assert result.observation.qa_by_id("optional_field_type_invalid_rows").blocks_publish
    assert result.observation.qa_by_id("unexpected_source_field_rows").blocks_publish
    assert result.observation.table.num_rows == 2


def test_invalid_required_row_is_quarantined_without_trimming_or_coercion() -> None:
    session, records = _fixture(
        [_row("GOOD", active=True), _row(None, active=True)],
        [_row("OLD", active=False)],
    )
    result = _run(session, records)
    assert result.observation.table.num_rows == 2
    assert len(result.quarantine_records) == 1
    assert result.quarantine_records[0].issue_code == "required_field_invalid_rows"
    assert len(result.all_quarantine_records) == 3
    for table_result in (result.observation, result.version, result.universe):
        assert table_result.row_funnel.quarantined_source_rows == 1
        assert len(table_result.quarantine_records) == 1
        assert table_result.quarantine_records[0].table_name == table_result.contract.table
    assert result.observation.qa_by_id("required_field_invalid_rows").blocks_publish


def test_nanosecond_timestamp_and_offset_are_preserved_and_ranked() -> None:
    session, records = _fixture(
        [
            _row("NS", active=True, updated="2026-07-01T12:00:00.123456788Z"),
            _row("NS", active=True, updated="2026-07-01T12:00:00.123456789Z"),
            _row("OFFSET", active=True, updated="2026-07-01T08:00:00-04:00"),
        ],
        [_row("OLD", active=False)],
    )
    result = _run(session, records)

    versions = [row for row in result.version.table.to_pylist() if row["ticker"] == "NS"]
    assert [row["selection_rank"] for row in versions] == [1, 2]
    selected = versions[0]
    assert selected["is_selected"] is True
    assert (
        pd.Timestamp(selected["last_updated_at_utc"]).value
        == pd.Timestamp("2026-07-01T12:00:00.123456789Z").value
    )
    offset = next(row for row in result.observation.table.to_pylist() if row["ticker"] == "OFFSET")
    assert offset["last_updated_at_utc"] == datetime(2026, 7, 1, 12, tzinfo=UTC)
    assert result.observation.qa_by_id("timestamp_parse_invalid_rows").numerator == 0


def test_duplicate_page_ordinals_fail_source_integrity_gate() -> None:
    session, records = _fixture(
        [_row("ONE", active=True), _row("TWO", active=True)],
        [_row("OLD", active=False)],
    )
    duplicated_pointer = replace(records[1], source_row_ordinal=0)
    result = _run(session, (records[0], duplicated_pointer, *records[2:]))

    assert result.observation.qa_by_id("source_integrity_invalid").blocks_publish


@pytest.mark.parametrize(
    ("funnel_number", "result_name", "check_id"),
    [(2, "version", "row_funnel_unreconciled"), (3, "universe", "universe_row_formula_invalid")],
)
def test_funnel_qa_uses_independent_source_counts(
    monkeypatch: pytest.MonkeyPatch,
    funnel_number: int,
    result_name: str,
    check_id: str,
) -> None:
    session, records = _success_fixture()
    real_row_funnel = asset_transform.RowFunnel
    calls = 0

    def drifted_row_funnel(**values: object):
        nonlocal calls
        calls += 1
        if calls == funnel_number:
            values["input_rows"] = int(values["input_rows"]) + 1
            values["accepted_source_rows"] = int(values["accepted_source_rows"]) + 1
        return real_row_funnel(**values)

    monkeypatch.setattr(asset_transform, "RowFunnel", drifted_row_funnel)
    result = _run(session, records)
    table_result = getattr(result, result_name)

    assert table_result.qa_by_id(check_id).numerator == 1
    assert table_result.qa_by_id(check_id).blocks_publish


def test_qa_uses_natural_denominators_and_rejects_invalid_counts() -> None:
    session, records = _success_fixture()
    result = _run(session, records)

    assert result.observation.qa_by_id("timestamp_parse_invalid_rows").denominator == 11
    assert result.version.qa_by_id("unresolved_version_groups").denominator == 3
    assert result.universe.qa_by_id("lineage_invalid_rows").denominator == 8

    metrics = {
        item.check_id: (item.numerator, item.denominator) for item in result.observation.qa_checks
    }
    metrics["schema_exact"] = (2, 1)
    with pytest.raises(AssetTransformError, match="numerator exceeds denominator"):
        asset_transform._qa_results(
            result.observation.contract,
            metrics,
            partition_key=SESSION_DATE.isoformat(),
        )


def test_calendar_lookup_is_cached_per_distinct_capture_timestamp() -> None:
    session, records = _success_fixture()
    asset_transform._first_market_open_after.cache_clear()

    _run(session, records)

    cache = asset_transform._first_market_open_after.cache_info()
    assert cache.misses == 2
    assert cache.hits > cache.misses


def test_record_pointer_drift_is_counted_by_source_plan_gate() -> None:
    session, records = _fixture([_row("GOOD", active=True)], [_row("OLD", active=False)])
    bad = replace(records[0], source_request_id="f" * 64)
    result = _run(session, (bad, *records[1:]))
    assert result.observation.qa_by_id("source_plan_invalid").blocks_publish


def test_rejects_invalid_build_id() -> None:
    session, records = _success_fixture()
    with pytest.raises(AssetTransformError, match="build_id"):
        transform_asset_session(session, records, build_id="not-a-digest")
