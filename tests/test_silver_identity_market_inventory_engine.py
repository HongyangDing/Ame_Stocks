from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime

import pytest

from ame_stocks_api.silver.identity_market_inventory_engine import (
    ASSET_AUTHORITY_TABLE,
    LINEAGE_RULE_VERSION,
    SCAN_ORDER_RULE,
    CompositeInventoryCaps,
    CompositeInventoryEngine,
    CompositeInventoryError,
    figi_invalid_reason,
)

RELEASE = hashlib.sha256(b"asset-release").hexdigest()
OTHER_RELEASE = hashlib.sha256(b"other-asset-release").hexdigest()
US_FIGI = "BBG000000001"
FOREIGN_FIGI = "BBG000000002"
SHARE_A = "BBG000000101"
SHARE_B = "BBG000000102"
CAPTURED_AT = datetime(2024, 1, 1, 12, tzinfo=UTC)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _asset(
    session: date,
    ordinal: int,
    *,
    ticker: str = "AAA",
    composite: str | None = US_FIGI,
    share: str | None = SHARE_A,
    active: bool = True,
    locale: str | None = "us",
    market: str | None = "stocks",
    exchange: str | None = "XNAS",
) -> dict[str, object]:
    source_id = _digest(f"{session.isoformat()}-{ordinal}")
    return {
        "session_year": session.year,
        "session_date": session,
        "requested_active": active,
        "provider_active": active,
        "ticker": ticker,
        "type_code": "CS",
        "name": f"{ticker} Incorporated",
        "market": market,
        "locale": locale,
        "primary_exchange_mic": exchange,
        "currency_name": "usd",
        "cik": f"{ordinal:010d}",
        "composite_figi": composite,
        "share_class_figi": share,
        "delisted_at_utc": None,
        "last_updated_at_utc": CAPTURED_AT,
        "reference_time_scope": "provider_historical_date_membership_snapshot_v1",
        "metadata_time_scope": "metadata_as_returned_at_source_capture_not_historical_vintage_v1",
        "source_capture_at_utc": CAPTURED_AT,
        "source_availability_quality": "reconstructed_historical_snapshot_without_archived_vintage",
        "source_record_id": source_id,
        "source_request_id": _digest(f"request-{ordinal}"),
        "source_provider_request_id": f"provider-{ordinal}",
        "source_artifact_sha256": _digest(f"bronze-{ordinal}"),
        "source_page_sequence": 0,
        "source_row_ordinal": ordinal,
        "source_row_hash": _digest(f"row-{ordinal}"),
    }


def _universe_from_asset(row: dict[str, object]) -> dict[str, object]:
    return {
        "session_year": row["session_year"],
        "session_date": row["session_date"],
        "ticker": row["ticker"],
        "active_on_date": row["provider_active"],
        "type_code": row["type_code"],
        "name": row["name"],
        "market": row["market"],
        "locale": row["locale"],
        "primary_exchange_mic": row["primary_exchange_mic"],
        "currency_name": row["currency_name"],
        "cik": row["cik"],
        "composite_figi": row["composite_figi"],
        "share_class_figi": row["share_class_figi"],
        "delisted_at_utc": row["delisted_at_utc"],
        "last_updated_at_utc": row["last_updated_at_utc"],
        "reference_time_scope": row["reference_time_scope"],
        "metadata_time_scope": row["metadata_time_scope"],
        "selected_source_capture_at_utc": row["source_capture_at_utc"],
        "source_availability_quality": row["source_availability_quality"],
        "selected_source_record_id": row["source_record_id"],
        "source_request_id": row["source_request_id"],
        "source_provider_request_id": row["source_provider_request_id"],
        "source_artifact_sha256": row["source_artifact_sha256"],
        "source_page_sequence": row["source_page_sequence"],
        "source_row_ordinal": row["source_row_ordinal"],
        "source_row_hash": row["source_row_hash"],
    }


def _asset_path(session: date) -> str:
    return f"silver/asset/session_date={session.isoformat()}/part.parquet"


def _universe_path(session: date) -> str:
    return f"silver/universe/session_date={session.isoformat()}/part.parquet"


def _consume_day(
    engine: CompositeInventoryEngine,
    session: date,
    assets: list[dict[str, object]],
    universes: list[dict[str, object]],
    *,
    asset_split: int | None = None,
    universe_split: int | None = None,
) -> None:
    engine.start_session(session)
    asset_chunks = _chunks(assets, asset_split)
    asset_base = 0
    for chunk in asset_chunks:
        engine.consume_asset_batch(
            chunk,
            artifact_path=_asset_path(session),
            artifact_sha256=_digest(f"asset-parquet-{session}"),
            row_group=0,
            row_index_base=asset_base,
        )
        asset_base += len(chunk)
    universe_chunks = _chunks(universes, universe_split)
    universe_base = 0
    for chunk in universe_chunks:
        engine.consume_universe_batch(
            chunk,
            artifact_path=_universe_path(session),
            artifact_sha256=_digest(f"universe-parquet-{session}"),
            row_group=0,
            row_index_base=universe_base,
        )
        universe_base += len(chunk)
    engine.finish_session()


def _chunks(
    rows: list[dict[str, object]],
    split: int | None,
) -> list[list[dict[str, object]]]:
    if not rows:
        return [[]]
    if split is None or split <= 0 or split >= len(rows):
        return [rows]
    return [rows[:split], rows[split:]]


def _run(
    sessions: list[tuple[date, list[dict[str, object]]]],
    *,
    release: str = RELEASE,
    caps: CompositeInventoryCaps | None = None,
    split: bool = False,
):
    engine = CompositeInventoryEngine(authority_release_id=release, caps=caps)
    for session, assets in sessions:
        universes = [_universe_from_asset(row) for row in assets]
        _consume_day(
            engine,
            session,
            assets,
            universes,
            asset_split=1 if split else None,
            universe_split=1 if split else None,
        )
    return engine.finalize()


def _expected_lineage(release: str, source_ids: list[str]) -> str:
    seed = json.dumps(
        {
            "parent_table": ASSET_AUTHORITY_TABLE,
            "release_id": release,
            "rule_version": LINEAGE_RULE_VERSION,
            "scan_order": SCAN_ORDER_RULE,
        },
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    digest = hashlib.sha256()
    digest.update(seed)
    for source_id in source_ids:
        digest.update(bytes.fromhex(source_id))
    return digest.hexdigest()


def test_streaming_aggregate_uses_only_asset_authority_and_exact_lineage() -> None:
    first = date(2024, 1, 2)
    second = date(2024, 1, 3)
    rows_1 = [
        _asset(first, 1, ticker="AAA", share=SHARE_B, active=True),
        _asset(
            first,
            2,
            ticker="AAA.P",
            share=SHARE_A,
            active=False,
            locale="US",
            market="STOCKS",
            exchange="XNYS",
        ),
    ]
    rows_2 = [_asset(second, 3, ticker="AAA", share=SHARE_A, active=True)]

    result = _run([(first, rows_1), (second, rows_2)], split=True)

    assert len(result.records) == 1
    record = result.records[0]
    assert record.observed_share_class_figis == (SHARE_A, SHARE_B)
    assert record.share_class_conflict is True
    assert (record.first_session, record.last_session, record.session_count) == (
        first,
        second,
        2,
    )
    assert (record.active_row_count, record.inactive_row_count) == (2, 1)
    assert (record.ticker_count, record.provider_locale_count) == (2, 2)
    assert (record.provider_market_count, record.primary_exchange_count) == (2, 2)
    assert record.source_record_lineage_digest == _expected_lineage(
        RELEASE,
        [str(row["source_record_id"]) for row in (*rows_1, *rows_2)],
    )
    assert result.diagnostics.authority_row_count == 3
    assert result.diagnostics.reconciliation_row_count == 3
    assert result.diagnostics.authority_universe_row_count_difference == 0
    assert result.diagnostics.nonselected_authority_row_count == 0


def test_batch_boundaries_do_not_change_result_but_row_order_and_release_do() -> None:
    session = date(2024, 1, 2)
    rows = [_asset(session, 1, ticker="AAA"), _asset(session, 2, ticker="BBB")]

    unsplit = _run([(session, rows)])
    split = _run([(session, rows)], split=True)
    reversed_result = _run([(session, list(reversed(rows)))])
    other_release = _run([(session, rows)], release=OTHER_RELEASE)

    assert unsplit == split
    assert unsplit.records[0].source_record_lineage_digest != (
        reversed_result.records[0].source_record_lineage_digest
    )
    assert unsplit.records[0].source_record_lineage_digest != (
        other_release.records[0].source_record_lineage_digest
    )


@pytest.mark.parametrize(
    ("value", "suffix"),
    [
        (None, "null"),
        ("", "empty"),
        (" \t", "whitespace_only"),
        (" BBG000000001", "surrounding_whitespace"),
        ("BBG1", "length_not_12"),
        ("BBG00000000a", "non_upper_ascii_alnum"),
        ("ABC000000001", "prefix_not_BBG"),
        (US_FIGI, None),
    ],
)
def test_figi_invalid_reason_has_exact_mutually_exclusive_precedence(
    value: str | None,
    suffix: str | None,
) -> None:
    expected = None if suffix is None else f"composite_figi_{suffix}"
    assert figi_invalid_reason(value) == expected
    expected_share = None if suffix is None else f"share_class_figi_{suffix}"
    assert figi_invalid_reason(value, field="share_class_figi") == expected_share


def test_invalid_composite_excludes_share_diagnostics_but_retains_bad_share() -> None:
    session = date(2024, 1, 2)
    rows = [
        _asset(session, 1, ticker="BAD", composite="", share=""),
        _asset(session, 2, ticker="NULL", share=None),
        _asset(session, 3, ticker="LOWER", share="BBG00000000a"),
    ]

    result = _run([(session, rows)])

    assert len(result.records) == 1
    assert result.records[0].observed_share_class_figis == ()
    assert result.records[0].active_row_count == 2
    assert dict(result.diagnostics.invalid_composite_reason_counts) == {"composite_figi_empty": 1}
    assert dict(result.diagnostics.invalid_share_class_reason_counts) == {
        "share_class_figi_null": 1,
        "share_class_figi_non_upper_ascii_alnum": 1,
    }
    assert len(result.diagnostics.bounded_invalid_examples) == 3
    example = result.diagnostics.bounded_invalid_examples[0]
    assert example.artifact_path == _asset_path(session)
    assert example.artifact_sha256 == _digest(f"asset-parquet-{session}")
    assert example.row_group == 0
    assert example.provider_active is True


def test_bounded_examples_keep_earliest_rows_per_exact_reason() -> None:
    session = date(2024, 1, 2)
    rows = [
        *[_asset(session, index, ticker=f"N{index}", composite=None) for index in range(3)],
        *[_asset(session, index + 10, ticker=f"E{index}", composite="") for index in range(3)],
    ]
    result = _run(
        [(session, rows)],
        caps=CompositeInventoryCaps(bounded_example_limit=2),
    )

    examples = result.diagnostics.bounded_invalid_examples
    assert len(examples) == 4
    assert [item.reason for item in examples] == [
        "composite_figi_null",
        "composite_figi_null",
        "composite_figi_empty",
        "composite_figi_empty",
    ]
    assert [item.row_index_in_group for item in examples] == [0, 1, 3, 4]


def test_inverse_bounce_and_case_variants_are_preserved_without_normalization() -> None:
    sessions = [date(2024, 1, day) for day in (2, 3, 4)]
    rows = [
        _asset(sessions[0], 1, ticker="AzPn", composite=US_FIGI, locale="us"),
        _asset(sessions[1], 2, ticker="AZPN", composite=FOREIGN_FIGI, locale="gb"),
        _asset(sessions[2], 3, ticker="AZPN", composite=US_FIGI, locale="US"),
    ]

    result = _run([(session, [row]) for session, row in zip(sessions, rows, strict=True)])

    assert [item.observed_composite_figi for item in result.records] == [US_FIGI, FOREIGN_FIGI]
    us_record, foreign_record = result.records
    assert (us_record.session_count, us_record.ticker_count, us_record.provider_locale_count) == (
        2,
        2,
        2,
    )
    assert foreign_record.session_count == 1


@pytest.mark.parametrize(
    ("breaker", "check_id"),
    [
        ("missing_parent", "universe_parent_missing"),
        ("projection", "universe_projection_mismatch"),
        ("duplicate_asset", "authority_source_record_duplicate"),
        ("duplicate_universe", "universe_selected_source_record_duplicate"),
        ("active", "requested_provider_active_mismatch"),
    ],
)
def test_daily_reconciliation_fails_closed(breaker: str, check_id: str) -> None:
    session = date(2024, 1, 2)
    row = _asset(session, 1)
    universe = _universe_from_asset(row)
    assets = [row]
    universes = [universe]
    if breaker == "missing_parent":
        universe["selected_source_record_id"] = _digest("missing")
    elif breaker == "projection":
        universe["market"] = "different"
    elif breaker == "duplicate_asset":
        assets.append(dict(row))
    elif breaker == "duplicate_universe":
        duplicate = dict(universe)
        duplicate["ticker"] = "BBB"
        universes.append(duplicate)
    else:
        row["requested_active"] = False

    engine = CompositeInventoryEngine(authority_release_id=RELEASE)
    with pytest.raises(CompositeInventoryError) as captured:
        _consume_day(engine, session, assets, universes)
    assert captured.value.check_id == check_id


def test_universe_is_reconciliation_only_and_reports_nonselected_authority_rows() -> None:
    session = date(2024, 1, 2)
    selected = _asset(session, 1, ticker="AAA")
    nonselected = _asset(session, 2, ticker="AAA")
    engine = CompositeInventoryEngine(authority_release_id=RELEASE)
    _consume_day(engine, session, [selected, nonselected], [_universe_from_asset(selected)])

    result = engine.finalize()

    assert result.records[0].active_row_count == 2
    assert result.diagnostics.authority_universe_row_count_difference == 1
    assert result.diagnostics.nonselected_authority_row_count == 1


def test_resource_caps_are_enforced_for_composites_and_pairs() -> None:
    session = date(2024, 1, 2)
    composite_rows = [
        _asset(session, 1, composite=US_FIGI),
        _asset(session, 2, ticker="BBB", composite=FOREIGN_FIGI),
    ]
    pair_rows = [
        _asset(session, 1, share=SHARE_A),
        _asset(session, 2, ticker="BBB", share=SHARE_B),
    ]

    for rows, caps in (
        (composite_rows, CompositeInventoryCaps(max_distinct_composite_figis=1)),
        (
            pair_rows,
            CompositeInventoryCaps(max_distinct_composite_share_class_pairs=1),
        ),
    ):
        engine = CompositeInventoryEngine(authority_release_id=RELEASE, caps=caps)
        with pytest.raises(CompositeInventoryError) as captured:
            _consume_day(
                engine,
                session,
                rows,
                [_universe_from_asset(row) for row in rows],
            )
        assert captured.value.check_id == "resource_cap_exceeded"


def test_session_state_machine_rejects_interleaving_and_missing_side() -> None:
    session = date(2024, 1, 2)
    row = _asset(session, 1)
    engine = CompositeInventoryEngine(authority_release_id=RELEASE)
    engine.start_session(session)
    with pytest.raises(CompositeInventoryError) as captured:
        engine.consume_universe_batch(
            [_universe_from_asset(row)],
            artifact_path=_universe_path(session),
            artifact_sha256=_digest("universe-parquet"),
            row_group=0,
            row_index_base=0,
        )
    assert captured.value.check_id == "source_binding_invalid"

    engine = CompositeInventoryEngine(authority_release_id=RELEASE)
    engine.start_session(session)
    engine.consume_asset_batch(
        [row],
        artifact_path=_asset_path(session),
        artifact_sha256=_digest("asset-parquet"),
        row_group=0,
        row_index_base=0,
    )
    with pytest.raises(CompositeInventoryError) as captured:
        engine.finish_session()
    assert captured.value.check_id == "session_spine_mismatch"


def test_physical_row_keys_must_be_strictly_increasing() -> None:
    session = date(2024, 1, 2)
    first = _asset(session, 1)
    second = _asset(session, 2, ticker="BBB")
    engine = CompositeInventoryEngine(authority_release_id=RELEASE)
    engine.start_session(session)
    engine.consume_asset_batch(
        [first],
        artifact_path=_asset_path(session),
        artifact_sha256=_digest("asset-parquet"),
        row_group=0,
        row_index_base=1,
    )
    with pytest.raises(CompositeInventoryError) as captured:
        engine.consume_asset_batch(
            [second],
            artifact_path=_asset_path(session),
            artifact_sha256=_digest("asset-parquet"),
            row_group=0,
            row_index_base=0,
        )
    assert captured.value.check_id == "source_binding_invalid"
