from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from ame_stocks_api.silver import condition_codes
from ame_stocks_api.silver.condition_code_contract import (
    CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT,
    CONDITION_CODE_DIM_CONTRACT,
)
from ame_stocks_api.silver.condition_code_source import (
    ConditionCodeSourceBatch,
    ConditionCodeSourcePage,
    ConditionCodeSourceSnapshot,
)
from ame_stocks_api.silver.condition_codes import transform_condition_code_batch
from ame_stocks_api.silver.contracts import QAStatus

CAPTURE_AT = datetime(2026, 7, 11, 18, 54, 40, tzinfo=UTC)
BUILD_ID = "d" * 64


def _rules() -> dict[str, object]:
    return {
        "consolidated": {
            "updates_high_low": True,
            "updates_open_close": False,
            "updates_volume": True,
        },
        "market_center": {
            "updates_high_low": False,
            "updates_open_close": True,
            "updates_volume": False,
        },
    }


def _row(
    condition_id: int,
    *,
    condition_type: str = "sale_condition",
    data_types: list[str] | None = None,
    name: str | None = None,
    legacy: bool | None = None,
    exchange: int | None = None,
    update_rules: bool = True,
    **extra: object,
) -> dict[str, object]:
    row: dict[str, object] = {
        "asset_class": "stocks",
        "data_types": ["trade"] if data_types is None else data_types,
        "id": condition_id,
        "name": name or f"Condition {condition_id}",
        "sip_mapping": {"CTA": str(condition_id % 10)},
        "type": condition_type,
    }
    if legacy is not None:
        row["legacy"] = legacy
    if exchange is not None:
        row["exchange"] = exchange
    if update_rules:
        row["update_rules"] = _rules()
    row.update(extra)
    return row


def _batch(rows: list[dict[str, object]]) -> ConditionCodeSourceBatch:
    page = ConditionCodeSourcePage(
        source_path="fixtures/condition_codes/page-00000.json.gz",
        source_artifact_sha256="b" * 64,
        sequence=0,
        source_provider_request_id="provider-request",
        rows=tuple(rows),
    )
    return ConditionCodeSourceBatch(
        (
            ConditionCodeSourceSnapshot(
                source_request_id="a" * 64,
                source_capture_at_utc=CAPTURE_AT,
                pages=(page,),
            ),
        )
    )


def _production_shape() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for ordinal in range(1, 95):
        if ordinal <= 55:
            data_types = ["trade"]
        elif ordinal <= 84:
            data_types = ["bbo", "nbbo"]
        elif ordinal <= 93:
            data_types = ["bbo"]
        else:
            data_types = ["nbbo"]
        condition_type = "sale_condition" if ordinal <= 55 else "quote_condition"
        condition_id = ordinal
        legacy = None
        name = None
        if ordinal == 81:
            condition_type = "quote_condition"
            condition_id = 30
            name = "In View Of Common"
        elif ordinal == 82:
            condition_type = "quote_condition"
            condition_id = 30
            name = "Equipment Changeover"
            legacy = True
        elif ordinal in {6, 27, 31, 33, 35, 55, 80}:
            legacy = True
        rows.append(
            _row(
                condition_id,
                condition_type=condition_type,
                data_types=data_types,
                name=name,
                legacy=legacy,
                exchange=1 if ordinal == 23 else 10 if ordinal == 24 else None,
                update_rules=ordinal <= 41,
            )
        )
    return rows


def test_production_shape_preserves_94_definitions_and_123_memberships() -> None:
    result = transform_condition_code_batch(
        _batch(_production_shape()),
        build_id=BUILD_ID,
        known_exchange_ids={1, 10},
    )

    assert result.dim.table.schema == CONDITION_CODE_DIM_CONTRACT.arrow_schema
    assert result.bridge.table.schema == CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT.arrow_schema
    assert (result.dim.table.num_rows, result.bridge.table.num_rows) == (94, 123)
    assert len(result.dim.table.column_names) == 29
    assert len(result.bridge.table.column_names) == 20
    assert result.dim.row_funnel.version_preserved_rows == 2
    assert result.bridge.row_funnel.version_preserved_rows == 2
    assert result.dim.row_funnel.output_rows_by_table == {"condition_code_dim": 94}
    assert result.bridge.row_funnel.output_rows_by_table == {"condition_code_data_type_bridge": 123}
    assert not result.dim.quarantine_records
    assert not result.bridge.quarantine_records
    assert all(check.status is QAStatus.PASSED for check in result.dim.qa_checks)
    assert all(check.status is QAStatus.PASSED for check in result.bridge.qa_checks)

    dim_rows = result.dim.table.to_pylist()
    versions = [
        row
        for row in dim_rows
        if row["condition_type"] == "quote_condition" and row["condition_id"] == 30
    ]
    assert [(row["is_legacy"], row["name"]) for row in versions] == [
        (False, "In View Of Common"),
        (True, "Equipment Changeover"),
    ]
    current = versions[0]
    assert current["legacy_source_present"] is False
    assert current["capture_date"].isoformat() == "2026-07-11"
    assert current["available_session"].isoformat() == "2026-07-13"
    assert current["available_at_utc"] == datetime(2026, 7, 13, 13, 30, tzinfo=UTC)
    assert json.loads(current["data_types_json"]) == ["bbo", "nbbo"]

    bridge_rows = result.bridge.table.to_pylist()
    version_links = [
        row
        for row in bridge_rows
        if row["condition_type"] == "quote_condition" and row["condition_id"] == 30
    ]
    assert {
        (row["is_legacy"], row["data_type"], row["source_data_type_ordinal"])
        for row in version_links
    } == {
        (False, "bbo", 0),
        (False, "nbbo", 1),
        (True, "bbo", 0),
        (True, "nbbo", 1),
    }

    repeated = transform_condition_code_batch(
        _batch(_production_shape()),
        build_id=BUILD_ID,
        known_exchange_ids={1, 10},
    )
    assert repeated.dim.table.equals(result.dim.table)
    assert repeated.bridge.table.equals(result.bridge.table)
    assert repeated.dim.qa_checks == result.dim.qa_checks


def test_missing_update_rules_remains_null_not_false() -> None:
    result = transform_condition_code_batch(
        _batch([_row(1, update_rules=False)]),
        build_id=BUILD_ID,
        known_exchange_ids=set(),
    )
    row = result.dim.table.to_pylist()[0]
    assert row["update_rules_json"] is None
    assert all(
        row[column] is None
        for column in (
            "consolidated_updates_high_low",
            "consolidated_updates_open_close",
            "consolidated_updates_volume",
            "market_center_updates_high_low",
            "market_center_updates_open_close",
            "market_center_updates_volume",
        )
    )


def test_invalid_data_types_quarantines_source_for_both_tables() -> None:
    result = transform_condition_code_batch(
        _batch([_row(1, data_types=["trade", "trade"])]),
        build_id=BUILD_ID,
        known_exchange_ids=set(),
    )
    assert result.dim.table.num_rows == result.bridge.table.num_rows == 0
    assert result.dim.row_funnel.quarantined_source_rows == 1
    assert result.bridge.row_funnel.quarantined_source_rows == 1
    assert result.dim.qa_by_id("data_types_invalid_rows").status is QAStatus.FAILED
    assert result.bridge.qa_by_id("data_types_invalid_rows").status is QAStatus.FAILED
    assert {item.issue_code for item in result.dim.quarantine_records} == {
        "data_types_invalid_rows"
    }


def test_unknown_exchange_is_retained_but_blocks_dim_publish() -> None:
    result = transform_condition_code_batch(
        _batch([_row(1, exchange=999)]),
        build_id=BUILD_ID,
        known_exchange_ids={1, 10},
    )
    assert result.dim.table.to_pylist()[0]["exchange_id"] == 999
    assert not result.dim.quarantine_records
    check = result.dim.qa_by_id("exchange_fk_unresolved_rows")
    assert (check.numerator, check.denominator, check.status) == (1, 1, QAStatus.FAILED)
    assert all(check.status is QAStatus.PASSED for check in result.bridge.qa_checks)


def test_primary_key_conflict_quarantines_both_source_versions() -> None:
    result = transform_condition_code_batch(
        _batch([_row(1, name="One"), _row(1, name="Different")]),
        build_id=BUILD_ID,
        known_exchange_ids=set(),
    )
    assert result.dim.table.num_rows == result.bridge.table.num_rows == 0
    assert result.dim.qa_by_id("primary_key_conflict_rows").numerator == 2
    assert result.bridge.qa_by_id("primary_key_conflict_rows").numerator == 2
    assert len(result.dim.quarantine_records) == len(result.bridge.quarantine_records) == 2


def test_domain_drift_is_preserved_and_visible_as_warning() -> None:
    row = _row(1, condition_type="future_condition", data_types=["future_feed"])
    result = transform_condition_code_batch(
        _batch([row]),
        build_id=BUILD_ID,
        known_exchange_ids=set(),
    )
    assert result.dim.table.to_pylist()[0]["condition_type"] == "future_condition"
    assert result.bridge.table.to_pylist()[0]["data_type"] == "future_feed"
    assert result.dim.qa_by_id("condition_type_unreviewed_rows").status is QAStatus.WARNING
    assert result.bridge.qa_by_id("data_type_unreviewed_rows").status is QAStatus.WARNING
    assert not result.dim.quarantine_records


def test_lineage_validator_recomputes_every_emitted_provenance_field() -> None:
    source = condition_codes._flatten(_batch([_row(1)]), "XNYS")[0]
    output = condition_codes._dim_output(source)
    assert condition_codes._lineage_valid(source, output)

    mutations = {
        "source_capture_at_utc": CAPTURE_AT + timedelta(seconds=1),
        "source_request_id": "c" * 64,
        "source_provider_request_id": "different-provider-request",
        "source_artifact_sha256": "c" * 64,
        "source_page_sequence": 1,
        "source_row_ordinal": 1,
        "source_row_hash": "c" * 64,
        "source_record_id": "c" * 64,
    }
    for field, value in mutations.items():
        damaged = dict(output)
        damaged[field] = value
        assert not condition_codes._lineage_valid(source, damaged), field


def test_canonical_json_validator_rejects_noncanonical_equivalent_json() -> None:
    source = condition_codes._flatten(_batch([_row(1)]), "XNYS")[0]
    output = condition_codes._dim_output(source)
    assert condition_codes._dim_json_valid(source, output)

    damaged = dict(output)
    damaged["sip_mapping_json"] = json.dumps(source.raw["sip_mapping"], sort_keys=True)
    assert json.loads(damaged["sip_mapping_json"]) == source.raw["sip_mapping"]
    assert not condition_codes._dim_json_valid(source, damaged)
