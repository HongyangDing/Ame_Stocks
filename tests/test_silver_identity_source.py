from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.identity_source import (
    S7_SIX_RELEASE_BINDING_ID,
    S7_SOURCE_PINS,
    IdentityPublishedSource,
    IdentitySourceBundle,
    IdentitySourceError,
)


def _value_for_field(name: str, data_type: pa.DataType) -> object:
    special = {
        "active_on_date": True,
        "composite_figi": "BBG000000001",
        "selected_source_record_id": "a" * 64,
        "ticker": "FIX",
    }
    if name in special:
        return special[name]
    if pa.types.is_string(data_type):
        return "fixture"
    if pa.types.is_boolean(data_type):
        return True
    if pa.types.is_int64(data_type):
        return 1
    if pa.types.is_float64(data_type):
        return 1.0
    if pa.types.is_date32(data_type):
        return date(2024, 1, 2)
    if pa.types.is_timestamp(data_type):
        return datetime(2024, 1, 2, tzinfo=UTC)
    raise AssertionError(data_type)


def test_production_source_pins_are_the_exact_six_release_profile() -> None:
    assert S7_SIX_RELEASE_BINDING_ID == (
        "49f3d20725f2609b43d6736df78993b2975c9f1b71947af93190dc0658366c64"
    )
    assert set(S7_SOURCE_PINS) == {
        "asset_observation_daily",
        "asset_observation_version",
        "ticker_change_event",
        "ticker_event_request_status",
        "ticker_overview_safe",
        "universe_source_daily",
    }
    assert sum(item.artifact_count for item in S7_SOURCE_PINS.values()) == 7_542
    assert sum(item.row_count for item in S7_SOURCE_PINS.values()) == 138_825_855


def test_bundle_rejects_any_incomplete_table_set() -> None:
    with pytest.raises(IdentitySourceError, match="exact six-table"):
        IdentitySourceBundle({})


def test_bundle_streams_bounded_selected_columns(tmp_path) -> None:
    contract = ASSET_OBSERVATION_DAILY_CONTRACT
    arrays = [
        pa.array([_value_for_field(field.name, field.type)], type=field.type)
        for field in contract.arrow_schema
    ]
    path = tmp_path / "source.parquet"
    pq.write_table(pa.Table.from_arrays(arrays, schema=contract.arrow_schema), path)
    published = SimpleNamespace(contract=contract, data_paths=(path,))
    sources = {
        table: IdentityPublishedSource(
            pin=pin,
            published=published,
            release_manifest_path="manifests/fixture.json",
            release_manifest_sha256="0" * 64,
        )
        for table, pin in S7_SOURCE_PINS.items()
    }
    bundle = IdentitySourceBundle(sources)
    batches = tuple(
        bundle.iter_batches(
            "asset_observation_daily",
            columns=("session_date", "ticker"),
            batch_size=1,
        )
    )
    assert len(batches) == 1
    assert batches[0].schema.names == ["session_date", "ticker"]
    assert batches[0].num_rows == 1
    with pytest.raises(IdentitySourceError, match="not contracted"):
        tuple(bundle.iter_batches("asset_observation_daily", columns=("secret",)))
    with pytest.raises(IdentitySourceError, match="batch_size"):
        tuple(bundle.iter_batches("asset_observation_daily", batch_size=0))


def test_bundle_adapts_universe_rows_for_bounce_detector(tmp_path) -> None:
    contract = UNIVERSE_SOURCE_DAILY_CONTRACT
    arrays = [
        pa.array([_value_for_field(field.name, field.type)], type=field.type)
        for field in contract.arrow_schema
    ]
    path = tmp_path / "universe.parquet"
    pq.write_table(pa.Table.from_arrays(arrays, schema=contract.arrow_schema), path)
    dummy = SimpleNamespace(
        contract=ASSET_OBSERVATION_DAILY_CONTRACT,
        data_paths=(tmp_path / "unused.parquet",),
    )
    universe = SimpleNamespace(contract=contract, data_paths=(path,))
    sources = {
        table: IdentityPublishedSource(
            pin=pin,
            published=universe if table == "universe_source_daily" else dummy,
            release_manifest_path="manifests/fixture.json",
            release_manifest_sha256="0" * 64,
        )
        for table, pin in S7_SOURCE_PINS.items()
    }
    rows = tuple(IdentitySourceBundle(sources).iter_bounce_observations(batch_size=1))
    assert len(rows) == 1
    assert rows[0].ticker == "FIX"
    assert rows[0].observed_composite_figi == "BBG000000001"
    assert rows[0].source_record_id == "a" * 64
