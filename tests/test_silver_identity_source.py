from __future__ import annotations

import inspect
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ame_stocks_api.silver.identity_source as identity_source_module
from ame_stocks_api.artifacts import sha256_file
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.contracts import ArtifactRef, ArtifactRole, arrow_schema_digest
from ame_stocks_api.silver.identity_source import (
    S7_SIX_RELEASE_BINDING_ID,
    S7_SOURCE_PINS,
    IdentityPublishedSource,
    IdentitySourceBundle,
    IdentitySourceError,
    open_approved_identity_preview_source_bundle,
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


def _published(contract, path, *, table: str, relative_path: str | None = None):
    ref = ArtifactRef(
        path=relative_path or f"silver/fixture/{table}/part-00000.parquet",
        sha256=sha256_file(path),
        bytes=path.stat().st_size,
        row_count=pq.ParquetFile(path).metadata.num_rows,
        media_type="application/vnd.apache.parquet",
        role=ArtifactRole.DATA,
        table=table,
        schema_digest=arrow_schema_digest(contract.arrow_schema),
    )
    return SimpleNamespace(
        contract=contract,
        data_paths=(path,),
        release=SimpleNamespace(outputs=(ref,)),
    )


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


def test_bundle_requires_factory_and_test_factory_rejects_incomplete_set() -> None:
    with pytest.raises(IdentitySourceError, match="must be opened"):
        IdentitySourceBundle({})
    with pytest.raises(IdentitySourceError, match="exact six-table"):
        IdentitySourceBundle._for_testing({})


def test_bundle_streams_bounded_selected_columns(tmp_path) -> None:
    contract = ASSET_OBSERVATION_DAILY_CONTRACT
    arrays = [
        pa.array([_value_for_field(field.name, field.type)], type=field.type)
        for field in contract.arrow_schema
    ]
    path = tmp_path / "source.parquet"
    pq.write_table(pa.Table.from_arrays(arrays, schema=contract.arrow_schema), path)
    published = _published(contract, path, table=contract.table)
    sources = {
        table: IdentityPublishedSource(
            pin=pin,
            published=published,
            release_manifest_path="manifests/fixture.json",
            release_manifest_sha256="0" * 64,
        )
        for table, pin in S7_SOURCE_PINS.items()
    }
    bundle = IdentitySourceBundle._for_testing(sources)
    assert bundle.official is False
    with pytest.raises(IdentitySourceError, match="cannot attest production"):
        bundle.require_official()
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

    physical = tuple(
        bundle.iter_physical_batches(
            "asset_observation_daily",
            columns=("session_date", "ticker"),
            batch_size=1,
        )
    )
    assert len(physical) == 1
    assert physical[0].artifact.ref.path.endswith("part-00000.parquet")
    assert physical[0].row_group == 0
    assert physical[0].row_index_in_group == 0
    assert physical[0].batch.schema.names == ["session_date", "ticker"]
    assert physical[0].official is False
    assert physical[0].artifact.official is False
    with pytest.raises(IdentitySourceError, match="cannot attest production"):
        physical[0].require_official()
    with pytest.raises(IdentitySourceError, match="cannot attest production"):
        physical[0].artifact.require_official()


def test_bundle_selects_one_exact_artifact_per_sorted_session(tmp_path) -> None:
    contract = UNIVERSE_SOURCE_DAILY_CONTRACT
    paths: list[Path] = []
    refs: list[ArtifactRef] = []
    for session in (date(2024, 1, 2), date(2024, 1, 3)):
        arrays = [
            pa.array(
                [
                    session
                    if field.name in {"session_date", "source_available_session"}
                    else _value_for_field(field.name, field.type)
                ],
                type=field.type,
            )
            for field in contract.arrow_schema
        ]
        path = tmp_path / f"{session.isoformat()}.parquet"
        pq.write_table(pa.Table.from_arrays(arrays, schema=contract.arrow_schema), path)
        published = _published(
            contract,
            path,
            table=contract.table,
            relative_path=(
                "silver/schema=v1/reference/universe_source_daily/build_id="
                f"{'a' * 64}/data/session_year=2024/session_date={session.isoformat()}/"
                "part-00000.parquet"
            ),
        )
        paths.extend(published.data_paths)
        refs.extend(published.release.outputs)
    universe = SimpleNamespace(
        contract=contract,
        data_paths=tuple(paths),
        release=SimpleNamespace(outputs=tuple(refs)),
    )
    dummy = SimpleNamespace(contract=ASSET_OBSERVATION_DAILY_CONTRACT, data_paths=())
    sources = {
        table: IdentityPublishedSource(
            pin=pin,
            published=universe if table == "universe_source_daily" else dummy,
            release_manifest_path="manifests/fixture.json",
            release_manifest_sha256="0" * 64,
        )
        for table, pin in S7_SOURCE_PINS.items()
    }
    bundle = IdentitySourceBundle._for_testing(sources)

    selected = bundle.daily_partition_artifacts(
        "universe_source_daily",
        (date(2024, 1, 2), date(2024, 1, 3)),
    )
    assert tuple(item.path for item in selected) == tuple(paths)
    with pytest.raises(IdentitySourceError, match="sorted and unique"):
        bundle.daily_partition_artifacts(
            "universe_source_daily",
            (date(2024, 1, 3), date(2024, 1, 2)),
        )
    with pytest.raises(IdentitySourceError, match="missing scoped sessions"):
        bundle.daily_partition_artifacts(
            "universe_source_daily",
            (date(2024, 1, 4),),
        )


def test_physical_batches_stream_one_row_group_at_the_requested_bound(tmp_path) -> None:
    contract = ASSET_OBSERVATION_DAILY_CONTRACT
    arrays = [
        pa.array(
            [_value_for_field(field.name, field.type) for _ in range(5)],
            type=field.type,
        )
        for field in contract.arrow_schema
    ]
    path = tmp_path / "bounded.parquet"
    pq.write_table(
        pa.Table.from_arrays(arrays, schema=contract.arrow_schema),
        path,
        row_group_size=5,
    )
    published = _published(contract, path, table=contract.table)
    sources = {
        table: IdentityPublishedSource(
            pin=pin,
            published=published,
            release_manifest_path="manifests/fixture.json",
            release_manifest_sha256="0" * 64,
        )
        for table, pin in S7_SOURCE_PINS.items()
    }
    batches = tuple(
        IdentitySourceBundle._for_testing(sources).iter_physical_batches(
            "asset_observation_daily",
            batch_size=2,
        )
    )

    assert tuple(item.batch.num_rows for item in batches) == (2, 2, 1)
    assert tuple(item.row_index_in_group for item in batches) == (0, 2, 4)
    assert {item.row_group for item in batches} == {0}


def test_bundle_adapts_universe_rows_for_bounce_detector(tmp_path) -> None:
    contract = UNIVERSE_SOURCE_DAILY_CONTRACT
    arrays = [
        pa.array([_value_for_field(field.name, field.type)], type=field.type)
        for field in contract.arrow_schema
    ]
    path = tmp_path / "universe.parquet"
    pq.write_table(pa.Table.from_arrays(arrays, schema=contract.arrow_schema), path)
    dummy = SimpleNamespace(contract=ASSET_OBSERVATION_DAILY_CONTRACT, data_paths=())
    universe = _published(contract, path, table=contract.table)
    sources = {
        table: IdentityPublishedSource(
            pin=pin,
            published=universe if table == "universe_source_daily" else dummy,
            release_manifest_path="manifests/fixture.json",
            release_manifest_sha256="0" * 64,
        )
        for table, pin in S7_SOURCE_PINS.items()
    }
    rows = tuple(IdentitySourceBundle._for_testing(sources).iter_bounce_observations(batch_size=1))
    assert len(rows) == 1
    assert rows[0].ticker == "FIX"
    assert rows[0].observed_composite_figi == "BBG000000001"
    assert rows[0].source_record_id == "a" * 64


def test_physical_artifact_capability_cannot_cross_test_bundles(tmp_path) -> None:
    contract = ASSET_OBSERVATION_DAILY_CONTRACT
    arrays = [
        pa.array([_value_for_field(field.name, field.type)], type=field.type)
        for field in contract.arrow_schema
    ]
    path = tmp_path / "source.parquet"
    pq.write_table(pa.Table.from_arrays(arrays, schema=contract.arrow_schema), path)
    published = _published(contract, path, table=contract.table)
    sources = {
        table: IdentityPublishedSource(
            pin=pin,
            published=published,
            release_manifest_path="manifests/fixture.json",
            release_manifest_sha256="0" * 64,
        )
        for table, pin in S7_SOURCE_PINS.items()
    }
    first = IdentitySourceBundle._for_testing(sources)
    second = IdentitySourceBundle._for_testing(sources)
    artifact = first.artifacts("asset_observation_daily")[0]

    with pytest.raises(IdentitySourceError, match="outside the exact release"):
        tuple(
            second.iter_physical_batches(
                "asset_observation_daily",
                artifacts=(artifact,),
            )
        )


def test_approved_preview_source_factory_has_no_caller_scope_inputs() -> None:
    assert tuple(inspect.signature(open_approved_identity_preview_source_bundle).parameters) == (
        "data_root",
        "plan_id",
        "expected_plan_sha256",
        "approval_id",
        "expected_approval_sha256",
    )


def test_preview_capability_requires_exact_two_table_session_cartesian_product(
    tmp_path: Path,
) -> None:
    session = date(2024, 1, 2)

    def membership(table: str) -> tuple[str, str, str, ArtifactRef]:
        pin = S7_SOURCE_PINS[table]
        ref = ArtifactRef(
            path=(
                f"silver/schema=v1/fixture/{table}/build_id={pin.build_id}/data/"
                f"session_year=2024/session_date={session.isoformat()}/part-00000.parquet"
            ),
            sha256="9" * 64,
            bytes=100,
            row_count=1,
            media_type="application/vnd.apache.parquet",
            role=ArtifactRole.DATA,
            table=table,
            schema_digest="8" * 64,
        )
        return (
            pin.release_id,
            f"manifests/silver/releases/release_id={pin.release_id}.json",
            pin.release_manifest_sha256,
            ref,
        )

    complete = {
        (table, value[3].path): value
        for table in ("asset_observation_daily", "universe_source_daily")
        for value in (membership(table),)
    }
    capability = identity_source_module._IdentitySourceCapability(
        official=True,
        data_root=tmp_path.resolve(),
        artifact_memberships=complete,
        physical_scope=identity_source_module._PREVIEW_PHYSICAL_SCOPE,
        authorized_sessions=(session,),
        preview_control_binding=("1" * 64, "2" * 64, "3" * 64, "4" * 64),
        _seal=identity_source_module._OFFICIAL_CAPABILITY_SEAL,
    )
    assert len(capability.artifact_memberships) == 2

    incomplete = dict(complete)
    incomplete.pop(next(key for key in incomplete if key[0] == "universe_source_daily"))
    with pytest.raises(IdentitySourceError, match="exact daily scope"):
        identity_source_module._IdentitySourceCapability(
            official=True,
            data_root=tmp_path.resolve(),
            artifact_memberships=incomplete,
            physical_scope=identity_source_module._PREVIEW_PHYSICAL_SCOPE,
            authorized_sessions=(session,),
            preview_control_binding=("1" * 64, "2" * 64, "3" * 64, "4" * 64),
            _seal=identity_source_module._OFFICIAL_CAPABILITY_SEAL,
        )
