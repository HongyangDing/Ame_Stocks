from __future__ import annotations

import hashlib
import json
import stat
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_silver_s7_5_i3_migration_core import _legacy_alias_and_masters
from test_silver_s7_5_i3_production_contract import _run_spec as _production_run_spec

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_i3_migration_io as migration_io
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.asset_incremental import (
    S4_BASE_TERMINAL_PARTITION_SET_RULE_VERSION,
)
from ame_stocks_api.silver.asset_incremental_contract import S4BaseFrontier
from ame_stocks_api.silver.identity_resolution_contract import S7_DERIVED_CONTRACTS
from ame_stocks_api.silver.incremental_contract import ArtifactPin, RowVersionOperation
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    LEGACY_S7_V1_RELEASE_SET_ID,
    S4_TERMINAL_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_i3_contract import I3_V2_TABLE_ORDER
from ame_stocks_api.silver.incremental_i3_migration_io import (
    CompactBaseMaterializationAttestation,
    CompactBaseMigrationMaterializer,
    CompactBasePreparedMaterialization,
    I3CompactBaseInputBinding,
    I3LegacyV1BasePins,
    I3MigrationIOError,
    I3MigrationParquetPin,
    I3S4BasePins,
    estimate_compact_base_resources,
    load_compact_base_input_binding,
    prepare_compact_base,
    verify_compact_base_materialization_attestation,
)
from ame_stocks_api.silver.incremental_i3_production import (
    I3ProductionPreparedMaterialization,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionI2BaseFrontierPin,
    I3ProductionOutputStorage,
    I3ProductionRowsetIndex,
)
from ame_stocks_api.silver.incremental_i3_production_semantics import (
    production_compact_base_initial_segment_id,
    production_native_v2_migration_id,
)

_SOURCE_FIELDS = {
    "identity_adjudication": (
        "source_identity_adjudication_release_id",
        "source_identity_adjudication_release_available_session",
    ),
    "identity_cross_market_adjudication": (
        "source_identity_cross_market_adjudication_release_id",
        "source_identity_cross_market_adjudication_release_available_session",
    ),
    "provider_composite_override": (
        "source_provider_composite_override_release_id",
        "source_provider_composite_override_release_available_session",
    ),
    "share_class_adjudication": (
        "source_share_class_adjudication_release_id",
        "source_share_class_adjudication_release_available_session",
    ),
    "asset_transition": (
        "source_asset_transition_release_id",
        "source_asset_transition_release_available_session",
    ),
}


def _canonical_control(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def _receipt(table_name: str, session: date | None) -> dict[str, object]:
    contract = S7_DERIVED_CONTRACTS[table_name]
    path = (
        f"silver/s7/candidate/data/{table_name}.parquet"
        if session is None
        else (
            "silver/s7/candidate/data/universe_daily/"
            f"session_date={session.isoformat()}/part-00000.parquet"
        )
    )
    return {
        "bytes": 101,
        "path": path,
        "row_count": 1,
        "schema_digest": contract.schema_digest,
        "sha256": stable_digest({"output": path}),
    }


def _synthetic_s7_release(
    root: Path,
    *,
    mutation: str | None = None,
) -> SimpleNamespace:
    sessions = (date(2026, 7, 9), date(2026, 7, 10))
    available = date(2026, 8, 3)
    release_availability = {"release_available_session": available.isoformat()}
    descriptors: list[dict[str, object]] = []
    for table_name in ("asset_master", "ticker_alias", "issuer_master", "universe_daily"):
        receipts = (
            [_receipt(table_name, None)]
            if table_name != "universe_daily"
            else [_receipt(table_name, session) for session in sessions]
        )
        if table_name == "universe_daily":
            if mutation == "schema":
                receipts[0]["schema_digest"] = stable_digest({"wrong": "schema"})
            elif mutation == "latest":
                receipts[0]["path"] = (
                    "silver/s7/candidate/latest/universe_daily/"
                    "session_date=2026-07-09/part-00000.parquet"
                )
            elif mutation == "glob":
                receipts[0]["path"] = (
                    "silver/s7/candidate/data/universe_daily/session_date=*/part-00000.parquet"
                )
            elif mutation == "unordered":
                receipts.reverse()
            elif mutation == "duplicate_session":
                receipts[1]["path"] = receipts[0]["path"]
        contract = S7_DERIVED_CONTRACTS[table_name]
        payload: dict[str, object] = {
            "approval": {"pin": "approval"},
            "approval_id": stable_digest({"approval": table_name}),
            "artifact_type": "s7_four_table_hidden_member_release",
            "candidate_id": stable_digest({"candidate": "base"}),
            "candidate_manifest": {"pin": "candidate"},
            "candidate_qa": {"pin": "qa"},
            "contract": {
                "contract_id": contract.contract_id,
                "resource_sha256": stable_digest({"resource": table_name}),
                "schema_digest": contract.schema_digest,
            },
            "full_completion": {"pin": "completion"},
            "full_completion_id": stable_digest({"completion": "base"}),
            "output_receipts": receipts,
            "output_set_digest": stable_digest(receipts),
            "plan": {"pin": "plan"},
            "plan_id": stable_digest({"plan": "base"}),
            "policy_version": "fixture-policy",
            "published_at_utc": "2026-08-02T00:00:00Z",
            "release_availability": release_availability,
            "release_version": 1,
            "row_count": sum(int(item["row_count"]) for item in receipts),
            "source_binding_id": stable_digest({"binding": "base"}),
            "state": "published_hidden_until_release_set",
            "table_name": table_name,
        }
        if mutation == "row_count" and table_name == "universe_daily":
            payload["row_count"] = int(payload["row_count"]) + 1
        member = {**payload, "release_id": stable_digest(payload)}
        content = _canonical_control(member)
        if mutation == "member_no_newline" and table_name == "asset_master":
            content = content.removesuffix(b"\n")
        member_path = root / f"manifests/s7/member-{table_name}.json"
        member_path.parent.mkdir(parents=True, exist_ok=True)
        member_path.write_bytes(content)
        descriptor = {
            "bytes": len(content),
            "path": member_path.relative_to(root).as_posix(),
            "release_id": member["release_id"],
            "sha256": hashlib.sha256(content).hexdigest(),
            "table_name": table_name,
        }
        if mutation == "member_hash" and table_name == "asset_master":
            descriptor["sha256"] = stable_digest({"wrong": "member-hash"})
        descriptors.append(descriptor)
    if mutation == "missing_member":
        descriptors.pop()
    marker_payload: dict[str, object] = {
        "approval": {"pin": "approval"},
        "approval_id": stable_digest({"approval": "base"}),
        "artifact_type": "s7_four_table_atomic_release_set",
        "candidate_id": stable_digest({"candidate": "base"}),
        "candidate_manifest": {"pin": "candidate"},
        "candidate_qa": {"pin": "qa"},
        "full_completion": {"pin": "completion"},
        "full_completion_id": stable_digest({"completion": "base"}),
        "intent": {"pin": "intent"},
        "intent_id": stable_digest({"intent": "base"}),
        "members": descriptors,
        "plan": {"pin": "plan"},
        "plan_id": stable_digest({"plan": "base"}),
        "policy_version": "fixture-policy",
        "published_at_utc": "2026-08-02T00:00:00Z",
        "release_availability": release_availability,
        "release_set_version": 1,
        "source_binding_id": stable_digest({"binding": "base"}),
        "state": "published",
        "table_order": [
            "asset_master",
            "ticker_alias",
            "issuer_master",
            "universe_daily",
        ],
        "visibility_rule": "all_four_members_visible_only_through_this_exact_marker_v1",
    }
    marker = {
        **marker_payload,
        "release_set_id": LEGACY_S7_V1_RELEASE_SET_ID,
    }
    marker_path = root / "manifests/s7/release-set.json"
    marker_content = _canonical_control(marker)
    if mutation == "marker_no_newline":
        marker_content = marker_content.removesuffix(b"\n")
    marker_path.write_bytes(marker_content)
    return SimpleNamespace(
        i0_oracle=SimpleNamespace(
            object_id=LEGACY_S7_V1_RELEASE_SET_ID,
            artifact=_artifact(marker_path, root),
            available_session=available,
        ),
        marker_payload=marker_payload,
    )


def _synthetic_s4_release(
    *,
    mutation: str | None = None,
) -> tuple[SimpleNamespace, SimpleNamespace, SimpleNamespace]:
    sessions = (date(2026, 7, 9), date(2026, 7, 10))
    release_id = stable_digest({"s4": "release"})
    release_pin = ArtifactPin(
        path=(f"manifests/silver/release-sets/assets/release_set_id={release_id}/manifest.json"),
        sha256=stable_digest({"s4": "marker"}),
        bytes=321,
    )
    members = []
    for table_name in S4_TERMINAL_TABLE_ORDER:
        contract = ASSET_CONTRACTS[table_name]
        outputs = [
            SimpleNamespace(
                table=table_name,
                row_count=1,
                schema_digest=contract.schema_digest,
                path=(
                    f"silver/s4/{table_name}/session_year={session.year}/"
                    f"session_date={session.isoformat()}/part-00000.parquet"
                ),
                sha256=stable_digest({"s4-output": f"{table_name}-{session}"}),
                bytes=101,
            )
            for session in sessions
        ]
        if table_name == "universe_source_daily":
            if mutation == "schema":
                outputs[0].schema_digest = stable_digest({"wrong": "schema"})
            elif mutation == "latest":
                outputs[0].path = (
                    "silver/s4/latest/universe_source_daily/"
                    "session_date=2026-07-09/part-00000.parquet"
                )
            elif mutation == "unordered":
                outputs.reverse()
            elif mutation == "duplicate_session":
                outputs[1].path = outputs[0].path
            elif mutation == "missing_session":
                outputs.pop()
        members.append(
            SimpleNamespace(
                table=table_name,
                contract_id=contract.contract_id,
                outputs=tuple(outputs),
            )
        )
    release = SimpleNamespace(release_set_id=release_id, members=tuple(members))
    document = SimpleNamespace(
        path=release_pin.path,
        sha256=release_pin.sha256,
        bytes=release_pin.bytes,
    )
    run_spec = SimpleNamespace(
        s4_v1_source=SimpleNamespace(
            object_id=release_id,
            artifact=release_pin,
            available_session=date(2026, 7, 29),
        ),
        terminal_session=sessions[-1],
    )
    return run_spec, release, document


def _artifact(path: Path, root: Path) -> ArtifactPin:
    content = path.read_bytes()
    return ArtifactPin(
        path=path.relative_to(root).as_posix(),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _write_parquet(
    root: Path,
    relative: str,
    *,
    table_name: str,
    rows: list[dict[str, object]],
    availability_session: date,
    session_date: date | None,
) -> I3MigrationParquetPin:
    contract = (
        S7_DERIVED_CONTRACTS[table_name]
        if table_name in S7_DERIVED_CONTRACTS
        else ASSET_CONTRACTS[table_name]
    )
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=contract.arrow_schema)
    pq.write_table(table, path, compression="zstd", version="2.6")
    return I3MigrationParquetPin(
        table_name=table_name,
        artifact=_artifact(path, root),
        row_count=table.num_rows,
        contract_id=contract.contract_id,
        schema_digest=contract.schema_digest,
        availability_session=availability_session,
        session_date=session_date,
    )


def _s4_source_row(session: date, selected_source_record_id: str) -> dict[str, object]:
    schema = ASSET_CONTRACTS["universe_source_daily"].arrow_schema
    timestamp = datetime(2026, 1, 6, 20, tzinfo=UTC)
    row: dict[str, object] = {}
    for field in schema:
        if field.nullable:
            row[field.name] = None
        elif pa.types.is_string(field.type):
            row[field.name] = "fixture_value"
        elif pa.types.is_boolean(field.type):
            row[field.name] = False
        elif pa.types.is_int64(field.type):
            row[field.name] = 0
        elif pa.types.is_date32(field.type):
            row[field.name] = session
        elif pa.types.is_timestamp(field.type):
            row[field.name] = timestamp
        else:  # pragma: no cover - frozen fixture schema guard
            raise AssertionError(f"unhandled S4 fixture type: {field.type}")
    row.update(
        {
            "session_year": session.year,
            "session_date": session,
            "ticker": "AAPL",
            "active_on_date": True,
            "type_code": "CS",
            "name": "Apple Inc.",
            "market": "stocks",
            "locale": "us",
            "primary_exchange_mic": "XNAS",
            "currency_name": "usd",
            "cik": "0000320193",
            "composite_figi": "BBG000B9XRY4",
            "share_class_figi": "BBG001S5N8V8",
            "identity_link_status": "linked",
            "selected_source_record_id": selected_source_record_id,
            "source_version_count": 1,
            "selection_status": "selected",
            "source_available_session": session,
        }
    )
    return row


def _production_lineage(
    row: dict[str, object],
    *,
    s4_release_set_id: str,
    run_spec,
) -> dict[str, object]:
    result = dict(row)
    result["source_s4_release_set_id"] = s4_release_set_id
    policy = {
        item.registry_kind.value: item for item in run_spec.identity_policy_bundle.registry_releases
    }
    for kind, (release_field, availability_field) in _SOURCE_FIELDS.items():
        result[release_field] = policy[kind].release_id
        result[availability_field] = policy[kind].release_available_session
    return result


def _base_fixture(tmp_path: Path):
    initial_spec = _production_run_spec()
    result, universe, alias, asset, issuer = _legacy_alias_and_masters()
    terminal = result.target_session
    universe = _production_lineage(
        universe,
        s4_release_set_id=initial_spec.s4_v1_source.object_id,
        run_spec=initial_spec,
    )
    alias = _production_lineage(
        alias,
        s4_release_set_id=initial_spec.s4_v1_source.object_id,
        run_spec=initial_spec,
    )
    asset = _production_lineage(
        asset,
        s4_release_set_id=initial_spec.s4_v1_source.object_id,
        run_spec=initial_spec,
    )
    issuer = _production_lineage(
        issuer,
        s4_release_set_id=initial_spec.s4_v1_source.object_id,
        run_spec=initial_spec,
    )

    legacy = I3LegacyV1BasePins(
        release_set_id=LEGACY_S7_V1_RELEASE_SET_ID,
        release_set_artifact=initial_spec.i0_oracle.artifact,
        member_outputs=(
            _write_parquet(
                tmp_path,
                "inputs/s7/data/asset_master.parquet",
                table_name="asset_master",
                rows=[asset],
                availability_session=initial_spec.i0_oracle.available_session,
                session_date=None,
            ),
            _write_parquet(
                tmp_path,
                "inputs/s7/data/ticker_alias.parquet",
                table_name="ticker_alias",
                rows=[alias],
                availability_session=initial_spec.i0_oracle.available_session,
                session_date=None,
            ),
            _write_parquet(
                tmp_path,
                "inputs/s7/data/issuer_master.parquet",
                table_name="issuer_master",
                rows=[issuer],
                availability_session=initial_spec.i0_oracle.available_session,
                session_date=None,
            ),
            _write_parquet(
                tmp_path,
                (
                    "inputs/s7/data/universe_daily/"
                    f"session_date={terminal.isoformat()}/part-00000.parquet"
                ),
                table_name="universe_daily",
                rows=[universe],
                availability_session=initial_spec.i0_oracle.available_session,
                session_date=terminal,
            ),
        ),
    )

    s4_by_table: dict[str, I3MigrationParquetPin] = {}
    for table_name in S4_TERMINAL_TABLE_ORDER:
        rows = (
            [_s4_source_row(terminal, str(universe["selected_source_record_id"]))]
            if table_name == "universe_source_daily"
            else []
        )
        s4_by_table[table_name] = _write_parquet(
            tmp_path,
            (
                f"inputs/s4/{table_name}/session_year={terminal.year}/"
                f"session_date={terminal.isoformat()}/part-00000.parquet"
            ),
            table_name=table_name,
            rows=rows,
            availability_session=initial_spec.s4_v1_source.available_session,
            session_date=terminal,
        )
    s4 = I3S4BasePins(
        release_set_id=initial_spec.s4_v1_source.object_id,
        release_set_artifact=initial_spec.s4_v1_source.artifact,
        universe_source_partitions=(s4_by_table["universe_source_daily"],),
        terminal_partitions=tuple(
            s4_by_table[table_name] for table_name in S4_TERMINAL_TABLE_ORDER
        ),
    )
    terminal_digest = stable_digest(
        {
            "base_release_set_id": s4.release_set_id,
            "partitions": [
                {
                    "artifact": item.artifact.to_dict(),
                    "contract_id": ASSET_CONTRACTS[item.table_name].contract_id,
                    "table": item.table_name,
                }
                for item in s4.terminal_partitions
            ],
            "rule_version": S4_BASE_TERMINAL_PARTITION_SET_RULE_VERSION,
            "terminal_session": terminal.isoformat(),
        }
    )
    frontier = S4BaseFrontier(
        base_release_set_id=s4.release_set_id,
        base_release_set_artifact=s4.release_set_artifact,
        terminal_session=terminal,
        terminal_partition_set_digest=terminal_digest,
        calendar_artifact_id=initial_spec.calendar.calendar_artifact_id,
        reference_binding_id=stable_digest({"fixture": "reference-binding"}),
        contract_ids_by_table={
            table: ASSET_CONTRACTS[table].contract_id for table in S4_TERMINAL_TABLE_ORDER
        },
        schema_digests_by_table={
            table: ASSET_CONTRACTS[table].schema_digest for table in S4_TERMINAL_TABLE_ORDER
        },
        transform_semantics_digest=stable_digest({"fixture": "s4-transform"}),
        parquet_writer_policy={"compression": "zstd", "version": "2.6"},
        release_available_session=initial_spec.s4_v1_source.available_session,
    )
    frontier_path = (
        tmp_path
        / "manifests/silver/incremental/s4/assets/base-frontiers"
        / f"frontier_id={frontier.frontier_id}"
        / "manifest.json"
    )
    frontier_path.parent.mkdir(parents=True, exist_ok=True)
    frontier_path.write_bytes(
        json.dumps(
            frontier.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )
    frontier_pin = _artifact(frontier_path, tmp_path)
    production_frontier = I3ProductionI2BaseFrontierPin(
        terminal_session=terminal,
        frontier_id=frontier.frontier_id,
        artifact=frontier_pin,
        frontier_available_session=frontier.release_available_session,
    )
    run_spec = replace(
        initial_spec,
        terminal_session=terminal,
        i2_base_frontier=production_frontier,
        native_v2_migration_id=production_native_v2_migration_id(
            i0_release_set_artifact=initial_spec.i0_oracle.artifact,
            s4_release_set_artifact=initial_spec.s4_v1_source.artifact,
            identity_policy_bundle=initial_spec.identity_policy_bundle,
            identity_policy_bundle_artifact=(initial_spec.identity_policy_bundle_artifact),
            calendar_artifact=initial_spec.calendar.artifact,
            i2_base_frontier_artifact=frontier_pin,
        ),
    )
    return run_spec, legacy, s4


def test_compact_base_streams_exact_parquet_into_prepared_materialization(
    tmp_path: Path,
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    workspace = tmp_path / "staging/run-001"
    workspace.mkdir(parents=True)

    prepared = prepare_compact_base(
        data_root=tmp_path,
        run_spec=run_spec,
        workspace=workspace,
        legacy=legacy,
        s4=s4,
    )

    assert isinstance(prepared, I3ProductionPreparedMaterialization)
    assert prepared.canonical_projection_difference_count == 0
    assert prepared.checkpoint.last_session == run_spec.terminal_session
    assert prepared.native_manifest.terminal_session == run_spec.terminal_session
    assert tuple(item.table_name for item in prepared.table_outputs) == I3_V2_TABLE_ORDER
    row_versions_by_table = {
        table_name: tuple(item for item in prepared.row_versions if item.table_name == table_name)
        for table_name in I3_V2_TABLE_ORDER[:-1]
    }
    for output in prepared.table_outputs[:-1]:
        assert output.storage is I3ProductionOutputStorage.ROWSET_INDEX
        assert output.rowset_index is not None
        assert len(output.rowset_index.segments) == 1
        segment = output.rowset_index.segments[0]
        assert segment.segment_id == production_compact_base_initial_segment_id(
            table_name=output.table_name,
            artifact=segment.artifact,
            terminal_session=run_spec.terminal_session,
            availability_session=run_spec.run_available_session,
            native_v2_migration_id=run_spec.native_v2_migration_id,
        )
        assert segment.availability_session == run_spec.run_available_session
        assert output.manifest_output.artifact.path.endswith("/index.json")
        assert segment.artifact.path.endswith("/base.parquet")
        assert output.manifest_output.artifact != segment.artifact
        assert (
            I3ProductionRowsetIndex.from_dict(
                json.loads((tmp_path / output.manifest_output.artifact.path).read_bytes())
            )
            == output.rowset_index
        )
        assert row_versions_by_table[output.table_name]
        assert all(
            item.index_artifact == segment.artifact
            for item in row_versions_by_table[output.table_name]
        )
    universe_output = prepared.table_outputs[I3_V2_TABLE_ORDER.index("universe_daily")]
    assert universe_output.storage is I3ProductionOutputStorage.DATASET_INDEX
    assert universe_output.dataset_index is not None
    assert tuple(item.session_date for item in universe_output.dataset_index.partitions) == (
        run_spec.terminal_session,
    )
    assert len(prepared.row_versions) == 3
    assert all(
        item.operation is RowVersionOperation.NEW_ROOT
        and item.predecessor_row_version_id is None
        and item.predecessor_payload_digest is None
        and item.row_locator == "row_index=0"
        for item in prepared.row_versions
    )
    assert prepared.resource_observation.minimum_disk_free_bytes >= 40 * 1024**3
    assert prepared.resource_observation.peak_rss_bytes > 0
    assert not (workspace / "universe_daily/fk-reference-index.json").exists()
    for path in workspace.rglob("*"):
        if path.is_file():
            assert stat.S_IMODE(path.stat().st_mode) & 0o222 == 0


def test_compact_base_mints_sealed_authority_output_and_projection_attestation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    workspace = tmp_path / "staging/sealed-base"
    workspace.mkdir(parents=True)
    prepared = prepare_compact_base(
        data_root=tmp_path,
        run_spec=run_spec,
        workspace=workspace,
        legacy=legacy,
        s4=s4,
    )
    binding = I3CompactBaseInputBinding(legacy=legacy, s4=s4)

    assert type(prepared) is CompactBasePreparedMaterialization
    attestation = prepared.base_materialization_attestation
    assert type(attestation) is CompactBaseMaterializationAttestation
    assert attestation.run_spec_id == run_spec.run_spec_id
    assert attestation.input_binding_id == binding.input_binding_id
    assert attestation.source_digest == prepared.source_digest
    assert attestation.native_manifest_id == prepared.native_manifest.release_id
    assert attestation.native_manifest_artifact == prepared.native_manifest_artifact
    assert attestation.checkpoint_id == prepared.checkpoint.checkpoint_id
    assert attestation.checkpoint_artifact == prepared.checkpoint_artifact
    assert attestation.canonical_projection_difference_count == 0
    assert attestation.attestation_id == stable_digest(attestation.logical_payload())
    assert (
        migration_io._verify_compact_base_materialization_attestation_with_binding(
            run_spec=run_spec,
            prepared=prepared,
            binding=binding,
        )
        is attestation
    )

    calls = 0

    def exact_loader(*, data_root: Path, run_spec) -> I3CompactBaseInputBinding:
        nonlocal calls
        calls += 1
        assert data_root == tmp_path
        assert run_spec is not None and run_spec.run_spec_id == attestation.run_spec_id
        return binding

    monkeypatch.setattr(migration_io, "load_compact_base_input_binding", exact_loader)
    assert (
        verify_compact_base_materialization_attestation(
            data_root=tmp_path,
            run_spec=run_spec,
            prepared=prepared,
        )
        is attestation
    )
    assert calls == 1


def test_base_attestation_rejects_structural_self_consistent_materializer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    workspace = tmp_path / "staging/malicious-structural"
    workspace.mkdir(parents=True)
    official = prepare_compact_base(
        data_root=tmp_path,
        run_spec=run_spec,
        workspace=workspace,
        legacy=legacy,
        s4=s4,
    )
    binding = I3CompactBaseInputBinding(legacy=legacy, s4=s4)

    class SelfConsistentMaterializer:
        def prepare(self) -> I3ProductionPreparedMaterialization:
            # All ordinary fields and physical pins are copied consistently;
            # only the migration module's nominal sealed capability is absent.
            return I3ProductionPreparedMaterialization(
                table_outputs=official.table_outputs,
                native_manifest=official.native_manifest,
                native_manifest_artifact=official.native_manifest_artifact,
                checkpoint=official.checkpoint,
                checkpoint_artifact=official.checkpoint_artifact,
                source_digest=official.source_digest,
                resource_observation=official.resource_observation,
                canonical_projection_difference_count=0,
                row_versions=official.row_versions,
            )

    malicious = SelfConsistentMaterializer().prepare()
    with pytest.raises(I3MigrationIOError, match="official nominal prepared capability"):
        migration_io._verify_compact_base_materialization_attestation_with_binding(
            run_spec=run_spec,
            prepared=malicious,
            binding=binding,
        )

    def unexpected_loader(**_kwargs):
        pytest.fail("structural BASE candidate reached exact input loading")

    monkeypatch.setattr(migration_io, "load_compact_base_input_binding", unexpected_loader)
    with pytest.raises(I3MigrationIOError, match="official nominal prepared capability"):
        verify_compact_base_materialization_attestation(
            data_root=tmp_path,
            run_spec=run_spec,
            prepared=malicious,
        )


def test_base_attestation_rejects_copied_seal_and_mutated_prepared_bindings(
    tmp_path: Path,
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    workspace = tmp_path / "staging/malicious-copy"
    workspace.mkdir(parents=True)
    prepared = prepare_compact_base(
        data_root=tmp_path,
        run_spec=run_spec,
        workspace=workspace,
        legacy=legacy,
        s4=s4,
    )
    binding = I3CompactBaseInputBinding(legacy=legacy, s4=s4)
    changed_source = stable_digest({"malicious": "self-consistent-source"})

    changed = replace(prepared, source_digest=changed_source)
    with pytest.raises(I3MigrationIOError, match="source differs"):
        migration_io._verify_compact_base_materialization_attestation_with_binding(
            run_spec=run_spec,
            prepared=changed,
            binding=binding,
        )

    copied_attestation = replace(
        prepared.base_materialization_attestation,
        source_digest=changed_source,
    )
    forged = replace(
        prepared,
        source_digest=changed_source,
        base_materialization_attestation=copied_attestation,
    )
    with pytest.raises(I3MigrationIOError, match="not module-sealed"):
        migration_io._verify_compact_base_materialization_attestation_with_binding(
            run_spec=run_spec,
            prepared=forged,
            binding=binding,
        )

    changed_checkpoint_pin = replace(
        prepared,
        checkpoint_artifact=replace(
            prepared.checkpoint_artifact,
            sha256=stable_digest({"malicious": "checkpoint-pin"}),
        ),
    )
    with pytest.raises(I3MigrationIOError, match="differs from exact inputs, outputs"):
        migration_io._verify_compact_base_materialization_attestation_with_binding(
            run_spec=run_spec,
            prepared=changed_checkpoint_pin,
            binding=binding,
        )

    forged_low_resource_observation = replace(
        prepared,
        resource_observation=replace(
            prepared.resource_observation,
            peak_rss_bytes=0,
            elapsed_seconds=0,
            minimum_disk_free_bytes=10 * 1024**4,
        ),
    )
    with pytest.raises(I3MigrationIOError, match="outputs, or observation"):
        migration_io._verify_compact_base_materialization_attestation_with_binding(
            run_spec=run_spec,
            prepared=forged_low_resource_observation,
            binding=binding,
        )

    changed_outputs = replace(prepared, table_outputs=tuple(reversed(prepared.table_outputs)))
    with pytest.raises(I3MigrationIOError, match="output order differs"):
        migration_io._verify_compact_base_materialization_attestation_with_binding(
            run_spec=run_spec,
            prepared=changed_outputs,
            binding=binding,
        )

    original_output = prepared.table_outputs[0]
    assert original_output.rowset_index is not None
    original_segment = original_output.rowset_index.segments[0]
    tampered_segment = replace(
        original_segment,
        segment_id=stable_digest({"malicious": "initial-segment-id"}),
    )
    tampered_rowset = replace(
        original_output.rowset_index,
        segments=(tampered_segment,),
    )
    tampered_output = replace(
        original_output,
        manifest_output=replace(
            original_output.manifest_output,
            artifact=tampered_rowset.exact_pin(path=original_output.manifest_output.artifact.path),
        ),
        rowset_index=tampered_rowset,
    )
    tampered_outputs = (tampered_output, *prepared.table_outputs[1:])
    with pytest.raises(I3MigrationIOError, match="differs from exact inputs, outputs"):
        migration_io._verify_compact_base_materialization_attestation_with_binding(
            run_spec=run_spec,
            prepared=replace(prepared, table_outputs=tampered_outputs),
            binding=binding,
        )


def test_compact_base_is_no_clobber_and_rejects_non_terminal_base(
    tmp_path: Path,
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    workspace = tmp_path / "staging/run-002"
    workspace.mkdir(parents=True)
    materializer = CompactBaseMigrationMaterializer(legacy, s4)
    first = materializer.prepare(
        data_root=tmp_path,
        run_spec=run_spec,
        parent=None,
        workspace=workspace,
    )
    assert first.source_digest
    with pytest.raises(I3MigrationIOError, match="not empty"):
        materializer.prepare(
            data_root=tmp_path,
            run_spec=run_spec,
            parent=None,
            workspace=workspace,
        )

    later = replace(
        run_spec,
        terminal_session=date(2026, 1, 7),
        i2_base_frontier=replace(
            run_spec.i2_base_frontier,
            terminal_session=date(2026, 1, 7),
        ),
    )
    empty_workspace = tmp_path / "staging/run-003"
    empty_workspace.mkdir(parents=True)
    with pytest.raises(I3MigrationIOError, match="exact v1 terminal"):
        prepare_compact_base(
            data_root=tmp_path,
            run_spec=later,
            workspace=empty_workspace,
            legacy=legacy,
            s4=s4,
        )


def test_resource_estimate_enforces_full_base_floor(tmp_path: Path) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    estimate = estimate_compact_base_resources(run_spec, legacy=legacy, s4=s4)
    assert estimate.minimum_free_disk_bytes_required == (
        40 * 1024**3 + estimate.estimated_output_bytes + estimate.estimated_temporary_bytes
    )
    assert estimate.estimated_peak_rss_bytes == 2 * 1024**3
    assert estimate.estimated_output_bytes > estimate.source_bytes


def test_base_preflight_reserves_output_and_temporary_peak_before_parquet_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    workspace = tmp_path / "staging/preflight-disk"
    workspace.mkdir(parents=True)
    estimate = estimate_compact_base_resources(run_spec, legacy=legacy, s4=s4)
    planned = replace(
        estimate,
        estimated_output_bytes=2 * 1024**3,
        estimated_temporary_bytes=1024**3,
        minimum_free_disk_bytes_required=43 * 1024**3,
    )
    reads: list[str] = []
    monkeypatch.setattr(
        migration_io,
        "estimate_compact_base_resources",
        lambda _spec, *, legacy, s4: planned,
    )
    monkeypatch.setattr(
        migration_io.shutil,
        "disk_usage",
        lambda _root: SimpleNamespace(free=41 * 1024**3),
    )
    monkeypatch.setattr(migration_io, "_peak_rss_bytes", lambda: 1)
    monkeypatch.setattr(
        migration_io,
        "_verify_source_parquet",
        lambda _root, pin: reads.append(pin.artifact.path),
    )

    with pytest.raises(I3MigrationIOError, match="estimated output and temporary peak"):
        prepare_compact_base(
            data_root=tmp_path,
            run_spec=run_spec,
            workspace=workspace,
            legacy=legacy,
            s4=s4,
        )
    assert reads == []
    assert list(workspace.iterdir()) == []


def test_base_preflight_accepts_space_at_the_conservative_required_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    estimate = estimate_compact_base_resources(run_spec, legacy=legacy, s4=s4)
    monkeypatch.setattr(
        migration_io.shutil,
        "disk_usage",
        lambda _root: SimpleNamespace(free=estimate.minimum_free_disk_bytes_required),
    )
    monkeypatch.setattr(migration_io, "_peak_rss_bytes", lambda: 1)
    assert (
        migration_io._check_preflight_resources(tmp_path, run_spec, estimate)
        == estimate.minimum_free_disk_bytes_required
    )


def test_source_digest_binds_complete_v1_and_s4_partition_pin_sets(tmp_path: Path) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    original = migration_io._compact_base_source_digest(
        run_spec,
        legacy=legacy,
        s4=s4,
    )
    legacy_items = list(legacy.member_outputs)
    legacy_items[-1] = replace(
        legacy_items[-1],
        artifact=replace(
            legacy_items[-1].artifact,
            sha256=stable_digest({"tampered": "legacy-partition"}),
        ),
    )
    changed_legacy = replace(legacy, member_outputs=tuple(legacy_items))
    assert original != migration_io._compact_base_source_digest(
        run_spec,
        legacy=changed_legacy,
        s4=s4,
    )

    changed_s4_universe = replace(
        s4.universe_source_partitions[-1],
        artifact=replace(
            s4.universe_source_partitions[-1].artifact,
            sha256=stable_digest({"tampered": "s4-partition"}),
        ),
    )
    changed_s4 = replace(
        s4,
        universe_source_partitions=(changed_s4_universe,),
        terminal_partitions=(*s4.terminal_partitions[:-1], changed_s4_universe),
    )
    assert original != migration_io._compact_base_source_digest(
        run_spec,
        legacy=legacy,
        s4=changed_s4,
    )


def test_exact_manifest_expanders_build_complete_sorted_pin_sets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy_spec = _synthetic_s7_release(tmp_path)
    real_digest = migration_io.stable_digest

    def release_digest(value: object) -> str:
        if value == legacy_spec.marker_payload:
            return LEGACY_S7_V1_RELEASE_SET_ID
        return real_digest(value)

    monkeypatch.setattr(migration_io, "stable_digest", release_digest)
    legacy = migration_io._load_legacy_release_member_pins(tmp_path, legacy_spec)
    assert len(legacy.member_outputs) == 5
    assert tuple(item.session_date for item in legacy.pins_for("universe_daily")) == (
        date(2026, 7, 9),
        date(2026, 7, 10),
    )

    s4_spec, release, document = _synthetic_s4_release()
    monkeypatch.setattr(migration_io, "AssetReleaseSet", SimpleNamespace)
    monkeypatch.setattr(
        migration_io,
        "load_exact_asset_release_set_control",
        lambda *args, **kwargs: (release, document),
    )
    s4 = migration_io._load_s4_release_member_pins(tmp_path, s4_spec)
    assert tuple(item.session_date for item in s4.universe_source_partitions) == (
        date(2026, 7, 9),
        date(2026, 7, 10),
    )
    assert tuple(item.table_name for item in s4.terminal_partitions) == (S4_TERMINAL_TABLE_ORDER)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("member_hash", "exact artifact differs"),
        ("row_count", "row count differs"),
        ("schema", "schema differs"),
        ("latest", "explicit"),
        ("glob", "explicit"),
        ("unordered", "sorted and session-unique"),
        ("duplicate_session", "session-unique"),
        ("missing_member", "member order differs"),
        ("marker_no_newline", "not canonical"),
        ("member_no_newline", "not canonical"),
    ),
)
def test_s7_manifest_expansion_rejects_tampered_or_ambiguous_members(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    run_spec = _synthetic_s7_release(tmp_path, mutation=mutation)
    real_digest = migration_io.stable_digest

    def release_digest(value: object) -> str:
        if value == run_spec.marker_payload:
            return LEGACY_S7_V1_RELEASE_SET_ID
        return real_digest(value)

    monkeypatch.setattr(migration_io, "stable_digest", release_digest)
    with pytest.raises(I3MigrationIOError, match=message):
        migration_io._load_legacy_release_member_pins(tmp_path, run_spec)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("schema", "output differs"),
        ("latest", "explicit"),
        ("unordered", "repeat or are unordered"),
        ("duplicate_session", "repeat or are unordered"),
        ("missing_session", "session ranges differ"),
    ),
)
def test_s4_manifest_expansion_rejects_schema_path_and_session_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    run_spec, release, document = _synthetic_s4_release(mutation=mutation)
    monkeypatch.setattr(migration_io, "AssetReleaseSet", SimpleNamespace)
    monkeypatch.setattr(
        migration_io,
        "load_exact_asset_release_set_control",
        lambda *args, **kwargs: (release, document),
    )
    with pytest.raises(I3MigrationIOError, match=message):
        migration_io._load_s4_release_member_pins(tmp_path, run_spec)


@pytest.mark.parametrize("unsafe", ("latest", "part-*.parquet", "part-[0].parquet"))
def test_explicit_pin_rejects_latest_and_glob_paths(unsafe: str) -> None:
    contract = S7_DERIVED_CONTRACTS["universe_daily"]
    with pytest.raises(I3MigrationIOError, match="explicit"):
        I3MigrationParquetPin(
            table_name="universe_daily",
            artifact=ArtifactPin(
                path=(f"silver/{unsafe}/universe_daily/session_date=2026-07-09/part-00000.parquet"),
                sha256=stable_digest({"unsafe": unsafe}),
                bytes=1,
            ),
            row_count=1,
            contract_id=contract.contract_id,
            schema_digest=contract.schema_digest,
            availability_session=date(2026, 7, 29),
            session_date=date(2026, 7, 9),
        )


def test_public_input_loader_never_enumerates_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    monkeypatch.setattr(
        migration_io,
        "_load_legacy_release_member_pins",
        lambda root, spec: legacy,
    )
    monkeypatch.setattr(
        migration_io,
        "_load_s4_release_member_pins",
        lambda root, spec: s4,
    )
    monkeypatch.setattr(
        migration_io,
        "_load_exact_base_frontier",
        lambda root, spec, *, s4: object(),
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("directory enumeration is forbidden")

    monkeypatch.setattr(Path, "glob", forbidden)
    monkeypatch.setattr(Path, "rglob", forbidden)
    monkeypatch.setattr(Path, "iterdir", forbidden)
    binding = load_compact_base_input_binding(data_root=tmp_path, run_spec=run_spec)
    assert binding == I3CompactBaseInputBinding(legacy=legacy, s4=s4)


def test_real_scale_resource_estimate_does_not_count_s4_reads_as_outputs() -> None:
    run_spec = _production_run_spec()
    terminal = run_spec.terminal_session
    partition_count = 2_513
    sessions = tuple(
        terminal - timedelta(days=partition_count - index - 1) for index in range(partition_count)
    )
    legacy_total = 8_689_015_118
    s4_universe_total = 7_661_290_322
    small_bytes = (12_000_000, 24_000_000, 12_000_000)
    universe_total = legacy_total - sum(small_bytes)
    universe_bytes, universe_remainder = divmod(universe_total, partition_count)
    s4_bytes, s4_remainder = divmod(s4_universe_total, partition_count)

    legacy_outputs: list[I3MigrationParquetPin] = []
    for table_name, size in zip(
        ("asset_master", "ticker_alias", "issuer_master"),
        small_bytes,
        strict=True,
    ):
        contract = S7_DERIVED_CONTRACTS[table_name]
        legacy_outputs.append(
            I3MigrationParquetPin(
                table_name=table_name,
                artifact=ArtifactPin(
                    path=f"silver/scale/data/{table_name}.parquet",
                    sha256=stable_digest({"scale": table_name}),
                    bytes=size,
                ),
                row_count=1,
                contract_id=contract.contract_id,
                schema_digest=contract.schema_digest,
                availability_session=run_spec.i0_oracle.available_session,
            )
        )
    universe_contract = S7_DERIVED_CONTRACTS["universe_daily"]
    for index, session in enumerate(sessions):
        legacy_outputs.append(
            I3MigrationParquetPin(
                table_name="universe_daily",
                artifact=ArtifactPin(
                    path=(
                        "silver/scale/data/universe_daily/"
                        f"session_date={session.isoformat()}/part-00000.parquet"
                    ),
                    sha256=stable_digest({"legacy-scale": index}),
                    bytes=universe_bytes + (index < universe_remainder),
                ),
                row_count=1,
                contract_id=universe_contract.contract_id,
                schema_digest=universe_contract.schema_digest,
                availability_session=run_spec.i0_oracle.available_session,
                session_date=session,
            )
        )
    legacy = I3LegacyV1BasePins(
        release_set_id=LEGACY_S7_V1_RELEASE_SET_ID,
        release_set_artifact=run_spec.i0_oracle.artifact,
        member_outputs=tuple(legacy_outputs),
    )

    s4_contract = ASSET_CONTRACTS["universe_source_daily"]
    s4_universe = tuple(
        I3MigrationParquetPin(
            table_name="universe_source_daily",
            artifact=ArtifactPin(
                path=(
                    "silver/scale/s4/universe_source_daily/"
                    f"session_date={session.isoformat()}/part-00000.parquet"
                ),
                sha256=stable_digest({"s4-scale": index}),
                bytes=s4_bytes + (index < s4_remainder),
            ),
            row_count=1,
            contract_id=s4_contract.contract_id,
            schema_digest=s4_contract.schema_digest,
            availability_session=run_spec.s4_v1_source.available_session,
            session_date=session,
        )
        for index, session in enumerate(sessions)
    )
    other_terminal = []
    for table_name in S4_TERMINAL_TABLE_ORDER[:2]:
        contract = ASSET_CONTRACTS[table_name]
        other_terminal.append(
            I3MigrationParquetPin(
                table_name=table_name,
                artifact=ArtifactPin(
                    path=(
                        f"silver/scale/s4/{table_name}/"
                        f"session_date={terminal.isoformat()}/part-00000.parquet"
                    ),
                    sha256=stable_digest({"s4-terminal": table_name}),
                    bytes=1_000_000,
                ),
                row_count=1,
                contract_id=contract.contract_id,
                schema_digest=contract.schema_digest,
                availability_session=run_spec.s4_v1_source.available_session,
                session_date=terminal,
            )
        )
    s4 = I3S4BasePins(
        release_set_id=run_spec.s4_v1_source.object_id,
        release_set_artifact=run_spec.s4_v1_source.artifact,
        universe_source_partitions=s4_universe,
        terminal_partitions=(*other_terminal, s4_universe[-1]),
    )
    estimate = estimate_compact_base_resources(run_spec, legacy=legacy, s4=s4)
    assert sum(item.artifact.bytes for item in legacy.member_outputs) == legacy_total
    assert sum(item.artifact.bytes for item in s4_universe) == s4_universe_total
    assert estimate.source_bytes > 16_000_000_000
    assert estimate.estimated_output_bytes < run_spec.resource_caps.output_bytes_hard_cap
    assert estimate.estimated_peak_rss_bytes < run_spec.resource_caps.rss_bytes_hard_cap
