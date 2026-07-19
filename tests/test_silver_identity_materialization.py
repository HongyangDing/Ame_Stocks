from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.identity_materialization import (
    S7IdentityMaterializationError,
    S7MaterializationRegistryPin,
    S7MaterializationSourceBinding,
    create_s7_materialization_plan,
    materialize_s7_identity_candidate,
    resolved_graph_digest,
    store_s7_materialization_source_binding,
    verify_s7_materialization_inputs,
)
from ame_stocks_api.silver.identity_relation_registries import (
    CompositeRegistryMatch,
    evaluate_composite_registry_collisions,
)
from ame_stocks_api.silver.identity_resolution_contract import S7_DERIVED_CONTRACTS
from ame_stocks_api.silver.identity_source import S7_S4_RELEASE_SET_ID, S7_SOURCE_PINS

_TABLES = ("asset_master", "ticker_alias", "issuer_master", "universe_daily")
_REGISTRIES = (
    "identity_adjudication",
    "identity_cross_market_adjudication",
    "provider_composite_override",
    "share_class_adjudication",
    "asset_transition",
)
_CUTOFF = date(2026, 7, 20)
_ASSET_ID = stable_digest({"fixture": "asset"})
_ALIAS_ID = stable_digest({"fixture": "alias"})
_ISSUER_ID = stable_digest({"fixture": "issuer"})
_SOURCE_RECORD_ID = stable_digest({"fixture": "source-row"})
_MANIFEST_ID = stable_digest({"fixture": "manifest"})


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )


def _default_value(field: pa.Field) -> object:
    if field.nullable:
        return None
    if pa.types.is_string(field.type):
        return "fixture"
    if pa.types.is_date32(field.type):
        return _CUTOFF
    if pa.types.is_boolean(field.type):
        return False
    if pa.types.is_int64(field.type):
        return 0
    if pa.types.is_list(field.type):
        return []
    raise AssertionError(f"fixture has no default for {field}")


def _row(table: str, overrides: Mapping[str, object], pins: Mapping[str, object]) -> dict:
    contract = S7_DERIVED_CONTRACTS[table]
    row = {field.name: _default_value(field) for field in contract.arrow_schema}
    row.update(
        {
            "identity_resolution_cutoff_session": _CUTOFF,
            "source_s4_release_set_id": S7_S4_RELEASE_SET_ID,
            "source_s5_status_release_id": S7_SOURCE_PINS["ticker_event_request_status"].release_id,
            "source_s5_event_release_id": S7_SOURCE_PINS["ticker_change_event"].release_id,
            "source_s6_overview_release_id": S7_SOURCE_PINS["ticker_overview_safe"].release_id,
            "source_identity_adjudication_release_id": pins["identity_adjudication"].release_id,
            "source_identity_adjudication_release_available_session": _CUTOFF,
            "source_identity_cross_market_adjudication_release_id": pins[
                "identity_cross_market_adjudication"
            ].release_id,
            "source_identity_cross_market_adjudication_release_available_session": _CUTOFF,
            "source_provider_composite_override_release_id": pins[
                "provider_composite_override"
            ].release_id,
            "source_provider_composite_override_release_available_session": _CUTOFF,
            "source_share_class_adjudication_release_id": pins[
                "share_class_adjudication"
            ].release_id,
            "source_share_class_adjudication_release_available_session": _CUTOFF,
            "source_asset_transition_release_id": pins["asset_transition"].release_id,
            "source_asset_transition_release_available_session": _CUTOFF,
        }
    )
    row.update(overrides)
    assert tuple(row) == tuple(column.name for column in contract.columns)
    return row


def _registry_pins(root: Path) -> tuple[S7MaterializationRegistryPin, ...]:
    result = []
    for name in _REGISTRIES:
        relative = f"manifests/test/{name}.json"
        content = _canonical_bytes({"registry_name": name, "release": "fixture"})
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_bytes(content)
        result.append(
            S7MaterializationRegistryPin(
                registry_name=name,
                release_id=stable_digest({"fixture_registry": name}),
                manifest_path=relative,
                manifest_sha256=hashlib.sha256(content).hexdigest(),
                manifest_bytes=len(content),
                release_available_session=_CUTOFF,
            )
        )
    return tuple(result)


def _rows(pins: tuple[S7MaterializationRegistryPin, ...]) -> dict[str, list[dict]]:
    by_name = {pin.registry_name: pin for pin in pins}
    asset = _row(
        "asset_master",
        {
            "asset_id": _ASSET_ID,
            "canonical_composite_figi": "BBG000000001",
            "backtest_identity_eligible": True,
            "predecessor_asset_ids": [],
            "successor_asset_ids": [],
        },
        by_name,
    )
    alias = _row(
        "ticker_alias",
        {
            "ticker_alias_id": _ALIAS_ID,
            "asset_id": _ASSET_ID,
            "ticker": "AAA",
            "valid_from_session": _CUTOFF,
            "valid_through_session": _CUTOFF,
            "observed_composite_figi": "BBG000000001",
            "observed_asset_id": _ASSET_ID,
            "canonical_composite_figi": "BBG000000001",
            "issuer_id": _ISSUER_ID,
            "first_source_record_id": _SOURCE_RECORD_ID,
            "last_source_record_id": _SOURCE_RECORD_ID,
            "backtest_identity_eligible": True,
            "asset_transition_ids": [],
            "composite_registry_match_count": 0,
            "composite_registry_collision": False,
        },
        by_name,
    )
    issuer = _row(
        "issuer_master",
        {
            "issuer_id": _ISSUER_ID,
            "cik_normalized": "0000000001",
            "first_observed_session": _CUTOFF,
            "last_observed_session": _CUTOFF,
        },
        by_name,
    )
    universe = _row(
        "universe_daily",
        {
            "session_year": 2026,
            "session_date": _CUTOFF,
            "ticker": "AAA",
            "active_on_date": True,
            "asset_id": _ASSET_ID,
            "issuer_id": _ISSUER_ID,
            "ticker_alias_id": _ALIAS_ID,
            "observed_composite_figi": "BBG000000001",
            "observed_asset_id": _ASSET_ID,
            "canonical_composite_figi": "BBG000000001",
            "identity_resolution_status": "resolved_direct",
            "backtest_identity_eligible": True,
            "selected_source_record_id": _SOURCE_RECORD_ID,
            "asset_transition_ids": [],
            "composite_registry_match_count": 0,
            "composite_registry_collision": False,
        },
        by_name,
    )
    return {
        "asset_master": [asset],
        "ticker_alias": [alias],
        "issuer_master": [issuer],
        "universe_daily": [universe],
    }


def _controls(root: Path, rows: Mapping[str, list[dict]]):
    pins = _registry_pins(root)
    binding = S7MaterializationSourceBinding(
        identity_resolution_cutoff_session=_CUTOFF,
        resolved_graph_digest=resolved_graph_digest(rows),
        table_row_counts={table: len(rows[table]) for table in _TABLES},
        registry_pins=pins,
    )
    path, checksum, size = store_s7_materialization_source_binding(root, binding)
    assert path == binding.relative_path
    plan = create_s7_materialization_plan(
        binding,
        source_binding_sha256=checksum,
        source_binding_bytes=size,
    )
    return binding, plan


def test_four_tables_materialize_atomically_and_idempotently(tmp_path: Path) -> None:
    pins = _registry_pins(tmp_path)
    rows = _rows(pins)
    _, plan = _controls(tmp_path, rows)
    evaluations = {_SOURCE_RECORD_ID: evaluate_composite_registry_collisions(())}
    graph = verify_s7_materialization_inputs(
        tmp_path,
        plan=plan,
        rows_by_table=rows,
        composite_evaluations_by_source_record_id=evaluations,
    )

    run = materialize_s7_identity_candidate(tmp_path, plan=plan, graph=graph)

    assert run.state == "awaiting_review"
    assert run.idempotent is False
    assert dict(run.table_rows) == {table: 1 for table in _TABLES}
    candidate = tmp_path / run.candidate_path
    manifest = json.loads((candidate / "manifest.json").read_bytes())
    assert manifest["capabilities"] == {
        "full_run_authorized": False,
        "publish_authorized": False,
        "registry_mutation_authorized": False,
    }
    for table in _TABLES:
        assert pq.read_table(candidate / f"data/{table}.parquet").schema == (
            S7_DERIVED_CONTRACTS[table].arrow_schema
        )

    repeated = materialize_s7_identity_candidate(tmp_path, plan=plan, graph=graph)
    assert repeated.candidate_id == run.candidate_id
    assert repeated.manifest_sha256 == run.manifest_sha256
    assert repeated.idempotent is True


def test_raw_composite_collision_is_retained_only_as_ineligible_membership(
    tmp_path: Path,
) -> None:
    pins = _registry_pins(tmp_path)
    rows = _rows(pins)
    first = stable_digest({"decision": 1})
    second = stable_digest({"decision": 2})
    universe = rows["universe_daily"][0]
    universe.update(
        {
            "asset_id": None,
            "share_class_id": None,
            "issuer_id": None,
            "ticker_alias_id": None,
            "canonical_composite_figi": None,
            "canonical_composite_market_code": None,
            "identity_resolution_status": "unresolved_registry_collision",
            "identity_adjudication_id": first,
            "adjudication_available_session": _CUTOFF,
            "provider_composite_override_id": second,
            "provider_composite_override_available_session": _CUTOFF,
            "backtest_identity_eligible": False,
            "composite_registry_match_count": 2,
            "composite_registry_collision": True,
        }
    )
    matches = (
        CompositeRegistryMatch(
            registry_name="identity_adjudication",
            decision_id=first,
            source_record_id=_SOURCE_RECORD_ID,
            observed_composite_figi="BBG000000001",
            canonical_composite_figi="BBG000000002",
        ),
        CompositeRegistryMatch(
            registry_name="provider_composite_override",
            decision_id=second,
            source_record_id=_SOURCE_RECORD_ID,
            observed_composite_figi="BBG000000001",
            canonical_composite_figi="BBG000000003",
        ),
    )
    _, plan = _controls(tmp_path, rows)
    graph = verify_s7_materialization_inputs(
        tmp_path,
        plan=plan,
        rows_by_table=rows,
        composite_evaluations_by_source_record_id={
            _SOURCE_RECORD_ID: evaluate_composite_registry_collisions(matches)
        },
    )

    run = materialize_s7_identity_candidate(tmp_path, plan=plan, graph=graph)

    assert run.raw_collision_rows == 1
    qa = json.loads((tmp_path / run.candidate_path / "qa/qa.json").read_bytes())
    assert qa["multi_registry_composite_override_collision_rows"] == 1
    assert qa["multi_registry_composite_override_collision_eligible_rows"] == 0
    assert qa["multi_registry_composite_override_collision_resolved_rows"] == 0
    assert qa["multi_registry_composite_override_collision_alias_rows"] == 0


def test_collision_cannot_be_emitted_as_backtest_eligible(tmp_path: Path) -> None:
    pins = _registry_pins(tmp_path)
    rows = _rows(pins)
    row = rows["universe_daily"][0]
    row.update(
        {
            "composite_registry_match_count": 2,
            "composite_registry_collision": True,
            "identity_resolution_status": "unresolved_registry_collision",
        }
    )
    _, plan = _controls(tmp_path, rows)
    graph = verify_s7_materialization_inputs(tmp_path, plan=plan, rows_by_table=rows)

    with pytest.raises(S7IdentityMaterializationError, match="did not fail closed"):
        materialize_s7_identity_candidate(tmp_path, plan=plan, graph=graph)


def test_registry_manifest_tampering_breaks_source_binding(tmp_path: Path) -> None:
    pins = _registry_pins(tmp_path)
    rows = _rows(pins)
    _, plan = _controls(tmp_path, rows)
    (tmp_path / pins[0].manifest_path).write_bytes(b"tampered\n")

    with pytest.raises(S7IdentityMaterializationError, match="immutable input changed"):
        verify_s7_materialization_inputs(tmp_path, plan=plan, rows_by_table=rows)


def test_existing_candidate_qa_tampering_fails_idempotent_replay(tmp_path: Path) -> None:
    pins = _registry_pins(tmp_path)
    rows = _rows(pins)
    _, plan = _controls(tmp_path, rows)
    graph = verify_s7_materialization_inputs(tmp_path, plan=plan, rows_by_table=rows)
    run = materialize_s7_identity_candidate(tmp_path, plan=plan, graph=graph)
    qa_path = tmp_path / run.candidate_path / "qa/qa.json"
    qa_path.write_bytes(b"{}\n")

    with pytest.raises(S7IdentityMaterializationError, match="candidate QA changed"):
        materialize_s7_identity_candidate(tmp_path, plan=plan, graph=graph)


@pytest.mark.parametrize(
    ("table", "column", "expected_message"),
    (
        (
            "issuer_master",
            "source_identity_adjudication_release_id",
            "identity_adjudication release binding changed",
        ),
        (
            "asset_master",
            "source_s6_overview_release_id",
            "ticker_overview_safe release binding changed",
        ),
    ),
)
def test_every_registry_and_fixed_s5_s6_release_is_row_bound(
    tmp_path: Path,
    table: str,
    column: str,
    expected_message: str,
) -> None:
    pins = _registry_pins(tmp_path)
    rows = _rows(pins)
    rows[table][0][column] = stable_digest({"tampered_column": column})
    _, plan = _controls(tmp_path, rows)

    with pytest.raises(S7IdentityMaterializationError, match=expected_message):
        verify_s7_materialization_inputs(tmp_path, plan=plan, rows_by_table=rows)


def test_partial_staging_never_exposes_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_materialization as module

    pins = _registry_pins(tmp_path)
    rows = _rows(pins)
    _, plan = _controls(tmp_path, rows)
    graph = verify_s7_materialization_inputs(tmp_path, plan=plan, rows_by_table=rows)
    original = module._write_parquet_exclusive
    calls = 0

    def interrupted(path: Path, table: pa.Table) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise S7IdentityMaterializationError("fixture interruption")
        original(path, table)

    monkeypatch.setattr(module, "_write_parquet_exclusive", interrupted)
    with pytest.raises(S7IdentityMaterializationError, match="fixture interruption"):
        materialize_s7_identity_candidate(tmp_path, plan=plan, graph=graph)

    published = tmp_path / "silver/identity/s7-derived-candidates"
    assert not published.exists() or not any(published.iterdir())
    assert len(list((tmp_path / "tmp/silver-s7-materialization").iterdir())) == 1
