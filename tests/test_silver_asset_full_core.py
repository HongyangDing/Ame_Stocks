from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from test_silver_asset_full import TABLES, _approved_fixture, _run

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import asset_full as full_module
from ame_stocks_api.silver.assets import transform_asset_session
from ame_stocks_api.silver.contracts import ArtifactRef, ArtifactRole
from ame_stocks_api.silver.store import SilverStoreError, WorkflowState


def _replace_column(table: pa.Table, name: str, values: list[object]) -> pa.Table:
    index = table.schema.get_field_index(name)
    return table.set_column(
        index, table.schema.field(index), pa.array(values, type=table.schema.field(index).type)
    )


def test_asset_full_global_reducer_uses_each_table_population_and_global_casefold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch, two_sessions=True)
    plans = {
        table: fixture.plan.store.load_full_run_plan(table, fixture.plan_ids[table])[0]
        for table in TABLES
    }
    inventory, _ = full_module._load_plan_inventory(fixture.plan.data_root, plans)
    reader, audit = full_module._authorized_reader(
        fixture.plan.data_root,
        inventory,
        authorization=fixture.plan.authorization,
        calendar_name="XNYS",
    )
    reducer = full_module._FullQAReducer()
    for session_index, session in enumerate(reader.sessions):
        records = tuple(reader.iter_session_records(session.session_date))
        transformed = transform_asset_session(
            session,
            records,
            build_id="a" * 64,
            current_ticker_types={"CS"},
            current_exchange_mics={"XNAS"},
        )
        if session_index == 1:
            observation = transformed.observation.table
            tickers = observation.column("ticker").to_pylist()
            discarded_index = next(
                index
                for index, ticker in enumerate(tickers)
                if ticker == "DEL" and index > tickers.index("DEL")
            )
            type_values = observation.column("type_code").to_pylist()
            exchange_values = observation.column("primary_exchange_mic").to_pylist()
            figi_values = observation.column("composite_figi").to_pylist()
            type_values[discarded_index] = "ZZ"
            exchange_values[discarded_index] = "XZZZ"
            figi_values[discarded_index] = "BBG-DISCARDED-VERSION"
            observation = _replace_column(observation, "type_code", type_values)
            observation = _replace_column(
                observation,
                "primary_exchange_mic",
                exchange_values,
            )
            observation = _replace_column(observation, "composite_figi", figi_values)
            transformed = replace(
                transformed,
                observation=replace(transformed.observation, table=observation),
            )
        reducer.add_session(full_module._transform_results_by_table(transformed))

    finalized = reducer.finalize(
        audit=audit,
        current_ticker_types=frozenset({"CS"}),
        current_exchange_mics=frozenset({"XNAS"}),
    )
    by_table = {
        table: {item.check_id: item for item in finalized[table]}
        for table in ("asset_observation_daily", "universe_source_daily")
    }
    observation = by_table["asset_observation_daily"]
    universe = by_table["universe_source_daily"]
    assert observation["source_plan_invalid"].denominator == (
        fixture.plan.authorization.expected_manifest_count
    )
    assert observation["source_calendar_coverage_invalid"].denominator == (
        fixture.plan.authorization.expected_session_count
    )
    assert (
        observation["current_type_dictionary_unmatched_values"].numerator,
        observation["current_type_dictionary_unmatched_values"].denominator,
    ) == (1, 2)
    assert (
        universe["current_type_dictionary_unmatched_values"].numerator,
        universe["current_type_dictionary_unmatched_values"].denominator,
    ) == (0, 1)
    assert (
        observation["current_exchange_dictionary_unmatched_values"].numerator,
        observation["current_exchange_dictionary_unmatched_values"].denominator,
    ) == (1, 2)
    assert (
        universe["current_exchange_dictionary_unmatched_values"].numerator,
        universe["current_exchange_dictionary_unmatched_values"].denominator,
    ) == (0, 1)
    assert observation["cross_session_ticker_identity_churn_groups"].numerator == 1
    assert universe["cross_session_ticker_identity_churn_groups"].numerator == 0
    assert (
        observation["casefold_collision_groups"].numerator,
        observation["casefold_collision_groups"].denominator,
    ) == (1, 7)
    assert (
        universe["casefold_collision_groups"].numerator,
        universe["casefold_collision_groups"].denominator,
    ) == (1, 7)


def test_asset_full_rejects_rehashed_main_reducer_tamper_before_transform(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch, two_sessions=True)

    def interrupt(stage: str) -> None:
        if stage.startswith("after_session:"):
            raise RuntimeError("checkpoint created")

    with pytest.raises(RuntimeError, match="checkpoint created"):
        _run(fixture, transition_barrier=interrupt)
    checkpoint = next(
        (fixture.plan.data_root / "manifests/silver/checkpoints/assets-full").rglob(
            "checkpoint.json"
        )
    )
    document = json.loads(checkpoint.read_bytes())
    document["payload"]["reducer"]["funnels"][TABLES[0]]["output_rows"] += 1
    document["payload_sha256"] = stable_digest(document["payload"])
    checkpoint.chmod(0o644)
    checkpoint.write_text(json.dumps(document), encoding="utf-8")

    def forbidden_transform(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("transform must not run after checkpoint tamper")

    with pytest.raises(SilverStoreError, match="reducer is not session-bound"):
        _run(fixture, transform_fn=forbidden_transform)
    assert all(
        fixture.plan.store.status(workflow_id).state is WorkflowState.APPROVED_FULL_RUN
        for workflow_id in fixture.plan.workflow_ids.values()
    )


def test_asset_full_rejects_pyarrow_drift_before_writes_or_transitions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch)
    event_counts = {
        table: len(fixture.plan.store.workflow_events(workflow_id))
        for table, workflow_id in fixture.plan.workflow_ids.items()
    }
    with pytest.raises(SilverStoreError, match="PyArrow"):
        _run(
            fixture,
            authorization=replace(fixture.plan.authorization, pyarrow_version="0.0.0"),
        )
    assert not (fixture.plan.data_root / "silver").exists()
    assert event_counts == {
        table: len(fixture.plan.store.workflow_events(workflow_id))
        for table, workflow_id in fixture.plan.workflow_ids.items()
    }


def test_asset_full_fast_path_rejects_extra_system_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch)
    completed = _run(fixture)
    table = TABLES[0]
    original = completed.table_runs[table].build
    sample = ArtifactRef(
        path=f"{full_module.SilverStore.build_output_prefix(original.intent)}/samples/forged.json",
        sha256="f" * 64,
        bytes=2,
        row_count=0,
        media_type="application/json",
        role=ArtifactRole.SAMPLE,
    )
    forged = replace(original, outputs=(*original.outputs, sample))
    monkeypatch.setattr(
        full_module,
        "_load_event_full",
        lambda store, selected_table, snapshot: (
            (forged, completed.table_runs[selected_table].build_document)
            if selected_table == table
            else (
                completed.table_runs[selected_table].build,
                completed.table_runs[selected_table].build_document,
            )
        ),
    )
    intents = {
        selected_table: full_module._full_intent(
            completed.table_runs[selected_table].plan,
            fixture.plan_ids[selected_table],
        )
        for selected_table in TABLES
    }
    with pytest.raises(SilverStoreError, match="partitions"):
        full_module._load_existing_full_runs(
            fixture.plan.store,
            snapshots={
                selected_table: completed.table_runs[selected_table].workflow
                for selected_table in TABLES
            },
            plans={
                selected_table: completed.table_runs[selected_table].plan
                for selected_table in TABLES
            },
            intents=intents,
            authorization=fixture.plan.authorization,
        )


def test_asset_full_shared_lock_rejects_concurrent_runner(tmp_path: Path) -> None:
    lock = tmp_path / "manifests/silver/locks/assets.lock"
    with (
        full_module._exclusive_run_lock(lock),
        pytest.raises(SilverStoreError, match="holds the shared run lock"),
        full_module._exclusive_run_lock(lock),
    ):
        pass


def test_asset_full_recovers_or_rejects_partial_session_orphan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch)
    real_writer = full_module._write_data_partition
    calls = 0

    def interrupted_writer(*args: Any, **kwargs: Any) -> ArtifactRef:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("killed between table writes")
        return real_writer(*args, **kwargs)

    monkeypatch.setattr(full_module, "_write_data_partition", interrupted_writer)
    with pytest.raises(RuntimeError, match="between table writes"):
        _run(fixture, transform_fn=transform_asset_session)
    assert calls == 2
    assert all(
        fixture.plan.store.status(workflow_id).state is WorkflowState.APPROVED_FULL_RUN
        for workflow_id in fixture.plan.workflow_ids.values()
    )
    monkeypatch.setattr(full_module, "_write_data_partition", real_writer)
    recovered = _run(fixture)
    assert recovered.completed_sessions == 1
    assert all(
        item.workflow.state is WorkflowState.FULL_READY for item in recovered.table_runs.values()
    )


def test_asset_full_recovers_after_manifests_without_retransform(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch)
    transformed = 0

    def counted_transform(*args: Any, **kwargs: Any) -> Any:
        nonlocal transformed
        transformed += 1
        return transform_asset_session(*args, **kwargs)

    def interrupt(stage: str) -> None:
        if stage == "after_manifests":
            raise RuntimeError("killed after manifests")

    with pytest.raises(RuntimeError, match="after manifests"):
        _run(
            fixture,
            transform_fn=counted_transform,
            transition_barrier=interrupt,
        )
    assert transformed == 1
    recovered = _run(fixture, transform_fn=counted_transform)
    assert transformed == 1
    assert all(
        item.workflow.state is WorkflowState.FULL_READY for item in recovered.table_runs.values()
    )


def test_asset_full_post_last_session_resource_gate_precedes_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch, two_sessions=True)
    real_gate = full_module._enforce_live_resources
    first_positive: int | None = None

    def fail_on_larger_checkpoint(root: Path, **kwargs: Any) -> tuple[str, ...]:
        nonlocal first_positive
        produced = kwargs["produced_bytes"]
        if produced > 0 and first_positive is None:
            first_positive = produced
        elif first_positive is not None and produced > first_positive:
            raise SilverStoreError("injected final-session resource gate")
        return real_gate(root, **kwargs)

    monkeypatch.setattr(full_module, "_enforce_live_resources", fail_on_larger_checkpoint)
    with pytest.raises(SilverStoreError, match="final-session resource gate"):
        _run(fixture)
    assert all(
        fixture.plan.store.status(workflow_id).state is WorkflowState.APPROVED_FULL_RUN
        for workflow_id in fixture.plan.workflow_ids.values()
    )
