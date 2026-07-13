from __future__ import annotations

import hashlib
import json
import math
import stat
from dataclasses import dataclass, replace
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_silver_asset_preview import (
    _git,
    _prepared_fixture,
    _run_fixture_preview,
    _runs_by_table,
)

from ame_stocks_api.silver import asset_full_run_plan as plan_module
from ame_stocks_api.silver.store import SilverStore, SilverStoreError, WorkflowState

TABLES = (
    "asset_observation_daily",
    "asset_observation_version",
    "universe_source_daily",
)
PLAN_RECORDED_AT = "2099-01-01T00:00:00+00:00"
APPROVAL_DECIDED_AT = "2099-01-02T00:00:00+00:00"


@dataclass(frozen=True, slots=True)
class _PlanFixture:
    data_root: Path
    repo_root: Path
    git_commit: str
    store: SilverStore
    workflow_ids: dict[str, str]
    awaiting_events: dict[str, str]
    profile_path: str
    profile_sha256: str
    authorization: plan_module.AssetFullRunPlanAuthorization


def _prepare_plan_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> _PlanFixture:
    (
        data_root,
        preview_fixture,
        repo_root,
        preview_git_commit,
        store,
        workflow_ids,
        code_events,
    ) = _prepared_fixture(tmp_path, monkeypatch)
    preview_run = _run_fixture_preview(
        data_root,
        workflow_ids=workflow_ids,
        event_sha256_by_table=code_events,
        repo_root=repo_root,
        git_commit=preview_git_commit,
        fixture=preview_fixture,
    )
    preview_runs = _runs_by_table(preview_run)
    inventory = plan_module.build_asset_source_inventory(
        data_root,
        manifest_paths=preview_fixture.authorization.manifest_paths,
        git_commit=preview_git_commit,
    )
    scope = plan_module._measure_manifest_scope(data_root, inventory)
    profile = {
        "authoritative_inputs": {
            "artifact_count": scope.page_count,
            "artifact_inventory_digest": scope.artifact_inventory_sha256,
            "bronze_audit_status": {
                "authoritative_plan": "passed",
                "physical_integrity": "passed",
            },
            "date_end": scope.date_end,
            "date_start": scope.date_start,
            "manifest_count": scope.manifest_count,
            "manifest_inventory_digest": scope.manifest_inventory_sha256,
            "session_count": scope.session_count,
        },
        "hard_gate_numerators": {"fixture_hard_gate": 0},
        "manifest_profile": {
            "active_pages": scope.active_pages,
            "active_rows": scope.active_rows,
            "complete_manifests": scope.manifest_count,
            "failed_or_in_progress_manifests": 0,
            "inactive_pages": scope.inactive_pages,
            "inactive_rows": scope.inactive_rows,
            "missing_active_inactive_session_pairs": 0,
        },
        "profile_summary_schema_version": 1,
        "row_funnel": {
            "accepted_observation_rows": scope.input_rows,
            "expected_universe_rows": 8,
            "source_rows": scope.input_rows,
            "version_member_rows": 6,
        },
        "total_pages": scope.page_count,
        "total_rows": scope.input_rows,
        "write_boundary": {
            "bronze_or_manifest_mtime_changes_after_profile_start": 0,
            "profile_artifact_written_to_data_root": False,
        },
    }
    profile_path = "docs/silver/source-profiles/assets-full-fixture.json"
    absolute_profile = repo_root / profile_path
    absolute_profile.parent.mkdir(parents=True, exist_ok=True)
    profile_content = (
        json.dumps(profile, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )
    absolute_profile.write_bytes(profile_content)
    _git(repo_root, "add", profile_path)
    _git(repo_root, "commit", "-q", "-m", "add fixture source profile")
    git_commit = _git(repo_root, "rev-parse", "HEAD")
    expected_output_rows = {
        "asset_observation_daily": 11,
        "asset_observation_version": 6,
        "universe_source_daily": 8,
    }
    authorization = plan_module.AssetFullRunPlanAuthorization(
        workflow_ids_by_table=workflow_ids,
        preview_build_ids_by_table={
            table: run.build.build_id for table, run in preview_runs.items()
        },
        preview_manifest_sha256_by_table={
            table: run.build_document.sha256 for table, run in preview_runs.items()
        },
        awaiting_review_event_sha256_by_table={
            table: run.workflow.event_sha256 for table, run in preview_runs.items()
        },
        source_profile_path=profile_path,
        source_profile_sha256=hashlib.sha256(profile_content).hexdigest(),
        date_start=scope.date_start,
        date_end=scope.date_end,
        expected_session_count=scope.session_count,
        expected_manifest_count=scope.manifest_count,
        expected_page_count=scope.page_count,
        expected_input_rows=scope.input_rows,
        expected_manifest_bytes=scope.manifest_bytes,
        expected_compressed_bytes=scope.compressed_bytes,
        expected_raw_bytes=scope.raw_bytes,
        manifest_inventory_sha256=scope.manifest_inventory_sha256,
        artifact_inventory_sha256=scope.artifact_inventory_sha256,
        expected_output_rows_by_table=expected_output_rows,
        estimated_data_bytes_by_table={
            "asset_observation_daily": 100,
            "asset_observation_version": 10,
            "universe_source_daily": 80,
        },
        estimated_data_bytes_total_point=190,
        exchange_release_id=preview_fixture.authorization.exchange_release_id,
        exchange_release_sha256=preview_fixture.authorization.exchange_release_sha256,
        ticker_type_release_id=preview_fixture.authorization.ticker_type_release_id,
        ticker_type_release_sha256=(
            preview_fixture.authorization.ticker_type_release_sha256
        ),
        pyarrow_version=version("pyarrow"),
        parquet_writer_policy=plan_module.ASSET_FULL_PARQUET_WRITER_POLICY,
        stable_output_cap_bytes=10_000,
        peak_incremental_cap_bytes=20_000,
        stable_project_cap_bytes=1_000_000_000,
        peak_project_cap_bytes=1_000_000_000,
        free_space_floor_bytes=1,
        free_space_warning_bytes=2,
        runtime_estimate_seconds=10,
        runtime_review_ceiling_seconds=20,
        expected_rss_ceiling_bytes=10_000,
        hard_rss_limit_bytes=20_000,
        dependency_lineage_required=False,
    )
    return _PlanFixture(
        data_root=data_root,
        repo_root=repo_root,
        git_commit=git_commit,
        store=store,
        workflow_ids=workflow_ids,
        awaiting_events={
            table: run.workflow.event_sha256 for table, run in preview_runs.items()
        },
        profile_path=profile_path,
        profile_sha256=hashlib.sha256(profile_content).hexdigest(),
        authorization=authorization,
    )


def _create_plans(
    fixture: _PlanFixture,
    *,
    expected_events: dict[str, str] | None = None,
    transition_barrier: Any = None,
) -> plan_module.AssetFullRunPlanRun:
    authorization = fixture.authorization
    return plan_module._create_asset_full_run_plans_authorized(
        fixture.data_root,
        repo_root=fixture.repo_root,
        workflow_ids=fixture.workflow_ids,
        expected_event_sha256_by_table=(
            fixture.awaiting_events if expected_events is None else expected_events
        ),
        source_profile_path=fixture.profile_path,
        expected_source_profile_sha256=fixture.profile_sha256,
        expected_manifest_inventory_sha256=(authorization.manifest_inventory_sha256),
        expected_artifact_inventory_sha256=(authorization.artifact_inventory_sha256),
        expected_input_rows=authorization.expected_input_rows,
        git_commit=fixture.git_commit,
        recorded_at=PLAN_RECORDED_AT,
        actor="fixture-plan-author",
        note="fixture full-run plan",
        authorization=authorization,
        transition_barrier=transition_barrier,
        git_verifier=lambda repo, commit: None,
    )


def _approval_arguments(
    run: plan_module.AssetFullRunPlanRun,
) -> dict[str, dict[str, Any]]:
    runs = {item.plan.table: item for item in run.table_runs}
    return {
        "plan_ids": {table: item.plan.plan_id for table, item in runs.items()},
        "plan_shas": {
            table: item.plan_document.sha256 for table, item in runs.items()
        },
        "plan_events": {
            table: item.workflow.event_sha256 for table, item in runs.items()
        },
        "waivers": {
            table: item.required_waived_qa_result_ids for table, item in runs.items()
        },
        "accepted": {
            table: item.required_accepted_quarantine_issue_ids
            for table, item in runs.items()
        },
    }


def _approve_plans(
    fixture: _PlanFixture,
    run: plan_module.AssetFullRunPlanRun,
    *,
    arguments: dict[str, dict[str, Any]] | None = None,
    transition_barrier: Any = None,
) -> plan_module.AssetFullRunPlanApprovalRun:
    values = _approval_arguments(run) if arguments is None else arguments
    return plan_module._approve_asset_full_run_plans_authorized(
        fixture.data_root,
        workflow_ids=fixture.workflow_ids,
        expected_plan_ids_by_table=values["plan_ids"],
        expected_plan_sha256_by_table=values["plan_shas"],
        expected_plan_event_sha256_by_table=values["plan_events"],
        waived_qa_result_ids_by_table=values["waivers"],
        accepted_quarantine_issue_ids_by_table=values["accepted"],
        approver="fixture-plan-reviewer",
        decided_at=APPROVAL_DECIDED_AT,
        note="fixture plan approval",
        authorization=fixture.authorization,
        transition_barrier=transition_barrier,
    )


def test_asset_full_run_plan_is_shared_bounded_and_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _prepare_plan_fixture(tmp_path, monkeypatch)
    first = _create_plans(fixture)

    assert {item.workflow.state for item in first.table_runs} == {
        WorkflowState.FULL_RUN_PLAN_REVIEW
    }
    assert {item.workflow.sequence for item in first.table_runs} == {6}
    assert {item.plan.parameters["preflight_id"] for item in first.table_runs} == {
        first.preflight.preflight_id
    }
    assert {item.plan.parameters["source_inventory_id"] for item in first.table_runs} == {
        first.inventory.inventory_id
    }
    assert first.scope.input_rows == fixture.authorization.expected_input_rows
    assert first.scope.page_count == fixture.authorization.expected_page_count
    for item in first.table_runs:
        assert item.plan.input_artifact_count == fixture.authorization.expected_page_count
        assert item.plan.input_rows == fixture.authorization.expected_input_rows
        assert item.plan.input_bytes == fixture.authorization.expected_compressed_bytes
        assert item.plan.parameters["pyarrow_version"] == version("pyarrow")
        assert dict(item.plan.parameters["parquet_writer_policy"]) == dict(
            plan_module.ASSET_FULL_PARQUET_WRITER_POLICY
        )
        assert item.plan.resource_projection["max_session_row_dates"] == (
            first.scope.max_session_row_dates
        )
        assert item.plan.resource_projection["max_session_page_dates"] == (
            first.scope.max_session_page_dates
        )

    repeated = _create_plans(
        fixture,
        expected_events={
            item.plan.table: item.workflow.event_sha256 for item in first.table_runs
        },
    )
    assert {
        item.plan.table: (item.plan.plan_id, item.plan_document.sha256)
        for item in repeated.table_runs
    } == {
        item.plan.table: (item.plan.plan_id, item.plan_document.sha256)
        for item in first.table_runs
    }
    assert {
        table: len(fixture.store.workflow_events(workflow_id))
        for table, workflow_id in fixture.workflow_ids.items()
    } == dict.fromkeys(TABLES, 6)
    assert all(
        all(
            event.event.to_state is not WorkflowState.FULL_READY
            for event in fixture.store.workflow_events(workflow_id)
        )
        for workflow_id in fixture.workflow_ids.values()
    )
    assert not (fixture.data_root / "silver").exists()
    assert not (fixture.data_root / "manifests/silver/releases").exists()


def test_asset_full_run_plan_partial_transition_recovers_same_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _prepare_plan_fixture(tmp_path, monkeypatch)
    observed_preflight: str | None = None

    def stop_after_first(table: str) -> None:
        nonlocal observed_preflight
        assert table == TABLES[0]
        preflights = tuple(
            (fixture.data_root / "manifests/silver/full-run-plan-preflights").rglob(
                "manifest.json"
            )
        )
        observed_preflight = hashlib.sha256(preflights[0].read_bytes()).hexdigest()
        raise RuntimeError("injected plan interruption")

    with pytest.raises(RuntimeError, match="injected plan interruption"):
        _create_plans(fixture, transition_barrier=stop_after_first)
    interrupted = {
        table: fixture.store.status(workflow_id)
        for table, workflow_id in fixture.workflow_ids.items()
    }
    assert interrupted[TABLES[0]].state is WorkflowState.FULL_RUN_PLAN_REVIEW
    assert all(
        interrupted[table].state is WorkflowState.AWAITING_REVIEW for table in TABLES[1:]
    )

    recovered = _create_plans(
        fixture,
        expected_events={
            table: snapshot.event_sha256 for table, snapshot in interrupted.items()
        },
    )
    assert recovered.preflight.document.sha256 == observed_preflight
    assert all(
        item.workflow.state is WorkflowState.FULL_RUN_PLAN_REVIEW
        for item in recovered.table_runs
    )


def test_asset_full_run_plan_source_tamper_fails_before_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _prepare_plan_fixture(tmp_path, monkeypatch)
    page = next((fixture.data_root / "bronze/massive/assets").rglob("*.json.gz"))
    page.chmod(0o644)
    content = bytearray(page.read_bytes())
    content[-1] ^= 1
    page.write_bytes(content)

    with pytest.raises((SilverStoreError, ValueError), match=r"checksum|artifact|integrity"):
        _create_plans(fixture)
    for workflow_id in fixture.workflow_ids.values():
        snapshot = fixture.store.status(workflow_id)
        assert snapshot.state is WorkflowState.AWAITING_REVIEW
        assert snapshot.sequence == 5


def test_asset_full_run_plan_approval_rejects_every_wrong_explicit_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _prepare_plan_fixture(tmp_path, monkeypatch)
    run = _create_plans(fixture)
    valid = _approval_arguments(run)

    mutations = (
        ("plan_ids", "0" * 64),
        ("plan_shas", "1" * 64),
        ("plan_events", "2" * 64),
    )
    for mapping_name, wrong_value in mutations:
        changed = {key: dict(value) for key, value in valid.items()}
        changed[mapping_name][TABLES[1]] = wrong_value
        with pytest.raises(SilverStoreError, match=r"mismatch|changed"):
            _approve_plans(fixture, run, arguments=changed)
        assert all(
            fixture.store.status(workflow_id).state
            is WorkflowState.FULL_RUN_PLAN_REVIEW
            for workflow_id in fixture.workflow_ids.values()
        )

    changed = {key: dict(value) for key, value in valid.items()}
    changed["waivers"][TABLES[1]] = ("3" * 64,)
    with pytest.raises(SilverStoreError, match="waiver set mismatch"):
        _approve_plans(fixture, run, arguments=changed)
    assert all(
        fixture.store.status(workflow_id).state is WorkflowState.FULL_RUN_PLAN_REVIEW
        for workflow_id in fixture.workflow_ids.values()
    )


@pytest.mark.parametrize(
    "mutation",
    ("transform_version", "calendar_version", "calendar_name", "availability_rule"),
)
def test_asset_full_run_plan_approval_rejects_forged_executable_scope_before_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    fixture = _prepare_plan_fixture(tmp_path, monkeypatch)
    original_build_plan = plan_module._build_plan

    def forged_build_plan(*args: Any, **kwargs: Any):
        plan = original_build_plan(*args, **kwargs)
        if plan.table != TABLES[1]:
            return plan
        if mutation == "transform_version":
            return replace(plan, transform_version="forged-transform")
        if mutation == "calendar_version":
            return replace(plan, exchange_calendar_version="exchange-calendars==0.0.0")
        parameters = dict(plan.parameters)
        if mutation == "calendar_name":
            parameters["calendar_name"] = "forged-calendar"
        else:
            parameters["asset_source_availability_rule"] = "forged-availability-rule"
        return replace(plan, parameters=parameters)

    monkeypatch.setattr(plan_module, "_build_plan", forged_build_plan)
    run = _create_plans(fixture)
    before = {
        table: fixture.store.status(workflow_id)
        for table, workflow_id in fixture.workflow_ids.items()
    }
    assert all(snapshot.state is WorkflowState.FULL_RUN_PLAN_REVIEW for snapshot in before.values())

    with pytest.raises(SilverStoreError, match="approved plan scope changed"):
        _approve_plans(fixture, run)

    assert before == {
        table: fixture.store.status(workflow_id)
        for table, workflow_id in fixture.workflow_ids.items()
    }


def test_asset_full_run_plan_approval_partial_recovery_and_idempotency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _prepare_plan_fixture(tmp_path, monkeypatch)
    run = _create_plans(fixture)

    def stop_after_first(table: str) -> None:
        assert table == TABLES[0]
        raise RuntimeError("injected approval interruption")

    with pytest.raises(RuntimeError, match="injected approval interruption"):
        _approve_plans(fixture, run, transition_barrier=stop_after_first)
    interrupted = {
        table: fixture.store.status(workflow_id)
        for table, workflow_id in fixture.workflow_ids.items()
    }
    assert interrupted[TABLES[0]].state is WorkflowState.APPROVED_FULL_RUN
    assert all(
        interrupted[table].state is WorkflowState.FULL_RUN_PLAN_REVIEW
        for table in TABLES[1:]
    )

    recovered = _approve_plans(fixture, run)
    assert all(
        snapshot.state is WorkflowState.APPROVED_FULL_RUN
        for snapshot in recovered.workflows_by_table.values()
    )
    event_counts = {
        table: len(fixture.store.workflow_events(workflow_id))
        for table, workflow_id in fixture.workflow_ids.items()
    }
    repeated = _approve_plans(fixture, run)
    assert all(
        snapshot.state is WorkflowState.APPROVED_FULL_RUN
        for snapshot in repeated.workflows_by_table.values()
    )
    assert event_counts == {
        table: len(fixture.store.workflow_events(workflow_id))
        for table, workflow_id in fixture.workflow_ids.items()
    }
    assert all(
        all(
            event.event.to_state is not WorkflowState.FULL_READY
            for event in fixture.store.workflow_events(workflow_id)
        )
        for workflow_id in fixture.workflow_ids.values()
    )
    assert not (fixture.data_root / "silver").exists()
    assert not (fixture.data_root / "manifests/silver/releases").exists()


def test_asset_full_run_plan_preflight_and_inventory_tamper_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _prepare_plan_fixture(tmp_path, monkeypatch)
    run = _create_plans(fixture)
    plan = run.table_runs[0].plan
    with pytest.raises(SilverStoreError, match="complete canonical source inventory"):
        plan_module._verify_plan_inventory_binding(
            fixture.data_root,
            replace(plan, inputs=plan.inputs[:-1]),
            authorization=fixture.authorization,
        )

    preflight_path = fixture.data_root / run.preflight.document.path
    preflight_path.chmod(0o644)
    assert stat.S_IMODE(preflight_path.stat().st_mode) == 0o644
    with pytest.raises(SilverStoreError, match="preflight remains writable"):
        _approve_plans(fixture, run)
    assert all(
        fixture.store.status(workflow_id).state is WorkflowState.FULL_RUN_PLAN_REVIEW
        for workflow_id in fixture.workflow_ids.values()
    )


@pytest.mark.parametrize(
    ("free_bytes", "project_bytes", "message"),
    (
        (20_000, 1, "free-space floor"),
        (1_000_000, 999_995_000, "stable project size"),
        (1_000_000, 999_985_000, "peak project size"),
    ),
)
def test_asset_full_run_plan_resource_gates_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    free_bytes: int,
    project_bytes: int,
    message: str,
) -> None:
    fixture = _prepare_plan_fixture(tmp_path, monkeypatch)
    with pytest.raises(SilverStoreError, match=message):
        plan_module._enforce_resource_values(
            free_bytes,
            project_bytes,
            fixture.authorization,
        )


def test_production_output_point_estimate_includes_every_daily_parquet_floor() -> None:
    authorization = plan_module.CURRENT_ASSET_FULL_RUN_PLAN_AUTHORIZATION
    reviewed_preview = {
        "asset_observation_daily": (35_647, 4_320_422, 5_163 + 1_912),
        "asset_observation_version": (82, 22_384, 4_936 + 1_912),
        "universe_source_daily": (35_606, 3_964_738, 5_112 + 1_912),
    }
    expected: dict[str, int] = {}
    for table, contract in plan_module._CONTRACTS_BY_TABLE.items():
        sink = pa.BufferOutputStream()
        pq.write_table(
            pa.Table.from_pylist([], schema=contract.arrow_schema),
            sink,
            **dict(authorization.parquet_writer_policy),
        )
        empty_partition_bytes = sink.getvalue().size
        preview_rows, preview_bytes, system_bytes = reviewed_preview[table]
        scaled_nonempty_payload = math.ceil(
            (preview_bytes - empty_partition_bytes)
            * authorization.expected_output_rows_by_table[table]
            / preview_rows
        )
        expected[table] = (
            scaled_nonempty_payload
            + empty_partition_bytes * authorization.expected_session_count
            + system_bytes
        )

    assert dict(authorization.estimated_data_bytes_by_table) == expected
    assert authorization.estimated_data_bytes_total_point == sum(expected.values())
