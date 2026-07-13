from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow.parquet as pq
import pytest
from test_silver_asset_full_run_plan import (
    TABLES,
    _approval_arguments,
    _approve_plans,
    _create_plans,
    _git,
    _PlanFixture,
    _prepare_plan_fixture,
)

from ame_stocks_api.cli import silver_assets_full as cli_module
from ame_stocks_api.silver import asset_full as full_module
from ame_stocks_api.silver import asset_full_run_plan as plan_module
from ame_stocks_api.silver.contracts import ArtifactRole, BuildKind
from ame_stocks_api.silver.store import SilverStoreError, WorkflowState

SECOND_SESSION = "2026-05-12"


@dataclass(frozen=True, slots=True)
class _ApprovedFixture:
    plan: _PlanFixture
    plan_ids: dict[str, str]
    plan_shas: dict[str, str]
    approved_events: dict[str, str]


def _extend_to_two_sessions(fixture: _PlanFixture) -> _PlanFixture:
    root = fixture.data_root
    manifest_directory = root / "manifests/massive/assets"
    for original_path in tuple(sorted(manifest_directory.glob("*.json"))):
        document = json.loads(original_path.read_bytes())
        active = document["request"]["parameters"]["active"]
        request_id = hashlib.sha256(f"asset-full-second-session:{active}".encode()).hexdigest()
        source_page = root / document["artifacts"][0]["path"]
        relative_page = f"bronze/massive/assets/request_id={request_id}/page-00000.json.gz"
        target_page = root / relative_page
        target_page.parent.mkdir(parents=True, exist_ok=True)
        target_page.write_bytes(source_page.read_bytes())
        document["request_id"] = request_id
        document["request"]["start"] = SECOND_SESSION
        document["request"]["end"] = SECOND_SESSION
        document["artifacts"][0]["path"] = relative_page
        content = json.dumps(document, sort_keys=True).encode()
        (manifest_directory / f"{request_id}.json").write_bytes(content)

    manifest_paths = tuple(
        path.relative_to(root).as_posix() for path in sorted(manifest_directory.glob("*.json"))
    )
    inventory = plan_module.build_asset_source_inventory(
        root,
        manifest_paths=manifest_paths,
        git_commit=fixture.git_commit,
    )
    scope = plan_module._measure_manifest_scope(root, inventory)
    output_rows = {
        "asset_observation_daily": 22,
        "asset_observation_version": 12,
        "universe_source_daily": 16,
    }
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
            "expected_universe_rows": output_rows["universe_source_daily"],
            "source_rows": scope.input_rows,
            "version_member_rows": output_rows["asset_observation_version"],
        },
        "total_pages": scope.page_count,
        "total_rows": scope.input_rows,
        "write_boundary": {
            "bronze_or_manifest_mtime_changes_after_profile_start": 0,
            "profile_artifact_written_to_data_root": False,
        },
    }
    content = (
        json.dumps(profile, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )
    profile_path = fixture.repo_root / fixture.profile_path
    profile_path.write_bytes(content)
    _git(fixture.repo_root, "add", fixture.profile_path)
    _git(fixture.repo_root, "commit", "-q", "-m", "extend fixture source profile")
    git_commit = _git(fixture.repo_root, "rev-parse", "HEAD")
    authorization = replace(
        fixture.authorization,
        source_profile_sha256=hashlib.sha256(content).hexdigest(),
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
        expected_output_rows_by_table=output_rows,
        estimated_data_bytes_by_table={
            "asset_observation_daily": 200,
            "asset_observation_version": 20,
            "universe_source_daily": 160,
        },
        estimated_data_bytes_total_point=380,
    )
    return replace(
        fixture,
        git_commit=git_commit,
        profile_sha256=hashlib.sha256(content).hexdigest(),
        authorization=authorization,
    )


def _approved_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    two_sessions: bool = False,
) -> _ApprovedFixture:
    fixture = _prepare_plan_fixture(tmp_path, monkeypatch)
    if two_sessions:
        fixture = _extend_to_two_sessions(fixture)
    fixture = replace(
        fixture,
        authorization=replace(
            fixture.authorization,
            stable_output_cap_bytes=2 * 1024 * 1024,
            peak_incremental_cap_bytes=4 * 1024 * 1024,
            stable_project_cap_bytes=64 * 1024 * 1024,
            peak_project_cap_bytes=64 * 1024 * 1024,
            free_space_floor_bytes=1,
            free_space_warning_bytes=2,
        ),
    )
    plan_run = _create_plans(fixture)
    values = _approval_arguments(plan_run)
    approval = _approve_plans(fixture, plan_run, arguments=values)
    assert all(
        item.state is WorkflowState.APPROVED_FULL_RUN
        for item in approval.workflows_by_table.values()
    )
    monkeypatch.setattr(
        full_module,
        "_load_reference_dictionaries",
        lambda root, store, authorization: (
            frozenset({"CS"}),
            frozenset({"XNAS"}),
            (),
        ),
    )
    monkeypatch.setattr(full_module, "_process_max_rss_bytes", lambda: 0)
    return _ApprovedFixture(
        plan=fixture,
        plan_ids=dict(values["plan_ids"]),
        plan_shas=dict(values["plan_shas"]),
        approved_events={
            table: fixture.store.status(workflow_id).event_sha256
            for table, workflow_id in fixture.workflow_ids.items()
        },
    )


def _run(
    fixture: _ApprovedFixture,
    **overrides: Any,
) -> full_module.AssetFullRun:
    arguments: dict[str, Any] = {
        "workflow_ids": fixture.plan.workflow_ids,
        "approved_event_sha256_by_table": fixture.approved_events,
        "approved_plan_id_by_table": fixture.plan_ids,
        "approved_plan_sha256_by_table": fixture.plan_shas,
        "git_commit": fixture.plan.git_commit,
        "repo_root": fixture.plan.repo_root,
        "workers": 1,
        "max_in_flight_sessions": 1,
        "actor": "fixture-full-runner",
        "calendar_name": "XNYS",
        "authorization": fixture.plan.authorization,
        "git_verifier": lambda repo, commit: None,
        "monotonic": lambda: 0.0,
        "now_utc": lambda: "2099-01-03T00:00:00+00:00",
    }
    arguments.update(overrides)
    return full_module._run_asset_full_authorized(fixture.plan.data_root, **arguments)


@pytest.mark.parametrize(
    "mapping_name",
    (
        "workflow_ids",
        "approved_event_sha256_by_table",
        "approved_plan_id_by_table",
        "approved_plan_sha256_by_table",
    ),
)
def test_asset_full_requires_exact_workflow_event_plan_and_sha_maps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mapping_name: str,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch)
    valid = {
        "workflow_ids": fixture.plan.workflow_ids,
        "approved_event_sha256_by_table": fixture.approved_events,
        "approved_plan_id_by_table": fixture.plan_ids,
        "approved_plan_sha256_by_table": fixture.plan_shas,
    }
    changed = dict(valid[mapping_name])
    changed[TABLES[1]] = "f" * 64
    with pytest.raises(SilverStoreError):
        _run(fixture, **{mapping_name: changed})
    assert all(
        fixture.plan.store.status(workflow_id).state is WorkflowState.APPROVED_FULL_RUN
        for workflow_id in fixture.plan.workflow_ids.values()
    )
    assert not (fixture.plan.data_root / "silver").exists()


def test_asset_full_tiny_multi_session_reaches_full_ready_without_publish_and_reruns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch, two_sessions=True)
    first = _run(fixture)

    assert first.completed_sessions == 2
    assert first.idempotent is False
    assert set(first.table_runs) == set(TABLES)
    for table, item in first.table_runs.items():
        assert item.workflow.state is WorkflowState.FULL_READY
        assert item.workflow.sequence == 8
        assert item.plan.plan_id == fixture.plan_ids[table]
        assert item.build.intent.kind is BuildKind.FULL
        assert item.build.preview is None
        roles = [artifact.role for artifact in item.build.outputs]
        assert roles.count(ArtifactRole.DATA) == 2
        assert roles.count(ArtifactRole.QA) == 1
        assert roles.count(ArtifactRole.QUARANTINE) == 1
        data_rows = sum(
            pq.ParquetFile(fixture.plan.data_root / artifact.path).metadata.num_rows
            for artifact in item.build.outputs
            if artifact.role is ArtifactRole.DATA
        )
        assert data_rows == fixture.plan.authorization.expected_output_rows_by_table[table]
        summary = cli_module._table_summary(item)
        assert summary["input_page_count"] == 4
        assert summary["output_data_partition_count"] == 2
        assert summary["output_rows"] == data_rows
        assert "inputs" not in summary
        assert "outputs" not in summary
    assert not (fixture.plan.data_root / "manifests/silver/releases").exists()
    assert not (fixture.plan.data_root / "manifests/silver/approvals/publish").exists()

    event_counts = {
        table: len(fixture.plan.store.workflow_events(workflow_id))
        for table, workflow_id in fixture.plan.workflow_ids.items()
    }
    repeated = _run(fixture)
    assert repeated.idempotent is True
    assert repeated.completed_sessions == 2
    assert event_counts == {
        table: len(fixture.plan.store.workflow_events(workflow_id))
        for table, workflow_id in fixture.plan.workflow_ids.items()
    }


def test_asset_full_recovers_after_session_without_retransforming_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch, two_sessions=True)
    real_transform = full_module.transform_asset_session
    transformed_dates: list[str] = []

    def counted_transform(*args: Any, **kwargs: Any):
        transformed_dates.append(args[0].session_date.isoformat())
        return real_transform(*args, **kwargs)

    def stop_after_first(stage: str) -> None:
        if stage.startswith("after_session:"):
            raise RuntimeError("injected session interruption")

    with pytest.raises(RuntimeError, match="session interruption"):
        _run(
            fixture,
            transition_barrier=stop_after_first,
            transform_fn=counted_transform,
        )
    assert transformed_dates == ["2026-05-11"]
    assert all(
        fixture.plan.store.status(workflow_id).state is WorkflowState.APPROVED_FULL_RUN
        for workflow_id in fixture.plan.workflow_ids.values()
    )

    recovered = _run(fixture, transform_fn=counted_transform)
    assert recovered.completed_sessions == 2
    assert transformed_dates == ["2026-05-11", SECOND_SESSION]
    assert all(
        item.workflow.state is WorkflowState.FULL_READY for item in recovered.table_runs.values()
    )


def test_asset_full_recovers_three_table_partial_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch)

    def stop_after_first_record(stage: str) -> None:
        if stage == f"after_record:{TABLES[0]}":
            raise RuntimeError("injected record interruption")

    with pytest.raises(RuntimeError, match="record interruption"):
        _run(fixture, transition_barrier=stop_after_first_record)
    assert fixture.plan.store.status(fixture.plan.workflow_ids[TABLES[0]]).state is (
        WorkflowState.FULL_READY
    )
    assert all(
        fixture.plan.store.status(fixture.plan.workflow_ids[table]).state
        is WorkflowState.APPROVED_FULL_RUN
        for table in TABLES[1:]
    )

    recovered = _run(fixture)
    assert recovered.idempotent is False
    assert all(
        item.workflow.state is WorkflowState.FULL_READY for item in recovered.table_runs.values()
    )
    assert not (fixture.plan.data_root / "manifests/silver/releases").exists()


@pytest.mark.parametrize("tamper_kind", ("source", "partition", "checkpoint"))
def test_asset_full_source_partition_and_checkpoint_tamper_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper_kind: str,
) -> None:
    fixture = _approved_fixture(
        tmp_path,
        monkeypatch,
        two_sessions=tamper_kind != "source",
    )
    if tamper_kind == "source":
        page = next((fixture.plan.data_root / "bronze/massive/assets").rglob("*.json.gz"))
        page.chmod(0o644)
        content = bytearray(page.read_bytes())
        content[-1] ^= 1
        page.write_bytes(content)
    else:

        def stop_after_first(stage: str) -> None:
            if stage.startswith("after_session:"):
                raise RuntimeError("injected tamper checkpoint")

        with pytest.raises(RuntimeError, match="tamper checkpoint"):
            _run(fixture, transition_barrier=stop_after_first)
        checkpoint = next(
            (fixture.plan.data_root / "manifests/silver/checkpoints/assets-full").rglob(
                "checkpoint.json"
            )
        )
        if tamper_kind == "partition":
            checkpoint_document = json.loads(checkpoint.read_bytes())
            partition = fixture.plan.data_root / checkpoint_document["payload"][
                "outputs_by_table"
            ][TABLES[0]][0]["path"]
            partition.chmod(0o644)
            content = bytearray(partition.read_bytes())
            content[-1] ^= 1
            partition.write_bytes(content)
        else:
            document = json.loads(checkpoint.read_bytes())
            document["payload"]["next_session_index"] = 0
            checkpoint.chmod(0o644)
            checkpoint.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(
        (SilverStoreError, ValueError),
        match=r"checksum|artifact|integrity|writable",
    ):
        _run(fixture)
    assert all(
        fixture.plan.store.status(workflow_id).state is WorkflowState.APPROVED_FULL_RUN
        for workflow_id in fixture.plan.workflow_ids.values()
    )


@pytest.mark.parametrize(
    ("kind", "message"),
    (
        ("disk", "disk|floor"),
        ("rss", "RSS"),
        ("runtime", "runtime"),
    ),
)
def test_asset_full_resource_floor_rss_and_runtime_gates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    message: str,
) -> None:
    point_estimate = (
        plan_module.CURRENT_ASSET_FULL_RUN_PLAN_AUTHORIZATION.estimated_data_bytes_total_point
    )
    authorization = replace(
        plan_module.CURRENT_ASSET_FULL_RUN_PLAN_AUTHORIZATION,
        stable_output_cap_bytes=point_estimate + 10,
        peak_incremental_cap_bytes=point_estimate + 20,
        stable_project_cap_bytes=point_estimate + 1_000,
        peak_project_cap_bytes=point_estimate + 1_000,
        free_space_floor_bytes=100,
        free_space_warning_bytes=200,
        runtime_estimate_seconds=10,
        runtime_review_ceiling_seconds=20,
        expected_rss_ceiling_bytes=50,
        hard_rss_limit_bytes=100,
    )
    free = 50 if kind == "disk" else point_estimate + 2_000
    monkeypatch.setattr(
        full_module.shutil,
        "disk_usage",
        lambda root: SimpleNamespace(free=free),
    )
    monkeypatch.setattr(full_module, "_process_max_rss_bytes", lambda: 101 if kind == "rss" else 0)
    elapsed = 21.0 if kind == "runtime" else 0.0
    with pytest.raises(SilverStoreError, match=message):
        full_module._enforce_live_resources(
            tmp_path,
            authorization=authorization,
            baseline_project_bytes=0,
            produced_bytes=0,
            elapsed_seconds=elapsed,
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    (
        ({"workers": 2}, "workers"),
        ({"max_in_flight_sessions": 2}, "max_in_flight"),
        ({"calendar_name": "XLON"}, "calendar"),
    ),
)
def test_asset_full_rejects_runtime_configuration_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, Any],
    message: str,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch)
    with pytest.raises(SilverStoreError, match=message):
        _run(fixture, **overrides)


def test_asset_full_rejects_git_head_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _approved_fixture(tmp_path, monkeypatch)
    with pytest.raises(SilverStoreError, match=r"Git|HEAD|commit"):
        _run(
            fixture,
            git_commit="0" * 40,
            git_verifier=full_module._verify_git_checkout,
        )


def test_asset_full_cli_help_and_machine_readable_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as help_exit:
        cli_module.build_parser().parse_args(["--help"])
    assert help_exit.value.code == 0
    assert "cannot approve a plan or publish" in capsys.readouterr().out

    class _Serializable:
        def __init__(self, value: dict[str, object]):
            self.value = value

        def to_dict(self) -> dict[str, object]:
            return self.value

    table_runs = {}
    for index, table in enumerate(TABLES):
        table_runs[table] = SimpleNamespace(
            build=SimpleNamespace(
                build_id=str(index) * 64,
                outputs=(
                    SimpleNamespace(
                        role=ArtifactRole.DATA,
                        bytes=100 + index,
                        row_count=10 + index,
                    ),
                ),
                qa_checks=(SimpleNamespace(status=SimpleNamespace(value="passed")),),
                row_funnel=_Serializable({"output_rows_by_table": {table: index}}),
            ),
            build_document=SimpleNamespace(path=f"manifest-{table}.json", sha256="b" * 64),
            plan=SimpleNamespace(
                plan_id="c" * 64,
                parameters={
                    "date_end": SECOND_SESSION,
                    "date_start": "2026-05-11",
                    "expected_input_rows": 22,
                    "input_compressed_bytes": 200,
                    "input_manifest_count": 4,
                    "input_page_count": 4,
                    "input_raw_bytes": 400,
                    "input_session_count": 2,
                    "forbidden_inputs_marker": "must-not-be-printed",
                },
            ),
            workflow=SimpleNamespace(
                sequence=8,
                state=WorkflowState.FULL_READY,
                event_path=f"event-{table}.json",
                event_sha256="d" * 64,
                workflow_id="e" * 64,
            ),
        )
    captured: dict[str, Any] = {}

    def fake_run(data_root: Path, **kwargs: Any):
        captured["data_root"] = data_root
        captured.update(kwargs)
        return SimpleNamespace(
            completed_sessions=2,
            idempotent=False,
            table_runs=table_runs,
            warnings=("reviewed warning",),
        )

    monkeypatch.setattr(cli_module, "run_asset_full", fake_run)
    arguments = ["--data-root", str(tmp_path), "--repo-root", str(tmp_path)]
    for index, table in enumerate(TABLES):
        option = table.replace("_", "-")
        arguments.extend(
            (
                f"--{option}-workflow-id",
                str(index + 1) * 64,
                f"--{option}-approved-event-sha256",
                str(index + 4) * 64,
                f"--{option}-approved-plan-id",
                str(index + 7) * 64,
                f"--{option}-approved-plan-sha256",
                chr(ord("a") + index) * 64,
            )
        )
    arguments.extend(("--git-commit", "f" * 40))

    assert cli_module.main(arguments) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["mode"] == "full_ready_only"
    assert output["completed_sessions"] == 2
    assert output["warnings"] == ["reviewed warning"]
    assert {item["state"] for item in output["tables"].values()} == {"full_ready"}
    assert all(item["output_data_partition_count"] == 1 for item in output["tables"].values())
    assert "must-not-be-printed" not in json.dumps(output)
    assert "inputs" not in output["tables"][TABLES[0]]
    assert "outputs" not in output["tables"][TABLES[0]]
    assert captured["workers"] == 1
    assert captured["max_in_flight_sessions"] == 1
    assert set(captured["approved_plan_id_by_table"]) == set(TABLES)
