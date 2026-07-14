from __future__ import annotations

import hashlib
import json
import stat
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_silver_asset_full import TABLES, _approved_fixture, _run
from test_silver_asset_preview import _git

from ame_stocks_api.cli import silver_assets_publish_plan as cli_module
from ame_stocks_api.silver import asset_publish_plan as publish_module
from ame_stocks_api.silver.asset_publish_plan import (
    ASSET_PUBLICATION_SCOPE,
    AssetPublishPlan,
    AssetRuntimeReviewEvidence,
    _create_asset_publish_plan,
    _load_runtime_review_evidence,
    _validate_s4_full_build_scope,
    _verify_git_checkout,
    _verify_reviewed_logic_closure,
    create_asset_publish_plan,
)
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.store import SilverStoreError, WorkflowState


def _full_ready_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    fixture = _approved_fixture(tmp_path, monkeypatch, two_sessions=True)
    full = _run(fixture)
    runtime_review = AssetRuntimeReviewEvidence(
        source_path="tmp/fixture-full-run.log",
        source_sha256="a" * 64,
        source_bytes=1,
        completed_sessions=fixture.plan.authorization.expected_session_count,
        qa_warning_counts_by_table={
            table: sum(
                check.status.value == "warning"
                for check in full.table_runs[table].build.qa_checks
            )
            for table in TABLES
        },
        warning_messages=("process RSS exceeded the reviewed 0.75 GiB estimate",),
        expected_rss_ceiling_bytes=(
            fixture.plan.authorization.expected_rss_ceiling_bytes
        ),
        hard_rss_limit_bytes=fixture.plan.authorization.hard_rss_limit_bytes,
    )
    arguments = {
        "workflow_ids_by_table": dict(fixture.plan.workflow_ids),
        "full_ready_event_sha256_by_table": {
            table: full.table_runs[table].workflow.event_sha256 for table in TABLES
        },
        "build_ids_by_table": {
            table: full.table_runs[table].build.build_id for table in TABLES
        },
        "build_manifest_sha256_by_table": {
            table: full.table_runs[table].build_document.sha256 for table in TABLES
        },
        "full_run_plan_ids_by_table": dict(fixture.plan_ids),
        "full_run_plan_sha256_by_table": dict(fixture.plan_shas),
        "repo_root": fixture.plan.repo_root,
        "orchestration_git_commit": fixture.plan.git_commit,
        "authorization": fixture.plan.authorization,
        "expected_materialization_git_commit": fixture.plan.git_commit,
        "git_verifier": lambda repo, orchestration, materialization: None,
        "runtime_review_loader": lambda root, maps, authorization: runtime_review,
    }
    return fixture, full, arguments


def test_asset_publish_plan_is_immutable_idempotent_and_never_mutates_workflows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, full, arguments = _full_ready_fixture(tmp_path, monkeypatch)
    event_counts = {
        table: len(fixture.plan.store.workflow_events(fixture.plan.workflow_ids[table]))
        for table in TABLES
    }
    approval_paths = tuple(
        sorted((fixture.plan.data_root / "manifests/silver/approvals").glob("*.json"))
    )

    first = _create_asset_publish_plan(fixture.plan.data_root, **arguments)

    assert first.idempotent is False
    assert first.plan.publication_scope == ASSET_PUBLICATION_SCOPE
    assert first.plan.backtest_identity_eligible is False
    assert first.plan.requires_release_set is True
    assert first.plan.requires_runtime_review_acceptance is True
    assert first.plan.runtime_review.observed_max_rss_bytes is None
    assert first.plan.runtime_review.warning_messages == (
        "process RSS exceeded the reviewed 0.75 GiB estimate",
    )
    assert tuple(item.table for item in first.plan.tables) == TABLES
    assert first.plan.warning_counts_by_table == {
        table: sum(
            check.status.value == "warning"
            for check in full.table_runs[table].build.qa_checks
        )
        for table in TABLES
    }
    assert all(item.accepted_quarantine_issue_ids == () for item in first.plan.tables)
    assert all(item.quarantine_issue_rows == 0 for item in first.plan.tables)
    assert all(item.output_data_partition_count == 2 for item in first.plan.tables)
    with pytest.raises(TypeError):
        first.plan.tables[0].quarantine_issue_ids_by_severity["low"] = ()  # type: ignore[index]
    assert AssetPublishPlan.from_dict(first.plan.to_dict()) == first.plan

    path = fixture.plan.data_root / first.document.path
    before = path.stat()
    assert stat.S_IMODE(before.st_mode) == 0o444
    assert before.st_nlink == 1
    assert json.loads(path.read_bytes()) == first.plan.to_dict()
    repeated = _create_asset_publish_plan(fixture.plan.data_root, **arguments)
    after = path.stat()
    assert repeated.idempotent is True
    assert repeated.plan == first.plan
    assert repeated.document == first.document
    assert (after.st_ino, after.st_mtime_ns, after.st_mode, after.st_nlink) == (
        before.st_ino,
        before.st_mtime_ns,
        before.st_mode,
        before.st_nlink,
    )

    assert event_counts == {
        table: len(fixture.plan.store.workflow_events(fixture.plan.workflow_ids[table]))
        for table in TABLES
    }
    assert all(
        fixture.plan.store.status(fixture.plan.workflow_ids[table]).state
        is WorkflowState.FULL_READY
        for table in TABLES
    )
    assert approval_paths == tuple(
        sorted((fixture.plan.data_root / "manifests/silver/approvals").glob("*.json"))
    )
    assert not (fixture.plan.data_root / "manifests/silver/releases").exists()


def test_public_asset_publish_gate_rejects_nonproduction_scope_before_git_or_data_checks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _full, arguments = _full_ready_fixture(tmp_path, monkeypatch)
    public_arguments = {
        key: value
        for key, value in arguments.items()
        if key
        not in {
            "authorization",
            "before_final_lock",
            "expected_materialization_git_commit",
            "git_verifier",
            "runtime_review_loader",
        }
    }

    with pytest.raises(SilverStoreError, match="authorized scope"):
        create_asset_publish_plan(fixture.plan.data_root, **public_arguments)

    assert not (
        fixture.plan.data_root / "manifests/silver/publish-plans/assets"
    ).exists()


def test_runtime_review_loader_binds_exact_full_run_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, full, arguments = _full_ready_fixture(tmp_path, monkeypatch)
    authorization = fixture.plan.authorization
    maps = {
        "workflow_ids": arguments["workflow_ids_by_table"],
        "full_ready_events": arguments["full_ready_event_sha256_by_table"],
        "build_ids": arguments["build_ids_by_table"],
        "build_shas": arguments["build_manifest_sha256_by_table"],
        "plan_ids": arguments["full_run_plan_ids_by_table"],
        "plan_shas": arguments["full_run_plan_sha256_by_table"],
    }
    tables: dict[str, object] = {}
    for table in TABLES:
        build = full.table_runs[table].build
        tables[table] = {
            "build_id": maps["build_ids"][table],
            "build_manifest_sha256": maps["build_shas"][table],
            "date_end": authorization.date_end,
            "date_start": authorization.date_start,
            "full_run_plan_id": maps["plan_ids"][table],
            "input_compressed_bytes": authorization.expected_compressed_bytes,
            "input_manifest_count": authorization.expected_manifest_count,
            "input_page_count": authorization.expected_page_count,
            "input_raw_bytes": authorization.expected_raw_bytes,
            "input_rows": authorization.expected_input_rows,
            "input_session_count": authorization.expected_session_count,
            "output_artifact_count": authorization.expected_session_count + 2,
            "output_data_partition_count": authorization.expected_session_count,
            "output_rows": authorization.expected_output_rows_by_table[table],
            "qa_status_counts": {
                "warning": sum(
                    check.status.value == "warning" for check in build.qa_checks
                )
            },
            "sequence": 8,
            "state": "full_ready",
            "workflow_event_sha256": maps["full_ready_events"][table],
            "workflow_id": maps["workflow_ids"][table],
        }
    document = {
        "completed_sessions": authorization.expected_session_count,
        "idempotent": False,
        "mode": "full_ready_only",
        "tables": tables,
        "warnings": ["process RSS exceeded the reviewed 0.75 GiB estimate"],
    }
    content = json.dumps(document, indent=2, sort_keys=True).encode()
    relative_path = "tmp/fixture-runtime-review.log"
    path = fixture.plan.data_root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)

    evidence = _load_runtime_review_evidence(
        fixture.plan.data_root,
        maps,
        authorization,
        expected_path=relative_path,
        expected_sha256=hashlib.sha256(content).hexdigest(),
        expected_bytes=len(content),
    )

    assert evidence.warning_messages == (
        "process RSS exceeded the reviewed 0.75 GiB estimate",
    )
    assert evidence.observed_max_rss_bytes is None
    assert evidence.qa_warning_counts_by_table == {
        table: sum(
            check.status.value == "warning"
            for check in full.table_runs[table].build.qa_checks
        )
        for table in TABLES
    }


@pytest.mark.parametrize("drift", ("version_rows", "qa_partition", "data_partition"))
def test_asset_publish_scope_validator_rejects_s4_semantic_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    fixture, full, _arguments = _full_ready_fixture(tmp_path, monkeypatch)
    table = TABLES[0]
    run = full.table_runs[table]
    build = run.build
    if drift == "version_rows":
        build = replace(
            build,
            row_funnel=replace(
                build.row_funnel,
                version_preserved_rows=build.row_funnel.version_preserved_rows + 1,
            ),
        )
    elif drift == "qa_partition":
        changed_check = replace(build.qa_checks[0], partition_key="full_history:wrong")
        build = replace(build, qa_checks=(changed_check, *build.qa_checks[1:]))
    else:
        data_index = next(
            index
            for index, artifact in enumerate(build.outputs)
            if artifact.role.value == "data"
        )
        changed_outputs = list(build.outputs)
        changed_outputs[data_index] = replace(
            changed_outputs[data_index],
            path=changed_outputs[data_index].path.replace(
                "session_date=2026-05-11",
                "session_date=2026-05-10",
            ),
        )
        build = replace(build, outputs=tuple(changed_outputs))

    with pytest.raises(SilverStoreError):
        _validate_s4_full_build_scope(
            build,
            contract=publish_module._CONTRACTS_BY_TABLE[table],
            workflow_id=fixture.plan.workflow_ids[table],
            full_plan=run.plan,
            full_plan_source_digest=run.plan.source_digest,
            authorization=fixture.plan.authorization,
            expected_session_dates=("2026-05-11", "2026-05-12"),
        )


def test_asset_publish_plan_workflow_race_leaves_no_orphan_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _full, arguments = _full_ready_fixture(tmp_path, monkeypatch)
    table = TABLES[0]

    def advance_before_locks() -> None:
        fixture.plan.store.request_publish(
            fixture.plan.workflow_ids[table],
            expected_event_sha256=arguments["full_ready_event_sha256_by_table"][table],
            actor="fixture-racer",
            created_at="2099-01-04T00:00:00+00:00",
        )

    arguments["before_final_lock"] = advance_before_locks
    with pytest.raises(SilverStoreError, match="changed before publish-plan commit"):
        _create_asset_publish_plan(fixture.plan.data_root, **arguments)

    assert not (
        fixture.plan.data_root / "manifests/silver/publish-plans/assets"
    ).exists()


def test_git_verifier_rejects_dirty_checkout_and_wrong_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    module_path = repo / "backend/ame_stocks_api/silver/asset_publish_plan.py"
    module_path.parent.mkdir(parents=True)
    module_path.write_text("# fixture\n")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "fixture@example.com")
    _git(repo, "config", "user.name", "Fixture")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "fixture")
    commit = _git(repo, "rev-parse", "HEAD")
    monkeypatch.setattr(publish_module, "__file__", str(module_path))

    _verify_git_checkout(repo, commit, commit)
    with pytest.raises(SilverStoreError, match="HEAD differs"):
        _verify_git_checkout(repo, "0" * 40, commit)
    module_path.write_text("# dirty\n")
    with pytest.raises(SilverStoreError, match="not clean"):
        _verify_git_checkout(repo, commit, commit)


def test_git_logic_closure_rejects_direct_dependency_drift(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    dependency = repo / "backend/ame_stocks_api/silver/reader.py"
    dependency.parent.mkdir(parents=True)
    dependency.write_text("VERSION = 1\n")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "fixture@example.com")
    _git(repo, "config", "user.name", "Fixture")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "materialization")
    materialization = _git(repo, "rev-parse", "HEAD")
    dependency.write_text("VERSION = 2\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "drift")
    orchestration = _git(repo, "rev-parse", "HEAD")

    with pytest.raises(SilverStoreError, match="materialization logic changed"):
        _verify_reviewed_logic_closure(repo, materialization, orchestration)


@pytest.mark.parametrize(
    "mapping_name",
    (
        "workflow_ids_by_table",
        "full_ready_event_sha256_by_table",
        "build_ids_by_table",
        "build_manifest_sha256_by_table",
        "full_run_plan_ids_by_table",
        "full_run_plan_sha256_by_table",
    ),
)
def test_asset_publish_plan_rejects_every_exact_binding_before_writing_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mapping_name: str,
) -> None:
    fixture, _full, arguments = _full_ready_fixture(tmp_path, monkeypatch)
    changed = dict(arguments[mapping_name])
    changed[TABLES[1]] = "f" * 64
    arguments[mapping_name] = changed

    with pytest.raises((SilverStoreError, OSError)):
        _create_asset_publish_plan(fixture.plan.data_root, **arguments)

    assert not (
        fixture.plan.data_root / "manifests/silver/publish-plans/assets"
    ).exists()
    assert all(
        fixture.plan.store.status(fixture.plan.workflow_ids[table]).state
        is WorkflowState.FULL_READY
        for table in TABLES
    )


def test_asset_publish_plan_schema_rejects_quarantine_and_digest_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _full, arguments = _full_ready_fixture(tmp_path, monkeypatch)
    run = _create_asset_publish_plan(fixture.plan.data_root, **arguments)

    with pytest.raises(SilverContractError, match="quarantine"):
        replace(run.plan.tables[0], quarantine_issue_rows=1)
    changed = run.plan.to_dict()
    changed["publication_scope"] = "permanent_asset_master"
    with pytest.raises(SilverContractError, match="scope"):
        AssetPublishPlan.from_dict(changed)
    changed = run.plan.to_dict()
    changed["plan_id"] = "0" * 64
    with pytest.raises(SilverContractError, match="digest"):
        AssetPublishPlan.from_dict(changed)
    changed = run.plan.to_dict()
    changed["runtime_review"]["warning_messages"] = []  # type: ignore[index]
    with pytest.raises(SilverContractError, match="RSS warning"):
        AssetPublishPlan.from_dict(changed)


def test_asset_publish_plan_cli_is_review_only_and_prints_exact_warnings(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    warning = SimpleNamespace(
        result_id="a" * 64,
        check=SimpleNamespace(
            bounded_examples_path=None,
            check_id="fixture_warning",
            denominator=10,
            numerator=1,
            partition_key="full_history:2020-01-01:2020-01-02",
            rate=0.1,
            severity=SimpleNamespace(value="medium"),
            status=SimpleNamespace(value="warning"),
            threshold="numerator == 0",
        ),
    )
    table_items = tuple(
        SimpleNamespace(
            table=table,
            accepted_quarantine_issue_ids=(),
            build_id="b" * 64,
            build_manifest_sha256="c" * 64,
            date_end="2026-07-09",
            date_start="2016-07-11",
            full_ready_event_sha256="d" * 64,
            full_run_plan_id="e" * 64,
            full_run_plan_sha256="f" * 64,
            input_manifest_count=5_026,
            input_page_count=72_038,
            input_rows=69_381_182,
            input_session_count=2_513,
            output_data_bytes=123,
            output_data_partition_count=2,
            output_rows=10,
            warnings=(warning,),
            workflow_id="1" * 64,
        )
        for table in TABLES
    )
    fake = SimpleNamespace(
        idempotent=False,
        document=SimpleNamespace(path="plan.json", sha256="2" * 64),
        plan=SimpleNamespace(
            backtest_identity_eligible=False,
            plan_id="3" * 64,
            publication_scope=ASSET_PUBLICATION_SCOPE,
            requires_release_set=True,
            requires_runtime_review_acceptance=True,
                runtime_review=SimpleNamespace(
                    completed_sessions=2_513,
                    evidence_limitation=(
                        "exact_process_max_rss_bytes_not_persisted_by_asset-full-v1"
                    ),
                expected_rss_ceiling_bytes=805_306_368,
                hard_rss_limit_bytes=2_147_483_648,
                    observed_max_rss_bytes=None,
                    qa_warning_counts_by_table={
                        table: 1 for table in TABLES
                    },
                    rss_review_status="estimate_exceeded_exact_peak_unavailable",
                source_bytes=5_649,
                source_path="tmp/s4-assets-full-adc28b5/full-run.log",
                source_sha256="6" * 64,
                warning_messages=(
                    "process RSS exceeded the reviewed 0.75 GiB estimate",
                ),
            ),
            tables=table_items,
        ),
    )
    monkeypatch.setattr(cli_module, "create_asset_publish_plan", lambda *a, **k: fake)
    arguments = [
        "--data-root",
        "/data",
        "--repo-root",
        "/repo",
        "--orchestration-git-commit",
        "4" * 40,
    ]
    for table in TABLES:
        option = table.replace("_", "-")
        for suffix in (
            "workflow-id",
            "full-ready-event-sha256",
            "build-id",
            "build-manifest-sha256",
            "full-run-plan-id",
            "full-run-plan-sha256",
        ):
            arguments.extend((f"--{option}-{suffix}", "5" * 64))

    assert cli_module.main(arguments) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["mode"] == "publish_plan_review_only"
    assert output["workflow_mutated"] is False
    assert output["requires_release_set"] is True
    assert output["requires_runtime_review_acceptance"] is True
    assert {item["state"] for item in output["tables"].values()} == {"full_ready"}
    assert all(item["warning_count"] == 1 for item in output["tables"].values())
    assert "release_id" not in output


def test_asset_publish_plan_help_denies_every_release_mutation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        cli_module.main(["--help"])
    assert exc.value.code == 0
    output = " ".join(capsys.readouterr().out.split())
    assert "cannot approve warnings" in output
    assert "request publication" in output
    assert "publish a table" in output
    assert "create a release" in output
