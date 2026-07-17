from __future__ import annotations

import ast
import inspect
import json
from pathlib import Path

import pytest

from ame_stocks_api.cli import silver_identity_market_inventory_request as cli
from ame_stocks_api.silver import identity_market_inventory_request as request_module
from ame_stocks_api.silver.calendar_artifact import (
    build_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.identity_market_inventory_plan import (
    APPROVAL_TEXT,
    APPROVAL_TEXT_SHA256,
    DAILY_SOURCE_ARTIFACT_COUNT,
    DAILY_SOURCE_BYTES,
    DAILY_SOURCE_ROW_COUNT,
    IdentityMarketInventoryPlanError,
    S7CompositeInventoryApprovalRequest,
    S7CompositeInventoryPlan,
    S7SchemaEvidenceApprovalBundle,
    StoredIdentityMarketInventoryDocument,
)
from ame_stocks_api.silver.identity_market_inventory_request import (
    FULL_XNYS_CALENDAR_END,
    FULL_XNYS_CALENDAR_START,
    PRIOR_PREVIEW_COMPLETION_PATH,
    S7PreviewLineagePreflight,
    S7ReleaseManifestPreflight,
    create_s7_market_inventory_request,
)

GIT_COMMIT = "a" * 40
RECORDED_AT = "2026-07-16T16:00:00+00:00"
APPROVAL_RECORDED_BY = "s7-schema-evidence-approval-recorder"
PLAN_CREATED_BY = "s7-composite-inventory-plan-author"
REQUEST_CREATED_BY = "s7-composite-inventory-request-author"


def _release_preflight() -> S7ReleaseManifestPreflight:
    return S7ReleaseManifestPreflight(
        release_manifest_count=6,
        daily_artifact_count=DAILY_SOURCE_ARTIFACT_COUNT,
        daily_row_count=DAILY_SOURCE_ROW_COUNT,
        daily_source_bytes=DAILY_SOURCE_BYTES,
    )


def _lineage_preflight() -> S7PreviewLineagePreflight:
    return S7PreviewLineagePreflight(
        completion_path=PRIOR_PREVIEW_COMPLETION_PATH,
        provider_evidence_manifest_count=19,
        case_evidence_set_digest=request_module.PREVIEW_CASE_EVIDENCE_SET_DIGEST,
    )


def _patch_exact_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, object]:
    repo = tmp_path / "repo"
    repo.mkdir()
    data_root = tmp_path / "data"
    data_root.mkdir()
    calendar = build_xnys_calendar_artifact(
        FULL_XNYS_CALENDAR_START,
        FULL_XNYS_CALENDAR_END,
    )
    write_xnys_calendar_artifact(data_root, calendar)

    monkeypatch.setattr(
        request_module,
        "_verify_git_checkout",
        lambda repo_root, git_commit: repo_root.resolve(),
    )
    monkeypatch.setattr(
        request_module,
        "_preflight_schema_and_external_evidence",
        lambda repo_root: (),
    )
    monkeypatch.setattr(
        request_module,
        "_verify_approved_subject_paths",
        lambda repo_root, paths: None,
    )
    monkeypatch.setattr(
        request_module,
        "_load_exact_existing_calendar",
        lambda root: calendar,
    )
    monkeypatch.setattr(
        request_module,
        "_preflight_release_manifests",
        lambda root, bound_calendar: _release_preflight(),
    )
    monkeypatch.setattr(
        request_module,
        "_preflight_existing_preview_lineage",
        lambda root, bound_calendar: _lineage_preflight(),
    )
    return repo, data_root, calendar


def _run(data_root: Path, repo: Path) -> request_module.S7MarketInventoryRequestRun:
    return create_s7_market_inventory_request(
        data_root,
        repo_root=repo,
        git_commit=GIT_COMMIT,
        recorded_at=RECORDED_AT,
        approval_recorded_by=APPROVAL_RECORDED_BY,
        plan_created_by=PLAN_CREATED_BY,
        request_created_by=REQUEST_CREATED_BY,
        approval_text_sha256=APPROVAL_TEXT_SHA256,
    )


def _files(root: Path) -> list[str]:
    return sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
    )


def test_orchestration_writes_exactly_three_control_json_documents_idempotently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, data_root, calendar = _patch_exact_environment(tmp_path, monkeypatch)

    first = _run(data_root, repo)
    first_mtimes = {
        path.relative_to(data_root).as_posix(): path.stat().st_mtime_ns
        for path in data_root.rglob("*")
        if path.is_file()
    }
    second = _run(data_root, repo)
    second_mtimes = {
        path.relative_to(data_root).as_posix(): path.stat().st_mtime_ns
        for path in data_root.rglob("*")
        if path.is_file()
    }

    assert first.all_documents_preexisting is False
    assert second.all_documents_preexisting is True
    assert second_mtimes == first_mtimes
    assert first.schema_approval.recorded_by == APPROVAL_RECORDED_BY
    assert first.plan.created_by == PLAN_CREATED_BY
    assert first.approval_request.created_by == REQUEST_CREATED_BY
    assert first.plan.plan_state == "awaiting_exact_plan_approval"
    assert first.approval_request.request_state == "awaiting_literal_human_approval"
    assert first.plan == second.plan
    assert first.approval_request == second.approval_request

    assert _files(data_root) == sorted(
        (
            calendar.relative_path,
            first.schema_approval_document.path,
            first.plan_document.path,
            first.approval_request_document.path,
        )
    )
    assert first.schema_approval_document.path.startswith(
        "manifests/silver/identity/schema-evidence-approval-bundles/"
    )
    assert first.plan_document.path.startswith(
        "manifests/silver/identity/composite-inventory-plans/"
    )
    assert first.approval_request_document.path.startswith(
        "manifests/silver/identity/composite-inventory-approval-requests/"
    )
    assert not (data_root / "staging").exists()
    assert not (data_root / "silver").exists()
    assert not (data_root / "manifests/silver/identity/adjudications").exists()
    assert not (data_root / "manifests/silver/releases").exists()


def test_every_preflight_finishes_before_any_new_control_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, data_root, calendar = _patch_exact_environment(tmp_path, monkeypatch)
    baseline = _files(data_root)
    monkeypatch.setattr(
        request_module,
        "_preflight_release_manifests",
        lambda root, bound_calendar: (_ for _ in ()).throw(
            IdentityMarketInventoryPlanError("release mismatch")
        ),
    )

    with pytest.raises(IdentityMarketInventoryPlanError, match="release mismatch"):
        _run(data_root, repo)

    assert _files(data_root) == baseline == [calendar.relative_path]


def test_dirty_git_or_wrong_approval_digest_fails_with_zero_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    data_root = tmp_path / "data"
    data_root.mkdir()
    monkeypatch.setattr(
        request_module,
        "_verify_git_checkout",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            IdentityMarketInventoryPlanError("checkout is not clean")
        ),
    )

    with pytest.raises(IdentityMarketInventoryPlanError, match="not clean"):
        _run(data_root, repo)
    assert _files(data_root) == []

    monkeypatch.setattr(
        request_module,
        "_verify_git_checkout",
        lambda repo_root, git_commit: repo_root.resolve(),
    )
    with pytest.raises(IdentityMarketInventoryPlanError, match="approval text SHA-256"):
        create_s7_market_inventory_request(
            data_root,
            repo_root=repo,
            git_commit=GIT_COMMIT,
            recorded_at=RECORDED_AT,
            approval_recorded_by=APPROVAL_RECORDED_BY,
            plan_created_by=PLAN_CREATED_BY,
            request_created_by=REQUEST_CREATED_BY,
            approval_text_sha256="f" * 64,
        )
    assert _files(data_root) == []


def test_conflicting_destination_is_detected_before_any_other_document_is_written(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, data_root, calendar = _patch_exact_environment(tmp_path, monkeypatch)
    instant = request_module._recorded_at(RECORDED_AT)
    approval = S7SchemaEvidenceApprovalBundle.create(
        recorded_by=APPROVAL_RECORDED_BY,
        recorded_at_utc=instant,
        exact_approval_text=APPROVAL_TEXT,
    )
    approval_receipt = StoredIdentityMarketInventoryDocument(
        approval.relative_path,
        approval.sha256,
        len(approval.content),
    )
    plan = S7CompositeInventoryPlan.create(
        created_by=PLAN_CREATED_BY,
        created_at_utc=instant,
        git_commit=GIT_COMMIT,
        approval=approval,
        approval_receipt=approval_receipt,
        calendar_artifact_id=calendar.calendar_artifact_id,
        calendar_artifact_sha256=calendar.sha256,
    )
    plan_receipt = StoredIdentityMarketInventoryDocument(
        plan.relative_path,
        plan.sha256,
        len(plan.content),
    )
    request = S7CompositeInventoryApprovalRequest.create(
        plan,
        plan_receipt,
        created_by=REQUEST_CREATED_BY,
        created_at_utc=instant,
    )
    conflicting = data_root / plan.relative_path
    conflicting.parent.mkdir(parents=True)
    conflicting.write_bytes(b"conflicting immutable bytes\n")

    with pytest.raises(IdentityMarketInventoryPlanError, match="conflicting bytes"):
        _run(data_root, repo)

    assert not (data_root / approval.relative_path).exists()
    assert not (data_root / request.relative_path).exists()
    assert _files(data_root) == sorted((calendar.relative_path, plan.relative_path))


def test_repository_schema_and_external_evidence_bytes_replay_exactly() -> None:
    repo = Path(__file__).resolve().parents[1]
    paths = request_module._preflight_schema_and_external_evidence(repo)

    assert len(request_module.APPROVED_CONTRACT_PINS) == 6
    assert request_module.EXTERNAL_EVIDENCE_MANIFEST_PATH in paths
    assert len(paths) == 38
    assert all((repo / path).is_file() for path in paths)
    request_module._verify_approved_subject_paths(repo, paths)


def test_existing_exact_calendar_is_verified_without_being_rewritten(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    calendar = build_xnys_calendar_artifact(
        FULL_XNYS_CALENDAR_START,
        FULL_XNYS_CALENDAR_END,
    )
    write_xnys_calendar_artifact(data_root, calendar)
    path = data_root / calendar.relative_path
    before = path.stat().st_mtime_ns

    loaded = request_module._load_exact_existing_calendar(data_root)

    assert loaded == calendar
    assert len(loaded.sessions) == 2_635
    assert path.stat().st_mtime_ns == before
    assert _files(data_root) == [calendar.relative_path]


def test_cli_surface_has_only_control_inputs_and_no_execution_or_api_knobs() -> None:
    parser = cli.build_parser()
    option_strings = {
        option
        for action in parser._actions
        for option in action.option_strings
        if option not in {"-h", "--help"}
    }
    assert option_strings == {
        "--approval-recorded-by",
        "--approval-text-sha256",
        "--data-root",
        "--git-commit",
        "--plan-created-by",
        "--recorded-at",
        "--repo-root",
        "--request-created-by",
    }
    forbidden = {
        "--active",
        "--api-key",
        "--approve",
        "--batch-size",
        "--cap",
        "--date",
        "--end-date",
        "--execute",
        "--locale",
        "--market",
        "--release-id",
        "--run",
        "--source",
        "--start-date",
        "--ticker",
        "--worker-count",
    }
    assert option_strings.isdisjoint(forbidden)


def test_request_module_has_no_parquet_network_or_runner_import_capability() -> None:
    source = inspect.getsource(request_module)
    tree = ast.parse(source)
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots.isdisjoint(
        {"boto3", "httpx", "pandas", "polars", "pyarrow", "requests"}
    )
    assert "run_source_bound_identity_streaming_preview" not in source
    assert "open_identity_source_bundle" not in source
    assert "write_xnys_calendar_artifact" not in source


def test_cli_prints_canonical_literal_and_all_execution_flags_false(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, data_root, _ = _patch_exact_environment(tmp_path, monkeypatch)
    arguments = [
        "--data-root",
        str(data_root),
        "--repo-root",
        str(repo),
        "--git-commit",
        GIT_COMMIT,
        "--recorded-at",
        RECORDED_AT,
        "--approval-recorded-by",
        APPROVAL_RECORDED_BY,
        "--plan-created-by",
        PLAN_CREATED_BY,
        "--request-created-by",
        REQUEST_CREATED_BY,
        "--approval-text-sha256",
        APPROVAL_TEXT_SHA256,
    ]

    assert cli.main(arguments) == 0
    output = json.loads(capsys.readouterr().out)

    assert output["mode"] == "schema_evidence_receipt_and_inventory_plan_request_only"
    assert output["approval_text"] == APPROVAL_TEXT
    assert output["approval_text_sha256"] == APPROVAL_TEXT_SHA256
    assert output["calendar"]["written_by_this_command"] is False
    assert output["release_manifest_preflight"] == {
        "daily_artifact_count": DAILY_SOURCE_ARTIFACT_COUNT,
        "daily_row_count": DAILY_SOURCE_ROW_COUNT,
        "daily_source_bytes": DAILY_SOURCE_BYTES,
        "release_manifest_count": 6,
    }
    assert set(output["authorization_flags"].values()) == {False}
    assert set(output["execution_results"].values()) == {False}
    assert output["authorization_flags"][
        "identity_market_consistency_scan_authorized"
    ] is False
    assert output["authorization_flags"]["adjudication_authorized"] is False
    assert output["execution_results"][
        "identity_market_consistency_scan_executed"
    ] is False
    assert output["execution_results"]["adjudication_created"] is False
    assert output["execution_results"]["adjudication_executed"] is False

    literal = json.loads(output["request"]["canonical_approval_literal"])
    assert literal["authorized_action"] == output["request"]["authorized_action"]
    assert literal["plan_id"] == output["plan"]["plan_id"]
    assert literal["plan_sha256"] == output["plan"]["sha256"]
    assert literal["request_event_id"] == output["request"]["request_event_id"]
    assert literal["request_event_sha256"] == output["request"]["sha256"]
    assert literal["resource_caps_digest"] == output["plan"]["resource_caps_digest"]
