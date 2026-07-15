from __future__ import annotations

import json
import subprocess
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from ame_stocks_api.cli import silver_identity_preview_request as cli
from ame_stocks_api.silver import identity_preview_request as request_module
from ame_stocks_api.silver.identity_preview_plan import (
    IdentityPreviewPlanError,
    S7DetectorPreviewResourceCaps,
)
from ame_stocks_api.silver.identity_preview_request import (
    FULL_XNYS_CALENDAR_END,
    FULL_XNYS_CALENDAR_START,
    S7SelectedPreviewSource,
    create_s7_detector_preview_request,
)
from ame_stocks_api.silver.identity_source import S7_SOURCE_PINS

TICKERS = (
    "AAPL",
    "AULT",
    "AZPN",
    "BOC",
    "BRK.B",
    "CEG",
    "CIBR",
    "CMS",
    "CR",
    "DPW",
    "FLOW",
    "GPUS",
    "KNTK",
    "MSFT",
    "NILE",
    "RCM",
    "SBGI",
    "SIRI",
    "SPY",
    "SWI",
    "TA",
    "TBLT",
    "TNXP",
    "VG",
    "WW",
)
START = date(2022, 2, 1)
END = date(2022, 3, 8)
RECORDED_AT = "2024-01-12T18:00:00+00:00"


def _caps(**overrides: int) -> S7DetectorPreviewResourceCaps:
    values = {
        "selected_row_cap": 625,
        "universe_scanned_row_cap": 735_884,
        "asset_parent_scanned_row_cap": 735_884,
        "total_scanned_row_cap": 1_471_768,
        "source_artifact_cap": 50,
        "source_bytes_cap": 168_141_801,
        "case_cap": 100,
        "batch_size": 8_192,
    }
    values.update(overrides)
    return S7DetectorPreviewResourceCaps(**values)


def _git(repo: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(repo), *arguments),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    for relative in request_module._TRACKED_ENTRYPOINTS:
        target = repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"# tracked fixture: {relative}\n", encoding="utf-8")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "tests@example.invalid")
    _git(repo, "config", "user.name", "Tests")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "fixture")
    monkeypatch.setattr(
        request_module,
        "__file__",
        str(repo / "backend/ame_stocks_api/silver/identity_preview_request.py"),
    )
    return repo, _git(repo, "rev-parse", "HEAD")


def _selected_sources() -> tuple[S7SelectedPreviewSource, ...]:
    sessions = tuple(
        item.session_date
        for item in request_module.build_xnys_calendar_artifact(
            FULL_XNYS_CALENDAR_START,
            FULL_XNYS_CALENDAR_END,
        ).sessions
        if START <= item.session_date <= END
    )
    assert len(sessions) == 25
    rows = [29_435] * 24 + [29_444]
    asset_bytes = [3_362_836] * 24 + [3_362_836]
    universe_bytes = [3_362_836] * 24 + [3_362_837]
    output: list[S7SelectedPreviewSource] = []
    for table, byte_counts in (
        ("asset_observation_daily", asset_bytes),
        ("universe_source_daily", universe_bytes),
    ):
        pin = S7_SOURCE_PINS[table]
        output.extend(
            S7SelectedPreviewSource(
                table=table,
                session_date=session,
                release_id=pin.release_id,
                release_manifest_sha256=pin.release_manifest_sha256,
                path=f"silver/{table}/session_date={session.isoformat()}/part.parquet",
                sha256=f"{index + 1:064x}",
                bytes=byte_count,
                row_count=row_count,
            )
            for index, (session, byte_count, row_count) in enumerate(
                zip(sessions, byte_counts, rows, strict=True)
            )
        )
    result = tuple(sorted(output))
    assert sum(item.row_count for item in result if item.table.startswith("asset")) == 735_884
    assert sum(item.row_count for item in result if item.table.startswith("universe")) == 735_884
    assert sum(item.bytes for item in result) == 168_141_801
    return result


def _run(
    data_root: Path,
    repo: Path,
    head: str,
) -> request_module.S7DetectorPreviewRequestRun:
    return create_s7_detector_preview_request(
        data_root,
        repo_root=repo,
        git_commit=head,
        recorded_at=RECORDED_AT,
        plan_created_by="s7-preview-plan-author",
        request_created_by="s7-preview-approval-request-author",
        tickers=TICKERS,
        expected_ticker_count=25,
        start_session=START,
        end_session=END,
        expected_session_count=25,
        resource_caps=_caps(),
    )


def test_plan_request_orchestration_writes_only_four_control_documents(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()
    selected = _selected_sources()
    monkeypatch.setattr(request_module, "_preflight_source_manifests", lambda *a, **k: selected)

    first = _run(data_root, repo, head)
    first_mtimes = {
        path.relative_to(data_root).as_posix(): path.stat().st_mtime_ns
        for path in data_root.rglob("*")
        if path.is_file()
    }
    second = _run(data_root, repo, head)
    second_mtimes = {
        path.relative_to(data_root).as_posix(): path.stat().st_mtime_ns
        for path in data_root.rglob("*")
        if path.is_file()
    }

    assert first.all_documents_preexisting is False
    assert second.all_documents_preexisting is True
    assert first.calendar.start_session == FULL_XNYS_CALENDAR_START
    assert first.calendar.end_session == FULL_XNYS_CALENDAR_END
    assert first.plan.session_count == 25
    assert first.plan.ticker_count == 25
    assert first.plan.resource_caps.to_dict() == _caps().to_dict()
    assert first.selected_sources == selected
    assert first.approval_request.request_state == "awaiting_literal_human_approval"
    assert first.approval_request.canonical_approval_literal == (
        second.approval_request.canonical_approval_literal
    )
    assert second_mtimes == first_mtimes
    files = sorted(
        path.relative_to(data_root).as_posix()
        for path in data_root.rglob("*")
        if path.is_file()
    )
    assert files == sorted(
        (
            first.calendar_document.path,
            first.ticker_allowlist_document.path,
            first.plan_document.path,
            first.approval_request_document.path,
        )
    )
    assert not (data_root / "manifests/silver/identity/detector-preview-plan-approvals").exists()
    assert not (data_root / "manifests/silver/identity/detector-preview-completions").exists()
    assert not (data_root / "manifests/silver/identity/provider-evidence").exists()
    assert not (data_root / "manifests/silver/identity/candidate-manifests").exists()
    assert not (data_root / "manifests/silver/identity/adjudications").exists()
    assert not (data_root / "staging").exists()
    assert not (data_root / "silver").exists()


def test_cli_prints_exact_literal_and_explicit_false_execution_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()
    selected = _selected_sources()
    monkeypatch.setattr(request_module, "_preflight_source_manifests", lambda *a, **k: selected)

    arguments = [
        "--data-root",
        str(data_root),
        "--repo-root",
        str(repo),
        "--git-commit",
        head,
        "--recorded-at",
        RECORDED_AT,
        "--plan-created-by",
        "s7-preview-plan-author",
        "--request-created-by",
        "s7-preview-approval-request-author",
        "--expected-ticker-count",
        "25",
        "--start-session",
        START.isoformat(),
        "--end-session",
        END.isoformat(),
        "--expected-session-count",
        "25",
        "--selected-row-cap",
        "625",
        "--universe-scanned-row-cap",
        "735884",
        "--asset-parent-scanned-row-cap",
        "735884",
        "--total-scanned-row-cap",
        "1471768",
        "--source-artifact-cap",
        "50",
        "--source-bytes-cap",
        "168141801",
        "--case-cap",
        "100",
        "--batch-size",
        "8192",
    ]
    for ticker in TICKERS:
        arguments.extend(("--ticker", ticker))

    assert cli.main(arguments) == 0
    output = json.loads(capsys.readouterr().out)

    assert output["mode"] == "plan_and_approval_request_only"
    assert output["approval_created"] is False
    assert output["detector_preview_executed"] is False
    assert output["candidate_artifacts_created"] is False
    assert output["publication_executed"] is False
    assert output["plan"]["session_count"] == 25
    assert output["ticker_allowlist"]["tickers"] == list(TICKERS)
    assert len(output["selected_sources"]) == 50
    literal = json.loads(output["approval_request"]["canonical_approval_literal"])
    assert literal["plan_id"] == output["plan"]["plan_id"]
    assert literal["plan_sha256"] == output["plan"]["sha256"]
    assert literal["request_event_id"] == output["approval_request"]["request_event_id"]
    assert literal["request_event_sha256"] == output["approval_request"]["sha256"]
    assert literal["resource_caps_digest"] == output["plan"]["resource_caps_digest"]
    assert literal["authorized_action"] == output["approval_request"]["authorized_action"]
    assert not (data_root / "manifests/silver/identity/detector-preview-plan-approvals").exists()


def test_all_input_and_source_preflights_happen_before_control_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()
    monkeypatch.setattr(
        request_module,
        "_preflight_source_manifests",
        lambda *a, **k: (_ for _ in ()).throw(IdentityPreviewPlanError("source mismatch")),
    )

    with pytest.raises(IdentityPreviewPlanError, match="source mismatch"):
        _run(data_root, repo, head)

    assert list(data_root.rglob("*")) == []


def test_rejects_dirty_wrong_head_future_time_and_inexact_selected_cap_before_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()
    selected = _selected_sources()
    monkeypatch.setattr(request_module, "_preflight_source_manifests", lambda *a, **k: selected)

    with pytest.raises(IdentityPreviewPlanError, match="HEAD differs"):
        create_s7_detector_preview_request(
            data_root,
            repo_root=repo,
            git_commit="f" * 40,
            recorded_at=RECORDED_AT,
            plan_created_by="planner",
            request_created_by="requester",
            tickers=TICKERS,
            expected_ticker_count=25,
            start_session=START,
            end_session=END,
            expected_session_count=25,
            resource_caps=_caps(),
        )
    (repo / "dirty.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(IdentityPreviewPlanError, match="not clean"):
        _run(data_root, repo, head)
    (repo / "dirty.txt").unlink()
    with pytest.raises(IdentityPreviewPlanError, match="future"):
        create_s7_detector_preview_request(
            data_root,
            repo_root=repo,
            git_commit=head,
            recorded_at="2999-01-01T00:00:00+00:00",
            plan_created_by="planner",
            request_created_by="requester",
            tickers=TICKERS,
            expected_ticker_count=25,
            start_session=START,
            end_session=END,
            expected_session_count=25,
            resource_caps=_caps(),
        )
    with pytest.raises(IdentityPreviewPlanError, match="selected_row_cap"):
        create_s7_detector_preview_request(
            data_root,
            repo_root=repo,
            git_commit=head,
            recorded_at=RECORDED_AT,
            plan_created_by="planner",
            request_created_by="requester",
            tickers=TICKERS,
            expected_ticker_count=25,
            start_session=START,
            end_session=END,
            expected_session_count=25,
            resource_caps=_caps(selected_row_cap=624),
        )
    assert list(data_root.rglob("*")) == []


def test_manifest_only_preflight_requires_exact_pins_and_exact_caps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "data"
    root.mkdir()
    calendar = request_module.build_xnys_calendar_artifact(
        FULL_XNYS_CALENDAR_START,
        FULL_XNYS_CALENDAR_END,
    )
    all_sessions = tuple(item.session_date for item in calendar.sessions[:2_513])
    selected_sessions = tuple(item for item in all_sessions if START <= item <= END)
    assert len(selected_sessions) == 25
    selected_rows = [29_435] * 24 + [29_444]
    selected_bytes_by_table = {
        "asset_observation_daily": [3_362_836] * 25,
        "universe_source_daily": [3_362_836] * 24 + [3_362_837],
    }
    releases: dict[str, object] = {}
    stored: dict[str, object] = {}
    for table in request_module._PREVIEW_SOURCE_TABLES:
        pin = S7_SOURCE_PINS[table]
        remaining_rows = pin.row_count - sum(selected_rows)
        filler_count = pin.artifact_count - len(selected_sessions)
        filler_base, filler_extra = divmod(remaining_rows, filler_count)
        selected_row_map = dict(zip(selected_sessions, selected_rows, strict=True))
        outputs = []
        filler_index = 0
        selected_index = 0
        for index, session in enumerate(all_sessions):
            if session in selected_row_map:
                row_count = selected_row_map[session]
                byte_count = selected_bytes_by_table[table][selected_index]
                selected_index += 1
            else:
                row_count = filler_base + (filler_index < filler_extra)
                byte_count = 1
                filler_index += 1
            outputs.append(
                SimpleNamespace(
                    table=table,
                    role=request_module.ArtifactRole.DATA,
                    media_type="application/vnd.apache.parquet",
                    row_count=row_count,
                    path=(
                        f"silver/{table}/session_date={session.isoformat()}/part.parquet"
                    ),
                    sha256=f"{index + 1:064x}",
                    bytes=byte_count,
                )
            )
        release = SimpleNamespace(
            release_id=pin.release_id,
            table=table,
            build_id=pin.build_id,
            outputs=tuple(outputs),
        )
        manifest_relative = f"manifests/silver/releases/release_id={pin.release_id}.json"
        manifest_path = root / manifest_relative
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text("fixture\n", encoding="utf-8")
        releases[pin.release_id] = release
        stored[pin.release_id] = SimpleNamespace(
            path=manifest_relative,
            sha256=pin.release_manifest_sha256,
        )

    class FakeStore:
        def __init__(self, data_root: Path) -> None:
            assert data_root == root

        def load_release(self, release_id: str):
            return releases[release_id], stored[release_id]

    monkeypatch.setattr(request_module, "SilverStore", FakeStore)
    monkeypatch.setattr(
        request_module,
        "sha256_file",
        lambda path: S7_SOURCE_PINS[
            "asset_observation_daily"
            if S7_SOURCE_PINS["asset_observation_daily"].release_id in path.name
            else "universe_source_daily"
        ].release_manifest_sha256,
    )

    selected = request_module._preflight_source_manifests(
        root,
        sessions=selected_sessions,
        resource_caps=_caps(),
    )
    assert len(selected) == 50
    assert sum(item.row_count for item in selected) == 1_471_768
    assert sum(item.bytes for item in selected) == 168_141_801
    with pytest.raises(IdentityPreviewPlanError, match="source_bytes_cap"):
        request_module._preflight_source_manifests(
            root,
            sessions=selected_sessions,
            resource_caps=_caps(source_bytes_cap=168_141_802),
        )


def test_plan_request_entrypoints_have_no_approval_or_runner_capability() -> None:
    root = Path(__file__).parents[1]
    for relative in (
        "backend/ame_stocks_api/silver/identity_preview_request.py",
        "backend/ame_stocks_api/cli/silver_identity_preview_request.py",
    ):
        source = (root / relative).read_text(encoding="utf-8")
        assert "S7DetectorPreviewPlanApproval" not in source
        assert "identity_preview_runner" not in source
        assert "store_approval(" not in source
        assert "run_source_bound_identity" not in source
    parser_options = {
        option
        for action in cli.build_parser()._actions
        for option in action.option_strings
    }
    assert not any("approval" in option for option in parser_options)
    assert not any("run" in option or "execute" in option for option in parser_options)
