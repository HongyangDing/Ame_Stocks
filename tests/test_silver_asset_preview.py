from __future__ import annotations

import gzip
import hashlib
import json
import stat
import subprocess
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from ame_stocks_api.silver import asset_preview as preview_module
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    ASSET_OBSERVATION_VERSION_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.asset_source import AssetSourceError
from ame_stocks_api.silver.contracts import ArtifactRole, BuildKind, UpstreamManifestRef
from ame_stocks_api.silver.store import SilverStore, SilverStoreError, WorkflowState

SESSION_DATE = date(2026, 5, 11)
ACTIVE_CAPTURE = datetime(2026, 7, 11, 14, 3, 15, tzinfo=UTC)
INACTIVE_CAPTURE = datetime(2026, 7, 11, 14, 4, 15, tzinfo=UTC)

CONTRACTS = (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    ASSET_OBSERVATION_VERSION_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)


def test_production_asset_preview_authorization_is_exactly_pinned() -> None:
    active_request = "9e1ab3e3c1d4c09ea91e346c8eaeaf07279b698b1f1d8ae14c6437992b1b15ff"
    inactive_request = "f7c3f67c5966c307f470ff7468af78fb7848d83b7d5f2e25e7cda1d36dfaf90f"
    active_manifest = f"manifests/massive/assets/{active_request}.json"
    inactive_manifest = f"manifests/massive/assets/{inactive_request}.json"
    authorization = preview_module.CURRENT_ASSET_PREVIEW_AUTHORIZATION

    assert preview_module.FULL_RUN_SCOPE_POLICY == "separate_approved_plan_v1"
    assert preview_module._AUTHORIZED_WORKFLOW_IDS == {
        "asset_observation_daily": (
            "c1bae241ed90e49aed1ae8a98b6801f511d6abaac2cef93c66ccba59d33775ec"
        ),
        "asset_observation_version": (
            "989c8c513905e2710714c0b6f94352119e8fb1128147d8c2db9486c1e03df6da"
        ),
        "universe_source_daily": (
            "918ebc04d2eded87243387804d58fa9f24e4282ee27a8a26ac6ac22f4390b755"
        ),
    }
    assert preview_module._AUTHORIZED_CODE_READY_EVENT_SHA256_BY_TABLE == {
        "asset_observation_daily": (
            "5c74b31676c709e6d9455da0c8ef8ec76fb4337754c2bc08c613be7dd9d89ef3"
        ),
        "asset_observation_version": (
            "3655311e84140d523af72e2ac7bcc9e4602c135f8292f7548111fcc186c7b9b2"
        ),
        "universe_source_daily": (
            "d3ac371c080fb9f7317dbc66e7ae0673875d08b66826d13b063847d73a297067"
        ),
    }
    assert dict(preview_module._AUTHORIZED_CONTRACT_IDS) == {
        contract.table: contract.contract_id for contract in CONTRACTS
    }
    assert authorization.session_date == SESSION_DATE
    assert authorization.manifest_paths == (active_manifest, inactive_manifest)
    assert dict(authorization.manifest_sha256_by_path) == {
        active_manifest: "b6ca5f53e3213649372c74f657ff106ad9d339d0eb5ae97bec0da5948a22ab45",
        inactive_manifest: "ffeb63f01b542f011fb4a9591096bb6abf1733582de89b85e41c78c04e745c14",
    }
    assert dict(authorization.request_ids_by_active) == {
        True: active_request,
        False: inactive_request,
    }
    assert authorization.expected_input_rows == 35_647
    assert authorization.expected_page_count == 37
    assert authorization.expected_observation_rows == 35_647
    assert authorization.expected_version_rows == 82
    assert authorization.expected_universe_rows == 35_606
    assert authorization.sample_limit == 100
    assert authorization.dependency_lineage_required is True
    assert authorization.exchange_release_id == (
        "feab0e1f32a5685d1115a6e4e87aab8ff50c18b99c6336a8790ecba44464d838"
    )
    assert authorization.exchange_release_sha256 == (
        "d8789e6cf760ffb6274077736c18e37bd69330139ea1c6ecf2f420bb56f93f07"
    )
    assert authorization.ticker_type_release_id == (
        "11a62f9c06ea5c609c159a7d619ba94cabbe39d3b07518fec279fa4758c882f6"
    )
    assert authorization.ticker_type_release_sha256 == (
        "5568a905bb1cdfe791a300f5b12fdd1e2041e3e1c1aacfbf6cc78f4890b95f47"
    )


def _row(
    ticker: str,
    *,
    active: bool,
    updated: str = "2026-07-01T12:00:00Z",
    delisted: str | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "active": active,
        "cik": "0000123456",
        "composite_figi": f"BBG-{ticker}",
        "currency_name": "usd",
        "last_updated_utc": updated,
        "locale": "us",
        "market": "stocks",
        "name": ticker,
        "primary_exchange": "XNAS",
        "share_class_figi": f"BBGS-{ticker}",
        "ticker": ticker,
        "type": "CS",
    }
    if delisted is not None:
        row["delisted_utc"] = delisted
    return row


def _fixture_rows() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    exact = _row("DUP", active=True)
    active = [
        _row("A", active=True),
        _row("a", active=True),
        _row("ONE", active=True),
        _row("TWO", active=True),
        exact,
        dict(exact),
        _row("LAST", active=True, updated="2026-06-01T12:00:00Z"),
        _row("LAST", active=True, updated="2026-07-01T12:00:00Z"),
        _row(
            "DEL",
            active=True,
            updated="2026-06-01T12:00:00Z",
            delisted="2026-05-01T00:00:00Z",
        ),
        _row(
            "DEL",
            active=True,
            updated="2026-07-01T12:00:00Z",
            delisted="2026-04-01T00:00:00Z",
        ),
    ]
    return active, [_row("OLD", active=False, delisted="2026-05-01T00:00:00Z")]


@dataclass(frozen=True, slots=True)
class _RequestFixture:
    active: bool
    request_id: str
    manifest_path: str
    manifest_sha256: str
    page_path: Path
    page_sha256: str


def _write_request(
    root: Path,
    *,
    active: bool,
    rows: list[dict[str, object]],
) -> _RequestFixture:
    label = "active" if active else "inactive"
    request_id = hashlib.sha256(f"asset-preview:{label}".encode()).hexdigest()
    capture_at = ACTIVE_CAPTURE if active else INACTIVE_CAPTURE
    response = {
        "count": len(rows),
        "request_id": f"provider-{label}-preview",
        "results": rows,
        "status": "OK",
    }
    raw = json.dumps(response, separators=(",", ":"), sort_keys=True).encode()
    compressed = gzip.compress(raw, mtime=0)
    relative_page = (
        f"bronze/massive/assets/request_id={request_id}/page-00000.json.gz"
    )
    page_path = root / relative_page
    page_path.parent.mkdir(parents=True, exist_ok=True)
    page_path.write_bytes(compressed)
    manifest = {
        "artifacts": [
            {
                "compressed_bytes": len(compressed),
                "content_type": "application/json",
                "is_last": True,
                "next_continuation": None,
                "path": relative_page,
                "raw_bytes": len(raw),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "record_count": len(rows),
                "sequence": 0,
                "stored_sha256": hashlib.sha256(compressed).hexdigest(),
            }
        ],
        "checkpoint": None,
        "completed_at": capture_at.isoformat(),
        "created_at": (capture_at - timedelta(seconds=2)).isoformat(),
        "dataset": "assets",
        "manifest_schema_version": 1,
        "provider": "massive",
        "provider_contract_version": "1.1",
        "provider_version": "1.2.0",
        "request": {
            "adjusted": False,
            "asset_ids": [],
            "dataset": "assets",
            "end": SESSION_DATE.isoformat(),
            "parameters": {"active": str(active).lower()},
            "start": SESSION_DATE.isoformat(),
        },
        "request_id": request_id,
        "status": "complete",
        "updated_at": (capture_at + timedelta(seconds=1)).isoformat(),
    }
    relative_manifest = f"manifests/massive/assets/{request_id}.json"
    manifest_content = json.dumps(manifest, sort_keys=True).encode()
    manifest_path = root / relative_manifest
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_bytes(manifest_content)
    return _RequestFixture(
        active=active,
        request_id=request_id,
        manifest_path=relative_manifest,
        manifest_sha256=hashlib.sha256(manifest_content).hexdigest(),
        page_path=page_path,
        page_sha256=hashlib.sha256(compressed).hexdigest(),
    )


@dataclass(frozen=True, slots=True)
class _PreviewFixture:
    authorization: preview_module.AssetPreviewAuthorization
    requests: tuple[_RequestFixture, _RequestFixture]


def _write_preview_fixture(root: Path) -> _PreviewFixture:
    active_rows, inactive_rows = _fixture_rows()
    active = _write_request(root, active=True, rows=active_rows)
    inactive = _write_request(root, active=False, rows=inactive_rows)
    authorization = preview_module.AssetPreviewAuthorization(
        session_date=SESSION_DATE,
        manifest_paths=(active.manifest_path, inactive.manifest_path),
        manifest_sha256_by_path={
            active.manifest_path: active.manifest_sha256,
            inactive.manifest_path: inactive.manifest_sha256,
        },
        request_ids_by_active={True: active.request_id, False: inactive.request_id},
        expected_input_rows=11,
        expected_page_count=2,
        expected_observation_rows=11,
        expected_version_rows=6,
        expected_universe_rows=8,
        sample_limit=100,
        exchange_release_id="e" * 64,
        exchange_release_sha256="1" * 64,
        ticker_type_release_id="a" * 64,
        ticker_type_release_sha256="2" * 64,
        dependency_lineage_required=False,
    )
    return _PreviewFixture(authorization, (active, inactive))


def _git(repo: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(repo), *arguments),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _init_preview_git_checkout(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, str]:
    repo = root / "repo"
    logic_closure = getattr(
        preview_module,
        "_LOGIC_CLOSURE",
        ("backend/ame_stocks_api/silver/asset_preview.py",),
    )
    for relative in logic_closure:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"synthetic tracked fixture for {relative}\n", encoding="utf-8")
    module_path = repo / "backend/ame_stocks_api/silver/asset_preview.py"
    if not module_path.exists():
        module_path.parent.mkdir(parents=True, exist_ok=True)
        module_path.write_text("# synthetic asset preview module\n", encoding="utf-8")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "asset-preview@example.test")
    _git(repo, "config", "user.name", "Asset Preview Test")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "asset preview fixture")
    monkeypatch.setattr(preview_module, "__file__", str(module_path))
    return repo, _git(repo, "rev-parse", "HEAD")


def _code_ready_workflows(root: Path) -> tuple[SilverStore, dict[str, str], dict[str, str]]:
    store = SilverStore(root)
    workflow_ids: dict[str, str] = {}
    event_sha256_by_table: dict[str, str] = {}
    for index, contract in enumerate(CONTRACTS):
        second = index * 3
        snapshot = store.create_workflow(
            contract,
            actor=f"asset-preview-author-{contract.table}",
            created_at=f"2026-07-13T01:00:{second:02d}+00:00",
        )
        snapshot = store.submit_schema_review(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            actor="asset-preview-author",
            created_at=f"2026-07-13T01:00:{second + 1:02d}+00:00",
        )
        snapshot = store.approve_schema(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            approver="asset-preview-reviewer",
            decided_at=f"2026-07-13T01:00:{second + 2:02d}+00:00",
        )
        assert snapshot.state is WorkflowState.CODE_READY
        workflow_ids[contract.table] = snapshot.workflow_id
        event_sha256_by_table[contract.table] = snapshot.event_sha256
    return store, workflow_ids, event_sha256_by_table


def _authorize_fixture_workflows(
    monkeypatch: pytest.MonkeyPatch,
    workflow_ids: dict[str, str],
    event_sha256_by_table: dict[str, str],
) -> None:
    monkeypatch.setattr(preview_module, "_AUTHORIZED_WORKFLOW_IDS", workflow_ids)
    monkeypatch.setattr(
        preview_module,
        "_AUTHORIZED_CODE_READY_EVENT_SHA256_BY_TABLE",
        event_sha256_by_table,
    )


def _run_fixture_preview(
    data_root: Path,
    *,
    workflow_ids: dict[str, str],
    event_sha256_by_table: dict[str, str],
    repo_root: Path,
    git_commit: str,
    fixture: _PreviewFixture,
    transition_barrier: Any = None,
) -> preview_module.AssetPreviewRun:
    authorization = fixture.authorization
    return preview_module._run_asset_preview_authorized(
        data_root,
        workflow_ids=workflow_ids,
        expected_event_sha256_by_table=event_sha256_by_table,
        manifest_paths=authorization.manifest_paths,
        expected_manifest_sha256_by_path=authorization.manifest_sha256_by_path,
        expected_input_rows=authorization.expected_input_rows,
        git_commit=git_commit,
        repo_root=repo_root,
        actor="asset-preview-test-runner",
        calendar_name="XNYS",
        sample_limit=authorization.sample_limit,
        authorization=authorization,
        transition_barrier=transition_barrier,
    )


def _prepared_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    Path,
    _PreviewFixture,
    Path,
    str,
    SilverStore,
    dict[str, str],
    dict[str, str],
]:
    data_root = tmp_path / "data"
    fixture = _write_preview_fixture(data_root)
    repo_root, git_commit = _init_preview_git_checkout(tmp_path, monkeypatch)
    store, workflow_ids, events = _code_ready_workflows(data_root)
    _authorize_fixture_workflows(monkeypatch, workflow_ids, events)
    monkeypatch.setattr(
        preview_module,
        "_load_reference_dictionaries",
        lambda root, silver_store, authorization: (
            frozenset({"CS"}),
            frozenset({"XNAS"}),
            (),
        ),
    )
    return data_root, fixture, repo_root, git_commit, store, workflow_ids, events


def _runs_by_table(run: preview_module.AssetPreviewRun) -> dict[str, Any]:
    return {
        run.observation.build.intent.table: run.observation,
        run.version.build.intent.table: run.version,
        run.universe.build.intent.table: run.universe,
    }


def test_bounded_asset_preview_transforms_once_and_stops_three_workflows_for_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        data_root,
        fixture,
        repo_root,
        git_commit,
        store,
        workflow_ids,
        events,
    ) = _prepared_fixture(tmp_path, monkeypatch)
    temporary_directory = data_root / "tmp" / "silver-asset-preview-immutable-writes"
    temporary_directory.mkdir(parents=True)
    interrupted_temporary = temporary_directory / ".part.parquet.tmp-interrupted"
    interrupted_temporary.write_bytes(b"unrelated interrupted write")
    real_transform = preview_module.transform_asset_session
    calls: list[date] = []

    def counted_transform(*args: Any, **kwargs: Any):
        calls.append(args[0].session_date)
        return real_transform(*args, **kwargs)

    monkeypatch.setattr(preview_module, "transform_asset_session", counted_transform)
    run = _run_fixture_preview(
        data_root,
        workflow_ids=workflow_ids,
        event_sha256_by_table=events,
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )

    assert calls == [SESSION_DATE]
    assert run.inventory.source_dataset == "assets"
    assert len(run.inventory.upstream_manifests) == 2
    assert len(run.inventory.artifacts) == 2
    expected_rows = {
        "asset_observation_daily": 11,
        "asset_observation_version": 6,
        "universe_source_daily": 8,
    }
    expected_unmapped = {
        "asset_observation_daily": 0,
        "asset_observation_version": 5,
        "universe_source_daily": 3,
    }
    for table, table_run in _runs_by_table(run).items():
        workflow = table_run.workflow
        build = table_run.build
        assert workflow.state is WorkflowState.AWAITING_REVIEW
        assert workflow.sequence == 5
        assert build.intent.kind is BuildKind.PREVIEW
        assert build.intent.git_commit == git_commit
        assert build.intent.parameters["full_run_scope_policy"] == (
            "separate_approved_plan_v1"
        )
        assert build.preview is not None
        assert build.preview.full_run_projection["scope_binding_mode"] == (
            "separate_approved_plan_v1"
        )
        assert build.row_funnel.input_rows == 11
        assert build.row_funnel.accepted_source_rows == 11
        assert build.row_funnel.exact_duplicate_excess == 0
        assert build.row_funnel.quarantined_source_rows == 0
        assert build.row_funnel.output_rows_by_table == {table: expected_rows[table]}
        assert build.row_funnel.unmapped_source_rows == expected_unmapped[table]
        assert build.row_funnel.version_preserved_rows == 6
        assert all(not check.blocks_publish for check in build.qa_checks)

        roles: dict[ArtifactRole, list[Any]] = {}
        for output in build.outputs:
            roles.setdefault(output.role, []).append(output)
            output_path = data_root / output.path
            assert output.path.startswith("staging/silver/")
            assert stat.S_IMODE(output_path.stat().st_mode) == 0o444
            assert output_path.stat().st_nlink == 1
            assert output_path.stat().st_size == output.bytes
            assert hashlib.sha256(output_path.read_bytes()).hexdigest() == output.sha256
        assert len(roles[ArtifactRole.DATA]) == 1
        assert len(roles[ArtifactRole.QA]) == 1
        assert len(roles[ArtifactRole.QUARANTINE]) == 1
        data_table = pq.ParquetFile(data_root / roles[ArtifactRole.DATA][0].path).read()
        contract = next(item for item in CONTRACTS if item.table == table)
        assert data_table.schema == contract.arrow_schema
        assert data_table.num_rows == expected_rows[table]
        store.verify_build(build, contract)

        declared_cases = preview_module._FIXED_CASE_IDS_BY_TABLE[table]
        assert build.preview.fixed_case_ids == declared_cases
        expected_cases = {
            "current_reference_snapshot",
            "delisting",
            "case_sensitive_tickers",
        }
        if table == "asset_observation_version":
            expected_cases.remove("current_reference_snapshot")
        assert set(declared_cases) == expected_cases
        qa_result_ids = {check.result_id for check in build.qa_checks}
        qa_by_check_id = {check.check_id: check.result_id for check in build.qa_checks}
        for case_id in declared_cases:
            expected_check_ids = preview_module._FIXED_CASE_CHECK_IDS_BY_TABLE[table][case_id]
            expected_result_ids = {qa_by_check_id[check_id] for check_id in expected_check_ids}
            assert set(build.preview.fixed_case_qa_result_ids[case_id]) == expected_result_ids
            assert expected_result_ids < qa_result_ids
        evidence_paths = {
            check.bounded_examples_path
            for check in build.qa_checks
            if check.bounded_examples_path is not None
        }
        assert len(evidence_paths) == 1
        evidence_path = next(iter(evidence_paths))
        evidence_rows = json.loads((data_root / evidence_path).read_text(encoding="utf-8"))
        assert len(evidence_rows) == len(declared_cases)
        assert {row["case_id"] for row in evidence_rows} == set(declared_cases)
        assert all(row["session_date"] == SESSION_DATE.isoformat() for row in evidence_rows)
        assert all(row["table"] == table for row in evidence_rows)
        casefold_row = next(
            row for row in evidence_rows if row["case_id"] == "case_sensitive_tickers"
        )
        assert casefold_row["exact_tickers"] == ["A", "a"]
        assert casefold_row["source_occurrence_count_by_ticker"] == {"A": 1, "a": 1}
        if table == "asset_observation_version":
            assert casefold_row["output_exact_tickers"] == []
            assert casefold_row["output_rows"] == []
        else:
            assert casefold_row["output_exact_tickers"] == ["A", "a"]
            assert {row["ticker"] for row in casefold_row["output_rows"]} == {"A", "a"}
        delisting_row = next(row for row in evidence_rows if row["case_id"] == "delisting")
        if table == "asset_observation_version":
            assert delisting_row["ticker"] == "DEL"
            assert delisting_row["difference_fields"] == [
                "delisted_utc",
                "last_updated_utc",
            ]
            version_rows = delisting_row["output_rows"]
            assert len(version_rows) == 2
            assert {row["version_count"] for row in version_rows} == {2}
            selected = [row for row in version_rows if row["is_selected"]]
            assert len(selected) == 1
            assert selected[0]["last_updated_at_utc"] == max(
                row["last_updated_at_utc"] for row in version_rows
            )
            assert selected[0]["delisted_at_utc"] == min(
                row["delisted_at_utc"] for row in version_rows
            )
        else:
            assert delisting_row["ticker"] in {"DEL", "OLD"}
            assert delisting_row["output_row"]["ticker"] == delisting_row["ticker"]
        if "current_reference_snapshot" in declared_cases:
            current_row = next(
                row for row in evidence_rows if row["case_id"] == "current_reference_snapshot"
            )
            assert (
                current_row["exchange_release_id"]
                == fixture.authorization.exchange_release_id
            )

        assert len(store.workflow_events(workflow.workflow_id)) == 5

    assert not (data_root / "silver").exists()
    assert not (data_root / "manifests/silver/releases").exists()
    assert not (data_root / "manifests/silver/approvals/full_run").exists()
    assert not (data_root / "manifests/silver/approvals/publish").exists()
    assert interrupted_temporary.read_bytes() == b"unrelated interrupted write"
    assert not tuple((data_root / "staging/silver").rglob(".*.tmp-*"))


def test_asset_preview_is_exactly_idempotent_at_awaiting_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root, fixture, repo_root, git_commit, store, workflow_ids, events = _prepared_fixture(
        tmp_path, monkeypatch
    )
    first = _run_fixture_preview(
        data_root,
        workflow_ids=workflow_ids,
        event_sha256_by_table=events,
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )
    first_runs = _runs_by_table(first)
    file_state = {
        output.path: (output.sha256, (data_root / output.path).stat().st_mtime_ns)
        for table_run in first_runs.values()
        for output in table_run.build.outputs
    }
    event_counts = {
        table: len(store.workflow_events(table_run.workflow.workflow_id))
        for table, table_run in first_runs.items()
    }
    current_events = {
        table: table_run.workflow.event_sha256 for table, table_run in first_runs.items()
    }
    real_transform = preview_module.transform_asset_session
    calls = 0

    def counted_transform(*args: Any, **kwargs: Any):
        nonlocal calls
        calls += 1
        return real_transform(*args, **kwargs)

    monkeypatch.setattr(preview_module, "transform_asset_session", counted_transform)
    repeated = _run_fixture_preview(
        data_root,
        workflow_ids=workflow_ids,
        event_sha256_by_table=current_events,
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )

    assert calls == 0
    repeated_runs = _runs_by_table(repeated)
    assert {
        table: table_run.build.build_id for table, table_run in repeated_runs.items()
    } == {table: table_run.build.build_id for table, table_run in first_runs.items()}
    assert {
        table: table_run.workflow.event_sha256 for table, table_run in repeated_runs.items()
    } == current_events
    assert {
        table: len(store.workflow_events(table_run.workflow.workflow_id))
        for table, table_run in repeated_runs.items()
    } == event_counts
    assert file_state == {
        output.path: (output.sha256, (data_root / output.path).stat().st_mtime_ns)
        for table_run in repeated_runs.values()
        for output in table_run.build.outputs
    }

    with pytest.raises(SilverStoreError, match="stale"):
        _run_fixture_preview(
            data_root,
            workflow_ids=workflow_ids,
            event_sha256_by_table=events,
            repo_root=repo_root,
            git_commit=git_commit,
            fixture=fixture,
        )


def test_asset_preview_source_corruption_fails_before_any_silver_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root, fixture, repo_root, git_commit, store, workflow_ids, events = _prepared_fixture(
        tmp_path, monkeypatch
    )
    page = fixture.requests[0].page_path
    mutated = bytearray(page.read_bytes())
    mutated[-1] ^= 1
    page.write_bytes(bytes(mutated))

    with pytest.raises(
        (AssetSourceError, SilverStoreError, ValueError),
        match=r"checksum|authorized|integrity",
    ):
        _run_fixture_preview(
            data_root,
            workflow_ids=workflow_ids,
            event_sha256_by_table=events,
            repo_root=repo_root,
            git_commit=git_commit,
            fixture=fixture,
        )

    assert not (data_root / "staging").exists()
    assert not (data_root / "manifests/silver/source-inventories/assets").exists()
    for workflow_id in workflow_ids.values():
        snapshot = store.status(workflow_id)
        assert snapshot.state is WorkflowState.CODE_READY
        assert snapshot.sequence == 3


def test_asset_preview_barrier_failure_recovers_without_full_or_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root, fixture, repo_root, git_commit, store, workflow_ids, events = _prepared_fixture(
        tmp_path, monkeypatch
    )

    def fail_second_record(label: str) -> None:
        if label == "before_record:asset_observation_version":
            raise RuntimeError("injected transition barrier failure")

    with pytest.raises(RuntimeError, match="injected transition barrier failure"):
        _run_fixture_preview(
            data_root,
            workflow_ids=workflow_ids,
            event_sha256_by_table=events,
            repo_root=repo_root,
            git_commit=git_commit,
            fixture=fixture,
            transition_barrier=fail_second_record,
        )

    interrupted = {
        table: store.status(workflow_id) for table, workflow_id in workflow_ids.items()
    }
    assert interrupted["asset_observation_daily"].state is WorkflowState.PREVIEW_READY
    assert interrupted["asset_observation_daily"].sequence == 4
    assert interrupted["asset_observation_version"].state is WorkflowState.CODE_READY
    assert interrupted["universe_source_daily"].state is WorkflowState.CODE_READY
    assert all(item.state is not WorkflowState.AWAITING_REVIEW for item in interrupted.values())

    recovered = _run_fixture_preview(
        data_root,
        workflow_ids=workflow_ids,
        event_sha256_by_table={
            table: snapshot.event_sha256 for table, snapshot in interrupted.items()
        },
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )
    for table_run in _runs_by_table(recovered).values():
        assert table_run.workflow.state is WorkflowState.AWAITING_REVIEW
        assert table_run.workflow.sequence == 5
    assert not (data_root / "silver").exists()
    assert not (data_root / "manifests/silver/releases").exists()
    assert not (data_root / "manifests/silver/approvals/full_run").exists()


def test_asset_preview_reuses_orphan_manifest_after_event_append_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root, fixture, repo_root, git_commit, store, workflow_ids, events = _prepared_fixture(
        tmp_path, monkeypatch
    )
    real_append = SilverStore._append_event
    injected = False

    def fail_first_preview_event(self: SilverStore, current: Any, **kwargs: Any):
        nonlocal injected
        if not injected and kwargs["next_state"] is WorkflowState.PREVIEW_READY:
            injected = True
            raise RuntimeError("injected post-manifest event failure")
        return real_append(self, current, **kwargs)

    monkeypatch.setattr(SilverStore, "_append_event", fail_first_preview_event)
    with pytest.raises(RuntimeError, match="injected post-manifest event failure"):
        _run_fixture_preview(
            data_root,
            workflow_ids=workflow_ids,
            event_sha256_by_table=events,
            repo_root=repo_root,
            git_commit=git_commit,
            fixture=fixture,
        )
    orphan_paths = tuple(
        (data_root / "manifests/silver/builds/asset_observation_daily").glob(
            "build_id=*/manifest.json"
        )
    )
    assert len(orphan_paths) == 1
    orphan_bytes = orphan_paths[0].read_bytes()
    assert all(
        store.status(workflow_id).state is WorkflowState.CODE_READY
        for workflow_id in workflow_ids.values()
    )

    monkeypatch.setattr(SilverStore, "_append_event", real_append)
    recovered = _run_fixture_preview(
        data_root,
        workflow_ids=workflow_ids,
        event_sha256_by_table=events,
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )
    assert orphan_paths[0].read_bytes() == orphan_bytes
    assert recovered.observation.workflow.state is WorkflowState.AWAITING_REVIEW
    assert recovered.observation.build_document.sha256 == hashlib.sha256(orphan_bytes).hexdigest()
    assert all(item.workflow.sequence == 5 for item in recovered.table_runs)


def test_asset_preview_rejects_orphan_with_forged_fixed_case_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root, fixture, repo_root, git_commit, store, workflow_ids, events = _prepared_fixture(
        tmp_path, monkeypatch
    )
    real_append = SilverStore._append_event
    injected = False

    def fail_first_preview_event(self: SilverStore, current: Any, **kwargs: Any):
        nonlocal injected
        if not injected and kwargs["next_state"] is WorkflowState.PREVIEW_READY:
            injected = True
            raise RuntimeError("injected post-manifest event failure")
        return real_append(self, current, **kwargs)

    monkeypatch.setattr(SilverStore, "_append_event", fail_first_preview_event)
    with pytest.raises(RuntimeError, match="injected post-manifest event failure"):
        _run_fixture_preview(
            data_root,
            workflow_ids=workflow_ids,
            event_sha256_by_table=events,
            repo_root=repo_root,
            git_commit=git_commit,
            fixture=fixture,
        )
    orphan_path = next(
        (data_root / "manifests/silver/builds/asset_observation_daily").glob(
            "build_id=*/manifest.json"
        )
    )
    orphan_build, _ = store.load_build(
        "asset_observation_daily",
        orphan_path.parent.name.removeprefix("build_id="),
    )
    assert orphan_build.preview is not None
    case_results = dict(orphan_build.preview.fixed_case_qa_result_ids)
    case_ids = list(orphan_build.preview.fixed_case_ids)
    case_results[case_ids[0]] = case_results[case_ids[1]]
    forged_build = replace(
        orphan_build,
        preview=replace(
            orphan_build.preview,
            fixed_case_qa_result_ids=case_results,
        ),
    )
    orphan_path.chmod(0o644)
    orphan_path.write_text(
        json.dumps(forged_build.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(SilverStore, "_append_event", real_append)

    with pytest.raises(SilverStoreError, match="fixed-case QA binding changed"):
        _run_fixture_preview(
            data_root,
            workflow_ids=workflow_ids,
            event_sha256_by_table=events,
            repo_root=repo_root,
            git_commit=git_commit,
            fixture=fixture,
        )
    assert all(
        store.status(workflow_id).state is WorkflowState.CODE_READY
        for workflow_id in workflow_ids.values()
    )


def test_asset_preview_git_guards_apply_before_run_and_before_each_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root, fixture, repo_root, git_commit, store, workflow_ids, events = _prepared_fixture(
        tmp_path, monkeypatch
    )
    with pytest.raises(SilverStoreError, match="HEAD differs"):
        _run_fixture_preview(
            data_root,
            workflow_ids=workflow_ids,
            event_sha256_by_table=events,
            repo_root=repo_root,
            git_commit="f" * 40,
            fixture=fixture,
        )
    assert not (data_root / "staging").exists()

    dirty = repo_root / "untracked.txt"
    dirty.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(SilverStoreError, match="not clean"):
        _run_fixture_preview(
            data_root,
            workflow_ids=workflow_ids,
            event_sha256_by_table=events,
            repo_root=repo_root,
            git_commit=git_commit,
            fixture=fixture,
        )
    dirty.unlink()

    def dirty_before_record(label: str) -> None:
        if label == "before_record:asset_observation_daily":
            dirty.write_text("became dirty\n", encoding="utf-8")

    with pytest.raises(SilverStoreError, match="not clean"):
        _run_fixture_preview(
            data_root,
            workflow_ids=workflow_ids,
            event_sha256_by_table=events,
            repo_root=repo_root,
            git_commit=git_commit,
            fixture=fixture,
            transition_barrier=dirty_before_record,
        )
    for workflow_id in workflow_ids.values():
        snapshot = store.status(workflow_id)
        assert snapshot.state is WorkflowState.CODE_READY
        assert snapshot.sequence == 3


def test_asset_preview_dependency_lineage_is_exact_and_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root, fixture, repo_root, git_commit, _, _, _ = _prepared_fixture(
        tmp_path, monkeypatch
    )
    authorization = replace(fixture.authorization, dependency_lineage_required=True)
    bronze_inventory = preview_module.build_asset_source_inventory(
        data_root,
        manifest_paths=authorization.manifest_paths,
        git_commit=git_commit,
    )
    expected = preview_module._expected_dependency_lineage(authorization)

    bound = preview_module._bind_dependency_lineage(
        bronze_inventory,
        expected,
        authorization=authorization,
    )

    assert len(bound.upstream_manifests) == 4
    assert {item.path: item.sha256 for item in bound.upstream_manifests} == {
        **dict(authorization.manifest_sha256_by_path),
        **{item.path: item.sha256 for item in expected},
    }
    preview_module._validate_authorized_inventory(bound, authorization=authorization)
    with pytest.raises(SilverStoreError, match="lineage changed"):
        preview_module._validate_authorized_inventory(
            bronze_inventory,
            authorization=authorization,
        )

    wrong = replace(expected[0], sha256="f" * 64)
    extra = UpstreamManifestRef(
        path=f"manifests/silver/releases/release_id={'b' * 64}.json",
        sha256="c" * 64,
    )
    for dependency_lineage in (
        expected[:1],
        tuple(sorted((wrong, expected[1]), key=lambda item: item.path)),
        tuple(sorted((*expected, extra), key=lambda item: item.path)),
    ):
        with pytest.raises(SilverStoreError, match="dependency release lineage changed"):
            preview_module._bind_dependency_lineage(
                bronze_inventory,
                dependency_lineage,
                authorization=authorization,
            )
    assert repo_root.is_dir()


def test_existing_preview_recovery_rebuilds_complete_registered_inventory_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root, fixture, repo_root, git_commit, store, workflow_ids, events = _prepared_fixture(
        tmp_path, monkeypatch
    )
    completed = _run_fixture_preview(
        data_root,
        workflow_ids=workflow_ids,
        event_sha256_by_table=events,
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )
    runs = _runs_by_table(completed)
    original = runs["asset_observation_daily"]
    subset_inputs = original.build.intent.inputs[:1]
    subset_rows = sum(int(item.row_count or 0) for item in subset_inputs)
    assert original.build.preview is not None
    forged = replace(
        original,
        build=replace(
            original.build,
            intent=replace(original.build.intent, inputs=subset_inputs),
            preview=replace(original.build.preview, full_run_inputs=subset_inputs),
            row_funnel=replace(
                original.build.row_funnel,
                input_rows=subset_rows,
                accepted_source_rows=subset_rows,
                version_preserved_rows=min(
                    original.build.row_funnel.version_preserved_rows,
                    subset_rows,
                ),
            ),
        ),
    )
    forged_runs = {**runs, "asset_observation_daily": forged}
    monkeypatch.setattr(
        preview_module,
        "_load_event_preview",
        lambda silver_store, table, snapshot: forged_runs[table],
    )

    with pytest.raises(SilverStoreError, match="intent differs"):
        preview_module._load_existing_table_runs(
            data_root,
            store,
            {table: run.workflow for table, run in runs.items()},
            git_commit=git_commit,
            calendar_version=original.build.intent.exchange_calendar_version,
            parameters=original.build.intent.parameters,
            authorization=fixture.authorization,
        )


def test_existing_preview_recovery_reasserts_authorized_funnel_and_git(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root, fixture, repo_root, git_commit, _, workflow_ids, events = _prepared_fixture(
        tmp_path, monkeypatch
    )
    completed = _run_fixture_preview(
        data_root,
        workflow_ids=workflow_ids,
        event_sha256_by_table=events,
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )
    table_run = completed.version
    bad_funnel = replace(
        table_run.build.row_funnel,
        unmapped_source_rows=table_run.build.row_funnel.unmapped_source_rows + 1,
    )

    with pytest.raises(SilverStoreError, match="evidence differs"):
        preview_module._require_matching_existing_preview(
            replace(table_run.build, row_funnel=bad_funnel),
            completed.inventory,
            intent=table_run.build.intent,
            authorization=fixture.authorization,
        )

    with pytest.raises(SilverStoreError, match="Git commit changed"):
        preview_module._require_matching_existing_preview(
            table_run.build,
            replace(completed.inventory, git_commit="f" * 40),
            intent=table_run.build.intent,
            authorization=fixture.authorization,
        )
