from __future__ import annotations

import gzip
import hashlib
import json
import stat
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import exchange_preview as exchange_preview_module
from ame_stocks_api.silver import exchange_release as exchange_release_module
from ame_stocks_api.silver.contracts import ArtifactRole, BuildKind, QAStatus, SourceLayer
from ame_stocks_api.silver.exchange_contract import EXCHANGE_DIM_CONTRACT
from ame_stocks_api.silver.exchange_source import (
    ExchangeSourceBatch,
    ExchangeSourceError,
    ExchangeSourcePage,
    ExchangeSourceSnapshot,
    build_exchange_source_inventory,
    read_exchange_source_inventory,
)
from ame_stocks_api.silver.exchanges import (
    EXCHANGE_AVAILABILITY_RULE,
    EXCHANGE_SNAPSHOT_SCOPE,
    ExchangeTransformError,
    transform_exchange_batch,
)
from ame_stocks_api.silver.store import SilverStore, SilverStoreError, WorkflowState

BUILD_ID = "c" * 64
CAPTURE_AT = datetime(2026, 7, 11, 15, 37, 41, tzinfo=UTC)


def _row(
    exchange_id: int = 1,
    *,
    name: str = "NYSE American, LLC",
    mic: str | None = "XASE",
    exchange_type: str = "exchange",
    **extra: object,
) -> dict[str, object]:
    row: dict[str, object] = {
        "asset_class": "stocks",
        "id": exchange_id,
        "locale": "us",
        "name": name,
        "operating_mic": "XNYS",
        "type": exchange_type,
        "url": "https://example.test/exchange",
    }
    if mic is not None:
        row["mic"] = mic
    row.update(extra)
    return row


def _snapshot(
    rows: tuple[dict[str, object], ...],
    *,
    request_id: str = "a" * 64,
    artifact_sha: str = "b" * 64,
    capture_at: datetime = CAPTURE_AT,
) -> ExchangeSourceSnapshot:
    return ExchangeSourceSnapshot(
        source_request_id=request_id,
        source_capture_at_utc=capture_at,
        pages=(
            ExchangeSourcePage(
                source_path=f"fixtures/{request_id}/page-00000.json.gz",
                source_artifact_sha256=artifact_sha,
                sequence=0,
                source_provider_request_id=f"provider-{request_id[:8]}",
                rows=rows,
            ),
        ),
    )


def _transform(*snapshots: ExchangeSourceSnapshot):
    return transform_exchange_batch(ExchangeSourceBatch(tuple(snapshots)), build_id=BUILD_ID)


def test_current_reference_snapshot_maps_point_in_time_and_preserves_orf() -> None:
    source_rows = (
        _row(acronym="AMEX", participant_id="A"),
        _row(
            5,
            name="Unlisted Trading Privileges",
            mic=None,
            exchange_type="SIP",
            participant_id="E",
        ),
        _row(
            62,
            name="OTC Equity Security",
            mic="OOTC",
            exchange_type="ORF",
            operating_mic="FINR",
        ),
    )
    result = _transform(_snapshot(source_rows))

    assert result.table.schema == EXCHANGE_DIM_CONTRACT.arrow_schema
    assert result.table.num_rows == 3
    rows = result.table.to_pylist()
    assert {row["exchange_type"] for row in rows} == {"exchange", "SIP", "ORF"}
    sip = next(row for row in rows if row["exchange_id"] == 5)
    assert sip["mic"] is None
    assert sip["capture_date"].isoformat() == "2026-07-11"
    assert sip["available_session"].isoformat() == "2026-07-13"
    assert sip["available_at_utc"] == datetime(2026, 7, 13, 13, 30, tzinfo=UTC)
    assert sip["snapshot_scope"] == EXCHANGE_SNAPSHOT_SCOPE
    assert sip["availability_rule"] == EXCHANGE_AVAILABILITY_RULE
    first = next(row for row in rows if row["exchange_id"] == 1)
    expected_row_hash = stable_digest(source_rows[0])
    assert first["source_row_hash"] == expected_row_hash
    assert first["source_record_id"] == stable_digest(
        {
            "dataset": "exchanges",
            "source_request_id": "a" * 64,
            "source_artifact_sha256": "b" * 64,
            "source_page_sequence": 0,
            "source_row_ordinal": 0,
            "source_row_hash": expected_row_hash,
        }
    )
    assert result.row_funnel.to_dict() == {
        "accepted_source_rows": 3,
        "exact_duplicate_excess": 0,
        "input_rows": 3,
        "output_rows_by_table": {"exchange_dim": 3},
        "quarantined_source_rows": 0,
        "unmapped_source_rows": 0,
        "version_preserved_rows": 0,
    }
    assert all(check.status is QAStatus.PASSED for check in result.qa_checks)
    assert result.quarantine_records == ()

    repeated = _transform(_snapshot(source_rows))
    assert repeated.table.equals(result.table)
    assert repeated.qa_checks == result.qa_checks


def test_nonblocking_source_drift_is_preserved_and_reported() -> None:
    row = _row(
        exchange_type="DARK",
        acronym="",
        url="ftp://invalid.example/exchange",
        provider_new_field={"nested": True},
    )
    result = _transform(_snapshot((row, dict(row))))

    assert result.table.num_rows == 1
    assert result.table.to_pylist()[0]["exchange_type"] == "DARK"
    assert result.table.to_pylist()[0]["acronym"] == ""
    assert result.row_funnel.exact_duplicate_excess == 1
    assert result.qa_by_id("exact_duplicate_excess_rows").numerator == 1
    assert result.qa_by_id("unreviewed_exchange_type_rows").numerator == 2
    assert result.qa_by_id("unexpected_source_field_rows").numerator == 2
    assert result.qa_by_id("empty_optional_string_rows").numerator == 2
    assert result.qa_by_id("url_invalid_rows").numerator == 2
    assert result.qa_by_id("unreviewed_exchange_type_rows").status is QAStatus.WARNING


def test_conflicting_primary_key_and_mic_rows_are_quarantined() -> None:
    primary = _transform(
        _snapshot(
            (
                _row(1, name="First representation"),
                _row(1, name="Conflicting representation"),
            )
        )
    )
    assert primary.table.num_rows == 0
    assert primary.row_funnel.quarantined_source_rows == 2
    assert primary.qa_by_id("primary_key_conflict_rows").numerator == 2
    assert primary.qa_by_id("primary_key_conflict_rows").status is QAStatus.FAILED
    assert {item.issue_code for item in primary.quarantine_records} == {
        "primary_key_conflict_rows"
    }

    mic = _transform(_snapshot((_row(1, mic="XASE"), _row(2, mic="XASE"))))
    assert mic.table.num_rows == 0
    assert mic.qa_by_id("mic_conflict_rows").numerator == 2
    assert {item.issue_code for item in mic.quarantine_records} == {"mic_conflict_rows"}


def test_invalid_required_domain_and_mic_values_are_fail_closed() -> None:
    invalid = _row(1, mic="bad", asset_class="crypto", locale="global", name=" ")
    result = _transform(_snapshot((invalid,)))

    assert result.table.num_rows == 0
    assert result.row_funnel.quarantined_source_rows == 1
    assert result.qa_by_id("required_field_invalid_rows").numerator == 1
    assert result.qa_by_id("mic_format_invalid_values").numerator == 1
    assert result.qa_by_id("asset_class_domain_invalid_rows").numerator == 1
    assert result.qa_by_id("locale_domain_invalid_rows").numerator == 1
    assert all(record.review_status.value == "pending" for record in result.quarantine_records)


def test_same_capture_date_has_exactly_one_source_request() -> None:
    result = _transform(
        _snapshot((_row(1),), request_id="a" * 64, artifact_sha="1" * 64),
        _snapshot((_row(2),), request_id="b" * 64, artifact_sha="2" * 64),
    )

    assert result.table.num_rows == 0
    assert result.row_funnel.quarantined_source_rows == 2
    check = result.qa_by_id("source_snapshot_cardinality_invalid")
    assert (check.numerator, check.denominator, check.status) == (1, 1, QAStatus.FAILED)


def test_later_capture_appends_a_new_partition_without_historical_backfill() -> None:
    result = _transform(
        _snapshot((_row(1),), request_id="a" * 64, artifact_sha="1" * 64),
        _snapshot(
            (_row(1),),
            request_id="b" * 64,
            artifact_sha="2" * 64,
            capture_at=datetime(2026, 7, 12, 15, 37, 41, tzinfo=UTC),
        ),
    )

    assert result.table.num_rows == 2
    assert [item.isoformat() for item in result.table.column("capture_date").to_pylist()] == [
        "2026-07-11",
        "2026-07-12",
    ]
    assert result.qa_by_id("source_snapshot_cardinality_invalid").status is QAStatus.PASSED


def test_optional_nonstring_value_is_not_silently_coerced() -> None:
    with pytest.raises(ExchangeTransformError, match="optional exchange field acronym"):
        _transform(_snapshot((_row(acronym=7),)))


def _write_bronze_fixture(
    root: Path,
    *,
    response_count: int | None = None,
) -> str:
    request_id = "1" * 64
    rows = [_row()]
    response = {
        "count": len(rows) if response_count is None else response_count,
        "request_id": "provider-request",
        "results": rows,
        "status": "OK",
    }
    raw = json.dumps(response, separators=(",", ":"), sort_keys=True).encode()
    compressed = gzip.compress(raw, mtime=0)
    artifact_relative = (
        f"bronze/massive/exchanges/request_id={request_id}/page-00000.json.gz"
    )
    artifact_path = root / artifact_relative
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(compressed)
    artifact_path.with_name(".page-00000.json.gz.swp").write_bytes(b"not-authoritative")
    manifest = {
        "artifacts": [
            {
                "compressed_bytes": len(compressed),
                "content_type": "application/json",
                "is_last": True,
                "next_continuation": None,
                "path": artifact_relative,
                "raw_bytes": len(raw),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "record_count": len(rows),
                "sequence": 0,
                "stored_sha256": hashlib.sha256(compressed).hexdigest(),
            }
        ],
        "checkpoint": None,
        "completed_at": CAPTURE_AT.isoformat(),
        "created_at": CAPTURE_AT.isoformat(),
        "dataset": "exchanges",
        "manifest_schema_version": 1,
        "provider": "massive",
        "provider_contract_version": "1.1",
        "provider_version": "1.2.0",
        "request": {
            "adjusted": False,
            "asset_ids": [],
            "dataset": "exchanges",
            "end": "2026-07-09",
            "parameters": {},
            "start": "2026-07-09",
        },
        "request_id": request_id,
        "status": "complete",
        "updated_at": CAPTURE_AT.isoformat(),
    }
    manifest_relative = f"manifests/massive/exchanges/{request_id}.json"
    manifest_path = root / manifest_relative
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return manifest_relative


@dataclass(frozen=True, slots=True)
class _PreviewFixture:
    authorization: exchange_preview_module.ExchangePreviewAuthorization
    swap_path: Path


def _write_preview_fixture(root: Path, *, row_count: int = 27) -> _PreviewFixture:
    request_id = "3" * 64
    rows = [
        _row(
            exchange_id,
            name=f"Review Exchange {exchange_id:02d}",
            mic=f"X{exchange_id:03d}",
        )
        for exchange_id in range(1, row_count + 1)
    ]
    response = {
        "count": len(rows),
        "request_id": "provider-preview-request",
        "results": rows,
        "status": "OK",
    }
    raw = json.dumps(response, separators=(",", ":"), sort_keys=True).encode()
    compressed = gzip.compress(raw, mtime=0)
    artifact_relative = (
        f"bronze/massive/exchanges/request_id={request_id}/page-00000.json.gz"
    )
    artifact_path = root / artifact_relative
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(compressed)
    swap_path = artifact_path.with_name(".page-00000.json.gz.swp")
    swap_path.write_bytes(b"not-authoritative")
    manifest = {
        "artifacts": [
            {
                "compressed_bytes": len(compressed),
                "content_type": "application/json",
                "is_last": True,
                "next_continuation": None,
                "path": artifact_relative,
                "raw_bytes": len(raw),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "record_count": len(rows),
                "sequence": 0,
                "stored_sha256": hashlib.sha256(compressed).hexdigest(),
            }
        ],
        "checkpoint": None,
        "completed_at": CAPTURE_AT.isoformat(),
        "created_at": CAPTURE_AT.isoformat(),
        "dataset": "exchanges",
        "manifest_schema_version": 1,
        "provider": "massive",
        "provider_contract_version": "1.1",
        "provider_version": "1.2.0",
        "request": {
            "adjusted": False,
            "asset_ids": [],
            "dataset": "exchanges",
            "end": "2026-07-09",
            "parameters": {},
            "start": "2026-07-09",
        },
        "request_id": request_id,
        "status": "complete",
        "updated_at": CAPTURE_AT.isoformat(),
    }
    manifest_relative = f"manifests/massive/exchanges/{request_id}.json"
    manifest_content = json.dumps(manifest, sort_keys=True).encode()
    manifest_path = root / manifest_relative
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_bytes(manifest_content)
    authorization = exchange_preview_module.ExchangePreviewAuthorization(
        manifest_path=manifest_relative,
        manifest_sha256=hashlib.sha256(manifest_content).hexdigest(),
        request_id=request_id,
        artifact_path=artifact_relative,
        artifact_sha256=hashlib.sha256(compressed).hexdigest(),
        expected_rows=27,
    )
    return _PreviewFixture(authorization=authorization, swap_path=swap_path)


def _init_preview_git_checkout(root: Path) -> tuple[Path, Path, str]:
    repo = root / "repo"
    module_path = repo / "backend/ame_stocks_api/silver/exchange_preview.py"
    module_path.parent.mkdir(parents=True)
    module_path.write_text("# synthetic provenance marker\n", encoding="utf-8")
    commands = (
        ("git", "init", "-q", str(repo)),
        ("git", "-C", str(repo), "config", "user.email", "preview@example.test"),
        ("git", "-C", str(repo), "config", "user.name", "Preview Test"),
        ("git", "-C", str(repo), "add", "."),
        ("git", "-C", str(repo), "commit", "-q", "-m", "preview fixture"),
    )
    for command in commands:
        subprocess.run(command, check=True, capture_output=True, text=True)
    head = subprocess.run(
        ("git", "-C", str(repo), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return repo, module_path, head


def _init_release_git_checkout(root: Path) -> tuple[Path, Path, str]:
    repo = root / "repo"
    for relative in exchange_release_module._REVIEWED_LOGIC_CLOSURE:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"# reviewed fixture: {relative}\n", encoding="utf-8")
    source_pyproject = Path(__file__).parents[1] / "pyproject.toml"
    (repo / "pyproject.toml").write_text(
        source_pyproject.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    commands = (
        ("git", "init", "-q", str(repo)),
        ("git", "-C", str(repo), "config", "user.email", "release@example.test"),
        ("git", "-C", str(repo), "config", "user.name", "Release Test"),
        ("git", "-C", str(repo), "add", "."),
        ("git", "-C", str(repo), "commit", "-q", "-m", "reviewed logic"),
    )
    for command in commands:
        subprocess.run(command, check=True, capture_output=True, text=True)
    head = subprocess.run(
        ("git", "-C", str(repo), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    preview_module = repo / "backend/ame_stocks_api/silver/exchange_preview.py"
    return repo, preview_module, head


def _advance_release_git_checkout(repo: Path, *, drift_logic: bool = False) -> tuple[Path, str]:
    release_module = repo / "backend/ame_stocks_api/silver/exchange_release.py"
    release_module.write_text("# orchestration adapter fixture\n", encoding="utf-8")
    if drift_logic:
        changed = repo / "backend/ame_stocks_api/silver/exchanges.py"
        changed.write_text("# forbidden reviewed logic drift\n", encoding="utf-8")
    subprocess.run(
        ("git", "-C", str(repo), "add", "."),
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ("git", "-C", str(repo), "commit", "-q", "-m", "add release adapter"),
        check=True,
        capture_output=True,
        text=True,
    )
    head = subprocess.run(
        ("git", "-C", str(repo), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return release_module, head


def _exchange_code_ready(root: Path) -> tuple[SilverStore, str, str]:
    store = SilverStore(root)
    snapshot = store.create_workflow(
        EXCHANGE_DIM_CONTRACT,
        actor="preview-test-author",
        created_at="2026-07-12T01:00:00+00:00",
    )
    snapshot = store.submit_schema_review(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="preview-test-author",
        created_at="2026-07-12T01:01:00+00:00",
    )
    snapshot = store.approve_schema(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        approver="preview-test-reviewer",
        decided_at="2026-07-12T01:02:00+00:00",
    )
    assert snapshot.state is WorkflowState.CODE_READY
    return store, snapshot.workflow_id, snapshot.event_sha256


def _run_fixture_preview(
    data_root: Path,
    *,
    workflow_id: str,
    event_sha256: str,
    repo_root: Path,
    git_commit: str,
    fixture: _PreviewFixture,
    sample_limit: int = 100,
):
    authorization = fixture.authorization
    return exchange_preview_module._run_exchange_preview_authorized(
        data_root,
        workflow_id=workflow_id,
        expected_event_sha256=event_sha256,
        manifest_paths=(authorization.manifest_path,),
        expected_manifest_sha256=authorization.manifest_sha256,
        expected_input_rows=authorization.expected_rows,
        git_commit=git_commit,
        repo_root=repo_root,
        actor="preview-test-runner",
        calendar_name="XNYS",
        sample_limit=sample_limit,
        authorization=authorization,
    )


def test_manifest_bound_inventory_ignores_unlisted_files_and_detects_mutation(
    tmp_path: Path,
) -> None:
    manifest_path = _write_bronze_fixture(tmp_path)
    inventory = build_exchange_source_inventory(
        tmp_path,
        manifest_paths=(manifest_path,),
        git_commit="a" * 40,
    )

    assert inventory.source_dataset == "exchanges"
    assert inventory.source_layer is SourceLayer.BRONZE
    assert len(inventory.upstream_manifests) == len(inventory.artifacts) == 1
    assert not inventory.artifacts[0].path.endswith(".swp")
    batch = read_exchange_source_inventory(tmp_path, inventory)
    assert (batch.source_object_count, batch.page_count, batch.row_count) == (2, 1, 1)
    assert _transform(*batch.snapshots).table.num_rows == 1

    artifact = tmp_path / inventory.artifacts[0].path
    artifact.write_bytes(artifact.read_bytes() + b"tampered")
    with pytest.raises(ExchangeSourceError, match="byte count mismatch"):
        read_exchange_source_inventory(tmp_path, inventory)


def test_manifest_reader_rejects_optional_envelope_count_mismatch(tmp_path: Path) -> None:
    manifest_path = _write_bronze_fixture(tmp_path, response_count=2)
    inventory = build_exchange_source_inventory(
        tmp_path,
        manifest_paths=(manifest_path,),
        git_commit="a" * 40,
    )
    with pytest.raises(ExchangeSourceError, match="count does not match"):
        read_exchange_source_inventory(tmp_path, inventory)


def test_approved_contract_can_enter_code_ready_without_a_preview(tmp_path: Path) -> None:
    store = SilverStore(tmp_path)
    snapshot = store.create_workflow(
        EXCHANGE_DIM_CONTRACT,
        actor="ame-stocks-author",
        created_at="2026-07-13T00:00:00+00:00",
    )
    snapshot = store.submit_schema_review(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="ame-stocks-author",
        created_at="2026-07-13T00:01:00+00:00",
    )
    snapshot = store.approve_schema(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        approver="user-approved-contract-1803d28f",
        decided_at="2026-07-13T00:02:00+00:00",
        note="Exact exchange_dim schema approved; no preview has run.",
    )

    assert snapshot.state is WorkflowState.CODE_READY
    approval, _ = store.load_approval(str(snapshot.evidence["approval_id"]))
    assert approval.subject_id == EXCHANGE_DIM_CONTRACT.contract_id
    assert approval.expected_event_sha256 is not None
    assert not (tmp_path / "staging").exists()
    assert not (tmp_path / "silver").exists()


def test_bounded_exchange_preview_writes_reviewable_27_row_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    fixture = _write_preview_fixture(data_root)
    repo_root, module_path, git_commit = _init_preview_git_checkout(tmp_path)
    monkeypatch.setattr(exchange_preview_module, "__file__", str(module_path))
    store, workflow_id, event_sha256 = _exchange_code_ready(data_root)

    run = _run_fixture_preview(
        data_root,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )

    assert run.workflow.state is WorkflowState.AWAITING_REVIEW
    assert run.build.intent.kind is BuildKind.PREVIEW
    assert run.build.intent.git_commit == git_commit
    assert run.inventory.inventory_id == run.build.preview.full_run_projection[
        "source_inventory_id"
    ]
    assert run.build.row_funnel.to_dict() == {
        "accepted_source_rows": 27,
        "exact_duplicate_excess": 0,
        "input_rows": 27,
        "output_rows_by_table": {"exchange_dim": 27},
        "quarantined_source_rows": 0,
        "unmapped_source_rows": 0,
        "version_preserved_rows": 0,
    }
    assert len(run.build.qa_checks) == 20
    assert all(check.status is QAStatus.PASSED for check in run.build.qa_checks)
    assert run.build.quarantine_issue_rows == run.build.quarantine_unique_source_rows == 0
    assert len(run.build.outputs) == 6

    outputs_by_role: dict[ArtifactRole, list] = {}
    for output in run.build.outputs:
        outputs_by_role.setdefault(output.role, []).append(output)
        path = data_root / output.path
        details = path.stat()
        assert stat.S_IMODE(details.st_mode) == 0o444
        assert details.st_nlink == 1
        assert details.st_size == output.bytes
        assert hashlib.sha256(path.read_bytes()).hexdigest() == output.sha256
    assert {role: len(items) for role, items in outputs_by_role.items()} == {
        ArtifactRole.DATA: 1,
        ArtifactRole.QA: 1,
        ArtifactRole.QUARANTINE: 1,
        ArtifactRole.SAMPLE: 3,
    }

    data_output = outputs_by_role[ArtifactRole.DATA][0]
    data_table = pq.ParquetFile(data_root / data_output.path).read()
    assert data_table.schema == EXCHANGE_DIM_CONTRACT.arrow_schema
    assert data_table.num_rows == 27
    assert len(data_table.column_names) == 23
    qa_table = pq.ParquetFile(data_root / outputs_by_role[ArtifactRole.QA][0].path).read()
    assert qa_table.num_rows == 20
    assert set(qa_table.column("status").to_pylist()) == {"passed"}
    quarantine_table = pq.ParquetFile(
        data_root / outputs_by_role[ArtifactRole.QUARANTINE][0].path
    ).read()
    assert quarantine_table.num_rows == 0

    preview = run.build.preview
    assert preview is not None
    assert preview.input_sample_rows == preview.output_sample_rows == 27
    assert preview.examples_truncated is False
    input_rows = json.loads((data_root / preview.input_sample_path).read_text())
    output_rows = json.loads((data_root / preview.output_sample_path).read_text())
    assert len(input_rows) == len(output_rows) == 27
    fixed_output = next(
        item for item in outputs_by_role[ArtifactRole.SAMPLE] if "current-reference" in item.path
    )
    fixed_rows = json.loads((data_root / fixed_output.path).read_text())
    assert len(fixed_rows) == 4
    assert all(item["passed"] is True for item in fixed_rows)
    fixed_checks = {
        check.check_id: check
        for check in run.build.qa_checks
        if check.check_id in exchange_preview_module._FIXED_CASE_QA_CHECKS
    }
    assert set(fixed_checks) == set(exchange_preview_module._FIXED_CASE_QA_CHECKS)
    assert {check.bounded_examples_path for check in fixed_checks.values()} == {
        fixed_output.path
    }
    assert set(preview.fixed_case_qa_result_ids["current_reference_snapshot"]) == {
        check.result_id for check in fixed_checks.values()
    }

    store.verify_build(run.build, EXCHANGE_DIM_CONTRACT)
    assert fixture.swap_path.read_bytes() == b"not-authoritative"
    assert not (data_root / "silver").exists()
    assert not (data_root / "manifests/silver/releases").exists()


def test_exchange_preview_is_exactly_idempotent_at_awaiting_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    fixture = _write_preview_fixture(data_root)
    repo_root, module_path, git_commit = _init_preview_git_checkout(tmp_path)
    monkeypatch.setattr(exchange_preview_module, "__file__", str(module_path))
    store, workflow_id, event_sha256 = _exchange_code_ready(data_root)
    first = _run_fixture_preview(
        data_root,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )
    file_state = {
        item.path: (item.sha256, (data_root / item.path).stat().st_mtime_ns)
        for item in first.build.outputs
    }
    event_count = len(store.workflow_events(workflow_id))

    repeated = _run_fixture_preview(
        data_root,
        workflow_id=workflow_id,
        event_sha256=first.workflow.event_sha256,
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )

    assert repeated.build.build_id == first.build.build_id
    assert repeated.workflow.event_sha256 == first.workflow.event_sha256
    assert len(store.workflow_events(workflow_id)) == event_count
    assert file_state == {
        item.path: (item.sha256, (data_root / item.path).stat().st_mtime_ns)
        for item in repeated.build.outputs
    }
    with pytest.raises(SilverStoreError, match="exact run intent"):
        _run_fixture_preview(
            data_root,
            workflow_id=workflow_id,
            event_sha256=first.workflow.event_sha256,
            repo_root=repo_root,
            git_commit=git_commit,
            fixture=fixture,
            sample_limit=99,
        )
    with pytest.raises(SilverStoreError, match="stale"):
        _run_fixture_preview(
            data_root,
            workflow_id=workflow_id,
            event_sha256=event_sha256,
            repo_root=repo_root,
            git_commit=git_commit,
            fixture=fixture,
        )
    assert len(store.workflow_events(workflow_id)) == event_count


def test_exchange_preview_rejects_scope_row_calendar_and_git_drift_before_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    fixture = _write_preview_fixture(data_root, row_count=26)
    repo_root, module_path, git_commit = _init_preview_git_checkout(tmp_path)
    monkeypatch.setattr(exchange_preview_module, "__file__", str(module_path))
    _, workflow_id, event_sha256 = _exchange_code_ready(data_root)
    authorization = fixture.authorization
    common = {
        "workflow_id": workflow_id,
        "expected_event_sha256": event_sha256,
        "expected_manifest_sha256": authorization.manifest_sha256,
        "expected_input_rows": authorization.expected_rows,
        "git_commit": git_commit,
        "repo_root": repo_root,
        "actor": "preview-test-runner",
        "sample_limit": 100,
        "authorization": authorization,
    }

    with pytest.raises(SilverStoreError, match="one exact manifest"):
        exchange_preview_module._run_exchange_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path, authorization.manifest_path),
            calendar_name="XNYS",
            **common,
        )
    with pytest.raises(SilverStoreError, match="pinned to XNYS"):
        exchange_preview_module._run_exchange_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XHKG",
            **common,
        )
    with pytest.raises(SilverStoreError, match="source page is not the authorized object"):
        exchange_preview_module._run_exchange_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XNYS",
            **common,
        )
    assert not (data_root / "staging").exists()
    assert not (data_root / "manifests/silver/source-inventories").exists()

    wrong_head = dict(common)
    wrong_head["git_commit"] = "f" * 40
    with pytest.raises(SilverStoreError, match="HEAD differs"):
        exchange_preview_module._run_exchange_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XNYS",
            **wrong_head,
        )
    (repo_root / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(SilverStoreError, match="not clean"):
        exchange_preview_module._run_exchange_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XNYS",
            **common,
        )
    assert not (data_root / "staging").exists()


def test_exchange_preview_cli_requires_all_fail_closed_guards() -> None:
    from ame_stocks_api.cli.silver_exchanges_preview import build_parser

    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--data-root",
                "/tmp/data",
                "--workflow-id",
                "a" * 64,
                "--expected-event-sha256",
                "b" * 64,
                "--manifest",
                "manifests/massive/exchanges/example.json",
                "--git-commit",
                "c" * 40,
            ]
        )


def test_reviewed_exchange_preview_completes_one_verified_published_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    fixture = _write_preview_fixture(data_root)
    repo_root, preview_module, preview_commit = _init_release_git_checkout(tmp_path)
    monkeypatch.setattr(exchange_preview_module, "__file__", str(preview_module))
    store, workflow_id, event_sha256 = _exchange_code_ready(data_root)
    preview_run = _run_fixture_preview(
        data_root,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
        repo_root=repo_root,
        git_commit=preview_commit,
        fixture=fixture,
    )
    release_module, runner_commit = _advance_release_git_checkout(repo_root)
    monkeypatch.setattr(exchange_release_module, "__file__", str(release_module))

    completed = exchange_release_module._complete_exchange_release_authorized(
        data_root,
        workflow_id=workflow_id,
        expected_event_sha256=preview_run.workflow.event_sha256,
        reviewed_preview_build_id=preview_run.build.build_id,
        reviewed_preview_manifest_sha256=preview_run.build_document.sha256,
        repo_root=repo_root,
        runner_git_commit=runner_commit,
        actor="release-test-runner",
        approver="user-approved-s1-completion",
        authorization=fixture.authorization,
    )

    assert completed.workflow.state is WorkflowState.PUBLISHED
    assert completed.workflow.sequence == 9
    assert len(store.workflow_events(workflow_id)) == 9
    assert completed.full.intent.kind is BuildKind.FULL
    assert completed.full.intent.git_commit == preview_commit
    assert completed.full.intent.parameters["approved_preview_build_id"] == (
        preview_run.build.build_id
    )
    assert completed.full.preview is None
    assert completed.full.row_funnel == preview_run.build.row_funnel
    assert len(completed.full.qa_checks) == 20
    assert all(check.status is QAStatus.PASSED for check in completed.full.qa_checks)
    assert completed.full.quarantine_issue_rows == 0
    assert len(completed.full.outputs) == 7

    preview_data = next(
        item for item in preview_run.build.outputs if item.role is ArtifactRole.DATA
    )
    full_data = next(item for item in completed.full.outputs if item.role is ArtifactRole.DATA)
    assert full_data.sha256 == preview_data.sha256
    assert full_data.path.startswith("silver/schema=v1/reference/exchange_dim/")
    assert completed.release.outputs == (full_data,)
    assert completed.published.data_paths == (data_root / full_data.path,)
    assert not (data_root / "silver/schema=v1/reference/exchange_dim/current").exists()

    provenance_output = next(
        item for item in completed.full.outputs if item.path.endswith("runtime-provenance.json")
    )
    provenance_rows = json.loads((data_root / provenance_output.path).read_text())
    assert provenance_rows[0]["runner_git_commit"] == runner_commit
    assert provenance_rows[0]["preview_transform_git_commit"] == preview_commit
    assert provenance_rows[0]["full_build_id"] == completed.full.build_id
    assert set(provenance_rows[0]["logic_closure_paths"]) == set(
        exchange_release_module._REVIEWED_LOGIC_CLOSURE
    )
    for output in completed.full.outputs:
        details = (data_root / output.path).stat()
        assert stat.S_IMODE(details.st_mode) == 0o444
        assert details.st_nlink == 1

    approval_events = [
        record
        for record in store.workflow_events(workflow_id)
        if record.event.to_state
        in {WorkflowState.APPROVED_FULL_RUN, WorkflowState.PUBLISHED}
    ]
    assert len(approval_events) == 2
    for event in approval_events:
        approval, _ = store.load_approval(str(event.event.evidence["approval_id"]))
        assert approval.waived_qa_result_ids == ()
        assert approval.accepted_quarantine_issue_ids == ()

    file_state = {
        item.path: (item.sha256, (data_root / item.path).stat().st_mtime_ns)
        for item in completed.full.outputs
    }
    repeated = exchange_release_module._complete_exchange_release_authorized(
        data_root,
        workflow_id=workflow_id,
        expected_event_sha256=completed.workflow.event_sha256,
        reviewed_preview_build_id=preview_run.build.build_id,
        reviewed_preview_manifest_sha256=preview_run.build_document.sha256,
        repo_root=repo_root,
        runner_git_commit=runner_commit,
        actor="release-test-runner",
        approver="user-approved-s1-completion",
        authorization=fixture.authorization,
    )
    assert repeated.release.release_id == completed.release.release_id
    assert len(store.workflow_events(workflow_id)) == 9
    assert file_state == {
        item.path: (item.sha256, (data_root / item.path).stat().st_mtime_ns)
        for item in repeated.full.outputs
    }


def test_exchange_release_refuses_reviewed_logic_drift_before_full_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    fixture = _write_preview_fixture(data_root)
    repo_root, preview_module, preview_commit = _init_release_git_checkout(tmp_path)
    monkeypatch.setattr(exchange_preview_module, "__file__", str(preview_module))
    store, workflow_id, event_sha256 = _exchange_code_ready(data_root)
    preview_run = _run_fixture_preview(
        data_root,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
        repo_root=repo_root,
        git_commit=preview_commit,
        fixture=fixture,
    )
    release_module, runner_commit = _advance_release_git_checkout(
        repo_root,
        drift_logic=True,
    )
    monkeypatch.setattr(exchange_release_module, "__file__", str(release_module))

    with pytest.raises(SilverStoreError, match="logic closure changed"):
        exchange_release_module._complete_exchange_release_authorized(
            data_root,
            workflow_id=workflow_id,
            expected_event_sha256=preview_run.workflow.event_sha256,
            reviewed_preview_build_id=preview_run.build.build_id,
            reviewed_preview_manifest_sha256=preview_run.build_document.sha256,
            repo_root=repo_root,
            runner_git_commit=runner_commit,
            actor="release-test-runner",
            approver="user-approved-s1-completion",
            authorization=fixture.authorization,
        )
    assert store.status(workflow_id).state is WorkflowState.AWAITING_REVIEW
    assert not (data_root / "silver").exists()


@pytest.mark.parametrize(
    ("failing_method", "expected_state"),
    (
        ("record_full_build", WorkflowState.APPROVED_FULL_RUN),
        ("request_publish", WorkflowState.FULL_READY),
        ("publish", WorkflowState.AWAITING_PUBLISH),
    ),
)
def test_exchange_release_resumes_from_each_post_review_hard_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failing_method: str,
    expected_state: WorkflowState,
) -> None:
    data_root = tmp_path / "data"
    fixture = _write_preview_fixture(data_root)
    repo_root, preview_module, preview_commit = _init_release_git_checkout(tmp_path)
    monkeypatch.setattr(exchange_preview_module, "__file__", str(preview_module))
    store, workflow_id, event_sha256 = _exchange_code_ready(data_root)
    preview_run = _run_fixture_preview(
        data_root,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
        repo_root=repo_root,
        git_commit=preview_commit,
        fixture=fixture,
    )
    release_module, runner_commit = _advance_release_git_checkout(repo_root)
    monkeypatch.setattr(exchange_release_module, "__file__", str(release_module))
    arguments = {
        "workflow_id": workflow_id,
        "reviewed_preview_build_id": preview_run.build.build_id,
        "reviewed_preview_manifest_sha256": preview_run.build_document.sha256,
        "repo_root": repo_root,
        "runner_git_commit": runner_commit,
        "actor": "release-test-runner",
        "approver": "user-approved-s1-completion",
        "authorization": fixture.authorization,
    }

    def simulated_interruption(*_args, **_kwargs):
        raise RuntimeError("simulated release interruption")

    with monkeypatch.context() as interruption:
        interruption.setattr(SilverStore, failing_method, simulated_interruption)
        with pytest.raises(RuntimeError, match="simulated release interruption"):
            exchange_release_module._complete_exchange_release_authorized(
                data_root,
                expected_event_sha256=preview_run.workflow.event_sha256,
                **arguments,
            )

    stopped = store.status(workflow_id)
    assert stopped.state is expected_state
    completed = exchange_release_module._complete_exchange_release_authorized(
        data_root,
        expected_event_sha256=stopped.event_sha256,
        **arguments,
    )
    assert completed.workflow.state is WorkflowState.PUBLISHED
    assert completed.workflow.sequence == 9


def test_exchange_release_cli_requires_review_and_provenance_guards() -> None:
    from ame_stocks_api.cli.silver_exchanges_release import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "--data-root",
                "/tmp/data",
                "--repo-root",
                "/tmp/repo",
                "--workflow-id",
                "a" * 64,
                "--expected-event-sha256",
                "b" * 64,
                "--reviewed-preview-build-id",
                "c" * 64,
                "--runner-git-commit",
                "d" * 40,
                "--actor",
                "runner",
                "--approver",
                "reviewer",
            ]
        )
