from __future__ import annotations

import errno
import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ame_stocks_api.silver.identity_market_inventory_contract import (
    COMPOSITE_FIGI_INVENTORY_CONTRACT,
)
from ame_stocks_api.silver.identity_market_inventory_engine import (
    LINEAGE_RULE_VERSION,
    SCAN_ORDER_RULE,
    UNIVERSE_PARENT_PROJECTION,
    CompositeInventoryDiagnostics,
)
from ame_stocks_api.silver.identity_market_inventory_runner import (
    CANDIDATE_STATE,
    IdentityMarketInventoryRunnerError,
    InventoryOutputArtifactRef,
    _LoadedControls,
    _qa_document,
    _validate_bounded_examples,
    composite_inventory_completion_path,
    run_source_bound_composite_inventory,
)
from ame_stocks_api.silver.identity_source import S7_SOURCE_PINS

PLAN_ID = "1" * 64
PLAN_SHA = "2" * 64
APPROVAL_ID = "3" * 64
APPROVAL_SHA = "4" * 64
REQUEST_ID = "5" * 64
REQUEST_SHA = "6" * 64
INPUT_DIGEST = "7" * 64
RUNTIME_DIGEST = "8" * 64
VERIFICATION_DIGEST = "9" * 64
CAPS_DIGEST = "a" * 64
SESSION = date(2024, 1, 2)


class _FakeArtifact:
    def __init__(
        self,
        table: str,
        *,
        path: str,
        row_count: int,
        byte_count: int,
    ) -> None:
        pin = S7_SOURCE_PINS[table]
        self.table = table
        self.release_id = pin.release_id
        self.release_manifest_sha256 = pin.release_manifest_sha256
        self.ref = SimpleNamespace(
            table=table,
            path=path,
            sha256=hashlib.sha256(path.encode()).hexdigest(),
            bytes=byte_count,
            row_count=row_count,
        )

    def require_official(self) -> None:
        return None


class _FakePhysical:
    def __init__(self, artifact: _FakeArtifact, rows: list[dict[str, object]]) -> None:
        self.artifact = artifact
        self.batch = pa.RecordBatch.from_pylist(rows)
        self.row_group = 0
        self.row_index_in_group = 0

    def require_official(self) -> None:
        return None


class _FakeBundle:
    def __init__(
        self,
        asset_rows: list[dict[str, object]],
        universe_rows: list[dict[str, object]],
    ) -> None:
        self.asset = _FakeArtifact(
            "asset_observation_daily",
            path="silver/assets/session_date=2024-01-02/part.parquet",
            row_count=len(asset_rows),
            byte_count=11,
        )
        self.universe = _FakeArtifact(
            "universe_source_daily",
            path="silver/universe/session_date=2024-01-02/part.parquet",
            row_count=len(universe_rows),
            byte_count=13,
        )
        self.rows = {
            "asset_observation_daily": asset_rows,
            "universe_source_daily": universe_rows,
        }
        self.sources = {table: SimpleNamespace(pin=pin) for table, pin in S7_SOURCE_PINS.items()}
        self.scan_count = 0

    def require_official(self) -> None:
        return None

    def daily_partition_artifacts(
        self, table: str, sessions: tuple[date, ...]
    ) -> tuple[_FakeArtifact, ...]:
        assert sessions == (SESSION,)
        return (self.asset if table == "asset_observation_daily" else self.universe,)

    def iter_physical_batches(
        self,
        table: str,
        *,
        columns: tuple[str, ...],
        batch_size: int,
        artifacts: tuple[_FakeArtifact, ...],
    ):
        assert batch_size > 0
        assert artifacts[0].table == table
        self.scan_count += 1
        rows = [{field: row[field] for field in columns} for row in self.rows[table]]
        yield _FakePhysical(artifacts[0], rows)


def _source_id(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _asset_row(
    ticker: str,
    *,
    source_id: str,
    composite: str,
    share: str | None,
    active: bool,
) -> dict[str, object]:
    row: dict[str, object] = {
        "session_year": SESSION.year,
        "session_date": SESSION,
        "source_record_id": source_id,
        "ticker": ticker,
        "requested_active": active,
        "provider_active": active,
        "type_code": "CS",
        "name": ticker,
        "market": "stocks",
        "locale": "us",
        "primary_exchange_mic": "XNAS",
        "currency_name": "usd",
        "cik": None,
        "composite_figi": composite,
        "share_class_figi": share,
        "delisted_at_utc": None,
        "last_updated_at_utc": None,
        "reference_time_scope": "point_in_time",
        "metadata_time_scope": "provider_asof",
        "source_capture_at_utc": "2024-01-03T00:00:00+00:00",
        "source_availability_quality": "exact",
        "source_request_id": "req",
        "source_provider_request_id": "provider-req",
        "source_artifact_sha256": "b" * 64,
        "source_page_sequence": 1,
        "source_row_ordinal": 1,
        "source_row_hash": "c" * 64,
    }
    return row


def _universe_row(asset: dict[str, object]) -> dict[str, object]:
    row: dict[str, object] = {
        universe_field: asset[asset_field]
        for asset_field, universe_field in UNIVERSE_PARENT_PROJECTION
    }
    row["selected_source_record_id"] = asset["source_record_id"]
    row["active_on_date"] = asset["provider_active"]
    row["selected_source_capture_at_utc"] = asset["source_capture_at_utc"]
    return row


def _fixture_rows() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    rows = [
        _asset_row(
            "AAA",
            source_id=_source_id("a"),
            composite="BBG000000001",
            share="BBG000000011",
            active=True,
        ),
        _asset_row(
            "BBB",
            source_id=_source_id("b"),
            composite="BBG000000001",
            share="BBG000000011",
            active=False,
        ),
    ]
    # Only one selected membership row: universe must not increment inventory counts.
    return rows, [_universe_row(rows[0])]


def _controls(
    *,
    distinct_cap: int = 10,
    output_cap: int = 10_000_000,
    disk_warning: int = 0,
) -> _LoadedControls:
    caps = SimpleNamespace(
        scanned_artifact_cap=2,
        scanned_row_cap=3,
        source_bytes_cap=24,
        distinct_composite_cap=distinct_cap,
        composite_share_class_pair_cap=10,
        output_bytes_cap=output_cap,
        tmp_bytes_cap=10_000_000,
        rss_bytes_cap=10**15,
        batch_size=100,
        worker_count=1,
        wall_clock_seconds_cap=60,
        disk_free_floor_bytes=0,
        disk_free_warning_bytes=disk_warning,
        bounded_example_cap=20,
        resource_check_interval_batches=1,
        digest=CAPS_DIGEST,
    )
    plan = SimpleNamespace(
        plan_id=PLAN_ID,
        sha256=PLAN_SHA,
        input_binding_digest=INPUT_DIGEST,
        runtime_file_set_digest=RUNTIME_DIGEST,
        verification_file_set_digest=VERIFICATION_DIGEST,
        execution_data_root="/tmp/unbound-s7-test-root",
        resource_caps=caps,
        inventory_contract=SimpleNamespace(
            contract_id="d" * 64,
            schema_digest="e" * 64,
        ),
    )
    approval = SimpleNamespace(
        approval_id=APPROVAL_ID,
        sha256=APPROVAL_SHA,
        request_event_id=REQUEST_ID,
        request_event_sha256=REQUEST_SHA,
        execution_data_root="/tmp/unbound-s7-test-root",
        approved_at_utc=datetime(2024, 1, 1, tzinfo=UTC),
    )
    return _LoadedControls(
        plan=plan,
        approval=approval,
        calendar=SimpleNamespace(),
        sessions=(SESSION,),
    )


def _install_fixture(
    monkeypatch: pytest.MonkeyPatch,
    bundle: _FakeBundle,
    controls,
    root: Path,
):
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    controls.plan.execution_data_root = str(root.resolve())
    controls.approval.execution_data_root = str(root.resolve())
    monkeypatch.setattr(runner, "_load_controls", lambda *args, **kwargs: controls)
    monkeypatch.setattr(runner, "_verify_git_checkout_and_pins", lambda plan: None)
    monkeypatch.setattr(runner, "open_identity_source_bundle", lambda root: bundle)
    monkeypatch.setattr(runner._ResourceMonitor, "check", lambda self: None)


def _run(root: Path):
    return run_source_bound_composite_inventory(
        root,
        plan_id=PLAN_ID,
        expected_plan_sha256=PLAN_SHA,
        approval_id=APPROVAL_ID,
        expected_approval_sha256=APPROVAL_SHA,
    )


def test_bounded_example_cap_is_per_reason() -> None:
    examples = [
        *({"reason": "composite_figi_null"} for _ in range(20)),
        *({"reason": "composite_figi_empty"} for _ in range(20)),
    ]
    _validate_bounded_examples(examples, per_reason_cap=20)

    with pytest.raises(
        IdentityMarketInventoryRunnerError,
        match="per-reason cap",
    ):
        _validate_bounded_examples(
            [*examples, {"reason": "composite_figi_null"}],
            per_reason_cap=20,
        )


def test_bounded_examples_match_diagnostics_and_reason_precedence() -> None:
    diagnostics = {
        "invalid_composite_reason_counts": {
            "composite_figi_null": 3,
            "composite_figi_empty": 1,
        },
        "invalid_share_class_reason_counts": {},
    }
    complete = [
        {"reason": "composite_figi_null"},
        {"reason": "composite_figi_null"},
        {"reason": "composite_figi_empty"},
    ]
    _validate_bounded_examples(
        complete,
        per_reason_cap=2,
        diagnostics=diagnostics,
    )

    with pytest.raises(
        IdentityMarketInventoryRunnerError,
        match="completely represent diagnostics",
    ):
        _validate_bounded_examples(
            complete[:-1],
            per_reason_cap=2,
            diagnostics=diagnostics,
        )
    with pytest.raises(
        IdentityMarketInventoryRunnerError,
        match="reason precedence",
    ):
        _validate_bounded_examples(
            list(reversed(complete)),
            per_reason_cap=2,
            diagnostics=diagnostics,
        )


def test_qa_warning_rows_link_to_bounded_figi_evidence() -> None:
    diagnostics = CompositeInventoryDiagnostics(
        authority_row_count=3,
        reconciliation_row_count=3,
        authority_universe_row_count_difference=0,
        nonselected_authority_row_count=0,
        completed_session_count=1,
        valid_composite_row_count=1,
        invalid_composite_reason_counts=(("composite_figi_empty", 1),),
        invalid_share_class_reason_counts=(("share_class_figi_null", 1),),
        share_class_conflict_groups=0,
        distinct_composite_share_class_pair_count=0,
        bounded_invalid_examples=(),
    )
    examples_ref = InventoryOutputArtifactRef(
        role="bounded_examples",
        path="examples/invalid-figi.json",
        sha256="f" * 64,
        bytes=1,
        media_type="application/json",
    )
    document = _qa_document(
        _controls(),
        diagnostics=diagnostics,
        examples_ref=examples_ref,
    )
    paths = {item["check_id"]: item["bounded_examples_path"] for item in document["results"]}
    assert paths["malformed_composite_rows"] == "examples/invalid-figi.json"
    assert paths["valid_composite_missing_share_class_rows"] == ("examples/invalid-figi.json")


def test_success_is_idempotent_and_universe_is_reconciliation_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)

    completion = _run(tmp_path)
    assert completion.completion_state == CANDIDATE_STATE
    assert completion.authority_row_count == 2
    assert completion.reconciliation_row_count == 1
    assert completion.inventory_row_count == 1
    assert bundle.scan_count == 2

    table = pq.read_table(tmp_path / completion.data_path)
    assert table.schema.equals(COMPOSITE_FIGI_INVENTORY_CONTRACT.arrow_schema)
    row = table.to_pylist()[0]
    assert row["active_row_count"] == 1
    assert row["inactive_row_count"] == 1
    assert row["ticker_count"] == 2
    seed = json.dumps(
        {
            "parent_table": "asset_observation_daily",
            "release_id": S7_SOURCE_PINS["asset_observation_daily"].release_id,
            "rule_version": LINEAGE_RULE_VERSION,
            "scan_order": SCAN_ORDER_RULE,
        },
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    lineage = hashlib.sha256(seed)
    lineage.update(bytes.fromhex(_source_id("a")))
    lineage.update(bytes.fromhex(_source_id("b")))
    assert row["source_record_lineage_digest"] == lineage.hexdigest()
    retained_staging = list((tmp_path / "tmp/silver-identity-composite-inventory").glob("run_id=*"))
    assert len(retained_staging) == 1
    staged_completion = retained_staging[0] / "completion.json"
    stable_completion = tmp_path / completion.relative_path
    assert staged_completion.is_file()
    assert (staged_completion.stat().st_dev, staged_completion.stat().st_ino) == (
        stable_completion.stat().st_dev,
        stable_completion.stat().st_ino,
    )

    repeated = _run(tmp_path)
    assert repeated == completion
    assert bundle.scan_count == 2


def test_cap_failure_leaves_only_staging_and_no_stable_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asset, universe = _fixture_rows()
    asset[1]["composite_figi"] = "BBG000000002"
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(distinct_cap=1), tmp_path)

    with pytest.raises(IdentityMarketInventoryRunnerError, match="physical S4 inventory scan"):
        _run(tmp_path)
    assert not (tmp_path / "manifests/silver/identity/composite-inventory-candidates").exists()
    assert not (tmp_path / composite_inventory_completion_path(PLAN_ID, APPROVAL_ID)).exists()
    assert (tmp_path / "tmp/silver-identity-composite-inventory").is_dir()


def test_output_cap_fails_before_any_stable_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(output_cap=1), tmp_path)

    with pytest.raises(IdentityMarketInventoryRunnerError, match="output bytes"):
        _run(tmp_path)
    candidates = tmp_path / "manifests/silver/identity/composite-inventory-candidates"
    assert not candidates.exists() or not any(candidates.iterdir())
    assert not (tmp_path / composite_inventory_completion_path(PLAN_ID, APPROVAL_ID)).exists()


def test_tampered_candidate_is_rejected_without_rescan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)
    completion = _run(tmp_path)
    data = tmp_path / completion.data_path
    data.chmod(0o644)
    data.write_bytes(data.read_bytes() + b"tamper")

    with pytest.raises(IdentityMarketInventoryRunnerError, match="stored candidate data differs"):
        _run(tmp_path)
    assert bundle.scan_count == 2


def test_idempotent_readback_rejects_current_source_ref_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)
    _run(tmp_path)
    bundle.asset.ref.sha256 = "0" * 64

    with pytest.raises(
        IdentityMarketInventoryRunnerError,
        match="source binding differs",
    ):
        _run(tmp_path)


def test_actual_data_root_mismatch_stops_before_git_or_source_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    controls = _controls()
    other = tmp_path / "copied-controls-root"
    controls.plan.execution_data_root = str(other)
    controls.approval.execution_data_root = str(other)
    git_checked = False
    source_opened = False

    def check_git(plan: object) -> None:
        nonlocal git_checked
        git_checked = True

    def open_source(root: Path) -> None:
        nonlocal source_opened
        source_opened = True

    monkeypatch.setattr(runner, "_load_controls", lambda *args, **kwargs: controls)
    monkeypatch.setattr(runner, "_verify_git_checkout_and_pins", check_git)
    monkeypatch.setattr(runner, "open_identity_source_bundle", open_source)

    with pytest.raises(
        IdentityMarketInventoryRunnerError,
        match="actual data_root differs",
    ):
        _run(tmp_path)
    assert git_checked is False
    assert source_opened is False


def test_idempotent_readback_enforces_live_resource_monitor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)
    _run(tmp_path)
    checks = 0

    def fail_during_readback(self: object) -> None:
        nonlocal checks
        checks += 1
        if checks == 2:
            raise IdentityMarketInventoryRunnerError("resource_cap_exceeded: wall clock")

    monkeypatch.setattr(runner._ResourceMonitor, "check", fail_during_readback)
    with pytest.raises(IdentityMarketInventoryRunnerError, match="wall clock"):
        _run(tmp_path)
    assert checks == 2
    assert bundle.scan_count == 2


def test_idempotent_readback_rejects_stored_resource_cap_violation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    controls = _controls()
    _install_fixture(monkeypatch, bundle, controls, tmp_path)
    completion = _run(tmp_path)
    tampered = replace(
        completion,
        peak_rss_bytes=controls.plan.resource_caps.rss_bytes_cap + 1,
        output_bytes=0,
    )
    candidate_bytes = (
        tampered.candidate_bytes
        + tampered.data_bytes
        + tampered.qa_bytes
        + tampered.bounded_examples_bytes
    )
    for _ in range(4):
        expected = candidate_bytes + len(tampered.content)
        if tampered.output_bytes == expected:
            break
        tampered = replace(tampered, output_bytes=expected)
    assert tampered.output_bytes == candidate_bytes + len(tampered.content)
    completion_path = tmp_path / completion.relative_path
    completion_path.chmod(0o644)
    completion_path.write_bytes(tampered.content)

    with pytest.raises(
        IdentityMarketInventoryRunnerError,
        match="stored completion exceeds resource cap: RSS",
    ):
        _run(tmp_path)
    assert bundle.scan_count == 2


def test_idempotent_readback_rejects_output_byte_fixed_point_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)
    completion = _run(tmp_path)
    tampered = replace(completion, output_bytes=completion.output_bytes + 1)
    completion_path = tmp_path / completion.relative_path
    completion_path.chmod(0o644)
    completion_path.write_bytes(tampered.content)

    with pytest.raises(
        IdentityMarketInventoryRunnerError,
        match="output-byte fixed point differs",
    ):
        _run(tmp_path)
    assert bundle.scan_count == 2


def test_completion_records_disk_warning_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(disk_warning=10**20), tmp_path)

    completion = _run(tmp_path)

    assert completion.disk_free_warning_triggered is True


def test_wrong_controls_and_dirty_git_stop_before_source_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    opened = False

    def open_source(root: Path):
        nonlocal opened
        opened = True
        raise AssertionError

    monkeypatch.setattr(
        runner,
        "_load_controls",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            IdentityMarketInventoryRunnerError("wrong exact controls")
        ),
    )
    monkeypatch.setattr(runner, "open_identity_source_bundle", open_source)
    with pytest.raises(IdentityMarketInventoryRunnerError, match="wrong exact controls"):
        _run(tmp_path)
    assert opened is False

    controls = _controls()
    controls.plan.execution_data_root = str(tmp_path.resolve())
    controls.approval.execution_data_root = str(tmp_path.resolve())
    monkeypatch.setattr(runner, "_load_controls", lambda *args, **kwargs: controls)
    monkeypatch.setattr(
        runner,
        "_verify_git_checkout_and_pins",
        lambda plan: (_ for _ in ()).throw(
            IdentityMarketInventoryRunnerError("Git checkout is not clean")
        ),
    )
    with pytest.raises(IdentityMarketInventoryRunnerError, match="not clean"):
        _run(tmp_path)
    assert opened is False


def test_candidate_without_completion_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)
    completion = _run(tmp_path)
    completion_path = tmp_path / completion.relative_path
    completion_path.chmod(0o644)
    completion_path.unlink()

    with pytest.raises(IdentityMarketInventoryRunnerError, match="without completion"):
        _run(tmp_path)
    assert bundle.scan_count == 2


def test_completion_commit_attempt_failure_retains_stable_candidate_and_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)
    monkeypatch.setattr(
        runner,
        "_publish_completion_link",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("injected")),
    )
    with pytest.raises(IdentityMarketInventoryRunnerError, match="stable outputs retained"):
        _run(tmp_path)
    candidates = tmp_path / "manifests/silver/identity/composite-inventory-candidates"
    assert len(list(candidates.glob("candidate_id=*/manifest.json"))) == 1
    assert not (tmp_path / composite_inventory_completion_path(PLAN_ID, APPROVAL_ID)).exists()
    assert list(
        (tmp_path / "tmp/silver-identity-composite-inventory").glob("run_id=*/completion.json")
    )


def test_pre_candidate_attempt_identity_failure_is_not_swallowed_or_read_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)
    original = runner._require_directory_identity
    staged_candidate_checks = 0
    readback_called = False

    def fail_final_staged_candidate_check(
        path: Path,
        expected: object,
        label: str,
    ) -> None:
        nonlocal staged_candidate_checks
        if label == "staged candidate":
            staged_candidate_checks += 1
            if staged_candidate_checks == 5:
                raise IdentityMarketInventoryRunnerError("injected pre-attempt identity failure")
        original(path, expected, label)

    def mark_readback(*args: object, **kwargs: object) -> None:
        nonlocal readback_called
        readback_called = True
        raise AssertionError("unreachable")

    monkeypatch.setattr(
        runner,
        "_require_directory_identity",
        fail_final_staged_candidate_check,
    )
    monkeypatch.setattr(runner, "_read_and_revalidate_completion", mark_readback)

    with pytest.raises(IdentityMarketInventoryRunnerError, match="commit was not attempted"):
        _run(tmp_path)
    assert staged_candidate_checks == 5
    assert readback_called is False
    assert list(
        (tmp_path / "tmp/silver-identity-composite-inventory").glob(
            "run_id=*/candidate/manifest.json"
        )
    )


def test_candidate_publish_race_preserves_foreign_empty_directory_and_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)
    original = runner._rename_directory_noreplace
    foreign_inode: int | None = None

    def inject_empty_target(source: Path, target: Path) -> None:
        nonlocal foreign_inode
        target.mkdir(parents=True, exist_ok=False)
        foreign_inode = target.stat().st_ino
        original(source, target)

    monkeypatch.setattr(runner, "_rename_directory_noreplace", inject_empty_target)
    with pytest.raises(IdentityMarketInventoryRunnerError, match="stable outputs retained"):
        _run(tmp_path)

    candidates = tmp_path / "manifests/silver/identity/composite-inventory-candidates"
    foreign = next(candidates.iterdir())
    assert foreign.is_dir()
    assert foreign.stat().st_ino == foreign_inode
    assert not any(foreign.iterdir())
    assert list(
        (tmp_path / "tmp/silver-identity-composite-inventory").glob(
            "run_id=*/candidate/manifest.json"
        )
    )
    assert not (tmp_path / composite_inventory_completion_path(PLAN_ID, APPROVAL_ID)).exists()


def test_completion_conflict_is_preserved_and_never_moved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)
    original = runner._publish_completion_link

    def inject_conflict(root: Path, staged: Path, target: Path, **kwargs: object):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"foreign-conflict\n")
        return original(root, staged, target, **kwargs)

    monkeypatch.setattr(runner, "_publish_completion_link", inject_conflict)
    with pytest.raises(IdentityMarketInventoryRunnerError, match="stable outputs retained"):
        _run(tmp_path)

    completion = tmp_path / composite_inventory_completion_path(PLAN_ID, APPROVAL_ID)
    assert completion.read_bytes() == b"foreign-conflict\n"
    candidates = tmp_path / "manifests/silver/identity/composite-inventory-candidates"
    assert len(list(candidates.glob("candidate_id=*/manifest.json"))) == 1


def test_completion_attempt_preserves_foreign_staging_and_stable_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)

    def inject_foreign_staging(root: Path, staged: Path, target: Path, **kwargs: object):
        stage_candidate = staged.parent / "candidate"
        stage_candidate.mkdir()
        (stage_candidate / "foreign.txt").write_text("foreign\n", encoding="utf-8")
        raise RuntimeError("injected")

    monkeypatch.setattr(runner, "_publish_completion_link", inject_foreign_staging)
    with pytest.raises(
        IdentityMarketInventoryRunnerError,
        match="stable outputs retained",
    ):
        _run(tmp_path)

    assert list(
        (tmp_path / "tmp/silver-identity-composite-inventory").glob(
            "run_id=*/candidate/foreign.txt"
        )
    )
    candidates = tmp_path / "manifests/silver/identity/composite-inventory-candidates"
    stable = list(candidates.glob("candidate_id=*/manifest.json"))
    assert len(stable) == 1
    assert not (tmp_path / composite_inventory_completion_path(PLAN_ID, APPROVAL_ID)).exists()


def test_completion_attempt_never_moves_replaced_stable_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)
    owned_path: Path | None = None
    foreign_path: Path | None = None

    def replace_stable_candidate(
        root: Path,
        staged: Path,
        target: Path,
        **kwargs: object,
    ) -> None:
        nonlocal owned_path, foreign_path
        candidates = root / "manifests/silver/identity/composite-inventory-candidates"
        stable = next(candidates.glob("candidate_id=*"))
        owned_path = stable.with_name(f"{stable.name}.owned-away")
        stable.rename(owned_path)
        stable.mkdir()
        (stable / "foreign.txt").write_text("foreign\n", encoding="utf-8")
        foreign_path = stable
        raise RuntimeError("injected")

    monkeypatch.setattr(runner, "_publish_completion_link", replace_stable_candidate)
    with pytest.raises(
        IdentityMarketInventoryRunnerError,
        match="stable outputs retained",
    ):
        _run(tmp_path)

    assert foreign_path is not None
    assert (foreign_path / "foreign.txt").read_text(encoding="utf-8") == "foreign\n"
    assert owned_path is not None
    assert (owned_path / "manifest.json").is_file()
    assert not (tmp_path / composite_inventory_completion_path(PLAN_ID, APPROVAL_ID)).exists()


def test_candidate_has_no_forbidden_identity_or_publish_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)
    completion = _run(tmp_path)
    manifest = json.loads((tmp_path / completion.candidate_path).read_bytes())

    assert all(value is False for value in manifest["capabilities"].values())
    assert not {
        "canonical_asset_id",
        "market_classification",
        "backtest_identity_eligible",
        "adjudication_id",
    }.intersection(COMPOSITE_FIGI_INVENTORY_CONTRACT.arrow_schema.names)
    created = {str(path.relative_to(tmp_path)) for path in tmp_path.rglob("*") if path.is_file()}
    assert not any(
        marker in path
        for path in created
        for marker in ("adjudication", "asset-master", "ticker-alias", "universe-daily", "releases")
    )


def test_lock_path_replacement_after_flock_fails_closed_and_closes_fd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    lock_path = tmp_path / "runner.lock"
    original_flock = runner.fcntl.flock
    original_close = runner.os.close
    closed: list[int] = []
    replaced = False

    def replacing_flock(fd: int, operation: int) -> None:
        nonlocal replaced
        original_flock(fd, operation)
        if not replaced and operation & runner.fcntl.LOCK_EX:
            replaced = True
            lock_path.unlink()
            lock_path.write_bytes(b"replacement")

    def recording_close(fd: int) -> None:
        closed.append(fd)
        original_close(fd)

    monkeypatch.setattr(runner.fcntl, "flock", replacing_flock)
    monkeypatch.setattr(runner.os, "close", recording_close)

    with (
        pytest.raises(IdentityMarketInventoryRunnerError, match="lock path changed"),
        runner._exclusive_nonblocking_lock(lock_path),
    ):
        raise AssertionError("unreachable")
    assert replaced is True
    assert len(closed) == 1


def test_platform_exclusive_directory_rename_moves_without_clobber(tmp_path: Path) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    (source / "payload").write_bytes(b"payload")
    source_inode = source.stat().st_ino

    runner._rename_directory_noreplace(source, target)

    assert not source.exists()
    assert target.stat().st_ino == source_inode
    assert (target / "payload").read_bytes() == b"payload"


@pytest.mark.parametrize("target_kind", ["file", "symlink", "empty_dir", "nonempty_dir"])
def test_platform_exclusive_directory_rename_preserves_conflicts(
    tmp_path: Path,
    target_kind: str,
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    (source / "source.txt").write_text("source\n", encoding="utf-8")
    if target_kind == "file":
        target.write_text("foreign\n", encoding="utf-8")
    elif target_kind == "symlink":
        foreign = tmp_path / "foreign"
        foreign.mkdir()
        target.symlink_to(foreign, target_is_directory=True)
    else:
        target.mkdir()
        if target_kind == "nonempty_dir":
            (target / "foreign.txt").write_text("foreign\n", encoding="utf-8")
    target_lstat = target.lstat()

    with pytest.raises(IdentityMarketInventoryRunnerError, match="target already exists"):
        runner._rename_directory_noreplace(source, target)

    assert (source / "source.txt").read_text(encoding="utf-8") == "source\n"
    assert (target.lstat().st_dev, target.lstat().st_ino) == (
        target_lstat.st_dev,
        target_lstat.st_ino,
    )


def test_unsupported_exclusive_directory_rename_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    monkeypatch.setattr(
        runner,
        "_exclusive_rename_primitive",
        lambda source, target: (_ for _ in ()).throw(OSError(errno.ENOSYS, "unsupported")),
    )

    with pytest.raises(IdentityMarketInventoryRunnerError, match="does not support"):
        runner._rename_directory_noreplace(source, target)
    assert source.is_dir()
    assert not target.exists()


def test_parquet_staging_uses_exclusive_create_and_preserves_foreign_file(
    tmp_path: Path,
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    target = tmp_path / "data.parquet"
    target.write_bytes(b"foreign")
    table = pa.table({"value": [1]})

    with pytest.raises(IdentityMarketInventoryRunnerError, match="target already exists"):
        runner._write_parquet_exclusive(table, target)
    assert target.read_bytes() == b"foreign"


def test_post_link_failure_never_unlinks_staged_or_stable_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ame_stocks_api.silver.identity_market_inventory_runner as runner

    asset, universe = _fixture_rows()
    bundle = _FakeBundle(asset, universe)
    _install_fixture(monkeypatch, bundle, _controls(), tmp_path)

    def link_then_fail(root: Path, staged: Path, target: Path, **kwargs: object) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.hardlink_to(staged)
        raise RuntimeError("post-link injected failure")

    monkeypatch.setattr(runner, "_publish_completion_link", link_then_fail)
    with pytest.raises(IdentityMarketInventoryRunnerError, match="stable outputs retained"):
        _run(tmp_path)

    stable = tmp_path / composite_inventory_completion_path(PLAN_ID, APPROVAL_ID)
    staged = next(
        (tmp_path / "tmp/silver-identity-composite-inventory").glob("run_id=*/completion.json")
    )
    assert stable.is_file()
    assert staged.is_file()
    assert (stable.stat().st_dev, stable.stat().st_ino) == (
        staged.stat().st_dev,
        staged.stat().st_ino,
    )
