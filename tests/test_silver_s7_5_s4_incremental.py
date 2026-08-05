from __future__ import annotations

import gzip
import hashlib
import json
import os
import sys
import tomllib
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from ame_stocks_api.artifacts import write_bytes_immutable
from ame_stocks_api.cli import silver_assets_incremental as incremental_cli
from ame_stocks_api.cli import silver_assets_incremental_bootstrap as bootstrap_cli
from ame_stocks_api.silver import asset_incremental as incremental
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.asset_incremental import (
    S4_ASSET_INCREMENTAL_PARQUET_WRITER_POLICY,
    S4_ASSET_INCREMENTAL_TRANSFORM_SEMANTICS_DIGEST,
    S4AssetIncrementalError,
    derive_s4_base_frontier,
    load_completed_s4_asset_session_run,
    parent_frontier_from_session_receipt,
    prepare_s4_asset_session_run_spec,
    run_s4_asset_session_incremental,
    write_s4_base_frontier,
)
from ame_stocks_api.silver.asset_incremental_contract import (
    S4BaseFrontier,
    S4ParentFrontierPin,
    S4ParentKind,
    S4ReferenceBinding,
)
from ame_stocks_api.silver.calendar_artifact import (
    build_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.contracts import ArtifactRef, ArtifactRole, arrow_schema_digest
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.store import StoredDocument
from ame_stocks_core import ProviderDataset, ProviderRequest

_REAL_DERIVE_S4_BASE_FRONTIER = derive_s4_base_frontier
_BASE_FRONTIER_FIXTURES: dict[tuple[Path, str], tuple[S4BaseFrontier, str]] = {}


def _row(ticker: str, *, active: bool) -> dict[str, object]:
    return {
        "active": active,
        "cik": "0000123456",
        "composite_figi": f"BBG-{ticker}",
        "currency_name": "usd",
        "last_updated_utc": "2026-05-01T12:00:00Z",
        "locale": "us",
        "market": "stocks",
        "name": ticker,
        "primary_exchange": "XNAS",
        "share_class_figi": f"BBGS-{ticker}",
        "ticker": ticker,
        "type": "CS",
    }


def _request(session: date, *, active: bool) -> ProviderRequest:
    return ProviderRequest(
        dataset=ProviderDataset.ASSETS,
        start=session,
        end=session,
        adjusted=False,
        parameters=(("active", str(active).lower()),),
    )


def _write_request(
    root: Path,
    *,
    session: date,
    active: bool,
    rows: list[dict[str, object]],
    completed_at: datetime,
    continuations: tuple[str | None, ...] | None = None,
) -> str:
    request = _request(session, active=active)
    page_rows = [rows]
    continuation_values = continuations or (None,)
    artifacts: list[dict[str, object]] = []
    for sequence, values in enumerate(page_rows):
        continuation = continuation_values[sequence]
        response = {
            "count": len(values),
            "next_url": continuation,
            "request_id": f"provider-{request.request_id[:12]}-{sequence}",
            "results": values,
            "status": "OK",
        }
        raw = json.dumps(response, separators=(",", ":"), sort_keys=True).encode()
        compressed = gzip.compress(raw, mtime=0)
        relative_page = (
            f"bronze/massive/assets/request_id={request.request_id}/page-{sequence:05d}.json.gz"
        )
        page_path = root / relative_page
        page_path.parent.mkdir(parents=True, exist_ok=True)
        page_path.write_bytes(compressed)
        artifacts.append(
            {
                "compressed_bytes": len(compressed),
                "content_type": "application/json",
                "is_last": sequence == len(page_rows) - 1,
                "next_continuation": continuation,
                "path": relative_page,
                "raw_bytes": len(raw),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "record_count": len(values),
                "sequence": sequence,
                "stored_sha256": hashlib.sha256(compressed).hexdigest(),
            }
        )
    manifest = {
        "artifacts": artifacts,
        "checkpoint": None,
        "completed_at": completed_at.isoformat(),
        "created_at": (completed_at - timedelta(seconds=2)).isoformat(),
        "dataset": "assets",
        "manifest_schema_version": 1,
        "provider": "massive",
        "provider_contract_version": "1.1",
        "provider_version": "1.2.0",
        "request": request.canonical_dict(),
        "request_id": request.request_id,
        "status": "complete",
        "updated_at": (completed_at + timedelta(seconds=1)).isoformat(),
    }
    relative_manifest = f"manifests/massive/assets/{request.request_id}.json"
    manifest_path = root / relative_manifest
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return relative_manifest


def _write_pair(
    root: Path,
    *,
    session: date,
    active_rows: list[dict[str, object]] | None = None,
    inactive_rows: list[dict[str, object]] | None = None,
    completed_at: datetime | None = None,
) -> tuple[str, str]:
    capture = completed_at or datetime.combine(
        session - timedelta(days=1),
        datetime.min.time(),
        tzinfo=UTC,
    ).replace(hour=20)
    active = _write_request(
        root,
        session=session,
        active=True,
        rows=active_rows if active_rows is not None else [_row("A", active=True)],
        completed_at=capture,
    )
    inactive = _write_request(
        root,
        session=session,
        active=False,
        rows=inactive_rows if inactive_rows is not None else [_row("OLD", active=False)],
        completed_at=capture + timedelta(minutes=1),
    )
    return active, inactive


def _reference_binding(root: Path) -> S4ReferenceBinding:
    pins: list[ArtifactPin] = []
    for name in ("exchange-release", "ticker-type-release"):
        relative = f"fixtures/reference/{name}.json"
        stored = write_bytes_immutable(root, root / relative, f'{{"name":"{name}"}}\n'.encode())
        pins.append(
            ArtifactPin(
                path=str(stored["path"]),
                sha256=str(stored["sha256"]),
                bytes=int(stored["bytes"]),
            )
        )
    return S4ReferenceBinding(
        ticker_types=("CS",),
        exchange_mics=("XNAS",),
        dependency_pins=tuple(pins),
    )


@pytest.fixture(autouse=True)
def _exact_reference_release_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    """Model the production release-chain loader with tiny immutable fixtures."""

    def load_fixture(_root: Path, pins: tuple[ArtifactPin, ...]) -> S4ReferenceBinding:
        return S4ReferenceBinding(
            ticker_types=("CS",),
            exchange_mics=("XNAS",),
            dependency_pins=pins,
        )

    monkeypatch.setattr(
        incremental,
        "_load_reference_binding_from_exact_releases",
        load_fixture,
    )
    _BASE_FRONTIER_FIXTURES.clear()

    def derive_fixture(
        _root: Path,
        *,
        release_set_artifact: ArtifactPin,
        calendar_artifact_id: str,
        calendar_artifact_sha256: str,
        reference_binding: S4ReferenceBinding,
    ) -> S4BaseFrontier:
        release_set_id = Path(release_set_artifact.path).parent.name.removeprefix("release_set_id=")
        base, expected_calendar_sha256 = _BASE_FRONTIER_FIXTURES[(_root.resolve(), release_set_id)]
        if (
            base.base_release_set_artifact != release_set_artifact
            or base.calendar_artifact_id != calendar_artifact_id
            or expected_calendar_sha256 != calendar_artifact_sha256
            or base.reference_binding_id != reference_binding.binding_id
        ):
            raise S4AssetIncrementalError("fixture base controls changed")
        return base

    monkeypatch.setattr(incremental, "derive_s4_base_frontier", derive_fixture)


def _controls(
    root: Path,
    *,
    parent_session: date = date(2026, 5, 8),
    end_session: date = date(2026, 5, 15),
    release_available_session: date | None = None,
):
    calendar = build_xnys_calendar_artifact(parent_session, end_session)
    calendar_stored = write_xnys_calendar_artifact(root, calendar)
    reference = _reference_binding(root)
    release_set_id = "1" * 64
    base = S4BaseFrontier(
        base_release_set_id=release_set_id,
        base_release_set_artifact=ArtifactPin(
            path=(
                "manifests/silver/release-sets/assets/"
                f"release_set_id={release_set_id}/manifest.json"
            ),
            sha256="3" * 64,
            bytes=1_000,
        ),
        terminal_session=parent_session,
        terminal_partition_set_digest="2" * 64,
        calendar_artifact_id=calendar.calendar_artifact_id,
        reference_binding_id=reference.binding_id,
        contract_ids_by_table={
            table: contract.contract_id for table, contract in ASSET_CONTRACTS.items()
        },
        schema_digests_by_table={
            table: arrow_schema_digest(contract.arrow_schema)
            for table, contract in ASSET_CONTRACTS.items()
        },
        transform_semantics_digest=S4_ASSET_INCREMENTAL_TRANSFORM_SEMANTICS_DIGEST,
        parquet_writer_policy=S4_ASSET_INCREMENTAL_PARQUET_WRITER_POLICY,
        release_available_session=release_available_session or parent_session,
    )
    _BASE_FRONTIER_FIXTURES[(root.resolve(), release_set_id)] = (
        base,
        str(calendar_stored["sha256"]),
    )
    parent = write_s4_base_frontier(root, base)
    return calendar, calendar_stored, parent, reference


def _prepare(
    root: Path,
    *,
    session: date,
    parent=None,
    calendar=None,
    calendar_stored=None,
    reference=None,
    receipt_available_session: date | None = None,
):
    if calendar is None or calendar_stored is None or parent is None or reference is None:
        calendar, calendar_stored, parent, reference = _controls(root)
    spec = prepare_s4_asset_session_run_spec(
        root,
        session_date=session,
        parent_frontier=parent,
        calendar_artifact_id=calendar.calendar_artifact_id,
        calendar_artifact_sha256=str(calendar_stored["sha256"]),
        reference_binding=reference,
        receipt_available_session=receipt_available_session or session,
        writer_git_commit="a" * 40,
    )
    return spec, calendar, calendar_stored, parent, reference


def _trusted_base_derivation_fixture(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> SimpleNamespace:
    calendar = build_xnys_calendar_artifact(date(2026, 5, 8), date(2026, 5, 13))
    calendar_stored = write_xnys_calendar_artifact(root, calendar)
    exchange_release_id = "4" * 64
    ticker_type_release_id = "5" * 64
    reference = S4ReferenceBinding(
        ticker_types=("CS",),
        exchange_mics=("XNAS",),
        dependency_pins=(
            ArtifactPin(
                path=(f"manifests/silver/releases/release_id={exchange_release_id}.json"),
                sha256="6" * 64,
                bytes=100,
            ),
            ArtifactPin(
                path=(f"manifests/silver/releases/release_id={ticker_type_release_id}.json"),
                sha256="7" * 64,
                bytes=101,
            ),
        ),
    )
    release_set_id = "8" * 64
    release_set_artifact = ArtifactPin(
        path=(
            f"manifests/silver/release-sets/assets/release_set_id={release_set_id}/manifest.json"
        ),
        sha256="9" * 64,
        bytes=4_000,
    )
    sessions = (date(2026, 5, 8), date(2026, 5, 11))
    members: list[SimpleNamespace] = []
    plans: dict[str, tuple[SimpleNamespace, StoredDocument]] = {}
    for index, table in enumerate(sorted(ASSET_CONTRACTS)):
        contract = ASSET_CONTRACTS[table]
        build_id = f"{index + 1}" * 64
        plan_id = f"{index + 2}" * 64
        plan_sha = f"{index + 3}" * 64
        outputs = tuple(
            ArtifactRef(
                path=(
                    f"silver/schema=v{contract.schema_version}/{contract.domain}/{table}/"
                    f"build_id={build_id}/data/session_year={session.year}/"
                    f"session_date={session.isoformat()}/part-00000.parquet"
                ),
                sha256=hashlib.sha256(f"{table}:{session}".encode()).hexdigest(),
                bytes=1_000 + index,
                row_count=1,
                media_type="application/vnd.apache.parquet",
                role=ArtifactRole.DATA,
                table=table,
                schema_digest=arrow_schema_digest(contract.arrow_schema),
            )
            for session in sessions
        )
        members.append(
            SimpleNamespace(
                table=table,
                domain=contract.domain,
                schema_version=contract.schema_version,
                contract_id=contract.contract_id,
                full_run_plan_id=plan_id,
                full_run_plan_sha256=plan_sha,
                build_id=build_id,
                outputs=outputs,
            )
        )
        parameters = {
            **dict(incremental._FULL_PLAN_COMPATIBILITY_PARAMETERS),
            "date_end": sessions[-1].isoformat(),
            "date_start": sessions[0].isoformat(),
            "exchange_release_id": exchange_release_id,
            "exchange_release_sha256": "6" * 64,
            "parquet_writer_policy": dict(incremental.S4_ASSET_INCREMENTAL_PARQUET_WRITER_POLICY),
            "ticker_type_release_id": ticker_type_release_id,
            "ticker_type_release_sha256": "7" * 64,
        }
        plans[table] = (
            SimpleNamespace(
                table=table,
                domain=contract.domain,
                schema_version=contract.schema_version,
                contract_id=contract.contract_id,
                transform_version=incremental.ASSET_TRANSFORM_VERSION,
                parameters=parameters,
            ),
            StoredDocument(
                path=(f"manifests/silver/full-run-plans/{table}/plan_id={plan_id}/manifest.json"),
                sha256=plan_sha,
                bytes=1_000,
            ),
        )
    release_set = SimpleNamespace(
        release_set_id=release_set_id,
        committed_at="2026-05-11T20:00:00+00:00",
        members=tuple(members),
    )
    release_document = StoredDocument(
        path=release_set_artifact.path,
        sha256=release_set_artifact.sha256,
        bytes=release_set_artifact.bytes,
    )
    monkeypatch.setattr(
        incremental,
        "load_exact_asset_release_set_control",
        lambda *_args, **_kwargs: (release_set, release_document),
    )
    monkeypatch.setattr(
        incremental,
        "_load_reference_binding_from_exact_releases",
        lambda _root, pins: replace(reference, dependency_pins=pins),
    )
    monkeypatch.setattr(
        incremental.SilverStore,
        "load_full_run_plan",
        lambda _store, table, _plan_id: plans[table],
    )
    return SimpleNamespace(
        calendar=calendar,
        calendar_stored=calendar_stored,
        reference=reference,
        release_set=release_set,
        release_set_artifact=release_set_artifact,
        members=members,
        plans=plans,
    )


def test_single_session_run_writes_three_manifest_derived_partitions_and_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = date(2026, 5, 11)
    _write_pair(tmp_path, session=target)
    spec, calendar, calendar_stored, parent, reference = _prepare(
        tmp_path,
        session=target,
    )

    calls = 0
    real_transform = incremental.transform_asset_session

    def counted_transform(*args, **kwargs):
        nonlocal calls
        calls += 1
        return real_transform(*args, **kwargs)

    monkeypatch.setattr(incremental, "transform_asset_session", counted_transform)
    first = run_s4_asset_session_incremental(tmp_path, spec)
    assert first.idempotent is False
    assert calls == 1
    assert [item.table_name for item in first.receipt.partition_receipts] == [
        "asset_observation_daily",
        "asset_observation_version",
        "universe_source_daily",
    ]
    assert [item.row_count for item in first.receipt.partition_receipts] == [2, 0, 2]
    assert all(
        f"session_date={target.isoformat()}" in item.artifact.path
        for item in first.receipt.partition_receipts
    )

    receipt_bytes = (tmp_path / first.receipt_artifact.path).read_bytes()

    def forbidden_transform(*_args, **_kwargs):
        raise AssertionError("idempotent retry must not call the transform")

    monkeypatch.setattr(incremental, "transform_asset_session", forbidden_transform)
    second = run_s4_asset_session_incremental(tmp_path, spec)
    assert second.idempotent is True
    assert second.receipt == first.receipt
    assert second.receipt_artifact == first.receipt_artifact
    assert (tmp_path / second.receipt_artifact.path).read_bytes() == receipt_bytes

    retry_after_runtime_only_change = prepare_s4_asset_session_run_spec(
        tmp_path,
        session_date=target,
        parent_frontier=parent,
        calendar_artifact_id=calendar.calendar_artifact_id,
        calendar_artifact_sha256=str(calendar_stored["sha256"]),
        reference_binding=reference,
        receipt_available_session=target,
        writer_git_commit="b" * 40,
    )
    assert retry_after_runtime_only_change.run_spec_id == spec.run_spec_id
    third = run_s4_asset_session_incremental(tmp_path, retry_after_runtime_only_change)
    assert third.idempotent is True
    assert third.run_spec.writer_git_commit == "a" * 40
    assert third.run_spec_artifact == first.run_spec_artifact

    for source in spec.source_binding.inventory.artifacts:
        source_path = tmp_path / source.path
        os.chmod(source_path, 0o644)
        source_path.write_bytes(b"corrupt Bronze must not be reread after final receipt")
    loaded = load_completed_s4_asset_session_run(tmp_path, target)
    assert loaded is not None and loaded.idempotent is True
    fourth = run_s4_asset_session_incremental(tmp_path, spec)
    assert fourth.receipt_artifact == first.receipt_artifact

    qa = json.loads((tmp_path / first.receipt.qa_details_artifact.path).read_text())
    assert all(item["scope"] == "session_local" for item in qa["tables"].values())
    assert all(
        "cross_session_ticker_identity_churn_groups" in item["deferred_full_history_check_ids"]
        for table, item in qa["tables"].items()
        if table != "asset_observation_version"
    )


def test_2026_07_10_inverse_manifest_sort_round_trips_and_retries_idempotently(
    tmp_path: Path,
) -> None:
    target = date(2026, 7, 10)
    calendar, stored, parent, reference = _controls(
        tmp_path,
        parent_session=date(2026, 7, 9),
        end_session=date(2026, 7, 13),
    )
    _write_pair(tmp_path, session=target)
    spec, *_ = _prepare(
        tmp_path,
        session=target,
        parent=parent,
        calendar=calendar,
        calendar_stored=stored,
        reference=reference,
    )
    canonical = incremental.canonical_asset_session_manifest_paths(target)
    round_tripped_inventory = type(spec.source_binding.inventory).from_dict(
        spec.source_binding.inventory.to_dict()
    )
    persisted_order = tuple(item.path for item in round_tripped_inventory.upstream_manifests)
    assert persisted_order == tuple(sorted(canonical))
    assert persisted_order != canonical

    first = run_s4_asset_session_incremental(tmp_path, spec)
    loaded = load_completed_s4_asset_session_run(tmp_path, target)
    second = run_s4_asset_session_incremental(tmp_path, spec)
    assert first.idempotent is False
    assert loaded is not None and loaded.idempotent is True
    assert second.idempotent is True
    assert loaded.receipt == second.receipt == first.receipt


@pytest.mark.parametrize(
    ("parent_session", "target_session"),
    [
        (date(2026, 5, 8), date(2026, 5, 11)),
        (date(2026, 7, 2), date(2026, 7, 6)),
    ],
)
def test_calendar_accepts_exact_next_session_across_weekend_or_holiday(
    tmp_path: Path,
    parent_session: date,
    target_session: date,
) -> None:
    end = target_session + timedelta(days=5)
    while end.weekday() >= 5:
        end += timedelta(days=1)
    calendar, stored, parent, reference = _controls(
        tmp_path,
        parent_session=parent_session,
        end_session=end,
    )
    _write_pair(tmp_path, session=target_session)
    spec, *_ = _prepare(
        tmp_path,
        session=target_session,
        parent=parent,
        calendar=calendar,
        calendar_stored=stored,
        reference=reference,
    )
    assert spec.source_binding.session_date == target_session


def test_source_gap_fails_before_any_session_output(tmp_path: Path) -> None:
    calendar, stored, parent, reference = _controls(tmp_path)
    with pytest.raises(S4AssetIncrementalError, match="source_gap"):
        _prepare(
            tmp_path,
            session=date(2026, 5, 12),
            parent=parent,
            calendar=calendar,
            calendar_stored=stored,
            reference=reference,
        )
    assert not (tmp_path / "silver").exists()


def test_source_knowledge_time_is_separate_from_parent_control_visibility(
    tmp_path: Path,
) -> None:
    target = date(2026, 5, 11)
    control_available = date(2026, 7, 1)
    calendar, stored, parent, reference = _controls(
        tmp_path,
        end_session=date(2026, 7, 2),
        release_available_session=control_available,
    )
    _write_pair(tmp_path, session=target)
    spec, *_ = _prepare(
        tmp_path,
        session=target,
        parent=parent,
        calendar=calendar,
        calendar_stored=stored,
        reference=reference,
        receipt_available_session=control_available,
    )
    assert spec.source_binding.pair_available_session == target
    assert spec.receipt_available_session == control_available
    assert run_s4_asset_session_incremental(tmp_path, spec).receipt.receipt_available_session == (
        control_available
    )

    other = tmp_path / "too-early"
    calendar, stored, parent, reference = _controls(
        other,
        end_session=date(2026, 7, 2),
        release_available_session=control_available,
    )
    _write_pair(other, session=target)
    with pytest.raises(S4AssetIncrementalError, match="receipt availability precedes"):
        _prepare(
            other,
            session=target,
            parent=parent,
            calendar=calendar,
            calendar_stored=stored,
            reference=reference,
            receipt_available_session=target,
        )


def test_base_compatibility_and_exact_reference_values_are_revalidated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = date(2026, 5, 11)
    calendar, stored, parent, reference = _controls(tmp_path)
    base_document = json.loads((tmp_path / parent.artifact.path).read_text())
    base = S4BaseFrontier.from_dict(base_document)
    incompatible_parent = write_s4_base_frontier(
        tmp_path,
        replace(base, transform_semantics_digest="f" * 64),
    )
    _write_pair(tmp_path, session=target)
    with pytest.raises(S4AssetIncrementalError, match="not clean-append compatible"):
        _prepare(
            tmp_path,
            session=target,
            parent=incompatible_parent,
            calendar=calendar,
            calendar_stored=stored,
            reference=reference,
        )

    monkeypatch.setattr(
        incremental,
        "_load_reference_binding_from_exact_releases",
        lambda _root, pins: S4ReferenceBinding(
            ticker_types=("CS", "ETF"),
            exchange_mics=("XNAS",),
            dependency_pins=pins,
        ),
    )
    with pytest.raises(S4AssetIncrementalError, match="reference values differ"):
        _prepare(
            tmp_path,
            session=target,
            parent=parent,
            calendar=calendar,
            calendar_stored=stored,
            reference=reference,
        )


def test_trusted_base_frontier_is_derived_without_historical_s4_parquet_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _trusted_base_derivation_fixture(tmp_path, monkeypatch)

    def forbidden_parquet(*_args, **_kwargs):
        raise AssertionError("base bootstrap must not read historical S4 Parquet")

    monkeypatch.setattr(incremental.pq, "read_table", forbidden_parquet)
    first = _REAL_DERIVE_S4_BASE_FRONTIER(
        tmp_path,
        release_set_artifact=fixture.release_set_artifact,
        calendar_artifact_id=fixture.calendar.calendar_artifact_id,
        calendar_artifact_sha256=str(fixture.calendar_stored["sha256"]),
        reference_binding=fixture.reference,
    )
    second = _REAL_DERIVE_S4_BASE_FRONTIER(
        tmp_path,
        release_set_artifact=fixture.release_set_artifact,
        calendar_artifact_id=fixture.calendar.calendar_artifact_id,
        calendar_artifact_sha256=str(fixture.calendar_stored["sha256"]),
        reference_binding=fixture.reference,
    )
    assert first == second
    assert first.base_release_set_id == fixture.release_set.release_set_id
    assert first.base_release_set_artifact == fixture.release_set_artifact
    assert first.terminal_session == date(2026, 5, 11)
    assert first.release_available_session == date(2026, 5, 12)


def test_trusted_base_frontier_rejects_partition_gap_and_forged_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _trusted_base_derivation_fixture(tmp_path, monkeypatch)
    fixture.members[0].outputs = fixture.members[0].outputs[:-1]
    with pytest.raises(S4AssetIncrementalError, match="session sets differ"):
        _REAL_DERIVE_S4_BASE_FRONTIER(
            tmp_path,
            release_set_artifact=fixture.release_set_artifact,
            calendar_artifact_id=fixture.calendar.calendar_artifact_id,
            calendar_artifact_sha256=str(fixture.calendar_stored["sha256"]),
            reference_binding=fixture.reference,
        )

    fixture = _trusted_base_derivation_fixture(tmp_path / "forged", monkeypatch)
    derived = _REAL_DERIVE_S4_BASE_FRONTIER(
        tmp_path / "forged",
        release_set_artifact=fixture.release_set_artifact,
        calendar_artifact_id=fixture.calendar.calendar_artifact_id,
        calendar_artifact_sha256=str(fixture.calendar_stored["sha256"]),
        reference_binding=fixture.reference,
    )
    monkeypatch.setattr(incremental, "derive_s4_base_frontier", _REAL_DERIVE_S4_BASE_FRONTIER)
    with pytest.raises(S4AssetIncrementalError, match="not clean-append compatible"):
        incremental._require_base_compatibility(
            tmp_path / "forged",
            replace(derived, terminal_partition_set_digest="f" * 64),
            calendar_artifact_id=fixture.calendar.calendar_artifact_id,
            calendar_artifact_sha256=str(fixture.calendar_stored["sha256"]),
            reference_binding=fixture.reference,
        )


def test_trusted_base_frontier_rejects_full_plan_semantics_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _trusted_base_derivation_fixture(tmp_path, monkeypatch)
    plan, document = fixture.plans["asset_observation_daily"]
    plan.parameters["asset_source_availability_rule"] = "forged"
    fixture.plans["asset_observation_daily"] = (plan, document)
    with pytest.raises(S4AssetIncrementalError, match="not incrementally compatible"):
        _REAL_DERIVE_S4_BASE_FRONTIER(
            tmp_path,
            release_set_artifact=fixture.release_set_artifact,
            calendar_artifact_id=fixture.calendar.calendar_artifact_id,
            calendar_artifact_sha256=str(fixture.calendar_stored["sha256"]),
            reference_binding=fixture.reference,
        )


def test_trusted_base_frontier_rejects_shared_calendar_gap_and_reference_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _trusted_base_derivation_fixture(tmp_path, monkeypatch)
    for member in fixture.members:
        member.outputs = member.outputs[:1]
    with pytest.raises(S4AssetIncrementalError, match="complete pinned-calendar"):
        _REAL_DERIVE_S4_BASE_FRONTIER(
            tmp_path,
            release_set_artifact=fixture.release_set_artifact,
            calendar_artifact_id=fixture.calendar.calendar_artifact_id,
            calendar_artifact_sha256=str(fixture.calendar_stored["sha256"]),
            reference_binding=fixture.reference,
        )

    fixture = _trusted_base_derivation_fixture(tmp_path / "reference", monkeypatch)
    first, second = fixture.reference.dependency_pins
    drifted_reference = replace(
        fixture.reference,
        dependency_pins=(replace(first, sha256="a" * 64), second),
    )
    with pytest.raises(S4AssetIncrementalError, match="differ from the published FullRunPlans"):
        _REAL_DERIVE_S4_BASE_FRONTIER(
            tmp_path / "reference",
            release_set_artifact=fixture.release_set_artifact,
            calendar_artifact_id=fixture.calendar.calendar_artifact_id,
            calendar_artifact_sha256=str(fixture.calendar_stored["sha256"]),
            reference_binding=drifted_reference,
        )


def test_executor_rejects_noncanonical_request_identity_in_direct_run_spec(
    tmp_path: Path,
) -> None:
    target = date(2026, 5, 11)
    _write_pair(tmp_path, session=target)
    spec, *_ = _prepare(tmp_path, session=target)
    noncanonical_source = replace(spec.source_binding, active_request_id="f" * 64)
    direct_spec = replace(spec, source_binding=noncanonical_source)
    with pytest.raises(S4AssetIncrementalError, match="not the canonical"):
        run_s4_asset_session_incremental(tmp_path, direct_spec)
    assert not (tmp_path / incremental._receipt_relative_path(target)).exists()


def test_completed_fast_path_reauthenticates_manifests_without_reading_bronze_pages(
    tmp_path: Path,
) -> None:
    target = date(2026, 5, 11)
    _write_pair(tmp_path, session=target)
    spec, *_ = _prepare(tmp_path, session=target)
    run_s4_asset_session_incremental(tmp_path, spec)

    _write_request(
        tmp_path,
        session=target,
        active=True,
        rows=[_row("A", active=True), _row("CORRECTED", active=True)],
        completed_at=datetime(2026, 5, 10, 20, 0, tzinfo=UTC),
    )
    with pytest.raises(S4AssetIncrementalError, match="correction_required"):
        load_completed_s4_asset_session_run(tmp_path, target)


def test_cli_completed_fast_path_skips_prepare_and_bronze(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    target = date(2026, 5, 11)
    _write_pair(tmp_path, session=target)
    spec, *_ = _prepare(tmp_path, session=target)
    run_s4_asset_session_incremental(tmp_path, spec)

    monkeypatch.setattr(incremental_cli, "verify_s4_incremental_git_checkout", lambda *_: None)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("completed CLI fast path must not prepare or read Bronze pages")

    monkeypatch.setattr(incremental_cli, "load_current_s4_reference_binding", forbidden)
    monkeypatch.setattr(incremental_cli, "prepare_s4_asset_session_run_spec", forbidden)
    parent = spec.parent_frontier
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ame-silver-assets-incremental",
            "--data-root",
            str(tmp_path),
            "--session",
            target.isoformat(),
            "--git-commit",
            "b" * 40,
            "--repo-root",
            str(tmp_path),
            "--parent-kind",
            parent.parent_kind.value,
            "--parent-terminal-session",
            parent.terminal_session.isoformat(),
            "--parent-terminal-receipt-id",
            parent.terminal_receipt_id,
            "--parent-artifact-path",
            parent.artifact.path,
            "--parent-artifact-sha256",
            parent.artifact.sha256,
            "--parent-artifact-bytes",
            str(parent.artifact.bytes),
            "--calendar-artifact-id",
            spec.calendar_artifact_id,
            "--calendar-artifact-sha256",
            spec.calendar_artifact.sha256,
            "--receipt-available-session",
            spec.receipt_available_session.isoformat(),
        ],
    )
    incremental_cli.main()
    assert json.loads(capsys.readouterr().out)["idempotent"] is True


def test_bootstrap_cli_freezes_only_the_derived_frontier(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calendar, stored, parent, reference = _controls(tmp_path)
    base = S4BaseFrontier.from_dict(json.loads((tmp_path / parent.artifact.path).read_text()))
    calls: list[tuple[str, object]] = []

    def verify(repo_root: Path, git_commit: str) -> None:
        assert repo_root == tmp_path
        assert git_commit == "a" * 40
        calls.append(("verify", git_commit))

    def load_reference(data_root: Path) -> S4ReferenceBinding:
        assert data_root == tmp_path
        calls.append(("reference", data_root))
        return reference

    def derive(data_root: Path, **kwargs) -> S4BaseFrontier:
        assert data_root == tmp_path
        assert kwargs == {
            "release_set_artifact": base.base_release_set_artifact,
            "calendar_artifact_id": calendar.calendar_artifact_id,
            "calendar_artifact_sha256": str(stored["sha256"]),
            "reference_binding": reference,
        }
        calls.append(("derive", kwargs["release_set_artifact"]))
        return base

    def write(data_root: Path, frontier: S4BaseFrontier) -> S4ParentFrontierPin:
        assert data_root == tmp_path
        assert frontier is base
        calls.append(("write", frontier.frontier_id))
        return parent

    monkeypatch.setattr(bootstrap_cli, "verify_s4_incremental_git_checkout", verify)
    monkeypatch.setattr(bootstrap_cli, "load_current_s4_reference_binding", load_reference)
    monkeypatch.setattr(bootstrap_cli, "derive_s4_base_frontier", derive)
    monkeypatch.setattr(bootstrap_cli, "write_s4_base_frontier", write)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ame-silver-assets-incremental-bootstrap",
            "--data-root",
            str(tmp_path),
            "--repo-root",
            str(tmp_path),
            "--git-commit",
            "a" * 40,
            "--release-set-id",
            base.base_release_set_id,
            "--release-set-sha256",
            base.base_release_set_artifact.sha256,
            "--release-set-bytes",
            str(base.base_release_set_artifact.bytes),
            "--calendar-artifact-id",
            calendar.calendar_artifact_id,
            "--calendar-artifact-sha256",
            str(stored["sha256"]),
        ],
    )
    bootstrap_cli.main()
    output = json.loads(capsys.readouterr().out)
    assert [item[0] for item in calls] == ["verify", "reference", "derive", "write"]
    assert output["frontier"] == base.to_dict()
    assert output["parent_frontier"] == parent.to_dict()
    project = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())
    assert project["project"]["scripts"]["ame-silver-assets-incremental-bootstrap"] == (
        "ame_stocks_api.cli.silver_assets_incremental_bootstrap:main"
    )


def test_provider_active_mismatch_and_active_inactive_overlap_write_no_receipt(
    tmp_path: Path,
) -> None:
    target = date(2026, 5, 11)
    _write_pair(
        tmp_path,
        session=target,
        active_rows=[_row("A", active=False)],
    )
    spec, *_ = _prepare(tmp_path, session=target)
    with pytest.raises(Exception, match="active flag does not match"):
        run_s4_asset_session_incremental(tmp_path, spec)
    assert not (tmp_path / incremental._receipt_relative_path(target)).exists()

    second_root = tmp_path / "overlap"
    _write_pair(
        second_root,
        session=target,
        active_rows=[_row("BOTH", active=True)],
        inactive_rows=[_row("BOTH", active=False)],
    )
    second_spec, *_ = _prepare(second_root, session=target)
    with pytest.raises(S4AssetIncrementalError, match="blocking QA"):
        run_s4_asset_session_incremental(second_root, second_spec)
    assert not (second_root / incremental._receipt_relative_path(target)).exists()


@pytest.mark.parametrize(
    "interrupt_stage",
    (
        "after_transform",
        "after_partition:asset_observation_daily",
        "after_partition:asset_observation_version",
        "after_partition:universe_source_daily",
        "after_qa_details",
        "before_receipt",
    ),
)
def test_interrupted_write_retries_to_one_exact_receipt(
    tmp_path: Path,
    interrupt_stage: str,
) -> None:
    target = date(2026, 5, 11)
    _write_pair(tmp_path, session=target)
    spec, *_ = _prepare(tmp_path, session=target)

    def interrupt(stage: str) -> None:
        if stage == interrupt_stage:
            raise RuntimeError("fixture interruption")

    with pytest.raises(RuntimeError, match="fixture interruption"):
        run_s4_asset_session_incremental(
            tmp_path,
            spec,
            transition_barrier=interrupt,
        )
    assert not (tmp_path / incremental._receipt_relative_path(target)).exists()

    recovered = run_s4_asset_session_incremental(tmp_path, spec)
    repeated = run_s4_asset_session_incremental(tmp_path, spec)
    assert recovered.idempotent is False
    assert repeated.idempotent is True
    assert recovered.receipt == repeated.receipt
    assert recovered.receipt_artifact == repeated.receipt_artifact


def test_next_session_does_not_read_parent_parquet_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_session = date(2026, 5, 11)
    second_session = date(2026, 5, 12)
    calendar, stored, base_parent, reference = _controls(tmp_path)
    _write_pair(tmp_path, session=first_session)
    first_spec, *_ = _prepare(
        tmp_path,
        session=first_session,
        parent=base_parent,
        calendar=calendar,
        calendar_stored=stored,
        reference=reference,
    )
    first = run_s4_asset_session_incremental(tmp_path, first_spec)
    forbidden_parent_parquet = {
        (tmp_path / item.artifact.path).resolve() for item in first.receipt.partition_receipts
    }
    for path in forbidden_parent_parquet:
        os.chmod(path, 0o644)
        path.write_bytes(b"intentionally corrupt parent parquet")

    real_sha256_file = incremental.sha256_file

    def guarded_sha256(path: Path) -> str:
        if path.resolve() in forbidden_parent_parquet:
            raise AssertionError("new session must not read parent Parquet content")
        return real_sha256_file(path)

    monkeypatch.setattr(incremental, "sha256_file", guarded_sha256)
    _write_pair(
        tmp_path,
        session=second_session,
        completed_at=datetime(2026, 5, 11, 22, 0, tzinfo=UTC),
    )
    next_parent = parent_frontier_from_session_receipt(
        first.receipt,
        first.receipt_artifact,
    )
    second_spec, *_ = _prepare(
        tmp_path,
        session=second_session,
        parent=next_parent,
        calendar=calendar,
        calendar_stored=stored,
        reference=reference,
    )
    second = run_s4_asset_session_incremental(tmp_path, second_spec)
    assert second.receipt.session_date == second_session


def test_next_session_rejects_rehashed_parent_receipt_inconsistent_with_its_run_spec(
    tmp_path: Path,
) -> None:
    first_session = date(2026, 5, 11)
    second_session = date(2026, 5, 12)
    calendar, stored, base_parent, reference = _controls(tmp_path)
    _write_pair(tmp_path, session=first_session)
    first_spec, *_ = _prepare(
        tmp_path,
        session=first_session,
        parent=base_parent,
        calendar=calendar,
        calendar_stored=stored,
        reference=reference,
    )
    first = run_s4_asset_session_incremental(tmp_path, first_spec)

    receipt_path = tmp_path / first.receipt_artifact.path
    forged = json.loads(receipt_path.read_text())
    forged["parent_frontier_id"] = "f" * 64
    logical = {
        key: value
        for key, value in forged.items()
        if key not in {"receipt_id", "run_spec_artifact"}
    }
    forged["receipt_id"] = incremental.stable_digest(logical)
    forged_bytes = incremental._canonical_json_bytes(forged)
    os.chmod(receipt_path, 0o644)
    receipt_path.write_bytes(forged_bytes)
    forged_parent = S4ParentFrontierPin(
        parent_kind=S4ParentKind.SESSION_RECEIPT,
        terminal_session=first_session,
        terminal_receipt_id=forged["receipt_id"],
        artifact=ArtifactPin(
            path=first.receipt_artifact.path,
            sha256=hashlib.sha256(forged_bytes).hexdigest(),
            bytes=len(forged_bytes),
        ),
    )
    _write_pair(
        tmp_path,
        session=second_session,
        completed_at=datetime(2026, 5, 11, 22, 0, tzinfo=UTC),
    )
    with pytest.raises(S4AssetIncrementalError, match="correction_required"):
        _prepare(
            tmp_path,
            session=second_session,
            parent=forged_parent,
            calendar=calendar,
            calendar_stored=stored,
            reference=reference,
        )


def test_changed_source_requires_correction_and_output_tamper_fails_closed(
    tmp_path: Path,
) -> None:
    target = date(2026, 5, 11)
    active_manifest, _ = _write_pair(tmp_path, session=target)
    spec, calendar, stored, parent, reference = _prepare(tmp_path, session=target)
    first = run_s4_asset_session_incremental(tmp_path, spec)

    output_path = tmp_path / first.receipt.partition_receipts[0].artifact.path
    os.chmod(output_path, 0o644)
    output_path.write_bytes(output_path.read_bytes() + b"tamper")
    with pytest.raises(S4AssetIncrementalError, match="pin mismatch"):
        run_s4_asset_session_incremental(tmp_path, spec)

    # Restore output so the source-binding check is the next failing boundary.
    first_partition = first.receipt.partition_receipts[0]
    output_path.write_bytes(
        pq_bytes := _read_original_partition_bytes(first_partition, first, tmp_path)
    )
    assert hashlib.sha256(pq_bytes).hexdigest() == first_partition.artifact.sha256

    # Rebuild the canonical active manifest/page with an extra row at the same paths.
    _write_request(
        tmp_path,
        session=target,
        active=True,
        rows=[_row("A", active=True), _row("NEW", active=True)],
        completed_at=datetime(2026, 5, 10, 20, 0, tzinfo=UTC),
    )
    assert (
        active_manifest
        == f"manifests/massive/assets/{_request(target, active=True).request_id}.json"
    )
    changed_spec, *_ = _prepare(
        tmp_path,
        session=target,
        parent=parent,
        calendar=calendar,
        calendar_stored=stored,
        reference=reference,
    )
    with pytest.raises(S4AssetIncrementalError, match="correction_required"):
        run_s4_asset_session_incremental(tmp_path, changed_spec)


def _read_original_partition_bytes(partition, run, root: Path) -> bytes:
    """Recreate deterministic bytes from the still-valid sibling receipt is impossible.

    The helper exists only to keep the tamper and source-change assertions independent:
    rerun the session in an isolated root and copy its byte-equivalent partition.
    """

    isolated = root / "isolated-rebuild"
    target = run.receipt.session_date
    _write_pair(isolated, session=target)
    isolated_spec, *_ = _prepare(isolated, session=target)
    rebuilt = run_s4_asset_session_incremental(isolated, isolated_spec)
    matching = next(
        item
        for item in rebuilt.receipt.partition_receipts
        if item.table_name == partition.table_name
    )
    return (isolated / matching.artifact.path).read_bytes()
