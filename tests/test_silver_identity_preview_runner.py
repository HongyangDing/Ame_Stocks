from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ame_stocks_api.artifacts import sha256_file
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.calendar_artifact import (
    build_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.contracts import ArtifactRef, ArtifactRole
from ame_stocks_api.silver.identity_bounce import SourceSession
from ame_stocks_api.silver.identity_preview_plan import (
    IdentityPreviewPlanStore,
    S7DetectorPreviewApprovalRequest,
    S7DetectorPreviewPlan,
    S7DetectorPreviewPlanApproval,
    S7DetectorPreviewResourceCaps,
    build_s7_ticker_allowlist,
)
from ame_stocks_api.silver.identity_preview_runner import (
    IdentityPreviewRunnerError,
    S7DetectorPreviewCompletion,
    _preflight_source_scope,
    detector_preview_completion_path,
    run_source_bound_identity_streaming_preview,
)
from ame_stocks_api.silver.identity_source import (
    S7_SOURCE_PINS,
    IdentityPublishedSource,
    IdentitySourceArtifact,
    IdentitySourceBatch,
    IdentitySourceBundle,
)
from ame_stocks_api.silver.identity_streaming_preview import (
    BoundedIdentityPreviewEngine,
    BoundedIdentityPreviewLimits,
    build_bounded_identity_preview_artifact,
    write_bounded_identity_preview_artifact,
)

CALENDAR = build_xnys_calendar_artifact(date(2024, 1, 2), date(2024, 2, 2))
RUN_AT = datetime(2024, 1, 8, 21, tzinfo=UTC)
TICKER = "FIX"
OUTER_FIGI = "BBG000000001"
MIDDLE_FIGI = "BBG000000002"


def _digest(character: str) -> str:
    return character * 64


def _base_value(field: pa.Field, *, session: date, captured_at: datetime) -> object:
    if pa.types.is_string(field.type):
        return "fixture"
    if pa.types.is_boolean(field.type):
        return True
    if pa.types.is_int64(field.type):
        return 1
    if pa.types.is_float64(field.type):
        return 1.0
    if pa.types.is_date32(field.type):
        return session
    if pa.types.is_timestamp(field.type):
        return captured_at
    raise AssertionError(field.type)


def _paired_rows(
    *,
    session: date,
    figi: str,
    source_record_id: str,
) -> tuple[dict[str, object], dict[str, object]]:
    captured_at = datetime.combine(session, datetime.min.time(), tzinfo=UTC) + timedelta(hours=21)
    available_session, available_at = CALENDAR.first_open_after(captured_at)
    request_id = _digest("b")
    provider_request_id = _digest("c")
    artifact_sha = _digest("d")
    row_hash = _digest("e")

    def build(schema: pa.Schema) -> dict[str, object]:
        return {
            field.name: _base_value(field, session=session, captured_at=captured_at)
            for field in schema
        }

    asset = build(ASSET_OBSERVATION_DAILY_CONTRACT.arrow_schema)
    asset.update(
        {
            "cik": "0000000001",
            "composite_figi": figi,
            "currency_name": "usd",
            "locale": "us",
            "market": "stocks",
            "name": "Fixture Corp",
            "primary_exchange_mic": "XNYS",
            "provider_active": True,
            "requested_active": True,
            "session_date": session,
            "session_year": session.year,
            "share_class_figi": "BBG000000003",
            "source_artifact_sha256": artifact_sha,
            "source_available_at_utc": available_at,
            "source_available_session": available_session,
            "source_availability_rule": "first_xnys_open_after_source_capture_v1",
            "source_capture_at_utc": captured_at,
            "source_page_sequence": 1,
            "source_provider_request_id": provider_request_id,
            "source_record_id": source_record_id,
            "source_request_id": request_id,
            "source_row_hash": row_hash,
            "source_row_ordinal": 1,
            "ticker": TICKER,
            "type_code": "CS",
        }
    )
    universe = build(UNIVERSE_SOURCE_DAILY_CONTRACT.arrow_schema)
    universe.update(
        {
            "active_on_date": True,
            "active_source_request_id": request_id,
            "cik": asset["cik"],
            "composite_figi": figi,
            "currency_name": asset["currency_name"],
            "delisted_at_utc": asset["delisted_at_utc"],
            "inactive_source_request_id": _digest("f"),
            "last_updated_at_utc": asset["last_updated_at_utc"],
            "locale": asset["locale"],
            "market": asset["market"],
            "name": asset["name"],
            "primary_exchange_mic": asset["primary_exchange_mic"],
            "selected_source_capture_at_utc": captured_at,
            "selected_source_record_id": source_record_id,
            "session_date": session,
            "session_year": session.year,
            "share_class_figi": asset["share_class_figi"],
            "source_artifact_sha256": artifact_sha,
            "source_available_at_utc": available_at,
            "source_available_session": available_session,
            "source_availability_rule": ("first_xnys_open_after_complete_active_inactive_pair_v1"),
            "source_page_sequence": 1,
            "source_pair_id": _digest("1"),
            "source_provider_request_id": provider_request_id,
            "source_request_id": request_id,
            "source_row_hash": row_hash,
            "source_row_ordinal": 1,
            "ticker": TICKER,
            "type_code": asset["type_code"],
            "universe_capture_completed_at_utc": captured_at,
            "version_group_id": _digest("2"),
        }
    )
    return asset, universe


def _daily_source(
    root: Path,
    *,
    table: str,
    rows: list[dict[str, object]],
) -> IdentityPublishedSource:
    contract = (
        ASSET_OBSERVATION_DAILY_CONTRACT
        if table == "asset_observation_daily"
        else UNIVERSE_SOURCE_DAILY_CONTRACT
    )
    pin = S7_SOURCE_PINS[table]
    refs: list[ArtifactRef] = []
    paths: list[Path] = []
    for row in rows:
        session = row["session_date"]
        assert type(session) is date
        relative = (
            f"silver/schema=v1/reference/{table}/build_id={pin.build_id}/data/"
            f"session_year={session.year}/session_date={session.isoformat()}/part-00000.parquet"
        )
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist([row], schema=contract.arrow_schema), path)
        refs.append(
            ArtifactRef(
                path=relative,
                sha256=sha256_file(path),
                bytes=path.stat().st_size,
                row_count=1,
                media_type="application/vnd.apache.parquet",
                role=ArtifactRole.DATA,
                table=table,
                schema_digest=contract.schema_digest,
            )
        )
        paths.append(path)
    return IdentityPublishedSource(
        pin=pin,
        published=SimpleNamespace(
            contract=contract,
            data_paths=tuple(paths),
            release=SimpleNamespace(outputs=tuple(refs)),
        ),
        release_manifest_path=f"manifests/silver/releases/release_id={pin.release_id}.json",
        release_manifest_sha256=pin.release_manifest_sha256,
        data_root=root.resolve(),
    )


def _bundle(
    root: Path,
    asset_rows: list[dict[str, object]],
    universe_rows: list[dict[str, object]],
) -> IdentitySourceBundle:
    asset = _daily_source(root, table="asset_observation_daily", rows=asset_rows)
    universe = _daily_source(root, table="universe_source_daily", rows=universe_rows)
    dummy = SimpleNamespace(
        contract=ASSET_OBSERVATION_DAILY_CONTRACT,
        data_paths=(),
        release=SimpleNamespace(outputs=()),
    )
    sources = {
        table: (
            asset
            if table == "asset_observation_daily"
            else universe
            if table == "universe_source_daily"
            else IdentityPublishedSource(
                pin=pin,
                published=dummy,
                release_manifest_path=f"manifests/silver/releases/release_id={pin.release_id}.json",
                release_manifest_sha256=pin.release_manifest_sha256,
                data_root=root.resolve(),
            )
        )
        for table, pin in S7_SOURCE_PINS.items()
    }
    return IdentitySourceBundle._for_testing(sources)


def _controls(
    root: Path,
    *,
    asset_row_cap: int = 3,
    universe_row_cap: int = 3,
    total_row_cap: int = 6,
) -> tuple[S7DetectorPreviewPlan, S7DetectorPreviewPlanApproval, tuple[date, ...]]:
    write_xnys_calendar_artifact(root, CALENDAR)
    sessions = tuple(item.session_date for item in CALENDAR.sessions[:3])
    store = IdentityPreviewPlanStore(root)
    allowlist = build_s7_ticker_allowlist((TICKER,))
    store.store_ticker_allowlist(allowlist)
    plan = S7DetectorPreviewPlan.create(
        created_by="fixture-planner",
        created_at_utc=datetime(2024, 1, 1, 12, tzinfo=UTC),
        git_commit="a" * 40,
        calendar_artifact_id=CALENDAR.calendar_artifact_id,
        calendar_artifact_sha256=CALENDAR.sha256,
        start_session=sessions[0],
        end_session=sessions[-1],
        session_count=len(sessions),
        ticker_allowlist=allowlist,
        resource_caps=S7DetectorPreviewResourceCaps(
            selected_row_cap=3,
            universe_scanned_row_cap=universe_row_cap,
            asset_parent_scanned_row_cap=asset_row_cap,
            total_scanned_row_cap=total_row_cap,
            source_artifact_cap=6,
            source_bytes_cap=10_000_000,
            case_cap=3,
            batch_size=2,
        ),
    )
    stored_plan = store.store_plan(plan)
    request = S7DetectorPreviewApprovalRequest.create(
        plan,
        stored_plan,
        created_by="fixture-requester",
        created_at_utc=datetime(2024, 1, 1, 13, tzinfo=UTC),
    )
    stored_request = store.store_approval_request(request)
    approval = S7DetectorPreviewPlanApproval.create(
        request,
        stored_request,
        approval_literal=request.canonical_approval_literal,
        approved_by="fixture-reviewer",
        approved_at_utc=datetime(2024, 1, 1, 14, tzinfo=UTC),
    )
    store.store_approval(approval)
    return plan, approval, sessions


def _rows(sessions: tuple[date, ...]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    figis = (OUTER_FIGI, MIDDLE_FIGI, OUTER_FIGI)
    source_ids = (_digest("3"), _digest("4"), _digest("5"))
    pairs = [
        _paired_rows(session=session, figi=figi, source_record_id=source_id)
        for session, figi, source_id in zip(sessions, figis, source_ids, strict=True)
    ]
    return [item[0] for item in pairs], [item[1] for item in pairs]


def _authorize_fixture_bundle(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    bundle: IdentitySourceBundle,
) -> None:
    monkeypatch.setattr(IdentitySourceBundle, "require_official", lambda self: None)
    monkeypatch.setattr(IdentitySourceArtifact, "require_official", lambda self: None)
    monkeypatch.setattr(IdentitySourceBatch, "require_official", lambda self: None)
    monkeypatch.setattr(
        IdentitySourceBundle,
        "data_root",
        property(lambda self: root.resolve()),
    )
    monkeypatch.setattr(
        "ame_stocks_api.silver.identity_preview_runner.open_identity_source_bundle",
        lambda data_root: bundle,
    )
    monkeypatch.setattr(
        "ame_stocks_api.silver.identity_preview_runner._verify_git_checkout",
        lambda expected_commit: None,
    )
    monkeypatch.setattr(
        "ame_stocks_api.silver.identity_preview_runner._utc_now",
        lambda: RUN_AT,
    )
    monkeypatch.setattr(
        "ame_stocks_api.silver.identity_provider_evidence._utc_now",
        lambda: RUN_AT,
    )


def _run(
    root: Path,
    plan: S7DetectorPreviewPlan,
    approval: S7DetectorPreviewPlanApproval,
) -> S7DetectorPreviewCompletion:
    return run_source_bound_identity_streaming_preview(
        root,
        plan_id=plan.plan_id,
        expected_plan_sha256=plan.sha256,
        approval_id=approval.approval_id,
        expected_approval_sha256=approval.sha256,
    )


def test_source_bound_runner_stops_at_attested_review_and_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, approval, sessions = _controls(tmp_path)
    asset_rows, universe_rows = _rows(sessions)
    bundle = _bundle(tmp_path, asset_rows, universe_rows)
    _authorize_fixture_bundle(monkeypatch, tmp_path, bundle)

    completion = _run(tmp_path, plan, approval)

    assert completion.status == "awaiting_review"
    assert completion.source_attested is True
    assert completion.source_attestation_scope == (
        "s4_asset_observation_and_universe_membership_only"
    )
    assert completion.corroboration_evaluation_state == "not_evaluated"
    assert completion.support_absence_verified is False
    assert completion.canonical_candidate_eligible is False
    assert completion.adjudication_eligible is False
    assert completion.backtest_identity_eligible is False
    assert completion.publication_eligible is False
    assert completion.case_count == 1
    assert completion.suspected_provider_figi_bounce_rows == 1
    assert completion.scanned_artifact_count == 6
    assert completion.scanned_row_count == 6
    assert len(completion.case_evidence) == 1
    completion_path = tmp_path / completion.relative_path
    preview_path = tmp_path / completion.preview_artifact_path
    evidence_path = tmp_path / completion.case_evidence[0].path
    assert completion_path.read_bytes() == completion.content
    assert preview_path.is_file()
    assert evidence_path.is_file()
    assert S7DetectorPreviewCompletion.from_dict(json.loads(completion.content)) == completion
    output_files = tuple(
        sorted(
            path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*") if path.is_file()
        )
    )
    assert not any(
        marker in path
        for path in output_files
        for marker in ("candidate", "adjudication", "full-run", "publish")
    )

    repeated = _run(tmp_path, plan, approval)
    assert repeated == completion
    assert (
        tuple(
            sorted(
                path.relative_to(tmp_path).as_posix()
                for path in tmp_path.rglob("*")
                if path.is_file()
            )
        )
        == output_files
    )

    physical_asset = bundle.sources["asset_observation_daily"].data_paths[0]
    physical_asset.write_bytes(b"corrupted")
    with pytest.raises(
        IdentityPreviewRunnerError,
        match="completion physical detector replay failed",
    ):
        _run(tmp_path, plan, approval)


def test_completion_is_not_written_after_authority_crosses_market_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, approval, sessions = _controls(tmp_path)
    asset_rows, universe_rows = _rows(sessions)
    bundle = _bundle(tmp_path, asset_rows, universe_rows)
    _authorize_fixture_bundle(monkeypatch, tmp_path, bundle)
    crossed_boundary = datetime(2024, 1, 9, 15, tzinfo=UTC)
    authority_clock = iter((RUN_AT, RUN_AT, RUN_AT, crossed_boundary))
    monkeypatch.setattr(
        "ame_stocks_api.silver.identity_provider_evidence._utc_now",
        lambda: next(authority_clock),
    )

    with pytest.raises(
        IdentityPreviewRunnerError,
        match="completion evidence authority is no longer live",
    ):
        _run(tmp_path, plan, approval)

    assert not (
        tmp_path / detector_preview_completion_path(plan.plan_id, approval.approval_id)
    ).exists()


def test_source_bound_runner_rejects_parent_lineage_mismatch_before_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, approval, sessions = _controls(tmp_path)
    asset_rows, universe_rows = _rows(sessions)
    asset_rows[1]["name"] = "Wrong Parent"
    bundle = _bundle(tmp_path, asset_rows, universe_rows)
    _authorize_fixture_bundle(monkeypatch, tmp_path, bundle)

    with pytest.raises(IdentityPreviewRunnerError, match="S4 parent lineage mismatch"):
        _run(tmp_path, plan, approval)

    assert not (
        tmp_path / detector_preview_completion_path(plan.plan_id, approval.approval_id)
    ).exists()
    assert not (tmp_path / "manifests/silver/identity-bounce-bounded-previews").exists()


def test_existing_zero_case_completion_cannot_bypass_physical_detector_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, approval, sessions = _controls(tmp_path)
    asset_rows, universe_rows = _rows(sessions)
    bundle = _bundle(tmp_path, asset_rows, universe_rows)
    _authorize_fixture_bundle(monkeypatch, tmp_path, bundle)
    asset_artifacts = bundle.daily_partition_artifacts("asset_observation_daily", sessions)
    universe_artifacts = bundle.daily_partition_artifacts("universe_source_daily", sessions)
    source_refs = _preflight_source_scope(
        plan,
        sessions,
        asset_artifacts,
        universe_artifacts,
    )
    available_session, _ = CALENDAR.first_open_after(RUN_AT)
    engine = BoundedIdentityPreviewEngine(
        six_release_binding_id=plan.six_release_binding_id,
        preview_manifest_available_session=available_session,
        scoped_tickers=(TICKER,),
        limits=BoundedIdentityPreviewLimits(
            max_sessions=3,
            max_tickers=1,
            max_selected_rows=3,
            max_scanned_rows=6,
            max_artifacts=6,
            max_bytes=plan.resource_caps.source_bytes_cap,
            max_cases=3,
        ),
    )
    refs_by_key = {(item.table, item.session_date): item for item in source_refs}
    for session in sessions:
        asset_ref = refs_by_key[("asset_observation_daily", session)]
        universe_ref = refs_by_key[("universe_source_daily", session)]
        engine.consume_session(
            SourceSession(session),
            (),
            scanned_row_count=asset_ref.row_count + universe_ref.row_count,
            scanned_artifact_count=2,
            scanned_bytes=asset_ref.bytes + universe_ref.bytes,
        )
    forged_preview = build_bounded_identity_preview_artifact(engine.finalize())
    preview_receipt = write_bounded_identity_preview_artifact(tmp_path, forged_preview)
    forged = S7DetectorPreviewCompletion(
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        approval_id=approval.approval_id,
        approval_sha256=approval.sha256,
        request_event_id=approval.request_event_id,
        request_event_sha256=approval.request_event_sha256,
        git_commit=plan.git_commit,
        calendar_artifact_id=CALENDAR.calendar_artifact_id,
        calendar_artifact_sha256=CALENDAR.sha256,
        ticker_allowlist_id=plan.ticker_allowlist_id,
        ticker_allowlist_sha256=plan.ticker_allowlist_sha256,
        start_session=plan.start_session,
        end_session=plan.end_session,
        session_count=plan.session_count,
        ticker_count=plan.ticker_count,
        source_artifacts=source_refs,
        preview_artifact_id=forged_preview.preview_artifact_id,
        preview_artifact_path=forged_preview.relative_path,
        preview_artifact_sha256=forged_preview.sha256,
        preview_artifact_bytes=int(preview_receipt["bytes"]),
        case_evidence=(),
        selected_observation_count=0,
        valid_active_observation_count=0,
        scanned_row_count=sum(item.row_count for item in source_refs),
        scanned_artifact_count=len(source_refs),
        scanned_bytes=sum(item.bytes for item in source_refs),
        case_count=0,
        suspected_provider_figi_bounce_rows=0,
        created_at_utc=RUN_AT,
        completion_available_session=available_session,
    )
    completion_path = tmp_path / forged.relative_path
    completion_path.parent.mkdir(parents=True, exist_ok=True)
    completion_path.write_bytes(forged.content)

    with pytest.raises(
        IdentityPreviewRunnerError,
        match="stored preview does not reproduce the bounded physical scan",
    ):
        _run(tmp_path, plan, approval)


def test_source_bound_runner_enforces_metadata_caps_before_scanning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, approval, sessions = _controls(
        tmp_path,
        asset_row_cap=2,
        universe_row_cap=3,
        total_row_cap=5,
    )
    asset_rows, universe_rows = _rows(sessions)
    bundle = _bundle(tmp_path, asset_rows, universe_rows)
    _authorize_fixture_bundle(monkeypatch, tmp_path, bundle)

    with pytest.raises(IdentityPreviewRunnerError, match="asset-parent preflight row cap exceeded"):
        _run(tmp_path, plan, approval)

    assert not (
        tmp_path / detector_preview_completion_path(plan.plan_id, approval.approval_id)
    ).exists()
    assert not (tmp_path / "manifests/silver/identity-bounce-bounded-previews").exists()
