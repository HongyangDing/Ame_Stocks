from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ame_stocks_api.silver.identity_provider_evidence as provider_evidence_module
from ame_stocks_api.artifacts import sha256_file, stable_digest
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.calendar_artifact import (
    build_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.contracts import ArtifactRef, ArtifactRole
from ame_stocks_api.silver.identity_bounce import IdentityObservation, SourceSession
from ame_stocks_api.silver.identity_preview_plan import (
    IdentityPreviewPlanStore,
    S7DetectorPreviewApprovalRequest,
    S7DetectorPreviewPlan,
    S7DetectorPreviewPlanApproval,
    S7DetectorPreviewResourceCaps,
    build_s7_ticker_allowlist,
)
from ame_stocks_api.silver.identity_provider_evidence import (
    ProviderEvidenceError,
    S4BounceProviderEvidenceManifest,
    _build_s4_bounce_provider_evidence_manifest_for_runner,
    _issue_runner_evidence_authority,
    _ProviderReplaySession,
    _write_s4_bounce_provider_evidence_manifest_from_official_bundle,
    attest_provider_row,
    attest_provider_rows,
    build_s4_bounce_case_evidence_usage,
    build_s4_bounce_provider_evidence_manifest,
    read_s4_bounce_provider_evidence_manifest,
    write_s4_bounce_provider_evidence_manifest,
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
)

CALENDAR = build_xnys_calendar_artifact(date(2024, 1, 2), date(2024, 2, 2))
OUTER_FIGI = "BBG000000001"
MIDDLE_FIGI = "BBG000000002"
TICKER = "FIX"
EVIDENCE_CREATED_AT = datetime(2024, 1, 8, 21, tzinfo=UTC)


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
    ticker: str,
    figi: str,
    source_record_id: str,
    active: bool = True,
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
            "session_year": session.year,
            "session_date": session,
            "requested_active": active,
            "provider_active": active,
            "ticker": ticker,
            "type_code": "CS",
            "name": "Fixture Corp",
            "market": "stocks",
            "locale": "us",
            "primary_exchange_mic": "XNYS",
            "currency_name": "usd",
            "cik": "0000000001",
            "composite_figi": figi,
            "share_class_figi": "BBG000000003",
            "source_capture_at_utc": captured_at,
            "source_available_session": available_session,
            "source_available_at_utc": available_at,
            "source_availability_rule": "first_xnys_open_after_source_capture_v1",
            "source_record_id": source_record_id,
            "source_request_id": request_id,
            "source_provider_request_id": provider_request_id,
            "source_artifact_sha256": artifact_sha,
            "source_page_sequence": 1,
            "source_row_ordinal": 1,
            "source_row_hash": row_hash,
        }
    )
    universe = build(UNIVERSE_SOURCE_DAILY_CONTRACT.arrow_schema)
    universe.update(
        {
            "session_year": session.year,
            "session_date": session,
            "ticker": ticker,
            "active_on_date": active,
            "type_code": asset["type_code"],
            "name": asset["name"],
            "market": asset["market"],
            "locale": asset["locale"],
            "primary_exchange_mic": asset["primary_exchange_mic"],
            "currency_name": asset["currency_name"],
            "cik": asset["cik"],
            "composite_figi": figi,
            "share_class_figi": asset["share_class_figi"],
            "delisted_at_utc": asset["delisted_at_utc"],
            "last_updated_at_utc": asset["last_updated_at_utc"],
            "selected_source_record_id": source_record_id,
            "selected_source_capture_at_utc": captured_at,
            "universe_capture_completed_at_utc": captured_at,
            "source_available_session": available_session,
            "source_available_at_utc": available_at,
            "source_availability_rule": ("first_xnys_open_after_complete_active_inactive_pair_v1"),
            "source_request_id": request_id,
            "source_provider_request_id": provider_request_id,
            "source_artifact_sha256": artifact_sha,
            "source_page_sequence": 1,
            "source_row_ordinal": 1,
            "source_row_hash": row_hash,
            "active_source_request_id": request_id if active else _digest("f"),
            "inactive_source_request_id": _digest("f") if active else request_id,
            "source_pair_id": _digest("1"),
            "version_group_id": _digest("2"),
        }
    )
    return asset, universe


def _table(rows: list[dict[str, object]], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=schema)


def _published_source(
    root: Path,
    *,
    table: str,
    rows: list[dict[str, object]],
    schema: pa.Schema,
) -> IdentityPublishedSource:
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{table}.parquet"
    pq.write_table(_table(rows, schema), path, row_group_size=max(1, len(rows)))
    ref = ArtifactRef(
        path=f"silver/fixture/{table}/part-00000.parquet",
        sha256=sha256_file(path),
        bytes=path.stat().st_size,
        row_count=len(rows),
        media_type="application/vnd.apache.parquet",
        role=ArtifactRole.DATA,
        table=table,
        schema_digest=(
            ASSET_OBSERVATION_DAILY_CONTRACT.schema_digest
            if table == "asset_observation_daily"
            else UNIVERSE_SOURCE_DAILY_CONTRACT.schema_digest
        ),
    )
    contract = (
        ASSET_OBSERVATION_DAILY_CONTRACT
        if table == "asset_observation_daily"
        else UNIVERSE_SOURCE_DAILY_CONTRACT
    )
    published = SimpleNamespace(
        contract=contract,
        data_paths=(path,),
        release=SimpleNamespace(outputs=(ref,)),
    )
    pin = S7_SOURCE_PINS[table]
    return IdentityPublishedSource(
        pin=pin,
        published=published,
        release_manifest_path=f"manifests/silver/releases/release_id={pin.release_id}.json",
        release_manifest_sha256=pin.release_manifest_sha256,
    )


def _bundle(
    root: Path,
    asset_rows: list[dict[str, object]],
    universe_rows: list[dict[str, object]],
) -> IdentitySourceBundle:
    asset = _published_source(
        root,
        table="asset_observation_daily",
        rows=asset_rows,
        schema=ASSET_OBSERVATION_DAILY_CONTRACT.arrow_schema,
    )
    universe = _published_source(
        root,
        table="universe_source_daily",
        rows=universe_rows,
        schema=UNIVERSE_SOURCE_DAILY_CONTRACT.arrow_schema,
    )
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
                release_manifest_path="manifests/fixture.json",
                release_manifest_sha256=_digest("0"),
            )
        )
        for table, pin in S7_SOURCE_PINS.items()
    }
    return IdentitySourceBundle._for_testing(sources)


def _allow_test_capability(
    monkeypatch: pytest.MonkeyPatch,
    data_root: Path,
) -> None:
    monkeypatch.setattr(IdentitySourceBundle, "require_official", lambda self: None)
    monkeypatch.setattr(
        IdentitySourceBundle,
        "require_approved_preview_scope",
        lambda self, **kwargs: None,
    )
    monkeypatch.setattr(IdentitySourceArtifact, "require_official", lambda self: None)
    monkeypatch.setattr(IdentitySourceBatch, "require_official", lambda self: None)
    monkeypatch.setattr(
        IdentitySourceBundle,
        "data_root",
        property(lambda self: data_root.resolve()),
    )
    monkeypatch.setattr(
        provider_evidence_module,
        "_utc_now",
        lambda: EVIDENCE_CREATED_AT,
    )


def _batches(bundle: IdentitySourceBundle) -> tuple[IdentitySourceBatch, IdentitySourceBatch]:
    asset = next(
        bundle.iter_physical_batches(
            "asset_observation_daily",
            batch_size=100,
        )
    )
    universe = next(
        bundle.iter_physical_batches(
            "universe_source_daily",
            batch_size=100,
        )
    )
    return asset, universe


def _preview_controls(root: Path):
    write_xnys_calendar_artifact(root, CALENDAR)
    sessions = tuple(item.session_date for item in CALENDAR.sessions[:3])
    store = IdentityPreviewPlanStore(root)
    allowlist = build_s7_ticker_allowlist((TICKER,))
    store.store_ticker_allowlist(allowlist)
    caps = S7DetectorPreviewResourceCaps(
        selected_row_cap=3,
        universe_scanned_row_cap=100,
        asset_parent_scanned_row_cap=100,
        total_scanned_row_cap=200,
        source_artifact_cap=10,
        source_bytes_cap=10_000_000,
        case_cap=3,
        batch_size=100,
    )
    plan = S7DetectorPreviewPlan.create(
        created_by="fixture-planner",
        created_at_utc=datetime(2024, 1, 2, 1, tzinfo=UTC),
        git_commit="a" * 40,
        calendar_artifact_id=CALENDAR.calendar_artifact_id,
        calendar_artifact_sha256=CALENDAR.sha256,
        start_session=sessions[0],
        end_session=sessions[-1],
        session_count=len(sessions),
        ticker_allowlist=allowlist,
        resource_caps=caps,
    )
    stored_plan = store.store_plan(plan)
    request = S7DetectorPreviewApprovalRequest.create(
        plan,
        stored_plan,
        created_by="fixture-requester",
        created_at_utc=datetime(2024, 1, 2, 2, tzinfo=UTC),
    )
    stored_request = store.store_approval_request(request)
    approval = S7DetectorPreviewPlanApproval.create(
        request,
        stored_request,
        approval_literal=request.canonical_approval_literal,
        approved_by="fixture-reviewer",
        approved_at_utc=datetime(2024, 1, 2, 3, tzinfo=UTC),
    )
    store.store_approval(approval)

    engine = BoundedIdentityPreviewEngine(
        six_release_binding_id=plan.six_release_binding_id,
        preview_manifest_available_session=CALENDAR.sessions[5].session_date,
        scoped_tickers=(TICKER,),
        limits=BoundedIdentityPreviewLimits(
            max_sessions=3,
            max_tickers=1,
            max_selected_rows=3,
            max_scanned_rows=6,
            max_artifacts=6,
            max_bytes=10_000,
            max_cases=3,
        ),
    )
    ids = (_digest("3"), _digest("4"), _digest("5"))
    figis = (OUTER_FIGI, MIDDLE_FIGI, OUTER_FIGI)
    for session, source_id, figi in zip(sessions, ids, figis, strict=True):
        captured_at = datetime.combine(session, datetime.min.time(), tzinfo=UTC) + timedelta(
            hours=21
        )
        available_session, _ = CALENDAR.first_open_after(captured_at)
        engine.consume_session(
            SourceSession(session),
            (
                IdentityObservation(
                    session_date=session,
                    ticker=TICKER,
                    observed_composite_figi=figi,
                    source_record_id=source_id,
                    source_available_session=available_session,
                ),
            ),
            scanned_row_count=2,
            scanned_artifact_count=2,
            scanned_bytes=100,
        )
    preview = build_bounded_identity_preview_artifact(engine.finalize())
    preview_path = root / preview.relative_path
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    preview_path.write_bytes(preview.content)
    case = preview.document["result"]["cases"]
    assert isinstance(case, list) and len(case) == 1
    return plan, approval, preview, preview.document["result"], sessions, ids, figis


def _case(preview):
    return preview.document["result"]["cases"][0]


def _case_object(preview):
    from ame_stocks_api.silver.identity_provider_evidence import _bounce_case_from_snapshot

    return _bounce_case_from_snapshot(_case(preview))


def test_test_bundle_cannot_mint_provider_attestation(tmp_path: Path) -> None:
    asset, universe = _paired_rows(
        session=CALENDAR.sessions[0].session_date,
        ticker=TICKER,
        figi=OUTER_FIGI,
        source_record_id=_digest("3"),
    )
    batch, _ = _batches(_bundle(tmp_path, [asset], [universe]))
    with pytest.raises(ProviderEvidenceError, match="official source batch"):
        attest_provider_row(batch, row_index_in_batch=0, calendar=CALENDAR)


def test_batch_attestation_replays_locator_once_and_rejects_bad_indices(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first, first_u = _paired_rows(
        session=CALENDAR.sessions[0].session_date,
        ticker=TICKER,
        figi=OUTER_FIGI,
        source_record_id=_digest("3"),
    )
    second, second_u = _paired_rows(
        session=CALENDAR.sessions[1].session_date,
        ticker=TICKER,
        figi=MIDDLE_FIGI,
        source_record_id=_digest("4"),
    )
    bundle = _bundle(tmp_path, [first, second], [first_u, second_u])
    _allow_test_capability(monkeypatch, tmp_path)
    batch, _ = _batches(bundle)
    attestations = attest_provider_rows(
        batch,
        row_indices_in_batch=(0, 1),
        calendar=CALENDAR,
    )
    assert tuple(item.row_index_in_row_group for item in attestations) == (0, 1)
    assert attestations[0] == attest_provider_row(
        batch,
        row_index_in_batch=0,
        calendar=CALENDAR,
    )
    assert attestations[0].full_row_snapshot["ticker"] == TICKER
    with pytest.raises(ProviderEvidenceError, match="nonempty and unique"):
        attest_provider_rows(batch, row_indices_in_batch=(0, 0), calendar=CALENDAR)
    with pytest.raises(ProviderEvidenceError, match="outside"):
        attest_provider_rows(batch, row_indices_in_batch=(2,), calendar=CALENDAR)


def test_provider_replay_session_batches_row_groups_and_memoizes_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pairs = [
        _paired_rows(
            session=CALENDAR.sessions[index].session_date,
            ticker=TICKER,
            figi=(OUTER_FIGI, MIDDLE_FIGI, OUTER_FIGI)[index],
            source_record_id=(_digest("3"), _digest("4"), _digest("5"))[index],
        )
        for index in range(3)
    ]
    bundle = _bundle(tmp_path, [item[0] for item in pairs], [item[1] for item in pairs])
    _allow_test_capability(monkeypatch, tmp_path)
    asset_batch, universe_batch = _batches(bundle)
    attestations = (
        *attest_provider_rows(
            asset_batch,
            row_indices_in_batch=(0, 1, 2),
            calendar=CALENDAR,
        ),
        *attest_provider_rows(
            universe_batch,
            row_indices_in_batch=(0, 1, 2),
            calendar=CALENDAR,
        ),
    )
    calls = {"artifact": 0, "row_group": 0}
    original_artifact = provider_evidence_module._validate_official_artifact
    original_rows = provider_evidence_module._read_physical_rows_bounded

    def count_artifact(*args: object, **kwargs: object) -> None:
        calls["artifact"] += 1
        original_artifact(*args, **kwargs)

    def count_rows(*args: object, **kwargs: object):
        calls["row_group"] += 1
        return original_rows(*args, **kwargs)

    monkeypatch.setattr(
        provider_evidence_module,
        "_validate_official_artifact",
        count_artifact,
    )
    monkeypatch.setattr(
        provider_evidence_module,
        "_read_physical_rows_bounded",
        count_rows,
    )
    session = _ProviderReplaySession(bundle=bundle, calendar=CALENDAR)

    assert session.replay(attestations) == attestations
    assert calls == {"artifact": 2, "row_group": 2}
    assert session.replay(attestations) == attestations
    assert calls == {"artifact": 2, "row_group": 2}


def test_s4_usage_binds_every_case_role_to_exact_plan_spine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, _, preview, _, sessions, ids, figis = _preview_controls(tmp_path)
    pairs = [
        _paired_rows(session=session, ticker=TICKER, figi=figi, source_record_id=source_id)
        for session, source_id, figi in zip(sessions, ids, figis, strict=True)
    ]
    bundle = _bundle(tmp_path, [item[0] for item in pairs], [item[1] for item in pairs])
    _allow_test_capability(monkeypatch, tmp_path)
    asset_batch, universe_batch = _batches(bundle)
    assets = attest_provider_rows(
        asset_batch,
        row_indices_in_batch=(0, 1, 2),
        calendar=CALENDAR,
    )
    universes = attest_provider_rows(
        universe_batch,
        row_indices_in_batch=(0, 1, 2),
        calendar=CALENDAR,
    )
    case = _case_object(preview)
    usages = tuple(
        build_s4_bounce_case_evidence_usage(
            case,
            plan=plan,
            preview=preview,
            asset_observation=asset,
            universe_membership=universe,
            calendar=CALENDAR,
        )
        for asset, universe in zip(assets, universes, strict=True)
    )
    assert tuple(item.case_role for item in usages) == ("left_outer", "middle", "right_outer")
    assert tuple(item.session_date for item in usages) == sessions
    alien_asset, alien_universe = _paired_rows(
        session=sessions[1],
        ticker=TICKER,
        figi=MIDDLE_FIGI,
        source_record_id=_digest("9"),
    )
    alien_bundle = _bundle(tmp_path / "alien", [alien_asset], [alien_universe])
    alien_a, alien_u = _batches(alien_bundle)
    alien_attestation = attest_provider_row(alien_a, row_index_in_batch=0, calendar=CALENDAR)
    alien_membership = attest_provider_row(alien_u, row_index_in_batch=0, calendar=CALENDAR)
    with pytest.raises(ProviderEvidenceError, match="outside the exact BounceCase"):
        build_s4_bounce_case_evidence_usage(
            case,
            plan=plan,
            preview=preview,
            asset_observation=alien_attestation,
            universe_membership=alien_membership,
            calendar=CALENDAR,
        )


def test_s4_usage_rejects_exact_inactive_parent_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, _, preview, _, sessions, ids, _ = _preview_controls(tmp_path)
    asset, universe = _paired_rows(
        session=sessions[1],
        ticker=TICKER,
        figi=MIDDLE_FIGI,
        source_record_id=ids[1],
        active=False,
    )
    bundle = _bundle(tmp_path / "inactive", [asset], [universe])
    _allow_test_capability(monkeypatch, tmp_path / "inactive")
    asset_batch, universe_batch = _batches(bundle)
    asset_attestation = attest_provider_row(
        asset_batch,
        row_index_in_batch=0,
        calendar=CALENDAR,
    )
    universe_attestation = attest_provider_row(
        universe_batch,
        row_index_in_batch=0,
        calendar=CALENDAR,
    )

    with pytest.raises(ProviderEvidenceError, match="requires active membership"):
        build_s4_bounce_case_evidence_usage(
            _case_object(preview),
            plan=plan,
            preview=preview,
            asset_observation=asset_attestation,
            universe_membership=universe_attestation,
            calendar=CALENDAR,
        )


def test_source_attested_case_manifest_roundtrips_and_physically_replays(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, approval, preview, _, sessions, ids, figis = _preview_controls(tmp_path)
    pairs = [
        _paired_rows(session=session, ticker=TICKER, figi=figi, source_record_id=source_id)
        for session, source_id, figi in zip(sessions, ids, figis, strict=True)
    ]
    bundle = _bundle(tmp_path, [item[0] for item in pairs], [item[1] for item in pairs])
    _allow_test_capability(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "ame_stocks_api.silver.identity_provider_evidence.open_identity_source_bundle",
        lambda root: bundle,
    )
    asset_batch, universe_batch = _batches(bundle)
    assets = attest_provider_rows(
        asset_batch,
        row_indices_in_batch=(0, 1, 2),
        calendar=CALENDAR,
    )
    universes = attest_provider_rows(
        universe_batch,
        row_indices_in_batch=(0, 1, 2),
        calendar=CALENDAR,
    )
    case = _case_object(preview)
    usages = tuple(
        build_s4_bounce_case_evidence_usage(
            case,
            plan=plan,
            preview=preview,
            asset_observation=asset,
            universe_membership=universe,
            calendar=CALENDAR,
        )
        for asset, universe in zip(assets, universes, strict=True)
    )
    attestations = tuple(item for pair in zip(assets, universes, strict=True) for item in pair)
    authority = _issue_runner_evidence_authority(
        data_root=tmp_path,
        bundle=bundle,
        plan=plan,
        approval=approval,
        calendar=CALENDAR,
        created_at_utc=EVIDENCE_CREATED_AT,
    )
    manifest = _build_s4_bounce_provider_evidence_manifest_for_runner(
        data_root=tmp_path,
        bundle=bundle,
        plan=plan,
        approval=approval,
        preview=preview,
        case=case,
        attestations=attestations,
        usages=usages,
        calendar=CALENDAR,
        _authority=authority,
    )
    assert manifest.source_attested_bounce is True
    assert S4BounceProviderEvidenceManifest.from_dict(json.loads(manifest.content)) == manifest
    with pytest.raises(ProviderEvidenceError, match=r"standalone.*writing is disabled"):
        write_s4_bounce_provider_evidence_manifest(tmp_path, manifest)
    stored = _write_s4_bounce_provider_evidence_manifest_from_official_bundle(
        tmp_path,
        manifest,
        bundle=bundle,
        calendar=CALENDAR,
        _authority=authority,
    )
    assert stored["sha256"] == manifest.sha256
    with pytest.raises(ProviderEvidenceError, match=r"standalone.*reading is disabled"):
        read_s4_bounce_provider_evidence_manifest(
            tmp_path,
            manifest_id=manifest.manifest_id,
            expected_sha256=manifest.sha256,
            plan_id=plan.plan_id,
            expected_plan_sha256=plan.sha256,
            approval_id=approval.approval_id,
            expected_approval_sha256=approval.sha256,
            preview_artifact_id=preview.preview_artifact_id,
            expected_preview_sha256=preview.sha256,
            calendar=CALENDAR,
        )

    near_boundary = CALENDAR.market_open(authority.manifest_available_session) - timedelta(
        seconds=30
    )
    monkeypatch.setattr(
        provider_evidence_module,
        "_utc_now",
        lambda: near_boundary,
    )
    with pytest.raises(ProviderEvidenceError, match="too close to its availability boundary"):
        _write_s4_bounce_provider_evidence_manifest_from_official_bundle(
            tmp_path,
            manifest,
            bundle=bundle,
            calendar=CALENDAR,
            _authority=authority,
        )

    monkeypatch.setattr(
        provider_evidence_module,
        "_utc_now",
        lambda: datetime(2024, 1, 9, 15, tzinfo=UTC),
    )
    with pytest.raises(ProviderEvidenceError, match="expired across an availability boundary"):
        _write_s4_bounce_provider_evidence_manifest_from_official_bundle(
            tmp_path,
            manifest,
            bundle=bundle,
            calendar=CALENDAR,
            _authority=authority,
        )

    with pytest.raises(ProviderEvidenceError, match="must equal manifest availability"):
        replace(
            manifest,
            manifest_available_session=CALENDAR.sessions[6].session_date,
        )

    forged_case = replace(
        case,
        right_evidence_available_session=sessions[0],
    )
    forged_digest = stable_digest(forged_case.to_manifest_dict())
    forged_usages = tuple(replace(item, case_snapshot_digest=forged_digest) for item in usages)
    with pytest.raises(ProviderEvidenceError, match="right-side availability"):
        S4BounceProviderEvidenceManifest(
            plan_id=plan.plan_id,
            plan_sha256=plan.sha256,
            approval_id=approval.approval_id,
            approval_sha256=approval.sha256,
            preview_artifact_id=preview.preview_artifact_id,
            preview_artifact_sha256=preview.sha256,
            case_snapshot=forged_case.to_manifest_dict(),
            row_attestations=attestations,
            usages=forged_usages,
            created_at_utc=datetime(2024, 1, 8, 21, tzinfo=UTC),
            manifest_available_session=CALENDAR.sessions[5].session_date,
            availability_calendar_id=CALENDAR.calendar_artifact_id,
            availability_calendar_sha256=CALENDAR.sha256,
        )


def test_case_manifest_rejects_cross_plan_usage_and_time_travel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, approval, preview, _, sessions, ids, figis = _preview_controls(tmp_path)
    pairs = [
        _paired_rows(session=session, ticker=TICKER, figi=figi, source_record_id=source_id)
        for session, source_id, figi in zip(sessions, ids, figis, strict=True)
    ]
    bundle = _bundle(tmp_path, [item[0] for item in pairs], [item[1] for item in pairs])
    _allow_test_capability(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "ame_stocks_api.silver.identity_provider_evidence.open_identity_source_bundle",
        lambda root: bundle,
    )
    asset_batch, universe_batch = _batches(bundle)
    assets = attest_provider_rows(
        asset_batch,
        row_indices_in_batch=(0, 1, 2),
        calendar=CALENDAR,
    )
    universes = attest_provider_rows(
        universe_batch,
        row_indices_in_batch=(0, 1, 2),
        calendar=CALENDAR,
    )
    case = _case_object(preview)
    usages = tuple(
        build_s4_bounce_case_evidence_usage(
            case,
            plan=plan,
            preview=preview,
            asset_observation=asset,
            universe_membership=universe,
            calendar=CALENDAR,
        )
        for asset, universe in zip(assets, universes, strict=True)
    )
    attestations = tuple(item for pair in zip(assets, universes, strict=True) for item in pair)
    with pytest.raises(ProviderEvidenceError, match="crosses plan"):
        S4BounceProviderEvidenceManifest(
            plan_id=plan.plan_id,
            plan_sha256=plan.sha256,
            approval_id=approval.approval_id,
            approval_sha256=approval.sha256,
            preview_artifact_id=preview.preview_artifact_id,
            preview_artifact_sha256=preview.sha256,
            case_snapshot=case.to_manifest_dict(),
            row_attestations=attestations,
            usages=(replace(usages[0], plan_id=_digest("f")), *usages[1:]),
            created_at_utc=datetime(2024, 1, 8, 21, tzinfo=UTC),
            manifest_available_session=CALENDAR.sessions[5].session_date,
            availability_calendar_id=CALENDAR.calendar_artifact_id,
            availability_calendar_sha256=CALENDAR.sha256,
        )
    with pytest.raises(ProviderEvidenceError, match=r"standalone.*building is disabled"):
        build_s4_bounce_provider_evidence_manifest(
            data_root=tmp_path,
            plan=plan,
            approval=approval,
            preview=preview,
            case=case,
            attestations=attestations,
            usages=usages,
            created_at_utc=datetime(2024, 1, 2, 1, tzinfo=UTC),
            calendar=CALENDAR,
        )
    with pytest.raises(ProviderEvidenceError, match="cannot be historically backfilled"):
        _issue_runner_evidence_authority(
            data_root=tmp_path,
            bundle=bundle,
            plan=plan,
            approval=approval,
            calendar=CALENDAR,
            created_at_utc=datetime(2024, 1, 5, 21, tzinfo=UTC),
        )


def test_manifest_bytes_and_preview_binding_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, approval, preview, _, sessions, ids, figis = _preview_controls(tmp_path)
    pairs = [
        _paired_rows(session=session, ticker=TICKER, figi=figi, source_record_id=source_id)
        for session, source_id, figi in zip(sessions, ids, figis, strict=True)
    ]
    bundle = _bundle(tmp_path, [item[0] for item in pairs], [item[1] for item in pairs])
    _allow_test_capability(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "ame_stocks_api.silver.identity_provider_evidence.open_identity_source_bundle",
        lambda root: bundle,
    )
    asset_batch, universe_batch = _batches(bundle)
    assets = attest_provider_rows(
        asset_batch,
        row_indices_in_batch=(0, 1, 2),
        calendar=CALENDAR,
    )
    universes = attest_provider_rows(
        universe_batch,
        row_indices_in_batch=(0, 1, 2),
        calendar=CALENDAR,
    )
    case = _case_object(preview)
    usages = tuple(
        build_s4_bounce_case_evidence_usage(
            case,
            plan=plan,
            preview=preview,
            asset_observation=asset,
            universe_membership=universe,
            calendar=CALENDAR,
        )
        for asset, universe in zip(assets, universes, strict=True)
    )
    attestations = tuple(item for pair in zip(assets, universes, strict=True) for item in pair)
    authority = _issue_runner_evidence_authority(
        data_root=tmp_path,
        bundle=bundle,
        plan=plan,
        approval=approval,
        calendar=CALENDAR,
        created_at_utc=EVIDENCE_CREATED_AT,
    )
    manifest = _build_s4_bounce_provider_evidence_manifest_for_runner(
        data_root=tmp_path,
        bundle=bundle,
        plan=plan,
        approval=approval,
        preview=preview,
        case=case,
        attestations=attestations,
        usages=usages,
        calendar=CALENDAR,
        _authority=authority,
    )
    _write_s4_bounce_provider_evidence_manifest_from_official_bundle(
        tmp_path,
        manifest,
        bundle=bundle,
        calendar=CALENDAR,
        _authority=authority,
    )
    with pytest.raises(ProviderEvidenceError, match=r"standalone.*reading is disabled"):
        read_s4_bounce_provider_evidence_manifest(
            tmp_path,
            manifest_id=manifest.manifest_id,
            expected_sha256=_digest("0"),
            plan_id=plan.plan_id,
            expected_plan_sha256=plan.sha256,
            approval_id=approval.approval_id,
            expected_approval_sha256=approval.sha256,
            preview_artifact_id=preview.preview_artifact_id,
            expected_preview_sha256=preview.sha256,
            calendar=CALENDAR,
        )
