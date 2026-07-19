from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ame_stocks_api.silver.identity_registry_exact_group_scopes as scopes_module
from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.calendar_artifact import build_xnys_calendar_artifact
from ame_stocks_api.silver.identity_exact_group_history_contract import (
    EXACT_GROUP_HISTORY_FIXED_COMPOSITES,
    EXACT_GROUP_HISTORY_S4_SOURCE_ARTIFACT_COUNT,
    EXACT_GROUP_HISTORY_S4_SOURCE_BYTES,
    EXACT_GROUP_HISTORY_S4_SOURCE_ROW_COUNT,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT,
)
from ame_stocks_api.silver.identity_exact_group_history_runner import (
    ASSET_TABLE,
    EXAMPLES_FILENAME,
    MANIFEST_FILENAME,
    QA_FILENAME,
    SEQUENCES_FILENAME,
    SLOTS_FILENAME,
    ExactGroupHistoryEngine,
    ExactGroupHistoryOutputRef,
    S7ExactGroupHistoryCandidate,
    S7ExactGroupHistoryCompletion,
    exact_group_history_completion_path,
)
from ame_stocks_api.silver.identity_market_inventory_engine import (
    UNIVERSE_PARENT_PROJECTION,
)
from ame_stocks_api.silver.identity_provider_evidence import (
    PROVIDER_ROW_ATTESTATION_RULE_VERSION,
    ProviderRowAttestation,
)
from ame_stocks_api.silver.identity_registry_exact_group_scopes import (
    ANABV_CONTAMINATED_SHARE_CLASS,
    ANABV_SCOPE_SESSIONS,
    SOR_OLD_COMPOSITE,
    SOR_OLD_SHARE_CLASS,
    SOR_PREDECESSOR_SESSION,
    SOR_SUCCESSOR_SESSION,
    SOR_SUCCESSOR_SHARE_CLASS,
    XZO_CONTAMINATED_SHARE_CLASS,
    XZO_SCOPE_SESSIONS,
    IdentityRegistryExactGroupScopeError,
    load_identity_registry_exact_group_scopes,
)
from ame_stocks_api.silver.identity_source import S7_SIX_RELEASE_BINDING_ID, S7_SOURCE_PINS

NOW = datetime(2026, 7, 20, 12, tzinfo=UTC)


def _digest(label: str) -> str:
    return stable_digest({"fixture": label})


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )


def _blank_row(table: str, session: date) -> dict[str, object]:
    result: dict[str, object] = {}
    for field in ASSET_CONTRACTS[table].arrow_schema:
        if field.nullable:
            result[field.name] = None
        elif pa.types.is_string(field.type):
            result[field.name] = f"fixture-{field.name}"
        elif pa.types.is_boolean(field.type):
            result[field.name] = False
        elif pa.types.is_int64(field.type):
            result[field.name] = 0
        elif pa.types.is_date32(field.type):
            result[field.name] = session.isoformat()
        elif pa.types.is_timestamp(field.type):
            result[field.name] = NOW.isoformat()
        else:  # pragma: no cover - contracts pin the supported primitive set
            raise AssertionError(field.type)
    return result


def _attestation(
    table: str,
    row: dict[str, object],
    *,
    ordinal: int,
    source_record_id: str,
) -> ProviderRowAttestation:
    contract = ASSET_CONTRACTS[table]
    release = S7_SOURCE_PINS[table]
    full_row_digest = stable_digest(
        {
            "arrow_schema_digest": contract.schema_digest,
            "namespace": "ame_stocks.identity.provider_full_row",
            "row": row,
            "rule_version": "s7_provider_full_row_digest_v1",
        }
    )
    session = date.fromisoformat(str(row["session_date"]))
    is_asset = table == ASSET_TABLE
    return ProviderRowAttestation(
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
        dataset=table,
        release_id=release.release_id,
        release_manifest_path=f"manifests/silver/releases/release_id={release.release_id}.json",
        release_manifest_sha256=release.release_manifest_sha256,
        contract_id=contract.contract_id,
        arrow_schema_digest=contract.schema_digest,
        silver_artifact_path=f"silver/{table}/session_date={session}/part.parquet",
        silver_artifact_sha256=_digest(f"artifact-{table}-{session}"),
        parquet_row_group=0,
        row_index_in_row_group=ordinal,
        primary_key={field: row[field] for field in contract.primary_key},
        source_record_id_field=("source_record_id" if is_asset else "selected_source_record_id"),
        source_record_id=source_record_id,
        source_request_id=str(row["source_request_id"]),
        full_row_digest=full_row_digest,
        full_row_snapshot=row,
        availability_basis_field=(
            "source_capture_at_utc" if is_asset else "universe_capture_completed_at_utc"
        ),
        availability_basis_at_utc=NOW,
        source_available_session=session,
        source_available_at_utc=NOW,
        source_availability_rule=(
            "first_xnys_open_after_source_capture_v1"
            if is_asset
            else "first_xnys_open_after_complete_active_inactive_pair_v1"
        ),
        availability_calendar_id="a" * 64,
        availability_calendar_sha256="b" * 64,
        attestation_rule_version=PROVIDER_ROW_ATTESTATION_RULE_VERSION,
    )


def _source_pair(
    ticker: str,
    composite: str,
    share_class: str,
    *,
    session: date,
    ordinal: int,
) -> tuple[ProviderRowAttestation, ProviderRowAttestation]:
    asset = _blank_row(ASSET_TABLE, session)
    record_id = _digest(f"record-{ticker}-{session}")
    asset.update(
        {
            "session_year": session.year,
            "session_date": session.isoformat(),
            "requested_active": True,
            "provider_active": True,
            "ticker": ticker,
            "type_code": "CS",
            "name": ticker,
            "market": "stocks",
            "locale": "us",
            "primary_exchange_mic": "XNYS",
            "currency_name": "usd",
            "cik": None,
            "composite_figi": composite,
            "share_class_figi": share_class,
            "reference_time_scope": "as_observed",
            "metadata_time_scope": "point_in_time",
            "source_capture_at_utc": NOW.isoformat(),
            "source_available_session": session.isoformat(),
            "source_available_at_utc": NOW.isoformat(),
            "source_availability_rule": "first_xnys_open_after_source_capture_v1",
            "source_availability_quality": "exact",
            "source_record_id": record_id,
            "source_request_id": _digest(f"request-{ticker}-{session}"),
            "source_provider_request_id": f"provider-{ticker}-{session}",
            "source_artifact_sha256": _digest(f"artifact-{ASSET_TABLE}-{session}"),
            "source_page_sequence": 0,
            "source_row_ordinal": ordinal,
            "source_row_hash": _digest(f"row-{ticker}-{session}"),
        }
    )
    asset_attestation = _attestation(
        ASSET_TABLE, asset, ordinal=ordinal, source_record_id=record_id
    )
    universe = _blank_row("universe_source_daily", session)
    for asset_field, universe_field in UNIVERSE_PARENT_PROJECTION:
        universe[universe_field] = asset[asset_field]
    universe.update(
        {
            "identity_link_status": "linked",
            "version_group_id": None,
            "source_version_count": 1,
            "selection_status": "selected",
            "selection_rule_version": "fixture-v1",
            "active_source_request_id": str(asset["source_request_id"]),
            "inactive_source_request_id": _digest(f"inactive-{ticker}-{session}"),
            "source_pair_id": _digest(f"pair-{ticker}-{session}"),
            "universe_capture_completed_at_utc": NOW.isoformat(),
            "source_available_session": session.isoformat(),
            "source_available_at_utc": NOW.isoformat(),
            "source_availability_rule": ("first_xnys_open_after_complete_active_inactive_pair_v1"),
        }
    )
    universe_attestation = _attestation(
        "universe_source_daily",
        universe,
        ordinal=ordinal,
        source_record_id=record_id,
    )
    return asset_attestation, universe_attestation


@dataclass(frozen=True)
class _Pin:
    artifact_id: str
    path: str
    sha256: str
    bytes: int


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _ref(directory: Path, role: str, relative: str, row_count: int | None = None):
    path = directory / relative
    content = path.read_bytes()
    return ExactGroupHistoryOutputRef(
        role=role,
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
        media_type=(
            "application/vnd.apache.parquet"
            if relative.endswith(".parquet")
            else "application/json"
        ),
        row_count=row_count,
    )


def _build_fixture(
    root: Path,
    *,
    wrong_xzo_share_outside: bool = False,
    corrupt_parent_projection: bool = False,
    source_count_delta: int = 0,
) -> tuple[_Pin, _Pin]:
    calendar = build_xnys_calendar_artifact(SOR_PREDECESSOR_SESSION, date(2026, 7, 9))
    override_sessions = tuple(
        item.session_date
        for item in calendar.sessions
        if item.session_date >= SOR_SUCCESSOR_SESSION
    )
    assert len(override_sessions) == 379
    engine = ExactGroupHistoryEngine()
    ordinal = 0
    sor_sessions = (SOR_PREDECESSOR_SESSION, *override_sessions)
    for session in sor_sessions:
        share = (
            SOR_OLD_SHARE_CLASS if session == SOR_PREDECESSOR_SESSION else SOR_SUCCESSOR_SHARE_CLASS
        )
        asset, universe = _source_pair(
            "SOR", SOR_OLD_COMPOSITE, share, session=session, ordinal=ordinal
        )
        engine.consume_session(
            session, asset_attestations=(asset,), universe_attestations=(universe,)
        )
        ordinal += 1
    for session in (*XZO_SCOPE_SESSIONS, date(2025, 11, 6)):
        share = (
            XZO_CONTAMINATED_SHARE_CLASS
            if session in XZO_SCOPE_SESSIONS or wrong_xzo_share_outside
            else "BBG01227MF17"
        )
        asset, universe = _source_pair(
            "XZO",
            EXACT_GROUP_HISTORY_FIXED_COMPOSITES["XZO"],
            share,
            session=session,
            ordinal=ordinal,
        )
        engine.consume_session(
            session, asset_attestations=(asset,), universe_attestations=(universe,)
        )
        ordinal += 1
    for session in (*ANABV_SCOPE_SESSIONS, date(2026, 4, 7)):
        share = (
            ANABV_CONTAMINATED_SHARE_CLASS if session in ANABV_SCOPE_SESSIONS else "BBG021GNPBR6"
        )
        asset, universe = _source_pair(
            "ANABV",
            EXACT_GROUP_HISTORY_FIXED_COMPOSITES["ANABV"],
            share,
            session=session,
            ordinal=ordinal,
        )
        engine.consume_session(
            session, asset_attestations=(asset,), universe_attestations=(universe,)
        )
        ordinal += 1
    plan = SimpleNamespace(
        plan_id="1" * 64,
        sha256="2" * 64,
        scope_set_id="3" * 64,
        source_artifact_set_digest="4" * 64,
        normalized_source_artifact_set_digest="5" * 64,
        inventory_completion_id="6" * 64,
        directional_preview_candidate_id="7" * 64,
        directional_preview_completion_id="8" * 64,
        source_binding_id="9" * 64,
        source_binding_sha256="a" * 64,
    )
    approval = SimpleNamespace(
        approval_id="b" * 64,
        sha256="c" * 64,
        request_event_id="d" * 64,
        request_event_sha256="e" * 64,
    )
    intent = SimpleNamespace(
        intent_id="f" * 64,
        sha256="0" * 64,
        relative_path="manifests/silver/identity/exact-group-history-intents/intent.json",
    )
    build = engine.finish(
        plan=plan,
        approval=approval,
        intent=intent,
        calendar=calendar,
        created_at_utc=NOW,
        runner_verified_critical={},
    )
    rows = [dict(item) for item in build.slots]
    if corrupt_parent_projection:
        row = next(
            item
            for item in rows
            if item["ticker"] == "XZO" and item["session_date"] == XZO_SCOPE_SESSIONS[0]
        )
        row["selected_parent_observed_composite_figi"] = "BBG000000001"

    staging = root / "staging"
    table = pa.Table.from_pylist(
        rows, schema=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.arrow_schema
    )
    (staging / SLOTS_FILENAME).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, staging / SLOTS_FILENAME, compression="zstd")
    _write(staging / SEQUENCES_FILENAME, _canonical(dict(build.sequences)))
    _write(staging / QA_FILENAME, _canonical(dict(build.qa)))
    _write(staging / EXAMPLES_FILENAME, _canonical(dict(build.examples)))
    for evidence in build.evidence_manifests:
        _write(staging / evidence.candidate_relative_path, evidence.content)
    refs = (
        _ref(staging, "review_slots", SLOTS_FILENAME, len(rows)),
        _ref(staging, "group_sequences", SEQUENCES_FILENAME),
        _ref(staging, "qa", QA_FILENAME),
        _ref(staging, "bounded_examples", EXAMPLES_FILENAME),
        *(
            _ref(
                staging,
                f"group_evidence:{evidence.ticker}",
                evidence.candidate_relative_path,
            )
            for evidence in build.evidence_manifests
        ),
    )
    candidate = S7ExactGroupHistoryCandidate(
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        approval_id=approval.approval_id,
        approval_sha256=approval.sha256,
        request_event_id=approval.request_event_id,
        request_event_sha256=approval.request_event_sha256,
        execution_intent_id=intent.intent_id,
        execution_intent_path=intent.relative_path,
        execution_intent_sha256=intent.sha256,
        source_binding_id=plan.source_binding_id,
        source_binding_sha256=plan.source_binding_sha256,
        source_artifact_set_digest=plan.source_artifact_set_digest,
        normalized_source_artifact_set_digest=plan.normalized_source_artifact_set_digest,
        review_scope_set_id=plan.scope_set_id,
        artifacts=refs,
        evidence_manifest_ids=tuple(evidence.manifest_id for evidence in build.evidence_manifests),
        created_at_utc=NOW,
    )
    final = root / candidate.relative_directory
    final.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(staging, final)
    _write(final / MANIFEST_FILENAME, candidate.content)
    output_bytes = sum(path.stat().st_size for path in final.rglob("*") if path.is_file())
    completion = S7ExactGroupHistoryCompletion(
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        approval_id=approval.approval_id,
        approval_sha256=approval.sha256,
        request_event_id=approval.request_event_id,
        request_event_sha256=approval.request_event_sha256,
        execution_intent_id=intent.intent_id,
        execution_intent_path=intent.relative_path,
        execution_intent_sha256=intent.sha256,
        candidate_id=candidate.candidate_id,
        candidate_path=f"{candidate.relative_directory}/{MANIFEST_FILENAME}",
        candidate_sha256=candidate.sha256,
        output_artifacts=candidate.artifacts,
        completed_at_utc=NOW,
        source_artifact_count=(EXACT_GROUP_HISTORY_S4_SOURCE_ARTIFACT_COUNT + source_count_delta),
        source_row_count=EXACT_GROUP_HISTORY_S4_SOURCE_ROW_COUNT,
        source_bytes=EXACT_GROUP_HISTORY_S4_SOURCE_BYTES,
        output_slot_row_count=len(rows),
        peak_rss_bytes=1,
        wall_clock_seconds=1.0,
        output_bytes=output_bytes,
    )
    completion_relative = exact_group_history_completion_path(plan.plan_id, approval.approval_id)
    _write(root / completion_relative, completion.content)
    return (
        _Pin(
            artifact_id=candidate.candidate_id,
            path=f"{candidate.relative_directory}/{MANIFEST_FILENAME}",
            sha256=candidate.sha256,
            bytes=len(candidate.content),
        ),
        _Pin(
            artifact_id=completion.completion_id,
            path=completion_relative,
            sha256=completion.sha256,
            bytes=len(completion.content),
        ),
    )


def test_loader_replays_fixed_exact_group_scopes(tmp_path: Path) -> None:
    candidate, completion = _build_fixture(tmp_path)
    loaded = load_identity_registry_exact_group_scopes(
        tmp_path, candidate_pin=candidate, completion_pin=completion
    )
    transition = loaded.require_scope("asset_transition:SOR")
    override = loaded.require_scope("provider_composite_override:SOR")
    xzo = loaded.require_scope("share_class_adjudication:XZO")
    anabv = loaded.require_scope("share_class_adjudication:ANABV")
    assert [row.session_date for row in transition.rows] == [
        SOR_PREDECESSOR_SESSION,
        SOR_SUCCESSOR_SESSION,
    ]
    assert [row.observed_share_class_figi for row in transition.rows] == [
        SOR_OLD_SHARE_CLASS,
        SOR_SUCCESSOR_SHARE_CLASS,
    ]
    assert len(override.rows) == 379
    assert override.rows[0].session_date == SOR_SUCCESSOR_SESSION
    assert override.rows[-1].session_date == date(2026, 7, 9)
    assert {row.observed_composite_figi for row in override.rows} == {SOR_OLD_COMPOSITE}
    assert len(xzo.rows) == 2
    assert {row.observed_share_class_figi for row in xzo.rows} == {XZO_CONTAMINATED_SHARE_CLASS}
    assert len(anabv.rows) == 1
    assert anabv.rows[0].observed_share_class_figi == ANABV_CONTAMINATED_SHARE_CLASS
    assert transition.to_dict()["scope_digest"] == transition.scope_digest
    with pytest.raises(IdentityRegistryExactGroupScopeError, match="outside the four"):
        loaded.require_scope("share_class_adjudication:ANAB")


def test_loader_rejects_pinned_output_mutation(tmp_path: Path) -> None:
    candidate, completion = _build_fixture(tmp_path)
    slots = tmp_path / Path(candidate.path).parent / SLOTS_FILENAME
    slots.write_bytes(slots.read_bytes() + b"mutation")
    with pytest.raises(IdentityRegistryExactGroupScopeError, match="hash or bytes"):
        load_identity_registry_exact_group_scopes(
            tmp_path, candidate_pin=candidate, completion_pin=completion
        )


def test_slot_loader_revalidates_and_parses_only_pinned_bytes(tmp_path: Path) -> None:
    candidate_pin, completion_pin = _build_fixture(tmp_path)
    candidate = S7ExactGroupHistoryCandidate.from_dict(
        json.loads((tmp_path / candidate_pin.path).read_bytes())
    )
    completion = S7ExactGroupHistoryCompletion.from_dict(
        json.loads((tmp_path / completion_pin.path).read_bytes())
    )
    directory = tmp_path / Path(candidate_pin.path).parent
    slots_ref = next(item for item in candidate.artifacts if item.role == "review_slots")
    slots_path = directory / slots_ref.path

    # Simulate a path replacement after an earlier receipt verification.  The
    # replacement is still valid Parquet with identical semantics, so a loader
    # that reopens the path without replaying the pinned bytes would accept it.
    scopes_module._read_output_ref(directory, slots_ref)
    table = pq.read_table(slots_path)
    pq.write_table(table, slots_path, compression="snappy")
    assert hashlib.sha256(slots_path.read_bytes()).hexdigest() != slots_ref.sha256

    with pytest.raises(IdentityRegistryExactGroupScopeError, match="hash or bytes"):
        scopes_module._load_and_validate_slots(directory, slots_ref, completion)


def test_loader_rejects_semantically_rehashed_parent_mismatch(tmp_path: Path) -> None:
    candidate, completion = _build_fixture(tmp_path, corrupt_parent_projection=True)
    with pytest.raises(IdentityRegistryExactGroupScopeError, match="selected-parent"):
        load_identity_registry_exact_group_scopes(
            tmp_path, candidate_pin=candidate, completion_pin=completion
        )


def test_loader_rejects_wrong_share_outside_exact_scope(tmp_path: Path) -> None:
    candidate, completion = _build_fixture(tmp_path, wrong_xzo_share_outside=True)
    with pytest.raises(IdentityRegistryExactGroupScopeError, match="outside its exact scope"):
        load_identity_registry_exact_group_scopes(
            tmp_path, candidate_pin=candidate, completion_pin=completion
        )


def test_loader_rejects_mutated_frozen_source_totals(tmp_path: Path) -> None:
    candidate, completion = _build_fixture(tmp_path, source_count_delta=-1)
    with pytest.raises(IdentityRegistryExactGroupScopeError, match="source totals"):
        load_identity_registry_exact_group_scopes(
            tmp_path, candidate_pin=candidate, completion_pin=completion
        )
