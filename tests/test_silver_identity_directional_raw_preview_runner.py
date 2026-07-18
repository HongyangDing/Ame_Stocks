from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pyarrow as pa
import pytest

from ame_stocks_api.artifacts import sha256_file, stable_digest
from ame_stocks_api.silver import identity_directional_raw_preview_runner as runner_module
from ame_stocks_api.silver.contracts import arrow_schema_digest
from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT,
)
from ame_stocks_api.silver.identity_directional_raw_preview_execution_plan import (
    S7DirectionalRawPreviewExecutionSourcePin,
)
from ame_stocks_api.silver.identity_directional_raw_preview_runner import (
    _RUNNER_VERIFIED_CRITICAL_QA_IDS,
    DirectionalRawPreviewEngine,
    DirectionalSourceArtifactRef,
    IdentityDirectionalRawPreviewRunnerError,
    _LoadedControls,
    _preflight_bundle,
    _publish_file_noreplace,
    _read_completion,
    _rename_directory_noreplace,
    _ResourceMonitor,
    _sha256_regular_nofollow,
    _stage_commit_and_complete,
)
from ame_stocks_api.silver.identity_provider_evidence import (
    _CONTRACTS,
    PROVIDER_ROW_ATTESTATION_RULE_VERSION,
    ProviderEvidenceError,
    ProviderRowAttestation,
)
from ame_stocks_api.silver.identity_source import S7_SIX_RELEASE_BINDING_ID, S7_SOURCE_PINS

NOW = datetime(2026, 7, 18, 6, 0, tzinfo=UTC)
CALENDAR_ID = "a" * 64
CALENDAR_SHA = "b" * 64
PLAN_ID = "1" * 64
PLAN_SHA = "2" * 64
APPROVAL_ID = "3" * 64
APPROVAL_SHA = "4" * 64
SCOPE_ID = "5" * 64


def _digest(label: str) -> str:
    return stable_digest({"fixture": label})


def _base_row(table: str, ticker: str, session: date, record: str) -> dict[str, object]:
    contract = _CONTRACTS[table]
    row: dict[str, object] = {}
    for field in contract.arrow_schema:
        if field.nullable:
            row[field.name] = None
        elif pa.types.is_string(field.type):
            row[field.name] = f"fixture-{field.name}"
        elif pa.types.is_boolean(field.type):
            row[field.name] = False
        elif pa.types.is_int64(field.type):
            row[field.name] = 0
        elif pa.types.is_date32(field.type):
            row[field.name] = session.isoformat()
        elif pa.types.is_timestamp(field.type):
            row[field.name] = NOW.isoformat()
        else:  # pragma: no cover - frozen S4 contracts only use these types
            raise AssertionError(field.type)
    row.update(
        {
            "session_year": session.year,
            "session_date": session.isoformat(),
            "ticker": ticker,
            "type_code": "CS",
            "name": f"{ticker} fixture",
            "market": "stocks",
            "locale": "us",
            "primary_exchange_mic": "XNYS",
            "currency_name": "usd",
            "cik": "0000000001",
            "composite_figi": "BBG000000001",
            "share_class_figi": "BBG001000001",
            "delisted_at_utc": None,
            "last_updated_at_utc": NOW.isoformat(),
            "reference_time_scope": "as_observed",
            "metadata_time_scope": "point_in_time",
            "source_available_session": session.isoformat(),
            "source_available_at_utc": NOW.isoformat(),
            "source_availability_quality": "exact",
            "source_request_id": _digest(f"request-{table}-{ticker}-{session}"),
            "source_provider_request_id": f"provider-{ticker}-{session}",
            "source_artifact_sha256": _digest(f"artifact-{table}-{session}"),
            "source_page_sequence": 0,
            "source_row_ordinal": 0,
            "source_row_hash": _digest(f"row-{table}-{record}"),
        }
    )
    if table == "asset_observation_daily":
        row.update(
            {
                "requested_active": True,
                "provider_active": True,
                "source_capture_at_utc": NOW.isoformat(),
                "source_availability_rule": "first_xnys_open_after_source_capture_v1",
                "source_record_id": record,
                "delisted_utc_raw": None,
                "last_updated_utc_raw": NOW.isoformat(),
            }
        )
    else:
        row.update(
            {
                "active_on_date": True,
                "identity_link_status": "strong",
                "selected_source_record_id": record,
                "version_group_id": None,
                "source_version_count": 1,
                "selection_status": "selected_singleton",
                "selection_rule_version": "fixture_v1",
                "active_source_request_id": _digest(f"active-{ticker}-{session}"),
                "inactive_source_request_id": _digest(f"inactive-{ticker}-{session}"),
                "source_pair_id": _digest(f"pair-{ticker}-{session}"),
                "selected_source_capture_at_utc": NOW.isoformat(),
                "universe_capture_completed_at_utc": NOW.isoformat(),
                "source_availability_rule": (
                    "first_xnys_open_after_complete_active_inactive_pair_v1"
                ),
            }
        )
    return row


def _attestation(
    table: str,
    ticker: str,
    session: date,
    *,
    record_label: str,
    composite: str,
    share_class: str,
    row_index: int = 0,
    active: bool = True,
    overrides: dict[str, object] | None = None,
) -> ProviderRowAttestation:
    record = _digest(record_label)
    row = _base_row(table, ticker, session, record)
    row["composite_figi"] = composite
    row["share_class_figi"] = share_class
    if table == "asset_observation_daily":
        row["requested_active"] = active
        row["provider_active"] = active
    else:
        row["active_on_date"] = active
    if overrides:
        row.update(overrides)
    contract = _CONTRACTS[table]
    schema_digest = arrow_schema_digest(contract.arrow_schema)
    full_digest = stable_digest(
        {
            "arrow_schema_digest": schema_digest,
            "namespace": "ame_stocks.identity.provider_full_row",
            "row": row,
            "rule_version": "s7_provider_full_row_digest_v1",
        }
    )
    pin = S7_SOURCE_PINS[table]
    basis_field = (
        "source_capture_at_utc"
        if table == "asset_observation_daily"
        else "universe_capture_completed_at_utc"
    )
    source_field = (
        "source_record_id"
        if table == "asset_observation_daily"
        else "selected_source_record_id"
    )
    return ProviderRowAttestation(
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
        dataset=table,
        release_id=pin.release_id,
        release_manifest_path=f"manifests/silver/releases/release_id={pin.release_id}.json",
        release_manifest_sha256=pin.release_manifest_sha256,
        contract_id=contract.contract_id,
        arrow_schema_digest=schema_digest,
        silver_artifact_path=f"silver/{table}/session_date={session}/part.parquet",
        silver_artifact_sha256=_digest(f"artifact-{table}-{session}"),
        parquet_row_group=0,
        row_index_in_row_group=row_index,
        primary_key={field: row[field] for field in contract.primary_key},
        source_record_id_field=source_field,
        source_record_id=str(row[source_field]),
        source_request_id=str(row["source_request_id"]),
        full_row_digest=full_digest,
        full_row_snapshot=row,
        availability_basis_field=basis_field,
        availability_basis_at_utc=NOW,
        source_available_session=session,
        source_available_at_utc=NOW,
        source_availability_rule=str(row["source_availability_rule"]),
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
        attestation_rule_version=PROVIDER_ROW_ATTESTATION_RULE_VERSION,
    )


def _source_refs() -> tuple[DirectionalSourceArtifactRef, ...]:
    sessions = sorted(
        {
            session
            for _, case_sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
            for session in case_sessions
        }
    )
    result = []
    for table in ("asset_observation_daily", "universe_source_daily"):
        pin = S7_SOURCE_PINS[table]
        contract = _CONTRACTS[table]
        for session in sessions:
            result.append(
                DirectionalSourceArtifactRef(
                    table=table,
                    session_date=session,
                    release_id=pin.release_id,
                    release_manifest_sha256=pin.release_manifest_sha256,
                    path=f"silver/{table}/session_date={session}/part.parquet",
                    sha256=_digest(f"artifact-{table}-{session}"),
                    bytes=100,
                    row_count=10,
                    source_contract_id=contract.contract_id,
                    schema_digest=contract.schema_digest,
                )
            )
    return tuple(result)


def _source_projection_context():
    raw_documents = []
    execution_pins = []
    for ref in sorted(_source_refs()):
        raw_documents.append(
            {
                "bytes": ref.bytes,
                "content_opened": False,
                "disk_is_regular_file": True,
                "disk_is_symlink": False,
                "disk_size_bytes": ref.bytes,
                "media_type": "application/vnd.apache.parquet",
                "path": ref.path,
                "release_id": ref.release_id,
                "release_manifest_sha256": ref.release_manifest_sha256,
                "role": "data",
                "row_count": ref.row_count,
                "session_date": ref.session_date.isoformat(),
                "sha256": ref.sha256,
                "source_contract_id": ref.source_contract_id,
                "source_schema_digest": ref.schema_digest,
                "table": ref.table,
            }
        )
        execution_pins.append(
            S7DirectionalRawPreviewExecutionSourcePin(
                table=ref.table,
                session_date=ref.session_date.isoformat(),
                release_id=ref.release_id,
                release_manifest_sha256=ref.release_manifest_sha256,
                path=ref.path,
                sha256=ref.sha256,
                bytes=ref.bytes,
                row_count=ref.row_count,
                source_contract_id=ref.source_contract_id,
                schema_digest=ref.schema_digest,
            )
        )
    source_binding = {
        "source_artifact_set_digest": stable_digest(raw_documents),
        "source_artifacts": raw_documents,
    }
    plan = SimpleNamespace(
        source_artifact_set_digest=stable_digest(
            [item.to_dict() for item in execution_pins]
        ),
        source_artifacts=tuple(execution_pins),
    )
    return source_binding, plan


def _approved_projection_controls(tmp_path):
    source_binding, plan = _source_projection_context()
    plan.__dict__.update(
        {
            "algorithm_digest": "a" * 64,
            "contract_candidate_sha256": "b" * 64,
            "contract_id": "c" * 64,
            "contract_schema_digest": "d" * 64,
            "execution_data_root": str(tmp_path),
            "input_binding_digest": "e" * 64,
            "inventory_completion_id": "f" * 64,
            "inventory_completion_sha256": "0" * 64,
            "manifest_preflight_intent_id": "1" * 64,
            "manifest_preflight_intent_path": "intent/manifest.json",
            "manifest_preflight_intent_sha256": "2" * 64,
            "plan_id": PLAN_ID,
            "qa_semantics_digest": "3" * 64,
            "resource_caps": SimpleNamespace(digest="4" * 64),
            "runtime_file_set_digest": "5" * 64,
            "scope_set_id": SCOPE_ID,
            "scope_set_sha256": "6" * 64,
            "sha256": PLAN_SHA,
            "source_binding_manifest_id": "7" * 64,
            "source_binding_manifest_path": "source-binding/manifest.json",
            "source_binding_manifest_sha256": "8" * 64,
            "verification_file_set_digest": "9" * 64,
        }
    )
    approval = SimpleNamespace(
        algorithm_digest=plan.algorithm_digest,
        approval_id=APPROVAL_ID,
        approved_at_utc=NOW,
        contract_candidate_sha256=plan.contract_candidate_sha256,
        contract_id=plan.contract_id,
        contract_schema_digest=plan.contract_schema_digest,
        execution_data_root=plan.execution_data_root,
        input_binding_digest=plan.input_binding_digest,
        inventory_completion_id=plan.inventory_completion_id,
        inventory_completion_sha256=plan.inventory_completion_sha256,
        manifest_preflight_intent_id=plan.manifest_preflight_intent_id,
        manifest_preflight_intent_path=plan.manifest_preflight_intent_path,
        manifest_preflight_intent_sha256=plan.manifest_preflight_intent_sha256,
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        qa_semantics_digest=plan.qa_semantics_digest,
        registry_semantics_digest=(
            runner_module.DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
        ),
        request_event_id="a" * 64,
        request_event_sha256="b" * 64,
        resource_caps_digest=plan.resource_caps.digest,
        runtime_file_set_digest=plan.runtime_file_set_digest,
        scope_set_id=plan.scope_set_id,
        scope_set_sha256=plan.scope_set_sha256,
        sha256=APPROVAL_SHA,
        source_artifact_set_digest=plan.source_artifact_set_digest,
        source_binding_manifest_id=plan.source_binding_manifest_id,
        source_binding_manifest_sha256=plan.source_binding_manifest_sha256,
        verification_file_set_digest=plan.verification_file_set_digest,
        preview_execution_authorized=True,
        data_read_authorized=True,
        parquet_read_authorized=True,
        once_to_awaiting_review=True,
        source_discovery_authorized=False,
        caller_scope_override_authorized=False,
        exact_group_history_read_authorized=False,
        network_access_authorized=False,
        external_evidence_capture_authorized=False,
        registry_evaluation_authorized=False,
        adjudication_authorized=False,
        table_materialization_authorized=False,
        full_run_authorized=False,
        publication_authorized=False,
        forced_liquidation_authorized=False,
    )
    source_binding["source_binding_id"] = plan.source_binding_manifest_id
    return source_binding, _LoadedControls(plan=plan, approval=approval, calendar=None)


def test_source_binding_projection_domains_validate_independently() -> None:
    source_binding, plan = _source_projection_context()
    assert source_binding["source_artifact_set_digest"] != plan.source_artifact_set_digest

    runner_module._verify_source_artifact_projection_domains(source_binding, plan)


def test_source_binding_raw_projection_digest_tamper_fails_closed() -> None:
    source_binding, plan = _source_projection_context()
    source_binding["source_artifact_set_digest"] = "f" * 64

    with pytest.raises(
        IdentityDirectionalRawPreviewRunnerError, match="s4_source_binding_invalid"
    ):
        runner_module._verify_source_artifact_projection_domains(source_binding, plan)


def test_source_binding_self_consistent_raw_execution_field_tamper_fails_closed() -> None:
    source_binding, plan = _source_projection_context()
    source_binding["source_artifacts"][0]["sha256"] = "f" * 64
    source_binding["source_artifact_set_digest"] = stable_digest(
        source_binding["source_artifacts"]
    )

    with pytest.raises(
        IdentityDirectionalRawPreviewRunnerError, match="s4_source_binding_invalid"
    ):
        runner_module._verify_source_artifact_projection_domains(source_binding, plan)


def test_source_binding_tamper_stops_before_source_open_or_staging(
    tmp_path, monkeypatch
) -> None:
    source_binding, controls = _approved_projection_controls(tmp_path)
    source_binding["source_artifact_set_digest"] = "f" * 64
    reads = []
    source_open_count = 0

    monkeypatch.setattr(
        runner_module, "_load_controls", lambda *_args, **_kwargs: controls
    )

    def read_bound_json(_root, _relative, _expected_sha256, label):
        reads.append(label)
        return source_binding

    def unexpected_source_open(*_args, **_kwargs):
        nonlocal source_open_count
        source_open_count += 1
        raise AssertionError("source bundle must not open")

    monkeypatch.setattr(runner_module, "_read_bound_json", read_bound_json)
    monkeypatch.setattr(
        runner_module, "_open_exact_source_bundle", unexpected_source_open
    )
    with pytest.raises(
        IdentityDirectionalRawPreviewRunnerError, match="s4_source_binding_invalid"
    ):
        runner_module.run_exact_s7_directional_raw_preview(
            tmp_path,
            plan_id=PLAN_ID,
            expected_plan_sha256=PLAN_SHA,
            approval_id=APPROVAL_ID,
            expected_approval_sha256=APPROVAL_SHA,
        )

    assert reads == ["S4 source binding"]
    assert source_open_count == 0
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("tamper", ["digest", "self_consistent_pin"])
def test_source_binding_execution_projection_tamper_fails_closed(tamper: str) -> None:
    source_binding, plan = _source_projection_context()
    if tamper == "digest":
        plan.source_artifact_set_digest = "f" * 64
    else:
        pins = list(plan.source_artifacts)
        pins[0] = replace(pins[0], sha256="f" * 64)
        plan.source_artifacts = tuple(pins)
        plan.source_artifact_set_digest = stable_digest(
            [item.to_dict() for item in pins]
        )

    with pytest.raises(
        IdentityDirectionalRawPreviewRunnerError, match="s4_source_binding_invalid"
    ):
        runner_module._verify_source_artifact_projection_domains(source_binding, plan)


def _fixture_attestations() -> tuple[ProviderRowAttestation, ...]:
    patterns = {
        "SOR": (
            ("BBG000KMY6N2", "BBG001S5W848"),
            ("BBG01RK6N4M5", "BBG01RK6N5G9"),
            ("BBG01RK6N4M5", "BBG01RK6N5G9"),
        ),
        "XZO": (
            ("BBG01XL8FHT0", "BBG01227MF17"),
            ("BBG01XL8FHT0", "BBG01227MF17"),
            ("BBG01XL8FHT0", "BBG01XL8FJS7"),
            ("BBG01XL8FHT0", "BBG01227MF17"),
        ),
        "ANABV": (
            ("BBG021DMXXT2", "BBG0026ZDHT8"),
            ("BBG021DMXXT2", "BBG021GNPBR6"),
            ("BBG021DMXXT2", "BBG021GNPBR6"),
            ("BBG021DMXXT2", "BBG021GNPBR6"),
        ),
    }
    result: list[ProviderRowAttestation] = []
    for ticker, sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS:
        for ordinal, (session, (composite, share)) in enumerate(
            zip(sessions, patterns[ticker], strict=True)
        ):
            record_label = f"{ticker}-{session}-selected"
            asset = _attestation(
                "asset_observation_daily",
                ticker,
                session,
                record_label=record_label,
                composite=composite,
                share_class=share,
                active=not (ticker == "ANABV" and ordinal == len(sessions) - 1),
            )
            universe_overrides = {
                universe_field: asset.full_row_snapshot[asset_field]
                for asset_field, universe_field in (
                    ("session_year", "session_year"),
                    ("session_date", "session_date"),
                    ("requested_active", "active_on_date"),
                    ("ticker", "ticker"),
                    ("type_code", "type_code"),
                    ("name", "name"),
                    ("market", "market"),
                    ("locale", "locale"),
                    ("primary_exchange_mic", "primary_exchange_mic"),
                    ("currency_name", "currency_name"),
                    ("cik", "cik"),
                    ("composite_figi", "composite_figi"),
                    ("share_class_figi", "share_class_figi"),
                    ("delisted_at_utc", "delisted_at_utc"),
                    ("last_updated_at_utc", "last_updated_at_utc"),
                    ("reference_time_scope", "reference_time_scope"),
                    ("metadata_time_scope", "metadata_time_scope"),
                    ("source_capture_at_utc", "selected_source_capture_at_utc"),
                    ("source_availability_quality", "source_availability_quality"),
                    ("source_record_id", "selected_source_record_id"),
                    ("source_request_id", "source_request_id"),
                    ("source_provider_request_id", "source_provider_request_id"),
                    ("source_artifact_sha256", "source_artifact_sha256"),
                    ("source_page_sequence", "source_page_sequence"),
                    ("source_row_ordinal", "source_row_ordinal"),
                    ("source_row_hash", "source_row_hash"),
                )
            }
            universe = _attestation(
                "universe_source_daily",
                ticker,
                session,
                record_label=record_label,
                composite=composite,
                share_class=share,
                active=bool(asset.full_row_snapshot["provider_active"]),
                overrides=universe_overrides,
            )
            result.extend((asset, universe))
    # One nonselected XZO provider version remains distinct and fully attested.
    session = date(2025, 11, 6)
    result.append(
        _attestation(
            "asset_observation_daily",
            "XZO",
            session,
            record_label="XZO-duplicate-version",
            composite="BBG01XL8FHT0",
            share_class="BBG01227MF17",
            row_index=1,
        )
    )
    return tuple(result)


def _finish(values: tuple[ProviderRowAttestation, ...]):
    engine = DirectionalRawPreviewEngine()
    engine.consume_attestations(values)
    return engine.finish(
        plan_id=PLAN_ID,
        plan_sha256=PLAN_SHA,
        approval_id=APPROVAL_ID,
        approval_sha256=APPROVAL_SHA,
        scope_set_id=SCOPE_ID,
        source_artifacts=_source_refs(),
        calendar=None,
        created_at_utc=NOW,
        runner_verified_critical_numerators={
            check_id: 0 for check_id in _RUNNER_VERIFIED_CRITICAL_QA_IDS
        },
    )


def test_three_case_directional_fixture_preserves_raw_lineage_only() -> None:
    build = _finish(_fixture_attestations())
    assert len(build.slots) == 11
    assert len(build.evidence_manifests) == 3
    by_ticker = {
        case["ticker"]: case for case in build.directional_review["cases"]
    }
    assert [edge["composite_changed"] for edge in by_ticker["SOR"]["sampled_edges"]] == [
        True,
        False,
    ]
    assert [edge["share_class_changed"] for edge in by_ticker["XZO"]["sampled_edges"]] == [
        False,
        True,
        True,
    ]
    assert by_ticker["ANABV"]["observations"][-1]["membership_status"] == (
        "present_inactive"
    )
    assert all(case["exact_effective_interval_proven"] is False for case in by_ticker.values())
    forbidden = {
        "asset_id",
        "canonical_composite_figi",
        "canonical_share_class_figi",
        "disposition",
        "effective_from",
        "effective_to",
    }
    assert not forbidden & set().union(*(set(row) for row in build.slots))
    assert all(row["registry_evaluation_state"] == "not_evaluated" for row in build.slots)
    assert all(row["canonical_candidate_eligible"] is False for row in build.slots)
    pa.Table.from_pylist(
        [dict(row) for row in build.slots],
        schema=IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT.arrow_schema,
    )


def test_missing_membership_is_explicit_and_not_inactive() -> None:
    values = tuple(
        item
        for item in _fixture_attestations()
        if not (
            item.dataset == "universe_source_daily"
            and item.full_row_snapshot["ticker"] == "SOR"
            and item.full_row_snapshot["session_date"] == "2025-01-03"
        )
    )
    build = _finish(values)
    slot = next(
        row
        for row in build.slots
        if row["ticker"] == "SOR" and row["session_date"] == date(2025, 1, 3)
    )
    assert slot["membership_status"] == "absent_source_membership"
    assert slot["active_on_date"] is None
    qa = {item["check_id"]: item for item in build.qa["checks"]}
    assert qa["requested_slot_missing_membership_rows"]["status"] == "warning"
    assert qa["asset_only_scope_rows"]["status"] == "warning"
    sor = next(case for case in build.directional_review["cases"] if case["ticker"] == "SOR")
    missing_edge = sor["sampled_edges"][-1]
    assert missing_edge["composite_comparable"] is False
    assert missing_edge["composite_changed"] is False


def test_nonselected_asset_version_is_retained_and_attested() -> None:
    build = _finish(_fixture_attestations())
    slot = next(
        row
        for row in build.slots
        if row["ticker"] == "XZO" and row["session_date"] == date(2025, 11, 6)
    )
    assert slot["asset_observation_match_count"] == 2
    assert slot["nonselected_asset_observation_count"] == 1
    assert len(json.loads(slot["asset_observation_attestation_ids_json"])) == 2
    manifest = next(
        item for item in build.evidence_manifests if item.review_case["ticker"] == "XZO"
    )
    assert len(manifest.row_attestations) == 9


@pytest.mark.parametrize("fault", ["parent_missing", "parent_multiple", "projection_mismatch"])
def test_selected_parent_reconciliation_fails_closed(fault: str) -> None:
    values = list(_fixture_attestations())
    target_session = "2024-12-31"
    if fault == "parent_missing":
        values = [
            item
            for item in values
            if not (
                item.dataset == "asset_observation_daily"
                and item.full_row_snapshot["ticker"] == "SOR"
                and item.full_row_snapshot["session_date"] == target_session
            )
        ]
    elif fault == "parent_multiple":
        selected = next(
            item
            for item in values
            if item.dataset == "asset_observation_daily"
            and item.full_row_snapshot["ticker"] == "SOR"
            and item.full_row_snapshot["session_date"] == target_session
        )
        values.append(
            _attestation(
                "asset_observation_daily",
                "SOR",
                date.fromisoformat(target_session),
                record_label="SOR-2024-12-31-selected",
                composite=str(selected.full_row_snapshot["composite_figi"]),
                share_class=str(selected.full_row_snapshot["share_class_figi"]),
                row_index=9,
            )
        )
    else:
        values = [
            item
            if not (
                item.dataset == "universe_source_daily"
                and item.full_row_snapshot["ticker"] == "SOR"
                and item.full_row_snapshot["session_date"] == target_session
            )
            else _attestation(
                "universe_source_daily",
                "SOR",
                date.fromisoformat(target_session),
                record_label="SOR-2024-12-31-selected",
                composite="BBG000KMY6N2",
                share_class="BBG001S5W848",
                overrides={"name": "projection mismatch"},
            )
            for item in values
        ]
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="critical"):
        _finish(tuple(values))


def test_scope_leakage_is_rejected_case_sensitively() -> None:
    engine = DirectionalRawPreviewEngine()
    leaked = _attestation(
        "asset_observation_daily",
        "sor",
        date(2024, 12, 31),
        record_label="lowercase-sor",
        composite="BBG000KMY6N2",
        share_class="BBG001S5W848",
    )
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="scope_leakage"):
        engine.consume_attestations((leaked,))


def test_provider_row_tamper_is_rejected_before_engine() -> None:
    good = _fixture_attestations()[0]
    payload = good.to_dict()
    payload["full_row_snapshot"]["composite_figi"] = "BBG999999999"
    with pytest.raises(ProviderEvidenceError, match="digest"):
        ProviderRowAttestation.from_dict(payload)


def test_missing_runner_qa_implementation_fails_closed() -> None:
    engine = DirectionalRawPreviewEngine()
    engine.consume_attestations(_fixture_attestations())
    checks = {check_id: 0 for check_id in _RUNNER_VERIFIED_CRITICAL_QA_IDS}
    checks.pop("row_attestation_replay_invalid_rows")
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="QA map"):
        engine.finish(
            plan_id=PLAN_ID,
            plan_sha256=PLAN_SHA,
            approval_id=APPROVAL_ID,
            approval_sha256=APPROVAL_SHA,
            scope_set_id=SCOPE_ID,
            source_artifacts=_source_refs(),
            calendar=None,
            created_at_utc=NOW,
            runner_verified_critical_numerators=checks,
        )


def _execution_context(tmp_path):
    build = _finish(_fixture_attestations())
    refs = _source_refs()
    source_digest = stable_digest([item.to_dict() for item in sorted(refs)])
    caps = SimpleNamespace(
        batch_size=8_192,
        digest="a" * 64,
        disk_free_floor_bytes=0,
        disk_free_warning_bytes=2**63 - 1,
        expected_physical_artifact_count=22,
        output_bytes_hard_cap=8 * 1024 * 1024,
        output_slot_row_cap=11,
        rss_bytes_hard_cap=2**63 - 1,
        scanned_asset_row_hard_cap=500_000,
        scanned_total_row_hard_cap=1_000_000,
        scanned_universe_row_hard_cap=500_000,
        selected_asset_row_cap=128,
        selected_total_source_row_cap=139,
        selected_universe_row_cap=11,
        source_bytes_hard_cap=256 * 1024 * 1024,
        temporary_bytes_hard_cap=64 * 1024 * 1024,
        wall_clock_seconds_hard_cap=1_800,
    )
    plan = SimpleNamespace(
        algorithm_digest="b" * 64,
        contract_candidate_sha256="c" * 64,
        contract_id="d" * 64,
        contract_schema_digest="e" * 64,
        created_at_utc=NOW,
        created_by="fixture-runner",
        execution_data_root=str(tmp_path),
        inventory_candidate_data_sha256="f" * 64,
        inventory_candidate_id="0" * 64,
        inventory_candidate_manifest_sha256="1" * 64,
        inventory_candidate_path="inventory/candidate.json",
        inventory_completion_id="2" * 64,
        inventory_completion_path="inventory/completion.json",
        inventory_completion_sha256="3" * 64,
        manifest_preflight_approval_id="3" * 64,
        manifest_preflight_approval_sha256="4" * 64,
        manifest_preflight_intent_id="4" * 64,
        manifest_preflight_intent_path="intent/manifest.json",
        manifest_preflight_intent_sha256="5" * 64,
        plan_id=PLAN_ID,
        qa_semantics_digest="6" * 64,
        relative_path=f"plans/plan_id={PLAN_ID}/manifest.json",
        runtime_file_set_digest="7" * 64,
        sha256=PLAN_SHA,
        input_binding_digest="6" * 64,
        source_artifacts=tuple(
            SimpleNamespace(**{**item.to_dict(), "session_date": item.session_date.isoformat()})
            for item in refs
        ),
        source_artifact_set_digest=source_digest,
        source_binding_manifest_id="8" * 64,
        source_binding_manifest_path="source-binding/manifest.json",
        source_binding_manifest_sha256="9" * 64,
        scope_set_id=SCOPE_ID,
        scope_set_sha256="7" * 64,
        verification_file_set_digest="a" * 64,
        resource_caps=caps,
    )
    approval = SimpleNamespace(
        approval_id=APPROVAL_ID,
        approval_literal_sha256="b" * 64,
        approved_by="fixture-approver",
        sha256=APPROVAL_SHA,
        relative_path=f"approvals/approval_id={APPROVAL_ID}/manifest.json",
        request_event_id="8" * 64,
        request_event_path="requests/request.json",
        request_event_sha256="9" * 64,
        approved_at_utc=NOW,
    )
    controls = _LoadedControls(plan=plan, approval=approval, calendar=None)
    staging = tmp_path / "staging"
    staging.mkdir()
    completion_path = tmp_path / (
        "manifests/silver/identity/directional-raw-preview-execution-completions/"
        f"plan_id={PLAN_ID}/approval_id={APPROVAL_ID}/manifest.json"
    )
    import time

    monitor = _ResourceMonitor(
        root=tmp_path,
        staging=staging,
        caps=caps,
        started=time.monotonic(),
    )
    return build, refs, controls, staging, completion_path, monitor


def _patch_local_bundle(monkeypatch) -> None:
    monkeypatch.setattr(
        runner_module,
        "replay_provider_row_attestations_from_official_bundle",
        lambda attestations, **_: tuple(attestations),
    )
    monkeypatch.setattr(
        runner_module, "_open_exact_source_bundle", lambda *args, **kwargs: object()
    )
    monkeypatch.setattr(runner_module, "_preflight_bundle", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        runner_module,
        "_scan_exact_selected_locators",
        lambda _bundle, _refs, **_: tuple(
            item.locator for item in sorted(_fixture_attestations(), key=lambda row: row.locator)
        ),
    )


def test_local_first_run_package_is_atomic_and_strictly_readable(
    tmp_path, monkeypatch
) -> None:
    build, refs, controls, staging, completion_path, monitor = _execution_context(
        tmp_path
    )
    _patch_local_bundle(monkeypatch)
    completion = _stage_commit_and_complete(
        tmp_path,
        controls,
        build,
        source_refs=refs,
        staging=staging,
        completion_path=completion_path,
        created_at_utc=NOW,
        monitor=monitor,
        bundle=object(),
    )
    assert completion_path.read_bytes() == completion.content
    assert completion.candidate_tree_bytes > completion.output_bytes
    assert _read_completion(tmp_path, completion_path, controls) == completion
    candidate_dir = (tmp_path / completion.candidate_path).parent
    slots = candidate_dir / "data/review-slots.parquet"
    slots.write_bytes(slots.read_bytes() + b"tamper")
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="readback_invalid"):
        _read_completion(tmp_path, completion_path, controls)


def test_parquet_and_candidate_directories_are_fsynced_before_rename(
    tmp_path, monkeypatch
) -> None:
    build, refs, controls, staging, completion_path, monitor = _execution_context(
        tmp_path
    )
    _patch_local_bundle(monkeypatch)
    events = []
    original_file_fsync = runner_module._fsync_regular_file
    original_tree_fsync = runner_module._fsync_tree_bottom_up
    original_rename = runner_module._rename_directory_noreplace

    def record_file(path):
        events.append(("file", path.name))
        return original_file_fsync(path)

    def record_tree(path):
        events.append(("tree", path.name))
        return original_tree_fsync(path)

    def record_rename(source, target):
        events.append(("rename", source.name))
        return original_rename(source, target)

    monkeypatch.setattr(runner_module, "_fsync_regular_file", record_file)
    monkeypatch.setattr(runner_module, "_fsync_tree_bottom_up", record_tree)
    monkeypatch.setattr(runner_module, "_rename_directory_noreplace", record_rename)
    _stage_commit_and_complete(
        tmp_path,
        controls,
        build,
        source_refs=refs,
        staging=staging,
        completion_path=completion_path,
        created_at_utc=NOW,
        monitor=monitor,
        bundle=object(),
    )
    assert events.index(("file", "review-slots.parquet")) < events.index(
        ("tree", "candidate")
    ) < events.index(("rename", "candidate"))


def test_fsync_failure_preserves_staging_and_prevents_irreversible_commit(
    tmp_path, monkeypatch
) -> None:
    build, refs, controls, staging, completion_path, monitor = _execution_context(
        tmp_path
    )
    _patch_local_bundle(monkeypatch)
    renamed = False

    def fail_fsync(_path):
        raise IdentityDirectionalRawPreviewRunnerError("injected fsync failure")

    def unexpected_rename(*_args):
        nonlocal renamed
        renamed = True

    monkeypatch.setattr(runner_module, "_fsync_tree_bottom_up", fail_fsync)
    monkeypatch.setattr(runner_module, "_rename_directory_noreplace", unexpected_rename)
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="injected fsync"):
        _stage_commit_and_complete(
            tmp_path,
            controls,
            build,
            source_refs=refs,
            staging=staging,
            completion_path=completion_path,
            created_at_utc=NOW,
            monitor=monitor,
            bundle=object(),
        )
    assert renamed is False
    assert (staging / "candidate").is_dir()
    assert not completion_path.exists()


def test_completion_link_failure_recovers_only_after_full_exact_validation(
    tmp_path, monkeypatch
) -> None:
    build, refs, controls, staging, completion_path, monitor = _execution_context(
        tmp_path
    )
    _patch_local_bundle(monkeypatch)
    original_publish = runner_module._publish_file_noreplace
    monkeypatch.setattr(
        runner_module,
        "_publish_file_noreplace",
        lambda *_: (_ for _ in ()).throw(
            IdentityDirectionalRawPreviewRunnerError("injected link failure")
        ),
    )
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="link failure"):
        _stage_commit_and_complete(
            tmp_path,
            controls,
            build,
            source_refs=refs,
            staging=staging,
            completion_path=completion_path,
            created_at_utc=NOW,
            monitor=monitor,
            bundle=object(),
        )
    assert (staging / "completion.json").is_file()
    assert not (staging / "candidate").exists()
    assert not completion_path.exists()
    monkeypatch.setattr(runner_module, "_publish_file_noreplace", original_publish)
    completion = runner_module._recover_after_candidate_commit(
        tmp_path,
        controls,
        staging=staging,
        completion_path=completion_path,
    )
    assert completion_path.read_bytes() == completion.content


def test_idempotent_readback_rejects_self_consistent_control_forgery(
    tmp_path, monkeypatch
) -> None:
    build, refs, controls, staging, completion_path, monitor = _execution_context(
        tmp_path
    )
    _patch_local_bundle(monkeypatch)
    _stage_commit_and_complete(
        tmp_path,
        controls,
        build,
        source_refs=refs,
        staging=staging,
        completion_path=completion_path,
        created_at_utc=NOW,
        monitor=monitor,
        bundle=object(),
    )
    document = json.loads(completion_path.read_bytes())
    document["control_binding"]["inventory"]["candidate_id"] = "f" * 64
    document.pop("completion_id")
    document["completion_id"] = stable_digest(document)
    completion_path.write_bytes(
        json.dumps(document, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="exact controls"):
        _read_completion(tmp_path, completion_path, controls)


def test_idempotent_readback_replays_all_evidence_against_official_bundle(
    tmp_path, monkeypatch
) -> None:
    build, refs, controls, staging, completion_path, monitor = _execution_context(
        tmp_path
    )
    _patch_local_bundle(monkeypatch)
    _stage_commit_and_complete(
        tmp_path,
        controls,
        build,
        source_refs=refs,
        staging=staging,
        completion_path=completion_path,
        created_at_utc=NOW,
        monitor=monitor,
        bundle=object(),
    )
    monkeypatch.setattr(
        runner_module,
        "replay_provider_row_attestations_from_official_bundle",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ProviderEvidenceError("injected official replay mismatch")
        ),
    )
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="evidence replay"):
        _read_completion(tmp_path, completion_path, controls)


def test_semantic_readback_rejects_self_consistent_evidence_omission(
    tmp_path, monkeypatch
) -> None:
    build, refs, controls, staging, completion_path, monitor = _execution_context(
        tmp_path
    )
    _patch_local_bundle(monkeypatch)
    complete = tuple(
        item.locator for item in sorted(_fixture_attestations(), key=lambda row: row.locator)
    )
    monkeypatch.setattr(
        runner_module,
        "_scan_exact_selected_locators",
        lambda *_args, **_kwargs: (
            *complete,
            ("asset_observation_daily", "f" * 64, "silver/extra.parquet", 0, 0),
        ),
    )
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="evidence omission"):
        _stage_commit_and_complete(
            tmp_path,
            controls,
            build,
            source_refs=refs,
            staging=staging,
            completion_path=completion_path,
            created_at_utc=NOW,
            monitor=monitor,
            bundle=object(),
        )
    assert not completion_path.exists()


def test_candidate_and_completion_publication_never_clobber(tmp_path) -> None:
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    target.mkdir()
    (source / "ours").write_text("ours")
    (target / "foreign").write_text("foreign")
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="already exists"):
        _rename_directory_noreplace(source, target)
    assert (source / "ours").read_text() == "ours"
    assert (target / "foreign").read_text() == "foreign"

    staged = tmp_path / "staged-completion.json"
    final = tmp_path / "final-completion.json"
    staged.write_bytes(b"complete\n")
    final.write_bytes(b"foreign-partial")
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="already exists"):
        _publish_file_noreplace(staged, final)
    assert staged.read_bytes() == b"complete\n"
    assert final.read_bytes() == b"foreign-partial"


def test_independent_selected_row_caps_fail_closed() -> None:
    engine = DirectionalRawPreviewEngine(selected_universe_row_cap=10)
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="universe row cap"):
        engine.consume_attestations(_fixture_attestations())


def test_second_physical_preflight_detects_source_mutation(tmp_path) -> None:
    refs = []
    artifacts_by_table = {"asset_observation_daily": [], "universe_source_daily": []}
    sessions = sorted(
        {
            session
            for _, case_sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
            for session in case_sessions
        }
    )
    for table in artifacts_by_table:
        pin = S7_SOURCE_PINS[table]
        contract = _CONTRACTS[table]
        for session in sessions:
            path = tmp_path / f"{table}-{session}.parquet"
            path.write_bytes(f"{table}-{session}".encode())
            digest = sha256_file(path)
            relative = f"silver/{table}/session_date={session}/part.parquet"
            ref = DirectionalSourceArtifactRef(
                table=table,
                session_date=session,
                release_id=pin.release_id,
                release_manifest_sha256=pin.release_manifest_sha256,
                path=relative,
                sha256=digest,
                bytes=path.stat().st_size,
                row_count=1,
                source_contract_id=contract.contract_id,
                schema_digest=contract.schema_digest,
            )
            refs.append(ref)
            artifacts_by_table[table].append(
                SimpleNamespace(
                    ref=SimpleNamespace(
                        path=relative,
                        sha256=digest,
                        bytes=path.stat().st_size,
                        row_count=1,
                        schema_digest=contract.schema_digest,
                    ),
                    path=path,
                    release_id=pin.release_id,
                    release_manifest_sha256=pin.release_manifest_sha256,
                )
            )
    bundle = SimpleNamespace(
        require_official=lambda: None,
        daily_partition_artifacts=lambda table, requested: tuple(
            artifacts_by_table[table]
        ),
        sources={
            table: SimpleNamespace(
                published=SimpleNamespace(
                    contract=SimpleNamespace(contract_id=_CONTRACTS[table].contract_id)
                )
            )
            for table in artifacts_by_table
        },
    )
    _preflight_bundle(bundle, tuple(refs))
    artifacts_by_table["asset_observation_daily"][0].path.write_bytes(b"mutated")
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="integrity"):
        _preflight_bundle(bundle, tuple(refs))


def test_source_hash_rejects_symlink_and_path_swap(tmp_path, monkeypatch) -> None:
    import os

    real = tmp_path / "real"
    other = tmp_path / "other"
    link = tmp_path / "link"
    real.write_bytes(b"source")
    other.write_bytes(b"other!")
    link.symlink_to(real)
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="following links"):
        _sha256_regular_nofollow(link, expected_size=real.stat().st_size)

    original_lstat = os.lstat

    def swapped_lstat(path):
        return original_lstat(other if path == real else path)

    monkeypatch.setattr(os, "lstat", swapped_lstat)
    with pytest.raises(IdentityDirectionalRawPreviewRunnerError, match="path changed"):
        _sha256_regular_nofollow(real, expected_size=real.stat().st_size)
