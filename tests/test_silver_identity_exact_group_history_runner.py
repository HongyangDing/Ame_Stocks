from __future__ import annotations

import time
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pyarrow as pa
import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import identity_exact_group_history_runner as runner_module
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.identity_exact_group_history_contract import (
    EXACT_GROUP_HISTORY_FIXED_GROUPS,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT,
)
from ame_stocks_api.silver.identity_exact_group_history_runner import (
    ExactGroupHistoryEngine,
    IdentityExactGroupHistoryRunnerError,
    _plan_cap,
)
from ame_stocks_api.silver.identity_provider_evidence import (
    PROVIDER_ROW_ATTESTATION_RULE_VERSION,
    ProviderRowAttestation,
)
from ame_stocks_api.silver.identity_source import S7_SIX_RELEASE_BINDING_ID, S7_SOURCE_PINS

NOW = datetime(2026, 7, 1, 20, 0, tzinfo=UTC)


def _digest(label: str) -> str:
    return stable_digest({"fixture": label})


def _asset_attestation(
    ticker: str,
    composite: str,
    share_class: str,
    *,
    session: date,
    row_index: int,
) -> ProviderRowAttestation:
    table = "asset_observation_daily"
    contract = ASSET_CONTRACTS[table]
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
        else:  # pragma: no cover
            raise AssertionError(field.type)
    record = _digest(f"record-{ticker}-{share_class}-{row_index}")
    row.update(
        {
            "session_year": session.year,
            "session_date": session.isoformat(),
            "source_record_id": record,
            "ticker": ticker,
            "requested_active": True,
            "provider_active": True,
            "type_code": "CS",
            "name": ticker,
            "market": "stocks",
            "locale": "us",
            "primary_exchange_mic": "XNYS",
            "currency_name": "usd",
            "composite_figi": composite,
            "share_class_figi": share_class,
            "reference_time_scope": "as_observed",
            "metadata_time_scope": "point_in_time",
            "source_capture_at_utc": NOW.isoformat(),
            "source_available_session": session.isoformat(),
            "source_available_at_utc": NOW.isoformat(),
            "source_availability_quality": "exact",
            "source_availability_rule": "first_xnys_open_after_source_capture_v1",
            "source_request_id": _digest(f"request-{ticker}-{row_index}"),
            "source_provider_request_id": f"provider-{ticker}-{row_index}",
            "source_artifact_sha256": _digest(f"artifact-{session}"),
            "source_page_sequence": 0,
            "source_row_ordinal": row_index,
            "source_row_hash": _digest(f"row-{ticker}-{row_index}"),
        }
    )
    full_digest = stable_digest(
        {
            "arrow_schema_digest": contract.schema_digest,
            "namespace": "ame_stocks.identity.provider_full_row",
            "row": row,
            "rule_version": "s7_provider_full_row_digest_v1",
        }
    )
    pin = S7_SOURCE_PINS[table]
    return ProviderRowAttestation(
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
        dataset=table,
        release_id=pin.release_id,
        release_manifest_path=f"manifests/silver/releases/release_id={pin.release_id}.json",
        release_manifest_sha256=pin.release_manifest_sha256,
        contract_id=contract.contract_id,
        arrow_schema_digest=contract.schema_digest,
        silver_artifact_path=f"silver/{table}/session_date={session}/part.parquet",
        silver_artifact_sha256=_digest(f"artifact-{session}"),
        parquet_row_group=0,
        row_index_in_row_group=row_index,
        primary_key={field: row[field] for field in contract.primary_key},
        source_record_id_field="source_record_id",
        source_record_id=record,
        source_request_id=str(row["source_request_id"]),
        full_row_digest=full_digest,
        full_row_snapshot=row,
        availability_basis_field="source_capture_at_utc",
        availability_basis_at_utc=NOW,
        source_available_session=session,
        source_available_at_utc=NOW,
        source_availability_rule="first_xnys_open_after_source_capture_v1",
        availability_calendar_id="a" * 64,
        availability_calendar_sha256="b" * 64,
        attestation_rule_version=PROVIDER_ROW_ATTESTATION_RULE_VERSION,
    )


def test_engine_never_uses_share_class_as_filter() -> None:
    ticker, composite = EXACT_GROUP_HISTORY_FIXED_GROUPS[0]
    session = date(2025, 1, 2)
    first = _asset_attestation(ticker, composite, "BBG001S5W848", session=session, row_index=0)
    second = _asset_attestation(ticker, composite, "BBG01RK6N5G9", session=session, row_index=1)
    engine = ExactGroupHistoryEngine()
    assert (
        engine.consume_session(
            session, asset_attestations=[first, second], universe_attestations=[]
        )
        == 2
    )
    assert {item.row_attestation_id for item in engine.retained_attestations} == {
        first.row_attestation_id,
        second.row_attestation_id,
    }


def test_engine_rejects_out_of_scope_ticker() -> None:
    session = date(2025, 1, 2)
    row = _asset_attestation("OTHER", "BBG000KMY6N2", "BBG001S5W848", session=session, row_index=0)
    with pytest.raises(IdentityExactGroupHistoryRunnerError, match="scope_leakage"):
        ExactGroupHistoryEngine().consume_session(
            session, asset_attestations=[row], universe_attestations=[]
        )


def test_resource_caps_use_frozen_exact_field_names() -> None:
    plan = SimpleNamespace(
        execution_resource_caps=SimpleNamespace(
            rss_bytes_hard_cap=2 * 1024**3,
            tmp_bytes_hard_cap=123,
            disk_free_bytes_hard_floor=456,
        )
    )
    assert _plan_cap(plan, "tmp_bytes_hard_cap", 999) == 123
    assert _plan_cap(plan, "disk_free_bytes_hard_floor", 999) == 456
    with pytest.raises(IdentityExactGroupHistoryRunnerError, match="absent"):
        _plan_cap(plan, "temporary_bytes_hard_cap", 999)


def test_engine_finish_emits_exact_frozen_schema_and_no_decisions() -> None:
    session = date(2025, 1, 2)
    engine = ExactGroupHistoryEngine()
    for index, (ticker, composite) in enumerate(EXACT_GROUP_HISTORY_FIXED_GROUPS):
        attestation = _asset_attestation(
            ticker,
            composite,
            f"BBG001TEST{index:02d}",
            session=session,
            row_index=index,
        )
        engine.consume_session(session, asset_attestations=[attestation], universe_attestations=[])
    plan = SimpleNamespace(
        plan_id="1" * 64,
        sha256="2" * 64,
        scope_set_id="3" * 64,
        source_artifact_set_digest="4" * 64,
        normalized_source_artifact_set_digest="5" * 64,
        inventory_completion_id="6" * 64,
        directional_preview_candidate_id="7" * 64,
        directional_preview_completion_id="8" * 64,
    )
    approval = SimpleNamespace(
        approval_id="9" * 64,
        sha256="a" * 64,
    )
    intent = SimpleNamespace(intent_id="b" * 64, sha256="c" * 64)
    calendar = SimpleNamespace(sessions=(SimpleNamespace(session_date=session),))
    build = engine.finish(
        plan=plan,
        approval=approval,
        intent=intent,
        calendar=calendar,
        created_at_utc=NOW,
        runner_verified_critical={},
    )
    assert len(build.slots) == 3
    table = pa.Table.from_pylist(
        [dict(item) for item in build.slots],
        schema=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.arrow_schema,
    )
    assert table.schema.equals(IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.arrow_schema)
    assert all(
        value is False
        for row in build.slots
        for key, value in row.items()
        if key.endswith("_eligible")
    )
    assert build.examples["reason_counts"]
    assert set(build.examples["reason_counts"]) <= set(build.examples["examples"])
    assert all(build.examples["examples"][key] for key in build.examples["reason_counts"])


def test_active_edge_includes_universe_only_state_change() -> None:
    common = {
        "exact_group_observed_share_class_figis_json": "[]",
        "exact_group_observed_ciks_json": "[]",
        "exact_group_observed_primary_exchange_mics_json": "[]",
        "exact_group_observed_type_codes_json": "[]",
        "exact_group_provider_active_values_json": "[true]",
        "previous_observed_session_is_adjacent_xnys": True,
        "review_group_id": "1" * 64,
        "ticker": "SOR",
    }
    slots = (
        {
            **common,
            "active_on_date": True,
            "membership_status": "present_active",
            "session_date": date(2025, 1, 2),
        },
        {
            **common,
            "active_on_date": False,
            "membership_status": "present_inactive",
            "session_date": date(2025, 1, 3),
        },
    )
    high = Counter({rule.check_id: 0 for rule in runner_module._high_rules()})
    examples: dict[str, list[dict[str, object]]] = defaultdict(list)
    runner_module._update_edge_warnings(slots, high, examples)
    assert high["observed_active_status_change_edges"] == 1
    assert examples["observed_active_status_change_edges"][0]["ticker"] == "SOR"


def test_candidate_and_completion_publish_no_clobber_and_read_back(monkeypatch, tmp_path) -> None:
    session = date(2025, 1, 2)
    engine = ExactGroupHistoryEngine()
    for index, (ticker, composite) in enumerate(EXACT_GROUP_HISTORY_FIXED_GROUPS):
        engine.consume_session(
            session,
            asset_attestations=[
                _asset_attestation(
                    ticker,
                    composite,
                    f"BBG001TEST{index:02d}",
                    session=session,
                    row_index=index,
                )
            ],
            universe_attestations=[],
        )
    caps = SimpleNamespace(
        physical_source_artifact_count=2,
        physical_source_row_count=3,
        physical_source_bytes=20,
        rss_bytes_hard_cap=2 * 1024**3,
        wall_clock_seconds_hard_cap=4 * 60 * 60,
        output_bytes_hard_cap=512 * 1024**2,
        selected_row_hard_cap=1_000_000,
    )
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
        execution_resource_caps=caps,
    )
    approval = SimpleNamespace(
        approval_id="b" * 64,
        sha256="c" * 64,
        request_event_id="d" * 64,
        request_event_sha256="e" * 64,
    )
    intent = runner_module.S7ExactGroupHistoryExecutionIntent(
        created_at_utc=NOW,
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        approval_id=approval.approval_id,
        approval_sha256=approval.sha256,
        request_event_id=approval.request_event_id,
        request_event_sha256=approval.request_event_sha256,
        execution_data_root=str(tmp_path),
        source_binding_id=plan.source_binding_id,
        source_binding_sha256=plan.source_binding_sha256,
        source_artifact_set_digest=plan.source_artifact_set_digest,
        normalized_source_artifact_set_digest=(plan.normalized_source_artifact_set_digest),
        fixed_scope_digest=runner_module.EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST,
        contract_id=runner_module.IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
        contract_schema_digest=(
            runner_module.IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST
        ),
        qa_semantics_digest=(
            runner_module.IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST
        ),
        observed_run_semantics_digest=(
            runner_module.EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST
        ),
    )
    intent_path = tmp_path / intent.relative_path
    intent_path.parent.mkdir(parents=True)
    intent_path.write_bytes(intent.content)
    build = engine.finish(
        plan=plan,
        approval=approval,
        intent=intent,
        calendar=SimpleNamespace(sessions=(SimpleNamespace(session_date=session),)),
        created_at_utc=NOW,
        runner_verified_critical={},
    )
    controls = runner_module._LoadedControls(plan, SimpleNamespace(), approval)
    staging = tmp_path / "staging"
    staging.mkdir()
    completion_path = tmp_path / runner_module.exact_group_history_completion_path(
        plan.plan_id, approval.approval_id
    )

    class Monitor:
        peak_rss_bytes = 1
        rss_cap = caps.rss_bytes_hard_cap
        wall_cap = caps.wall_clock_seconds_hard_cap
        output_cap = caps.output_bytes_hard_cap

        @staticmethod
        def check() -> None:
            return None

    monkeypatch.setattr(runner_module, "_verify_all_source_hashes", lambda *a, **k: None)
    monkeypatch.setattr(
        runner_module,
        "load_xnys_calendar_artifact",
        lambda *a, **k: SimpleNamespace(sessions=(SimpleNamespace(session_date=session),)),
    )
    completion = runner_module._stage_commit_and_complete(
        tmp_path,
        controls,
        build,
        refs=(),
        intent=intent,
        staging=staging,
        completion_path=completion_path,
        monitor=Monitor(),
        started=time.monotonic(),
        scanned=runner_module._ScanResult(
            engine=engine,
            exact_attestation_ids=frozenset(),
            scanned_artifacts=2,
            scanned_rows=3,
            scanned_bytes=20,
        ),
        created_at_utc=NOW,
        calendar=SimpleNamespace(),
    )
    assert completion.completion_state == "awaiting_review"
    assert completion.output_slot_row_count == 3
    assert len(completion.output_artifacts) == 7
    assert completion_path.read_bytes() == completion.content
    assert (
        runner_module._read_completed_without_source(tmp_path, completion_path, controls)
        == completion
    )


def test_parquet_exclusive_writer_preserves_foreign_target(tmp_path) -> None:
    target = tmp_path / "review-slots.parquet"
    target.write_bytes(b"foreign-owner\n")
    table = pa.Table.from_pylist(
        [], schema=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.arrow_schema
    )
    with pytest.raises(IdentityExactGroupHistoryRunnerError, match="no-clobber"):
        runner_module._write_parquet_exclusive(target, table)
    assert target.read_bytes() == b"foreign-owner\n"


def _orchestration_controls():
    plan = SimpleNamespace(plan_id="1" * 64, sha256="2" * 64)
    approval = SimpleNamespace(approval_id="3" * 64, sha256="4" * 64)
    return runner_module._LoadedControls(
        plan=plan,
        request=SimpleNamespace(),
        approval=approval,
    )


def test_execution_intent_is_persisted_before_execution_body(monkeypatch, tmp_path) -> None:
    controls = _orchestration_controls()
    order: list[str] = []
    sentinel = object()

    class Monitor:
        def __init__(self, **kwargs):
            del kwargs

        def check(self):
            return None

    monkeypatch.setattr(runner_module, "_load_controls", lambda *a, **k: controls)
    monkeypatch.setattr(runner_module, "_verify_controls_without_source_read", lambda *a, **k: ())
    monkeypatch.setattr(runner_module, "_ResourceMonitor", Monitor)

    def store_intent(*args, **kwargs):
        del args, kwargs
        order.append("intent")
        return SimpleNamespace()

    def execute(*args, **kwargs):
        del args, kwargs
        assert order == ["intent"]
        order.append("parquet-body")
        return sentinel

    monkeypatch.setattr(runner_module, "_store_execution_intent", store_intent)
    monkeypatch.setattr(runner_module, "_execute_after_intent", execute)
    result = runner_module.run_exact_s7_exact_group_history_review(
        tmp_path,
        plan_id="1" * 64,
        expected_plan_sha256="2" * 64,
        approval_id="3" * 64,
        expected_approval_sha256="4" * 64,
    )
    assert result is sentinel
    assert order == ["intent", "parquet-body"]


def test_completed_retry_does_not_touch_source_parquet(monkeypatch, tmp_path) -> None:
    controls = _orchestration_controls()
    completion = (
        tmp_path
        / "manifests/silver/identity/exact-group-history-execution-completions"
        / f"plan_id={'1' * 64}"
        / f"approval_id={'3' * 64}"
        / "manifest.json"
    )
    completion.parent.mkdir(parents=True)
    completion.write_text("completed\n")
    sentinel = object()
    monkeypatch.setattr(runner_module, "_load_controls", lambda *a, **k: controls)
    monkeypatch.setattr(runner_module, "_verify_controls_without_source_read", lambda *a, **k: ())
    monkeypatch.setattr(
        runner_module,
        "_read_completed_without_source",
        lambda *a, **k: sentinel,
    )
    monkeypatch.setattr(
        runner_module.pq,
        "ParquetFile",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("source opened")),
    )
    monkeypatch.setattr(
        runner_module,
        "_sha256_regular_nofollow",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("source hashed")),
    )
    result = runner_module.run_exact_s7_exact_group_history_review(
        tmp_path,
        plan_id="1" * 64,
        expected_plan_sha256="2" * 64,
        approval_id="3" * 64,
        expected_approval_sha256="4" * 64,
    )
    assert result is sentinel


def test_interrupted_intent_fails_closed_without_source_read(monkeypatch, tmp_path) -> None:
    controls = _orchestration_controls()
    intent = (
        tmp_path
        / "manifests/silver/identity/exact-group-history-execution-intents"
        / f"plan_id={'1' * 64}"
        / f"approval_id={'3' * 64}"
        / "manifest.json"
    )
    intent.parent.mkdir(parents=True)
    intent.write_text("intent\n")
    monkeypatch.setattr(runner_module, "_load_controls", lambda *a, **k: controls)
    monkeypatch.setattr(runner_module, "_verify_controls_without_source_read", lambda *a, **k: ())
    monkeypatch.setattr(
        runner_module.pq,
        "ParquetFile",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("source opened")),
    )
    with pytest.raises(IdentityExactGroupHistoryRunnerError, match="incomplete"):
        runner_module.run_exact_s7_exact_group_history_review(
            tmp_path,
            plan_id="1" * 64,
            expected_plan_sha256="2" * 64,
            approval_id="3" * 64,
            expected_approval_sha256="4" * 64,
        )


def _binding_controls(root, *, tamper: str | None = None):
    session = date(2026, 1, 2)
    pins = []
    for table in ("asset_observation_daily", "universe_source_daily"):
        contract = ASSET_CONTRACTS[table]
        release = S7_SOURCE_PINS[table]
        pins.append(
            SimpleNamespace(
                table=table,
                session_date=session,
                release_id=release.release_id,
                release_manifest_sha256=release.release_manifest_sha256,
                path=f"silver/{table}/session_date={session}/part.parquet",
                sha256=_digest(f"source-{table}"),
                bytes=10,
                row_count=1,
                source_contract_id=contract.contract_id,
                schema_digest=contract.schema_digest,
            )
        )
    normalized = stable_digest(
        [
            runner_module.ExactGroupHistorySourceArtifactRef.from_plan_pin(item).to_dict()
            for item in pins
        ]
    )
    plan = SimpleNamespace(
        plan_id="1" * 64,
        sha256="2" * 64,
        execution_data_root=str(root),
        source_artifacts=tuple(pins),
        normalized_source_artifact_set_digest=normalized,
        raw_source_artifact_set_digest="3" * 64,
        source_artifact_set_digest="3" * 64,
        inventory_projection_set_digest="4" * 64,
        source_binding_id="5" * 64,
        source_binding_sha256="6" * 64,
        source_binding_path=(
            "manifests/silver/identity/exact-group-history-source-bindings/"
            f"run_intent_id={'7' * 64}/manifest.json"
        ),
        manifest_plan_id="8" * 64,
        manifest_plan_sha256="9" * 64,
        contract_id=runner_module.IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
        contract_schema_digest=(
            runner_module.IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST
        ),
        execution_resource_caps=SimpleNamespace(
            physical_source_artifact_count=2,
            physical_source_row_count=2,
            physical_source_bytes=20,
            xnys_session_count=1,
            review_group_count=3,
            rss_bytes_hard_cap=2 * 1024**3,
        ),
    )
    request = SimpleNamespace(
        request_event_id="a" * 64,
        sha256="b" * 64,
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        canonical_approval_literal="{}",
    )
    approval_values = {
        name: False
        for name in (
            "source_discovery_authorized",
            "caller_scope_override_authorized",
            "share_class_filter_authorized",
            "network_access_authorized",
            "external_evidence_capture_authorized",
            "registry_evaluation_authorized",
            "adjudication_authorized",
            "override_generation_authorized",
            "table_materialization_authorized",
            "full_run_authorized",
            "publication_authorized",
            "membership_mutation_authorized",
            "forced_liquidation_authorized",
        )
    }
    approval_values.update(
        {
            "exact_group_history_execution_authorized": True,
            "source_read_authorized": True,
            "parquet_read_authorized": True,
            "once_to_awaiting_review": True,
        }
    )
    approval = SimpleNamespace(
        execution_data_root=str(root),
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        request_event_id=request.request_event_id,
        request_event_sha256=request.sha256,
        approval_literal=request.canonical_approval_literal,
        **approval_values,
    )
    source_binding = SimpleNamespace(
        source_binding_id=plan.source_binding_id,
        relative_path=plan.source_binding_path,
        sha256=plan.source_binding_sha256,
        raw_source_artifact_set_digest=plan.raw_source_artifact_set_digest,
        inventory_projection_set_digest=plan.inventory_projection_set_digest,
        normalized_source_artifact_set_digest=(plan.normalized_source_artifact_set_digest),
        execution_source_pins=plan.source_artifacts,
        manifest_plan_id=plan.manifest_plan_id,
        manifest_plan_sha256=plan.manifest_plan_sha256,
    )
    if tamper == "raw":
        source_binding.raw_source_artifact_set_digest = "0" * 64
    elif tamper == "normalized":
        source_binding.normalized_source_artifact_set_digest = "0" * 64
    elif tamper == "identity":
        source_binding.source_binding_id = "0" * 64
    return runner_module._LoadedControls(plan, request, approval), source_binding


@pytest.mark.parametrize("tamper", ["raw", "normalized", "identity"])
def test_pre_read_gate_rejects_source_binding_tamper(monkeypatch, tmp_path, tamper) -> None:
    controls, source_binding = _binding_controls(tmp_path, tamper=tamper)
    manifest_module = __import__(
        "ame_stocks_api.silver.identity_exact_group_history_manifest",
        fromlist=["ExactGroupHistoryManifestStore"],
    )

    class Store:
        def __init__(self, root):
            assert root == tmp_path

        def load_manifest_plan(self, *args):
            del args
            return SimpleNamespace()

        def load_source_binding(self, *args):
            del args
            return source_binding

    monkeypatch.setattr(manifest_module, "ExactGroupHistoryManifestStore", Store)
    monkeypatch.setattr(runner_module, "_verify_git_and_runtime_pins", lambda *a: None)
    monkeypatch.setattr(runner_module, "EXACT_GROUP_HISTORY_START_SESSION", date(2026, 1, 2))
    monkeypatch.setattr(runner_module, "EXACT_GROUP_HISTORY_END_SESSION", date(2026, 1, 2))
    with pytest.raises(IdentityExactGroupHistoryRunnerError, match="projection"):
        runner_module._verify_controls_without_source_read(tmp_path, controls)
