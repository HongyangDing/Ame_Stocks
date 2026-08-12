from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_silver_s7_5_i3_production_contract import _run_spec as _base_run_spec
from test_silver_s7_5_i3_runner import _legacy_row

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_i3_delta_io as delta_io
from ame_stocks_api.silver import incremental_i3_production as production_executor
from ame_stocks_api.silver import incremental_i3_production_contract as production_contract
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.asset_incremental_contract import S4SessionPartitionReceipt
from ame_stocks_api.silver.identity_exact_group_history_contract import (
    EXACT_GROUP_HISTORY_FIXED_TICKERS,
)
from ame_stocks_api.silver.identity_exact_group_history_runner import (
    ExactGroupHistoryOutputRef,
    S7ExactGroupHistoryCandidate,
    S7ExactGroupHistoryCompletion,
    exact_group_history_completion_path,
)
from ame_stocks_api.silver.identity_registry_workflow import (
    ExactSourceRow,
    ExactSourceScope,
    RegistryReleasePin,
)
from ame_stocks_api.silver.incremental_contract import (
    ArtifactPin,
    ControlObjectKind,
    ControlObjectPin,
    ManifestPin,
    ReleaseType,
    RowVersionChangeIndexPin,
)
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    NATIVE_V2_RELEASE_FAMILY,
    S4_TERMINAL_TABLE_ORDER,
    I3CheckpointState,
    NativeV2OutputArtifact,
    NativeV2ParentReleasePin,
    NativeV2ReleaseManifest,
    ResolvedPartitionState,
    S4TerminalPartitionPin,
    TerminalRowVersionState,
    i3_resolved_state_digest,
)
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_CONTRACTS,
    I3_V2_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionDatasetIndex,
    I3ProductionI2ReceiptPin,
    I3ProductionOutputSet,
    I3ProductionOutputStorage,
    I3ProductionParentAuthority,
    I3ProductionPartitionPin,
    I3ProductionRowsetIndex,
    I3ProductionRunKind,
    I3ProductionSegmentPin,
    I3ProductionTableOutput,
)

PARENT_SESSION = date(2026, 7, 9)
TARGET_SESSION = date(2026, 7, 10)
RUN_AVAILABLE = date(2026, 8, 5)
CALENDAR = (date(2026, 7, 8), PARENT_SESSION, TARGET_SESSION)

_SOURCE_FIELDS = {
    "identity_adjudication": (
        "source_identity_adjudication_release_id",
        "source_identity_adjudication_release_available_session",
    ),
    "identity_cross_market_adjudication": (
        "source_identity_cross_market_adjudication_release_id",
        "source_identity_cross_market_adjudication_release_available_session",
    ),
    "provider_composite_override": (
        "source_provider_composite_override_release_id",
        "source_provider_composite_override_release_available_session",
    ),
    "share_class_adjudication": (
        "source_share_class_adjudication_release_id",
        "source_share_class_adjudication_release_available_session",
    ),
    "asset_transition": (
        "source_asset_transition_release_id",
        "source_asset_transition_release_available_session",
    ),
}


def _digest(label: str) -> str:
    return stable_digest({"delta-fixture": label})


def _write_bytes(root: Path, relative: str, content: bytes) -> ArtifactPin:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return ArtifactPin(
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _dummy_pin(root: Path, relative: str, label: str) -> ArtifactPin:
    return _write_bytes(root, relative, f"{label}\n".encode())


def _delta_config() -> delta_io.I3ProductionDeltaRunConfig:
    parent_spec = _base_run_spec()
    control_root = (
        f"manifests/silver/identity/s7-5-native-v2-staging/run_spec_id={parent_spec.run_spec_id}"
    )
    return delta_io.I3ProductionDeltaRunConfig(
        parent_completion_artifact=ArtifactPin(
            path=f"{control_root}/completion.json",
            sha256=_digest("config-parent-completion"),
            bytes=10,
        ),
        parent_deep_attestation_artifact=ArtifactPin(
            path=f"{control_root}/deep-verification-attestation.json",
            sha256=_digest("config-parent-deep"),
            bytes=10,
        ),
        i2_receipt_artifact=ArtifactPin(
            path=(
                "manifests/silver/incremental/s4/assets/"
                "session_year=2026/session_date=2026-07-10/run-receipt.json"
            ),
            sha256=_digest("config-i2-receipt"),
            bytes=10,
        ),
        run_available_session=RUN_AVAILABLE,
        resource_caps=parent_spec.resource_caps,
    )


def _gate_c_aggregate_fixture(
    root: Path,
    *,
    output_bytes: tuple[int, ...] = (11, 12, 13, 14, 15),
    declared_output_bytes: int | None = None,
    detector_preview_bytes: int = 7,
) -> dict[str, SimpleNamespace]:
    plan_id = _digest("gate-c-plan")
    authorization_id = _digest("gate-c-authorization")
    plan = _write_bytes(
        root,
        "controls/gate-c/plan.json",
        delta_io._canonical_json_bytes({"plan_id": plan_id}),
    )
    authorization = _write_bytes(
        root,
        "controls/gate-c/authorization.json",
        delta_io._canonical_json_bytes({"authorization_id": authorization_id}),
    )
    roles = tuple(sorted(delta_io._GATE_C_OUTPUT_ROLES))
    assert len(roles) == len(output_bytes)
    outputs = {
        role: {
            "bytes": size,
            "path": f"opaque/gate-c/{role}.parquet",
            "sha256": _digest(f"gate-c-output-{role}"),
        }
        for role, size in zip(roles, output_bytes, strict=True)
    }
    measured = sum(output_bytes) if declared_output_bytes is None else declared_output_bytes
    candidate_id = _digest("gate-c-candidate")
    candidate_payload = {
        "artifact_type": "s7_full_market_sequence_candidate",
        "candidate_id": candidate_id,
        "outputs": outputs,
        "registry_loader_source_refs": {
            "detector_preview": {
                "bytes": detector_preview_bytes,
                "path": "opaque/gate-c/detector-preview.json",
                "preview_artifact_id": _digest("gate-c-detector-preview"),
                "sha256": _digest("gate-c-detector-preview-sha"),
            },
            "detector_preview_completion": {},
            "gate_a_candidate": {},
            "gate_a_completion": {},
            "gate_b_candidate": {},
            "gate_b_data": {},
            "reviewed_case_evidence": {},
            "reviewed_external_evidence": {},
        },
        "resource_measurements": {"output_bytes": measured},
    }
    candidate_manifest_id = stable_digest(candidate_payload)
    candidate = _write_bytes(
        root,
        "controls/gate-c/candidate.json",
        delta_io._canonical_json_bytes({**candidate_payload, "manifest_id": candidate_manifest_id}),
    )
    completion_payload = {
        "artifact_type": "s7_full_market_sequence_execution_completion",
        "authorization": {
            "authorization_id": authorization_id,
            "path": authorization.path,
            "sha256": authorization.sha256,
        },
        "candidate": {
            "bytes": candidate.bytes,
            "candidate_id": candidate_id,
            "manifest_id": candidate_manifest_id,
            "path": candidate.path,
            "sha256": candidate.sha256,
        },
        "outputs": outputs,
        "plan": {
            "path": plan.path,
            "plan_id": plan_id,
            "sha256": plan.sha256,
        },
        "resource_measurements": {"output_bytes": measured},
    }
    completion_id = stable_digest(completion_payload)
    completion = _write_bytes(
        root,
        "controls/gate-c/completion.json",
        delta_io._canonical_json_bytes({**completion_payload, "completion_id": completion_id}),
    )
    return {
        "source_gate_c_candidate_manifest": SimpleNamespace(
            artifact_id=candidate_id,
            path=candidate.path,
            sha256=candidate.sha256,
            bytes=candidate.bytes,
        ),
        "source_gate_c_completion_manifest": SimpleNamespace(
            artifact_id=completion_id,
            path=completion.path,
            sha256=completion.sha256,
            bytes=completion.bytes,
        ),
    }


def _exact_group_aggregate_fixture(
    root: Path,
    *,
    tree_byte_delta: int = 0,
) -> dict[str, SimpleNamespace]:
    artifacts = tuple(
        ExactGroupHistoryOutputRef(
            role=role,
            path=f"{role.replace(':', '-')}.json",
            sha256=_digest(f"exact-group-output-{role}"),
            bytes=10,
            media_type=(
                "application/vnd.apache.parquet" if role == "review_slots" else "application/json"
            ),
            row_count=1 if role == "review_slots" else None,
        )
        for role in (
            "review_slots",
            "group_sequences",
            "qa",
            "bounded_examples",
            *(f"group_evidence:{ticker}" for ticker in EXACT_GROUP_HISTORY_FIXED_TICKERS),
        )
    )
    fields = {
        "plan_id": _digest("exact-group-plan"),
        "plan_sha256": _digest("exact-group-plan-sha"),
        "approval_id": _digest("exact-group-approval"),
        "approval_sha256": _digest("exact-group-approval-sha"),
        "request_event_id": _digest("exact-group-request"),
        "request_event_sha256": _digest("exact-group-request-sha"),
        "execution_intent_id": _digest("exact-group-intent"),
        "execution_intent_path": "controls/exact-group/intent.json",
        "execution_intent_sha256": _digest("exact-group-intent-sha"),
    }
    candidate = S7ExactGroupHistoryCandidate(
        **fields,
        source_binding_id=_digest("exact-group-source-binding"),
        source_binding_sha256=_digest("exact-group-source-binding-sha"),
        source_artifact_set_digest=_digest("exact-group-source-set"),
        normalized_source_artifact_set_digest=_digest("exact-group-normalized-source-set"),
        review_scope_set_id=_digest("exact-group-review-scope"),
        artifacts=artifacts,
        evidence_manifest_ids=tuple(_digest(f"exact-group-evidence-{index}") for index in range(3)),
        created_at_utc=datetime(2026, 7, 20, tzinfo=UTC),
    )
    candidate_pin = _write_bytes(
        root,
        f"{candidate.relative_directory}/manifest.json",
        candidate.content,
    )
    completion = S7ExactGroupHistoryCompletion(
        **fields,
        candidate_id=candidate.candidate_id,
        candidate_path=candidate_pin.path,
        candidate_sha256=candidate_pin.sha256,
        output_artifacts=candidate.artifacts,
        completed_at_utc=datetime(2026, 7, 21, tzinfo=UTC),
        source_artifact_count=1,
        source_row_count=1,
        source_bytes=1,
        output_slot_row_count=1,
        peak_rss_bytes=1,
        wall_clock_seconds=1.0,
        output_bytes=candidate_pin.bytes + sum(item.bytes for item in artifacts) + tree_byte_delta,
    )
    completion_pin = _write_bytes(
        root,
        exact_group_history_completion_path(
            completion.plan_id,
            completion.approval_id,
        ),
        completion.content,
    )
    return {
        "source_exact_group_candidate_manifest": SimpleNamespace(
            artifact_id=candidate.candidate_id,
            path=candidate_pin.path,
            sha256=candidate_pin.sha256,
            bytes=candidate_pin.bytes,
        ),
        "source_exact_group_completion_manifest": SimpleNamespace(
            artifact_id=completion.completion_id,
            path=completion_pin.path,
            sha256=completion_pin.sha256,
            bytes=completion_pin.bytes,
        ),
    }


def _write_parquet(
    root: Path,
    relative: str,
    *,
    schema: pa.Schema,
    rows: list[dict[str, object]],
) -> ArtifactPin:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema),
        path,
        compression="zstd",
        version="2.6",
    )
    return ArtifactPin(
        path=relative,
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        bytes=path.stat().st_size,
    )


def _production_lineage(row: dict[str, object], run_spec) -> dict[str, object]:
    result = dict(row)
    result["source_s4_release_set_id"] = run_spec.s4_v1_source.object_id
    policy = {
        item.registry_kind.value: item for item in run_spec.identity_policy_bundle.registry_releases
    }
    for kind, (release_field, availability_field) in _SOURCE_FIELDS.items():
        result[release_field] = policy[kind].release_id
        result[availability_field] = policy[kind].release_available_session
    result["identity_resolution_cutoff_session"] = (
        run_spec.identity_policy_bundle.decision_cutoff_session
    )
    result["identity_evidence_available_session"] = max(
        result["membership_source_available_session"],
        run_spec.identity_policy_bundle.bundle_available_session,
    )
    return result


def _eligible_row(session: date, run_spec) -> dict[str, object]:
    row = _legacy_row(
        session,
        ticker="AAPL",
        policy_bundle=run_spec.identity_policy_bundle,
        source_label=f"AAPL-{session.isoformat()}",
    )
    row["source_selection_status"] = "selected_exact_source_record"
    return _production_lineage(row, run_spec)


def _pending_row(
    ticker: str,
    *,
    run_spec,
    sor_expired: bool = False,
) -> dict[str, object]:
    observed = "BBG000KMY6N2" if sor_expired else "BBG000B9XRY4"
    observed_share = "BBG01RK6N5G9" if sor_expired else "BBG001S5N8V8"
    row = _legacy_row(
        TARGET_SESSION,
        ticker=ticker,
        eligible=False,
        observed_composite=observed,
        observed_share=observed_share,
        observed_market="US" if sor_expired else None,
        policy_bundle=run_spec.identity_policy_bundle,
        source_label=f"{ticker}-{TARGET_SESSION.isoformat()}",
    )
    row.update(
        {
            "asset_id": None,
            "share_class_id": None,
            "canonical_share_class_figi": None,
            "issuer_id": None,
            "canonical_cik_normalized": None,
            "ticker_alias_id": None,
            "canonical_composite_figi": None,
            "canonical_composite_market_code": None,
            "observed_composite_market_code": "US" if sor_expired else None,
            "identity_resolution_status": "unresolved",
            "identity_resolution_method": (
                "provider_figi_bounce_pending_unresolved"
                if sor_expired
                else "cross_market_composite_pending_unresolved"
            ),
            "identity_disposition": (
                "pending_unresolved" if sor_expired else "pending_cross_market_review"
            ),
            "identity_case_id": None,
            "identity_case_available_session": None,
            "identity_adjudication_id": None,
            "cross_market_scope_id": None,
            "cross_market_adjudication_id": None,
            "cross_market_adjudication_available_session": None,
            "identity_case_resolution_role": None,
            "adjudication_available_session": None,
            "cross_market_classification_status": ("known_us" if sor_expired else "not_classified"),
            "backtest_identity_eligible": False,
            "position_continuity_status": (
                "identity_uncertain_no_new_trade_no_forced_exit_run_incomplete"
            ),
            "identity_quality_liquidation_signal": False,
            "current_reference_factor_eligible": False,
            "provider_composite_override_id": None,
            "provider_composite_override_available_session": None,
            "share_class_adjudication_id": None,
            "share_class_adjudication_available_session": None,
            "asset_transition_ids": [],
            "composite_registry_match_count": 0,
            "composite_registry_collision": False,
            "source_selection_status": "selected_exact_source_record",
            "resolution_rule_version": delta_io.I3_PRODUCTION_DELTA_RESOLUTION_RULE_VERSION,
        }
    )
    return _production_lineage(row, run_spec)


def _default_row(schema: pa.Schema, session: date) -> dict[str, object]:
    result: dict[str, object] = {}
    timestamp = datetime(2026, 7, 10, 20, tzinfo=UTC)
    for field in schema:
        if field.nullable:
            result[field.name] = None
        elif pa.types.is_string(field.type):
            result[field.name] = f"fixture-{field.name}"
        elif pa.types.is_boolean(field.type):
            result[field.name] = False
        elif pa.types.is_int64(field.type):
            result[field.name] = 0
        elif pa.types.is_date32(field.type):
            result[field.name] = session
        elif pa.types.is_timestamp(field.type):
            result[field.name] = timestamp
        else:  # pragma: no cover - frozen schema guard
            raise AssertionError(field)
    return result


def _i2_rows(target_rows: tuple[dict[str, object], ...]):
    source_rows: list[dict[str, object]] = []
    observation_rows: list[dict[str, object]] = []
    version_rows: list[dict[str, object]] = []
    for target in target_rows:
        source_id = str(target["selected_source_record_id"])
        ticker = str(target["ticker"])
        name = "Apple Inc." if ticker == "AAPL" else f"{ticker} Inc."
        source = _default_row(
            ASSET_CONTRACTS["universe_source_daily"].arrow_schema,
            TARGET_SESSION,
        )
        source.update(
            {
                "session_year": TARGET_SESSION.year,
                "session_date": TARGET_SESSION,
                "ticker": ticker,
                "active_on_date": True,
                "type_code": target["type_code"],
                "name": name,
                "market": "stocks",
                "locale": "us",
                "primary_exchange_mic": target["primary_exchange_mic"],
                "currency_name": "usd",
                "cik": target["observed_cik_normalized"],
                "composite_figi": target["observed_composite_figi"],
                "share_class_figi": target["observed_share_class_figi"],
                "delisted_at_utc": None,
                "last_updated_at_utc": None,
                "identity_link_status": "linked",
                "selected_source_record_id": source_id,
                "version_group_id": None,
                "source_version_count": 1,
                "selection_status": "singleton",
                "selection_rule_version": "fixture-selection-v1",
                "reference_time_scope": target["membership_time_scope"],
                "metadata_time_scope": target["metadata_time_scope"],
                "source_available_session": TARGET_SESSION,
            }
        )
        observation = _default_row(
            ASSET_CONTRACTS["asset_observation_daily"].arrow_schema,
            TARGET_SESSION,
        )
        observation.update(
            {
                "session_year": TARGET_SESSION.year,
                "session_date": TARGET_SESSION,
                "requested_active": True,
                "provider_active": True,
                "ticker": ticker,
                "type_code": source["type_code"],
                "name": name,
                "market": source["market"],
                "locale": source["locale"],
                "primary_exchange_mic": source["primary_exchange_mic"],
                "currency_name": source["currency_name"],
                "cik": source["cik"],
                "composite_figi": source["composite_figi"],
                "share_class_figi": source["share_class_figi"],
                "delisted_at_utc": source["delisted_at_utc"],
                "last_updated_at_utc": source["last_updated_at_utc"],
                "source_available_session": TARGET_SESSION,
                "source_record_id": source_id,
            }
        )
        source_rows.append(source)
        observation_rows.append(observation)
    return source_rows, observation_rows, version_rows


def _partition_receipt(
    root: Path,
    table_name: str,
    rows: list[dict[str, object]],
    *,
    source_binding_id: str,
) -> S4SessionPartitionReceipt:
    contract = ASSET_CONTRACTS[table_name]
    artifact = _write_parquet(
        root,
        (
            f"silver/s4/incremental/{table_name}/session_year=2026/"
            "session_date=2026-07-10/part-000.parquet"
        ),
        schema=contract.arrow_schema,
        rows=rows,
    )
    return S4SessionPartitionReceipt(
        table_name=table_name,
        session_date=TARGET_SESSION,
        artifact=artifact,
        row_count=len(rows),
        contract_id=contract.contract_id,
        schema_digest=contract.schema_digest,
        source_binding_id=source_binding_id,
        row_funnel_digest=_digest(f"funnel-{table_name}"),
        qa_result_set_digest=_digest(f"qa-{table_name}"),
    )


def _parent_fixture(root: Path):
    initial = _base_run_spec()
    base_spec = replace(
        initial,
        terminal_session=PARENT_SESSION,
        i2_base_frontier=replace(initial.i2_base_frontier, terminal_session=PARENT_SESSION),
    )
    parent_v1 = _eligible_row(PARENT_SESSION, base_spec)
    parent_day = delta_io._materialize_delta_day(
        target_rows=(parent_v1,),
        lookback_rows=(parent_v1,),
        target_session=PARENT_SESSION,
        calendar=CALENDAR,
        availability_session=RUN_AVAILABLE,
        policy=base_spec.identity_policy_bundle,
        policy_snapshot=SimpleNamespace(policy_bundle=base_spec.identity_policy_bundle),
        prior_open_aliases=(),
        prior_assets=(),
        prior_issuers=(),
        prior_unresolved=(),
        prior_terminal_rows=(),
        reference_metadata_by_source_id={
            str(parent_v1["selected_source_record_id"]): {
                "reference_name": "Apple Inc.",
                "sic_code": None,
            }
        },
        reference_metadata_available_session=RUN_AVAILABLE,
    )
    rows_by_table = {
        "asset_master": tuple(parent_day.asset_rows),
        "ticker_alias": tuple(parent_day.alias_rows),
        "issuer_master": tuple(parent_day.issuer_rows),
    }
    outputs: dict[str, I3ProductionTableOutput] = {}
    terminal_rows: list[TerminalRowVersionState] = []
    for table_name in I3_V2_TABLE_ORDER[:-1]:
        contract = I3_V2_CONTRACTS[table_name]
        artifact = _write_parquet(
            root,
            f"silver/i3/base/{table_name}/segments/initial.parquet",
            schema=contract.arrow_schema,
            rows=list(rows_by_table[table_name]),
        )
        segment = I3ProductionSegmentPin(
            table_name=table_name,
            segment_id=_digest(f"base-segment-{table_name}"),
            artifact=artifact,
            row_count=len(rows_by_table[table_name]),
            contract_id=contract.contract_id,
            schema_digest=contract.schema_digest,
            availability_session=RUN_AVAILABLE,
        )
        rowset = I3ProductionRowsetIndex(
            table_name=table_name,
            terminal_session=PARENT_SESSION,
            segments=(segment,),
        )
        index_artifact = _write_bytes(
            root,
            f"silver/i3/base/{table_name}/index.json",
            rowset.canonical_bytes(),
        )
        outputs[table_name] = I3ProductionTableOutput(
            storage=I3ProductionOutputStorage.ROWSET_INDEX,
            manifest_output=NativeV2OutputArtifact(
                table_name=table_name,
                session_date=PARENT_SESSION,
                row_count=rowset.row_count,
                contract_id=contract.contract_id,
                schema_digest=contract.schema_digest,
                artifact=index_artifact,
            ),
            rowset_index=rowset,
        )
        key_field, version_field, predecessor_field, availability_field = delta_io._VERSION_SHAPE[
            table_name
        ]
        for row in rows_by_table[table_name]:
            assert row[predecessor_field] is None
            assert row[availability_field] == RUN_AVAILABLE
            terminal_rows.append(
                TerminalRowVersionState(
                    table_name=table_name,
                    stable_row_key=str(row[key_field]),
                    row_version_id=str(row[version_field]),
                    predecessor_row_version_id=None,
                    row_payload_digest=stable_digest(delta_io._jsonable(row)),
                    index_artifact=artifact,
                    availability_session=RUN_AVAILABLE,
                )
            )

    parent_79 = dict(parent_day.universe_rows[0])
    parent_78 = dict(parent_79)
    parent_78.update({"session_year": 2026, "session_date": CALENDAR[0]})
    partitions: list[I3ProductionPartitionPin] = []
    for session, row in ((CALENDAR[0], parent_78), (PARENT_SESSION, parent_79)):
        contract = I3_V2_CONTRACTS["universe_daily"]
        artifact = _write_parquet(
            root,
            f"silver/i3/base/universe_daily/session_date={session}/part-000.parquet",
            schema=contract.arrow_schema,
            rows=[row],
        )
        partitions.append(
            I3ProductionPartitionPin(
                session_date=session,
                partition_receipt_id=_digest(f"base-universe-{session}"),
                artifact=artifact,
                row_count=1,
                contract_id=contract.contract_id,
                schema_digest=contract.schema_digest,
                availability_session=RUN_AVAILABLE,
            )
        )
    dataset = I3ProductionDatasetIndex(
        table_name="universe_daily",
        terminal_session=PARENT_SESSION,
        partitions=tuple(partitions),
    )
    dataset_artifact = _write_bytes(
        root,
        "silver/i3/base/universe_daily/index.json",
        dataset.canonical_bytes(),
    )
    universe_contract = I3_V2_CONTRACTS["universe_daily"]
    outputs["universe_daily"] = I3ProductionTableOutput(
        storage=I3ProductionOutputStorage.DATASET_INDEX,
        manifest_output=NativeV2OutputArtifact(
            table_name="universe_daily",
            session_date=PARENT_SESSION,
            row_count=dataset.row_count,
            contract_id=universe_contract.contract_id,
            schema_digest=universe_contract.schema_digest,
            artifact=dataset_artifact,
        ),
        dataset_index=dataset,
    )
    table_outputs = tuple(outputs[name] for name in I3_V2_TABLE_ORDER)
    s4_terminal_pins = tuple(
        S4TerminalPartitionPin(
            table_name=table_name,
            session_date=PARENT_SESSION,
            partition_receipt_id=_digest(f"parent-s4-{table_name}"),
            artifact=_dummy_pin(
                root,
                f"silver/s4/base/{table_name}/session_date=2026-07-09/part.parquet",
                table_name,
            ),
            availability_session=initial.s4_v1_source.available_session,
        )
        for table_name in S4_TERMINAL_TABLE_ORDER
    )
    resolved_map = tuple(
        ResolvedPartitionState(
            session_date=item.session_date,
            partition_receipt_id=item.partition_receipt_id,
            artifact=item.artifact,
            row_count=item.row_count,
            availability_session=item.availability_session,
        )
        for item in partitions
    )
    resolved_digest = i3_resolved_state_digest(
        last_session=PARENT_SESSION,
        source_cutoff_session=base_spec.source_cutoff_session,
        availability_cutoff_session=RUN_AVAILABLE,
        s4_terminal_pins=s4_terminal_pins,
        calendar_digest=base_spec.calendar.calendar_artifact_id,
        schema_digest=delta_io.I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=base_spec.transform_semantics_digest,
        identity_policy_bundle=base_spec.identity_policy_bundle,
        identity_policy_bundle_artifact=base_spec.identity_policy_bundle_artifact,
        open_aliases=parent_day.open_aliases,
        asset_aggregates=parent_day.asset_aggregates,
        issuer_aggregates=parent_day.issuer_aggregates,
        unresolved_subjects=parent_day.unresolved_subjects,
        resolved_partition_map=resolved_map,
        terminal_row_versions=tuple(sorted(terminal_rows, key=lambda item: item.map_key)),
    )
    native = NativeV2ReleaseManifest(
        release_family=NATIVE_V2_RELEASE_FAMILY,
        terminal_session=PARENT_SESSION,
        release_available_session=RUN_AVAILABLE,
        native_v2_migration_id=base_spec.native_v2_migration_id,
        identity_policy_bundle_id=base_spec.identity_policy_bundle.identity_policy_bundle_id,
        transform_semantics_digest=base_spec.transform_semantics_digest,
        resolved_state_digest=resolved_digest,
        output_artifacts=tuple(item.manifest_output for item in table_outputs),
    )
    native_artifact = _write_bytes(
        root,
        "manifests/i3/base/native-v2-release.json",
        native.canonical_bytes(),
    )
    checkpoint = I3CheckpointState(
        parent_release=NativeV2ParentReleasePin.from_manifest(
            native,
            path=native_artifact.path,
        ),
        last_session=PARENT_SESSION,
        source_cutoff_session=base_spec.source_cutoff_session,
        availability_cutoff_session=RUN_AVAILABLE,
        s4_terminal_pins=s4_terminal_pins,
        calendar_digest=base_spec.calendar.calendar_artifact_id,
        schema_digest=delta_io.I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=base_spec.transform_semantics_digest,
        identity_policy_bundle=base_spec.identity_policy_bundle,
        identity_policy_bundle_artifact=base_spec.identity_policy_bundle_artifact,
        open_aliases=parent_day.open_aliases,
        asset_aggregates=parent_day.asset_aggregates,
        issuer_aggregates=parent_day.issuer_aggregates,
        unresolved_subjects=parent_day.unresolved_subjects,
        resolved_partition_map=resolved_map,
        terminal_row_versions=tuple(sorted(terminal_rows, key=lambda item: item.map_key)),
    )
    checkpoint_artifact = _write_bytes(
        root,
        "manifests/i3/base/checkpoint.json",
        checkpoint.canonical_bytes(),
    )
    return base_spec, native, native_artifact, checkpoint, checkpoint_artifact, table_outputs


def _integration_fixture(root: Path):
    (
        base_spec,
        native,
        native_artifact,
        checkpoint,
        checkpoint_artifact,
        table_outputs,
    ) = _parent_fixture(root)
    target_rows = tuple(
        sorted(
            (
                _eligible_row(TARGET_SESSION, base_spec),
                *(
                    _pending_row(ticker, run_spec=base_spec)
                    for ticker in ("ALA", *(f"NEW{i:02d}" for i in range(15)))
                ),
                _pending_row("SOR", run_spec=base_spec, sor_expired=True),
            ),
            key=lambda row: str(row["ticker"]),
        )
    )
    source_rows, observation_rows, version_rows = _i2_rows(target_rows)
    source_binding_id = _digest("i2-source-binding")
    rows_by_table = {
        "asset_observation_daily": observation_rows,
        "asset_observation_version": version_rows,
        "universe_source_daily": source_rows,
    }
    i2_partitions = tuple(
        _partition_receipt(
            root,
            table_name,
            rows_by_table[table_name],
            source_binding_id=source_binding_id,
        )
        for table_name in S4_TERMINAL_TABLE_ORDER
    )
    i2_receipt_id = _digest("i2-receipt")
    i2_receipt_artifact = _write_bytes(
        root,
        "manifests/s4/incremental/session_date=2026-07-10/receipt.json",
        b'{"fixture":"i2-receipt"}\n',
    )
    i2_pin = I3ProductionI2ReceiptPin(
        session_date=TARGET_SESSION,
        receipt_id=i2_receipt_id,
        artifact=i2_receipt_artifact,
        receipt_available_session=date(2026, 8, 4),
    )
    completion_artifact = _dummy_pin(
        root,
        "manifests/i3/base/completion.json",
        "base-completion",
    )
    deep_artifact = _dummy_pin(
        root,
        "manifests/i3/base/deep-verification-attestation.json",
        "base-deep",
    )
    gate_a = ManifestPin(
        release_id=_digest("base-gate-a"),
        manifest_path="manifests/i3/base/gate-a-release.json",
        manifest_sha256=_digest("base-gate-a-bytes"),
        manifest_bytes=10,
        release_available_session=RUN_AVAILABLE,
    )
    delta_spec = replace(
        base_spec,
        run_kind=I3ProductionRunKind.DELTA,
        terminal_session=TARGET_SESSION,
        i2_base_frontier=None,
        i2_receipts=(i2_pin,),
        parent_release=NativeV2ParentReleasePin.from_manifest(
            native,
            path=native_artifact.path,
        ),
        parent_checkpoint_artifact=checkpoint_artifact,
        parent_gate_a_manifest=gate_a,
        parent_shadow_completion_artifact=completion_artifact,
        parent_deep_attestation_artifact=deep_artifact,
        parent_authority=I3ProductionParentAuthority.MIGRATION_SHADOW,
    )
    gate_a_run_spec = ControlObjectPin(
        object_kind=ControlObjectKind.RUN_SPEC,
        object_id=_digest("base-gate-a-run-spec"),
        artifact=_dummy_pin(
            root,
            "manifests/i3/base/gate-a-run-spec.json",
            "base-gate-a-run-spec",
        ),
    )
    gate_a_run_receipt = ControlObjectPin(
        object_kind=ControlObjectKind.RUN_RECEIPT,
        object_id=_digest("base-gate-a-run-receipt"),
        artifact=_dummy_pin(
            root,
            "manifests/i3/base/gate-a-run-receipt.json",
            "base-gate-a-run-receipt",
        ),
    )
    output_set = I3ProductionOutputSet(
        table_outputs=table_outputs,
        checkpoint_artifact=checkpoint_artifact,
        checkpoint_id=checkpoint.checkpoint_id,
        gate_a_manifest_pin=gate_a,
        release_manifest_artifact=native_artifact,
        release_id=native.release_id,
        resolved_state_digest=checkpoint.resolved_state_digest,
        resolved_content_digest=_digest("base-resolved-content"),
        gate_a_run_spec_pin=gate_a_run_spec,
        gate_a_run_receipt_pin=gate_a_run_receipt,
        control_extension_artifacts=(),
    )
    deep = SimpleNamespace(
        deep_attestation_id=_digest("base-deep-attestation"),
        completion_artifact=completion_artifact,
    )
    parent = SimpleNamespace(
        run_spec=base_spec,
        manifest=native,
        checkpoint=checkpoint,
        receipt=SimpleNamespace(output_set=output_set),
        deep_attestation=deep,
    )
    source_binding_artifact = _dummy_pin(
        root,
        "manifests/i3/delta/source-binding.json",
        "source-binding",
    )
    gate_b_manifest = _dummy_pin(
        root,
        "manifests/i3/delta/gate-b-manifest.json",
        "gate-b-manifest",
    )
    gate_b_data = _dummy_pin(
        root,
        "silver/i3/delta/gate-b.parquet",
        "gate-b-data",
    )
    parent_boundary = (
        table_outputs[-1].dataset_index.partitions[-2],
        table_outputs[-1].dataset_index.partitions[-1],
    )
    declared_inputs = delta_io._unique_artifact_pins(
        (
            completion_artifact,
            deep_artifact,
            i2_receipt_artifact,
            source_binding_artifact,
            gate_b_manifest,
            gate_b_data,
            *(item.artifact for item in i2_partitions),
            *(item.artifact for item in parent_boundary),
        )
    )
    binding = delta_io.I3ProductionDeltaInputBinding(
        run_spec_id=delta_spec.run_spec_id,
        parent_release_id=native.release_id,
        parent_checkpoint_id=checkpoint.checkpoint_id,
        parent_deep_attestation_id=deep.deep_attestation_id,
        parent_completion_artifact=completion_artifact,
        parent_deep_attestation_artifact=deep_artifact,
        i2_receipt_id=i2_receipt_id,
        i2_receipt_artifact=i2_receipt_artifact,
        i2_partitions=i2_partitions,
        parent_boundary_partitions=parent_boundary,
        requested_sessions=CALENDAR,
        source_binding_id=_digest("legacy-source-binding"),
        source_binding_artifact=source_binding_artifact,
        gate_b_manifest_artifact=gate_b_manifest,
        gate_b_data_artifact=gate_b_data,
        declared_input_artifacts=declared_inputs,
        parent_output_bytes=sum(item.bytes for item in declared_inputs),
        parent_output_rows=sum(item.manifest_output.row_count for item in table_outputs),
        asset_transition_decision_count=0,
        transitive_control_replay_bytes=1024,
        policy_snapshot_id=_digest("policy-snapshot"),
        policy_release_set_binding_digest=_digest("policy-release-binding"),
    )
    loaded = delta_io._LoadedDeltaInputs(
        binding=binding,
        parent=parent,
        i2_run=SimpleNamespace(receipt=SimpleNamespace(receipt_available_session=date(2026, 8, 4))),
        source_binding=SimpleNamespace(),
        gate_b_by_composite={},
        registries=SimpleNamespace(),
        policy_snapshot=SimpleNamespace(policy_bundle=delta_spec.identity_policy_bundle),
        calendar_sessions=CALENDAR,
    )
    return delta_spec, parent, loaded, target_rows


def test_selected_i2_join_rejects_version_and_observation_tampering() -> None:
    spec = replace(
        _base_run_spec(),
        terminal_session=PARENT_SESSION,
        i2_base_frontier=replace(
            _base_run_spec().i2_base_frontier,
            terminal_session=PARENT_SESSION,
        ),
    )
    target = (_eligible_row(TARGET_SESSION, spec),)
    sources, observations, versions = _i2_rows(target)

    metadata = delta_io._reference_metadata_by_selected_source(
        sources,
        observations,
        versions,
    )
    assert metadata[str(target[0]["selected_source_record_id"])] == {
        "reference_name": "Apple Inc.",
        "sic_code": None,
    }
    assert versions == []

    forged_version = _default_row(
        ASSET_CONTRACTS["asset_observation_version"].arrow_schema,
        TARGET_SESSION,
    )
    forged_version.update(
        {
            "session_year": TARGET_SESSION.year,
            "session_date": TARGET_SESSION,
            "requested_active": True,
            "ticker": str(target[0]["ticker"]),
            "version_group_id": _digest("forged-singleton-group"),
            "version_count": 1,
            "source_record_id": str(target[0]["selected_source_record_id"]),
            "identity_signature": _digest("forged-singleton-identity"),
            "difference_fields_json": "[]",
            "selection_rank": 1,
            "is_selected": True,
            "selection_status": "singleton",
            "selection_reason": "forged",
            "selection_rule_version": "fixture-selection-v1",
            "selected_source_record_id": str(target[0]["selected_source_record_id"]),
        }
    )
    with pytest.raises(delta_io.I3DeltaIOError, match="singleton version projection"):
        delta_io._reference_metadata_by_selected_source(
            sources,
            observations,
            [forged_version],
        )
    forged_observations = [dict(observations[0], composite_figi="BBG000KMY6N2")]
    with pytest.raises(delta_io.I3DeltaIOError, match="observation projection"):
        delta_io._reference_metadata_by_selected_source(
            sources,
            forged_observations,
            versions,
        )


def test_selected_i2_join_requires_complete_sparse_multi_version_groups() -> None:
    spec = replace(
        _base_run_spec(),
        terminal_session=PARENT_SESSION,
        i2_base_frontier=replace(
            _base_run_spec().i2_base_frontier,
            terminal_session=PARENT_SESSION,
        ),
    )
    target = (_eligible_row(TARGET_SESSION, spec),)
    sources, observations, _ = _i2_rows(target)
    selected_source_id = str(target[0]["selected_source_record_id"])
    rejected_source_id = _digest("multi-version-rejected-source")
    ticker = str(target[0]["ticker"])
    group_id = _digest("multi-version-group")
    selection_status = "resolved_multi_version"
    sources = [
        dict(
            sources[0],
            version_group_id=group_id,
            source_version_count=2,
            selection_status=selection_status,
        )
    ]
    observations = [
        observations[0],
        dict(
            observations[0],
            source_record_id=rejected_source_id,
            last_updated_at_utc=datetime(2026, 7, 10, 19, tzinfo=UTC),
        ),
    ]

    def version_row(
        source_record_id: str,
        *,
        selection_rank: int,
        is_selected: bool,
    ) -> dict[str, object]:
        row = _default_row(
            ASSET_CONTRACTS["asset_observation_version"].arrow_schema,
            TARGET_SESSION,
        )
        row.update(
            {
                "session_year": TARGET_SESSION.year,
                "session_date": TARGET_SESSION,
                "requested_active": True,
                "ticker": ticker,
                "version_group_id": group_id,
                "version_count": 2,
                "source_record_id": source_record_id,
                "identity_signature": _digest(f"identity-{source_record_id}"),
                "difference_fields_json": "[]",
                "selection_rank": selection_rank,
                "is_selected": is_selected,
                "selection_status": selection_status,
                "selection_reason": "fixture-multi-version",
                "selection_rule_version": "fixture-selection-v1",
                "selected_source_record_id": selected_source_id,
            }
        )
        return row

    versions = [
        version_row(selected_source_id, selection_rank=1, is_selected=True),
        version_row(rejected_source_id, selection_rank=2, is_selected=False),
    ]
    metadata = delta_io._reference_metadata_by_selected_source(
        sources,
        observations,
        versions,
    )
    assert metadata[selected_source_id]["reference_name"] == "Apple Inc."

    with pytest.raises(delta_io.I3DeltaIOError, match="version group row count"):
        delta_io._reference_metadata_by_selected_source(
            sources,
            observations,
            versions[:-1],
        )
    with pytest.raises(delta_io.I3DeltaIOError, match="version group lineage"):
        delta_io._reference_metadata_by_selected_source(
            sources,
            observations,
            [versions[0], dict(versions[1], selected_source_record_id=_digest("forged"))],
        )
    with pytest.raises(delta_io.I3DeltaIOError, match="absent from observations"):
        delta_io._reference_metadata_by_selected_source(
            sources,
            observations[:1],
            versions,
        )
    with pytest.raises(delta_io.I3DeltaIOError, match="unexpected ticker group"):
        delta_io._reference_metadata_by_selected_source(
            sources,
            observations,
            [*versions, dict(versions[1], ticker="EXTRA")],
        )


def test_delta_source_digest_commits_sparse_version_projection_rule(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_spec, _, loaded, _ = _integration_fixture(tmp_path)
    original = delta_io._delta_source_digest(run_spec, loaded.binding)
    monkeypatch.setattr(
        delta_io,
        "I3_PRODUCTION_DELTA_SOURCE_VERSION_PROJECTION_RULE_VERSION",
        "s7_5_i3_production_delta_sparse_source_version_projection_forged",
    )
    assert delta_io._delta_source_digest(run_spec, loaded.binding) != original


def test_pending_projectors_are_closed_for_gate_b_miss_and_expired_sor() -> None:
    base = _base_run_spec()
    spec = replace(
        base,
        terminal_session=PARENT_SESSION,
        i2_base_frontier=replace(base.i2_base_frontier, terminal_session=PARENT_SESSION),
    )
    registry_pins = tuple(
        RegistryReleasePin(
            registry_name=item.registry_kind.value,
            release_id=item.release_id,
            manifest_path=item.artifact.path,
            manifest_sha256=item.artifact.sha256,
            manifest_bytes=item.artifact.bytes,
            release_available_session=item.release_available_session,
        )
        for item in spec.identity_policy_bundle.registry_releases
    )
    binding = SimpleNamespace(
        s4_release_set_id=spec.s4_v1_source.object_id,
        registry_pins=registry_pins,
        gate_c=SimpleNamespace(
            identity_case_preview_id=_digest("case-preview"),
            identity_case_preview_manifest=SimpleNamespace(sha256=_digest("case-bytes")),
            candidate_id=_digest("market-candidate"),
            candidate_manifest=SimpleNamespace(sha256=_digest("market-bytes")),
        ),
    )
    target_rows = tuple(
        sorted(
            (
                _pending_row("ALA", run_spec=spec),
                _pending_row("SOR", run_spec=spec, sor_expired=True),
            ),
            key=lambda row: str(row["ticker"]),
        )
    )
    sources, _, _ = _i2_rows(target_rows)
    for source, reason, gate in (
        (sources[0], "gate_b_reference_unattempted", None),
        (
            sources[1],
            "provider_composite_override_scope_expired",
            {
                "classification": "known_us",
                "selected_market_code": "US",
                "source_available_session": RUN_AVAILABLE,
            },
        ),
    ):
        projection = delta_io._pending_projection(
            source,
            run_spec=replace(
                spec,
                run_kind=I3ProductionRunKind.DELTA,
                terminal_session=TARGET_SESSION,
                i2_base_frontier=None,
                i2_receipts=(
                    I3ProductionI2ReceiptPin(
                        session_date=TARGET_SESSION,
                        receipt_id=_digest("pending-i2"),
                        artifact=ArtifactPin(
                            path="manifests/s4/pending-i2.json",
                            sha256=_digest("pending-i2-bytes"),
                            bytes=10,
                        ),
                        receipt_available_session=date(2026, 8, 4),
                    ),
                ),
                parent_release=NativeV2ParentReleasePin.from_manifest(
                    NativeV2ReleaseManifest(
                        release_family=NATIVE_V2_RELEASE_FAMILY,
                        terminal_session=PARENT_SESSION,
                        release_available_session=RUN_AVAILABLE,
                        native_v2_migration_id=spec.native_v2_migration_id,
                        identity_policy_bundle_id=(
                            spec.identity_policy_bundle.identity_policy_bundle_id
                        ),
                        transform_semantics_digest=spec.transform_semantics_digest,
                        resolved_state_digest=_digest("pending-parent-state"),
                        output_artifacts=tuple(
                            NativeV2OutputArtifact(
                                table_name=name,
                                session_date=PARENT_SESSION,
                                row_count=0,
                                contract_id=I3_V2_CONTRACTS[name].contract_id,
                                schema_digest=I3_V2_CONTRACTS[name].schema_digest,
                                artifact=ArtifactPin(
                                    path=f"silver/i3/pending/{name}.json",
                                    sha256=_digest(f"pending-{name}"),
                                    bytes=10,
                                ),
                            )
                            for name in I3_V2_TABLE_ORDER
                        ),
                    ),
                    path="manifests/i3/pending/native.json",
                ),
                parent_checkpoint_artifact=ArtifactPin(
                    path="manifests/i3/pending/checkpoint.json",
                    sha256=_digest("pending-checkpoint"),
                    bytes=10,
                ),
                parent_gate_a_manifest=ManifestPin(
                    release_id=_digest("pending-gate"),
                    manifest_path="manifests/i3/pending/gate.json",
                    manifest_sha256=_digest("pending-gate-bytes"),
                    manifest_bytes=10,
                    release_available_session=RUN_AVAILABLE,
                ),
                parent_shadow_completion_artifact=ArtifactPin(
                    path="manifests/i3/pending/completion.json",
                    sha256=_digest("pending-completion"),
                    bytes=10,
                ),
                parent_deep_attestation_artifact=ArtifactPin(
                    path="manifests/i3/pending/deep.json",
                    sha256=_digest("pending-deep"),
                    bytes=10,
                ),
                parent_authority=I3ProductionParentAuthority.MIGRATION_SHADOW,
            ),
            gate_row=gate,
            reason=reason,
        )
        row = delta_io._build_delta_resolved_row(
            source,
            projection,
            run_spec=spec,
            source_binding=binding,
            fallback_reason=reason,
        )
        assert row["active_on_date"] is True
        assert row["backtest_identity_eligible"] is False
        assert row["identity_quality_liquidation_signal"] is False
        assert all(
            row[name] is None
            for name in (
                "asset_id",
                "issuer_id",
                "ticker_alias_id",
                "canonical_composite_figi",
                "canonical_cik_normalized",
            )
        )
    assert target_rows[1]["observed_composite_figi"] == "BBG000KMY6N2"


def test_expired_sor_requires_exact_terminal_scope_and_effective_precedence(
    tmp_path: Path,
) -> None:
    run_spec, _, _, target_rows = _integration_fixture(tmp_path)
    sources, _, _ = _i2_rows(target_rows)
    source = next(item for item in sources if item["ticker"] == "SOR")
    source_s4 = run_spec.s4_v1_source.object_id
    dates = (
        *(
            delta_io._SOR_OVERRIDE_VALID_FROM + timedelta(days=index)
            for index in range(delta_io._SOR_OVERRIDE_SOURCE_ROW_COUNT - 1)
        ),
        delta_io._SOR_OVERRIDE_VALID_THROUGH,
    )
    rows = tuple(
        ExactSourceRow(
            session_date=session,
            source_record_id=_digest(f"old-sor-{index}"),
            source_dataset="asset_observation_daily",
            source_s4_release_set_id=source_s4,
            provider_id="massive",
            provider_market="stocks",
            provider_locale="us",
            ticker="SOR",
            observed_composite_figi=delta_io._SOR_OVERRIDE_OBSERVED_COMPOSITE,
            observed_share_class_figi=delta_io._SOR_OVERRIDE_OBSERVED_SHARE_CLASS,
            primary_exchange_mic=source["primary_exchange_mic"],
        )
        for index, session in enumerate(dates)
    )
    scope = ExactSourceScope(rows=tuple(sorted(rows)))
    old_id = _digest("old-sor-override")
    decision = {
        "provider_id": "massive",
        "provider_market": "stocks",
        "provider_locale": "us",
        "observed_ticker": "SOR",
        "observed_composite_figi": delta_io._SOR_OVERRIDE_OBSERVED_COMPOSITE,
        "canonical_composite_figi": delta_io._SOR_OVERRIDE_CANONICAL_COMPOSITE,
        "observed_composite_market_code": "US",
        "canonical_composite_market_code": "US",
        "source_s4_release_set_id": source_s4,
        "valid_from_session": delta_io._SOR_OVERRIDE_VALID_FROM,
        "valid_through_session": delta_io._SOR_OVERRIDE_VALID_THROUGH,
        "scoped_source_record_count": delta_io._SOR_OVERRIDE_SOURCE_ROW_COUNT,
    }

    def registry(release):
        return SimpleNamespace(by_name=lambda name: release)

    release = SimpleNamespace(
        decision_rows={old_id: decision},
        source_scopes={old_id: scope},
        effective_decision_ids=lambda *, cutoff_session: (old_id,),
    )
    assert delta_io._expired_provider_override_subject(
        source,
        registries=registry(release),
        cutoff_session=run_spec.source_cutoff_session,
        source_s4_release_set_id=source_s4,
    )

    current_id = _digest("current-sor-override")
    current_scope = ExactSourceScope(
        rows=(
            ExactSourceRow(
                session_date=TARGET_SESSION,
                source_record_id=source["selected_source_record_id"],
                source_dataset="asset_observation_daily",
                source_s4_release_set_id=source_s4,
                provider_id="massive",
                provider_market="stocks",
                provider_locale="us",
                ticker="SOR",
                observed_composite_figi=source["composite_figi"],
                observed_share_class_figi=source["share_class_figi"],
                primary_exchange_mic=source["primary_exchange_mic"],
            ),
        )
    )
    release_with_current = SimpleNamespace(
        decision_rows={old_id: decision},
        source_scopes={old_id: scope, current_id: current_scope},
        effective_decision_ids=lambda *, cutoff_session: (old_id, current_id),
    )
    assert not delta_io._expired_provider_override_subject(
        source,
        registries=registry(release_with_current),
        cutoff_session=run_spec.source_cutoff_session,
        source_s4_release_set_id=source_s4,
    )

    malformed_rows = (
        *scope.rows[:-1],
        replace(
            scope.rows[-1],
            primary_exchange_mic=("XNYS" if source["primary_exchange_mic"] == "XNAS" else "XNAS"),
        ),
    )
    malformed = SimpleNamespace(
        decision_rows={old_id: decision},
        source_scopes={old_id: ExactSourceScope(rows=tuple(sorted(malformed_rows)))},
        effective_decision_ids=lambda *, cutoff_session: (old_id,),
    )
    with pytest.raises(delta_io.I3DeltaIOError, match="terminal observed row"):
        delta_io._expired_provider_override_subject(
            source,
            registries=registry(malformed),
            cutoff_session=run_spec.source_cutoff_session,
            source_s4_release_set_id=source_s4,
        )

    gate = {
        "classification": "known_us",
        "selected_market_code": "US",
        "source_available_session": RUN_AVAILABLE,
    }
    projection = delta_io._pending_projection(
        source,
        run_spec=run_spec,
        gate_row=gate,
        reason="provider_composite_override_scope_expired",
    )
    assert projection.identity_evidence_available_session == RUN_AVAILABLE
    with pytest.raises(delta_io.I3DeltaIOError, match="exact US Gate-B"):
        delta_io._pending_projection(
            source,
            run_spec=run_spec,
            gate_row={**gate, "classification": "known_non_us"},
            reason="provider_composite_override_scope_expired",
        )


def test_delta_parquet_append_prefix_seal_tamper_and_no_clobber(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_spec, parent, loaded, target_rows = _integration_fixture(tmp_path)
    with pytest.raises(delta_io.I3DeltaIOError, match="module-owned byte cap"):
        replace(
            loaded.binding,
            transitive_control_replay_bytes=(
                delta_io.I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP + 1
            ),
        )
    workspace = tmp_path / "silver/i3/delta/workspace"
    workspace.mkdir(parents=True)
    monkeypatch.setattr(delta_io, "_require_delta_controls", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        delta_io,
        "load_production_delta_input_binding",
        lambda **kwargs: loaded,
    )
    monkeypatch.setattr(
        delta_io,
        "_resolve_target_rows",
        lambda *args, **kwargs: (
            target_rows,
            {
                "gate_b_reference_unattempted": 16,
                "provider_composite_override_scope_expired": 1,
            },
        ),
    )

    prepared = delta_io.prepare_production_delta(
        data_root=tmp_path,
        run_spec=run_spec,
        parent=parent,
        workspace=workspace,
    )

    assert type(prepared) is delta_io.DeltaPreparedMaterialization
    assert prepared.checkpoint.last_session == TARGET_SESSION
    assert prepared.canonical_projection_difference_count == 0
    assert prepared.resource_observation.temporary_bytes > 0
    parent_outputs = {item.table_name: item for item in parent.receipt.output_set.table_outputs}
    for output in prepared.table_outputs[:-1]:
        parent_rowset = parent_outputs[output.table_name].rowset_index
        assert output.storage is I3ProductionOutputStorage.ROWSET_INDEX
        assert output.rowset_index.segments[:-1] == parent_rowset.segments
        assert len(output.rowset_index.segments) == len(parent_rowset.segments) + 1
    child_universe = prepared.table_outputs[-1].dataset_index
    parent_universe = parent_outputs["universe_daily"].dataset_index
    assert child_universe.partitions[:-1] == parent_universe.partitions
    assert child_universe.partitions[-1].session_date == TARGET_SESSION
    universe_rows = (
        pq.ParquetFile(tmp_path / child_universe.partitions[-1].artifact.path).read().to_pylist()
    )
    pending = [row for row in universe_rows if not row["backtest_identity_eligible"]]
    assert len(pending) == 17
    assert all(row["active_on_date"] is True for row in pending)
    assert all(row["alias_segment_id"] is None for row in pending)
    assert all(row["identity_quality_liquidation_signal"] is False for row in universe_rows)
    assert any(item.operation.value == "mechanical_successor" for item in prepared.row_versions)

    first_output = prepared.table_outputs[0]
    first_rowset = first_output.rowset_index
    assert first_rowset is not None
    wrong_segment_rowset = replace(
        first_rowset,
        segments=(
            *first_rowset.segments[:-1],
            replace(
                first_rowset.segments[-1],
                segment_id=_digest("executor-wrong-delta-segment"),
            ),
        ),
    )
    wrong_segment_output = replace(
        first_output,
        manifest_output=replace(
            first_output.manifest_output,
            artifact=wrong_segment_rowset.exact_pin(
                path=first_output.manifest_output.artifact.path
            ),
        ),
        rowset_index=wrong_segment_rowset,
    )
    with pytest.raises(production_executor.I3ProductionStageError, match="module-owned identity"):
        production_executor._validate_prepared(
            tmp_path,
            run_spec,
            replace(
                prepared,
                table_outputs=(wrong_segment_output, *prepared.table_outputs[1:]),
            ),
            parent=parent,
        )

    verified = delta_io.verify_delta_materialization_attestation(
        data_root=tmp_path,
        run_spec=run_spec,
        parent=parent,
        prepared=prepared,
    )
    assert verified is prepared.delta_materialization_attestation

    first_row = prepared.row_versions[0]
    wrong_validator = replace(
        prepared,
        row_versions=(
            replace(
                first_row,
                validator_semantics_digest=_digest("wrong-delta-row-validator"),
            ),
            *prepared.row_versions[1:],
        ),
    )
    with pytest.raises(production_executor.I3ProductionStageError, match="module-owned"):
        production_executor._gate_a_row_version_change_index(
            tmp_path,
            run_spec,
            wrong_validator,
            parent=parent,
        )

    row_index, _attestation = production_executor._gate_a_row_version_change_index(
        tmp_path,
        run_spec,
        prepared,
        parent=parent,
    )
    index_table = pq.read_table(tmp_path / row_index.artifact.path)
    index_rows = index_table.to_pylist()
    index_rows[0]["validator_semantics_digest"] = _digest("wrong-durable-validator")
    index_rows[0]["semantic_proof_digest"] = production_contract._row_change_index_proof_digest(
        index_rows[0]
    )
    tampered_table = pa.Table.from_pylist(
        index_rows,
        schema=production_contract._ROW_VERSION_CHANGE_INDEX_SCHEMA,
    )
    sink = pa.BufferOutputStream()
    pq.write_table(tampered_table, sink, compression="zstd", version="2.6")
    tampered_artifact = _write_bytes(
        tmp_path,
        "manifests/i3/delta/tampered-row-version-change-index.parquet",
        sink.getvalue().to_pybytes(),
    )
    superseded = tuple(
        sorted(
            str(item["predecessor_row_version_id"])
            for item in index_rows
            if item["predecessor_row_version_id"] is not None
        )
    )
    tampered_index = RowVersionChangeIndexPin(
        artifact=tampered_artifact,
        row_count=len(index_rows),
        logical_receipts_digest=(
            production_contract._row_change_index_logical_receipts_digest(index_rows)
        ),
        superseded_row_version_count=len(superseded),
        superseded_row_version_ids_digest=(
            production_contract._row_change_index_supersession_digest(superseded)
        ),
        schema_digest=production_contract._ROW_VERSION_CHANGE_INDEX_SCHEMA_DIGEST,
        availability_session=run_spec.run_available_session,
    )
    with pytest.raises(production_contract.I3ProductionContractError, match="module-owned"):
        production_contract._verify_gate_a_indexed_row_changes(
            tmp_path,
            SimpleNamespace(
                row_version_change_index=tampered_index,
                availability_cutoff_session=run_spec.run_available_session,
                release_type=ReleaseType.DELTA,
            ),
            prepared.checkpoint,
            versioned_tables_by_artifact={},
            parent_staging=parent,
        )

    forged_observation = replace(
        prepared.resource_observation,
        peak_rss_bytes=1,
        minimum_disk_free_bytes=run_spec.resource_caps.disk_free_bytes_hard_floor,
    )
    with pytest.raises(delta_io.I3DeltaIOError, match="attestation differs"):
        delta_io.verify_delta_materialization_attestation(
            data_root=tmp_path,
            run_spec=run_spec,
            parent=parent,
            prepared=replace(prepared, resource_observation=forged_observation),
        )
    forged_checkpoint_artifact = replace(
        prepared.checkpoint_artifact,
        sha256=_digest("forged-child-checkpoint"),
    )
    with pytest.raises(delta_io.I3DeltaIOError, match="attestation differs"):
        delta_io.verify_delta_materialization_attestation(
            data_root=tmp_path,
            run_spec=run_spec,
            parent=parent,
            prepared=replace(
                prepared,
                checkpoint_artifact=forged_checkpoint_artifact,
            ),
        )
    with pytest.raises(delta_io.I3DeltaIOError, match="official nominal"):
        delta_io.verify_delta_materialization_attestation(
            data_root=tmp_path,
            run_spec=run_spec,
            parent=parent,
            prepared=SimpleNamespace(
                table_outputs=prepared.table_outputs,
                native_manifest=prepared.native_manifest,
                native_manifest_artifact=prepared.native_manifest_artifact,
                checkpoint=prepared.checkpoint,
                checkpoint_artifact=prepared.checkpoint_artifact,
                source_digest=prepared.source_digest,
                resource_observation=prepared.resource_observation,
                canonical_projection_difference_count=0,
                row_versions=prepared.row_versions,
                delta_materialization_attestation=(prepared.delta_materialization_attestation),
            ),
        )
    with pytest.raises(delta_io.I3DeltaIOError, match="workspace is not empty"):
        delta_io.prepare_production_delta(
            data_root=tmp_path,
            run_spec=run_spec,
            parent=parent,
            workspace=workspace,
        )
    new_segment = prepared.table_outputs[0].rowset_index.segments[-1]
    segment_path = tmp_path / new_segment.artifact.path
    segment_path.chmod(0o644)
    segment_path.write_bytes(segment_path.read_bytes() + b"tampered")
    with pytest.raises(delta_io.I3DeltaIOError, match="exact artifact differs"):
        delta_io.verify_delta_materialization_attestation(
            data_root=tmp_path,
            run_spec=run_spec,
            parent=parent,
            prepared=prepared,
        )


def test_input_binding_and_explicit_paths_are_bounded() -> None:
    with pytest.raises(delta_io.I3DeltaIOError, match="explicit"):
        delta_io._explicit_path("silver/i3/latest/receipt.json")
    with pytest.raises(delta_io.I3DeltaIOError, match="explicit"):
        delta_io._explicit_path("silver/i3/session_date=*/part.parquet")
    assert delta_io.PRODUCTION_DELTA_BOUNDARY_SESSIONS == CALENDAR


def test_delta_config_is_canonical_immutable_idempotent_and_exact(tmp_path: Path) -> None:
    config = _delta_config()
    assert delta_io.I3ProductionDeltaRunConfig.from_dict(config.to_dict()) == config
    first = delta_io.store_i3_production_delta_config(tmp_path, config)
    second = delta_io.store_i3_production_delta_config(tmp_path, config)
    assert second == first == config.exact_pin(path=first.path)
    assert delta_io.load_i3_production_delta_config_exact(first, data_root=tmp_path) == config

    copied_content = config.canonical_bytes()
    copied = _write_bytes(
        tmp_path,
        "manifests/silver/identity/s7-5-native-v2-staging/copied-delta-config.json",
        copied_content,
    )
    with pytest.raises(delta_io.I3DeltaIOError, match="misplaced"):
        delta_io.load_i3_production_delta_config_exact(copied, data_root=tmp_path)

    path = tmp_path / first.path
    path.chmod(0o644)
    path.write_bytes(b'{"tampered":true}\n')
    with pytest.raises(delta_io.I3DeltaIOError, match="exact artifact differs"):
        delta_io.load_i3_production_delta_config_exact(first, data_root=tmp_path)
    with pytest.raises(delta_io.I3DeltaIOError, match="immutably store"):
        delta_io.store_i3_production_delta_config(tmp_path, config)


def test_delta_config_rejects_tmp_latest_noncanonical_and_unknown_fields(
    tmp_path: Path,
) -> None:
    config = _delta_config()
    with pytest.raises(delta_io.I3DeltaIOError, match="explicit"):
        replace(
            config,
            parent_completion_artifact=replace(
                config.parent_completion_artifact,
                path="tmp/copied-completion.json",
            ),
        )
    with pytest.raises(delta_io.I3DeltaIOError, match="explicit"):
        replace(
            config,
            parent_deep_attestation_artifact=replace(
                config.parent_deep_attestation_artifact,
                path="manifests/latest/deep.json",
            ),
        )
    with pytest.raises(delta_io.I3DeltaIOError, match="fixed target control"):
        replace(
            config,
            i2_receipt_artifact=replace(
                config.i2_receipt_artifact,
                path="manifests/silver/incremental/s4/assets/2026-07-10.json",
            ),
        )
    unknown = config.to_dict()
    unknown["extra"] = True
    with pytest.raises(delta_io.I3DeltaIOError, match="fields differ"):
        delta_io.I3ProductionDeltaRunConfig.from_dict(unknown)

    pretty = json.dumps(config.to_dict(), indent=2, sort_keys=True).encode() + b"\n"
    relative = delta_io._delta_config_relative(config)
    pin = _write_bytes(tmp_path, relative, pretty)
    with pytest.raises(delta_io.I3DeltaIOError, match="noncanonical"):
        delta_io.load_i3_production_delta_config_exact(pin, data_root=tmp_path)


def test_prepare_delta_run_spec_persists_idempotently_from_exact_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _delta_config()
    config_pin = delta_io.store_i3_production_delta_config(tmp_path, config)
    run_spec, _parent, _loaded, _target = _integration_fixture(tmp_path)
    observed: list[delta_io.I3ProductionDeltaRunConfig] = []

    def build(*, data_root: Path, config: delta_io.I3ProductionDeltaRunConfig):
        assert data_root == tmp_path
        observed.append(config)
        return run_spec

    monkeypatch.setattr(delta_io, "build_production_delta_run_spec", build)
    first = delta_io.prepare_i3_production_delta_run_spec(tmp_path, config_pin)
    second = delta_io.prepare_i3_production_delta_run_spec(tmp_path, config_pin)
    assert first == second
    assert first.config_artifact == config_pin
    assert first.run_spec == run_spec
    assert first.run_spec_artifact == run_spec.exact_pin(
        path=(
            "manifests/silver/identity/s7-5-native-v2-staging/run-specs/"
            f"run_spec_id={run_spec.run_spec_id}/run-spec.json"
        )
    )
    assert observed == [config, config]


def test_gate_c_aggregate_control_closes_bytes_before_leaf_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_by_role = _gate_c_aggregate_fixture(tmp_path)
    subtree = delta_io._gate_c_opaque_replay_subtree(tmp_path, source_by_role)
    assert subtree.subtree_id == source_by_role["source_gate_c_completion_manifest"].artifact_id
    assert subtree.replay_bytes == 65 + sum(item.bytes for item in subtree.control_artifacts)

    inconsistent_root = tmp_path / "inconsistent"
    inconsistent = _gate_c_aggregate_fixture(
        inconsistent_root,
        declared_output_bytes=66,
    )
    with pytest.raises(delta_io.I3DeltaIOError, match="byte aggregate differs"):
        delta_io._gate_c_opaque_replay_subtree(inconsistent_root, inconsistent)

    over_cap_root = tmp_path / "over-cap"
    over_cap = _gate_c_aggregate_fixture(
        over_cap_root,
        output_bytes=(
            delta_io.I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP,
            1,
            1,
            1,
            1,
        ),
    )
    with pytest.raises(delta_io.I3DeltaIOError, match="module-owned byte cap"):
        delta_io._gate_c_opaque_replay_subtree(over_cap_root, over_cap)

    oversized_preview_root = tmp_path / "oversized-preview"
    oversized_preview = _gate_c_aggregate_fixture(
        oversized_preview_root,
        detector_preview_bytes=(delta_io.I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP),
    )
    with pytest.raises(delta_io.I3DeltaIOError, match="module-owned byte cap"):
        delta_io._gate_c_opaque_replay_subtree(
            oversized_preview_root,
            oversized_preview,
        )

    read_labels: list[str] = []
    original_pin_from_path = delta_io._artifact_pin_from_path_sha
    original_control_reader = delta_io._canonical_control_document

    def oversized_plan(root: Path, path: str, sha256: str) -> ArtifactPin:
        pin = original_pin_from_path(root, path, sha256)
        if path == "controls/gate-c/plan.json":
            return replace(
                pin,
                bytes=delta_io.I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP,
            )
        return pin

    def observed_control_reader(root: Path, pin: ArtifactPin, label: str):
        read_labels.append(label)
        return original_control_reader(root, pin, label)

    monkeypatch.setattr(delta_io, "_artifact_pin_from_path_sha", oversized_plan)
    monkeypatch.setattr(delta_io, "_canonical_control_document", observed_control_reader)
    with pytest.raises(delta_io.I3DeltaIOError, match="module-owned byte cap"):
        delta_io._gate_c_opaque_replay_subtree(tmp_path, source_by_role)
    assert "Gate-C plan" not in read_labels
    assert "Gate-C authorization" not in read_labels


def test_exact_group_aggregate_control_binds_candidate_tree_bytes(tmp_path: Path) -> None:
    source_by_role = _exact_group_aggregate_fixture(tmp_path)
    subtree = delta_io._exact_group_opaque_replay_subtree(tmp_path, source_by_role)
    assert (
        subtree.subtree_id == source_by_role["source_exact_group_completion_manifest"].artifact_id
    )

    tampered_root = tmp_path / "tampered"
    tampered = _exact_group_aggregate_fixture(tampered_root, tree_byte_delta=1)
    with pytest.raises(delta_io.I3DeltaIOError, match="tree byte aggregate differs"):
        delta_io._exact_group_opaque_replay_subtree(tampered_root, tampered)


def test_transitive_replay_cap_and_unknown_registry_source_fail_closed() -> None:
    with pytest.raises(delta_io.I3DeltaIOError, match="module-owned byte cap"):
        delta_io._validate_transitive_control_replay_bytes(
            delta_io.I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP + 1
        )


def test_loader_rejects_fixed_resource_floor_before_parent_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_spec, _, _, _ = _integration_fixture(tmp_path)
    low_cap_spec = replace(
        run_spec,
        resource_caps=replace(
            run_spec.resource_caps,
            rss_bytes_hard_cap=(
                1024**3
                + delta_io.I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_RSS_RESERVE_BYTES
                - 1
            ),
        ),
    )
    parent_loader_called = False

    def unexpected_parent_loader(*args, **kwargs):
        nonlocal parent_loader_called
        parent_loader_called = True
        raise AssertionError("parent loader must not run below the fixed resource floor")

    monkeypatch.setattr(
        delta_io,
        "load_i3_production_parent_shallow_exact",
        unexpected_parent_loader,
    )
    with pytest.raises(delta_io.I3DeltaIOError, match="fixed reserve"):
        delta_io.load_production_delta_materializer(
            data_root=tmp_path,
            run_spec=low_cap_spec,
        )
    assert parent_loader_called is False
    with pytest.raises(delta_io.I3DeltaIOError, match="temporary cap"):
        delta_io._preflight_delta_entry_resources(
            tmp_path,
            replace(
                run_spec.resource_caps,
                temporary_bytes_hard_cap=(
                    delta_io.I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP - 1
                ),
            ),
            (),
        )
    with pytest.raises(delta_io.I3DeltaIOError, match="unknown or incomplete"):
        delta_io._registry_opaque_replay_subtree(
            Path("/does-not-matter"),
            SimpleNamespace(source_artifacts=(SimpleNamespace(role="caller_supplied_unknown"),)),
        )


def test_resource_estimate_reads_all_inputs_but_sizes_output_from_target() -> None:
    spec = _base_run_spec()
    run_spec = SimpleNamespace(
        run_kind=I3ProductionRunKind.DELTA,
        resource_caps=spec.resource_caps,
    )
    parent = tuple(
        SimpleNamespace(
            artifact=ArtifactPin(
                path=f"parent/{index}",
                sha256=_digest(str(index)),
                bytes=1_000,
            )
        )
        for index in range(2)
    )
    i2 = tuple(
        SimpleNamespace(
            table_name=table,
            row_count=10,
            artifact=ArtifactPin(
                path=f"i2/{table}",
                sha256=_digest(table),
                bytes=10_000 if table == "universe_source_daily" else 1_000_000,
            ),
        )
        for table in S4_TERMINAL_TABLE_ORDER
    )
    declared = tuple(
        sorted(
            (item.artifact for item in (*parent, *i2)),
            key=lambda item: item.path,
        )
    )
    binding = SimpleNamespace(
        parent_boundary_partitions=parent,
        i2_partitions=i2,
        declared_input_artifacts=declared,
        parent_output_bytes=5_000,
        parent_output_rows=100,
        asset_transition_decision_count=2,
        transitive_control_replay_bytes=1024,
    )

    estimate = delta_io.estimate_production_delta_resources(run_spec, binding)

    assert estimate.source_bytes == (
        2_012_000 + delta_io.I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP
    )
    assert estimate.estimated_output_bytes == 5_000 + 128 * 1024**2
    assert estimate.estimated_output_rows == 144
    assert estimate.estimated_temporary_bytes >= (
        delta_io.I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP
    )
    assert estimate.estimated_peak_rss_bytes >= 2 * 1024**3
    assert estimate.transitive_control_replay_bytes == 1024
    with pytest.raises(delta_io.I3DeltaIOError, match="output rows"):
        delta_io._validate_resource_estimate(
            SimpleNamespace(
                resource_caps=replace(
                    spec.resource_caps,
                    output_rows_hard_cap=estimate.estimated_output_rows - 1,
                )
            ),
            estimate,
        )
