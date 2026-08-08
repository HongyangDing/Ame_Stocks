from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_silver_s7_5_i3_migration_io import _base_fixture
from test_silver_s7_5_i3_production import _patch_exact_upstreams, _write_run_spec

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_i5_shadow_runtime as shadow
from ame_stocks_api.silver.identity_resolution_contract import S7_DERIVED_CONTRACTS
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_contract import I3_V2_CONTRACTS
from ame_stocks_api.silver.incremental_i3_migration_io import CompactBaseMigrationMaterializer
from ame_stocks_api.silver.incremental_i3_production import stage_i3_production_base
from ame_stocks_api.silver.incremental_i5_lifecycle import (
    EquivalenceProjection,
    FailureScenario,
    ResourceGatePolicy,
    validate_shadow_equivalence,
)

CHALLENGE = date(2023, 11, 24)
TARGET = date(2026, 7, 10)
AVAILABLE = date(2026, 8, 7)
REQUIRED_SCOPE = shadow.I5_REQUIRED_COMPARISON_SESSIONS


def _io_counter(
    *,
    rchar: int = 0,
    wchar: int = 0,
    syscr: int = 0,
    syscw: int = 0,
    read_bytes: int = 0,
    write_bytes: int = 0,
) -> shadow._ProcessIOCounter:
    return shadow._ProcessIOCounter(
        rchar=rchar,
        wchar=wchar,
        syscr=syscr,
        syscw=syscw,
        physical_read_bytes=read_bytes,
        physical_write_bytes=write_bytes,
        backend="linux_proc_io",
    )


def _digest(label: str) -> str:
    return stable_digest({"i5-shadow-fixture": label})


def _policy(**overrides: int) -> ResourceGatePolicy:
    values = {
        "max_wall_clock_seconds": 300,
        "max_peak_rss_bytes": 32 * 1024**3,
        "min_free_disk_bytes": 1,
        "max_read_bytes": 64 * 1024**2,
        "max_write_bytes": 64 * 1024**2,
        "max_chain_resolution_milliseconds": 10_000,
    }
    values.update(overrides)
    return ResourceGatePolicy(**values)


def _rows() -> dict[str, dict[str, tuple[dict[str, object], ...]]]:
    asset_id = _digest("asset")
    issuer_id = _digest("issuer")
    alias_id = _digest("alias")
    asset = {
        "asset_id": asset_id,
        "canonical_composite_figi": "BBG000000001",
    }
    issuer = {"issuer_id": issuer_id, "cik_normalized": "0000000001"}
    alias = {
        "ticker_alias_id": alias_id,
        "ticker": "AAA",
        "asset_id": asset_id,
    }

    def universe(session: date, source: str) -> dict[str, object]:
        return {
            "session_date": session.isoformat(),
            "ticker": "AAA",
            "ticker_alias_id": alias_id,
            "asset_id": asset_id,
            "issuer_id": issuer_id,
            "observed_cik_normalized": "0000000001",
            "observed_composite_figi": "BBG000000001",
            "observed_share_class_figi": "BBG000000011",
            "selected_source_record_id": _digest(source),
            "source_selection_status": "selected_unique",
            "source_version_count": 1,
        }

    return {
        "asset_master": {"__table__": (asset,)},
        "ticker_alias": {"__table__": (alias,)},
        "issuer_master": {"__table__": (issuer,)},
        "universe_daily": {
            session.isoformat(): (universe(session, f"source-{session.isoformat()}"),)
            for session in REQUIRED_SCOPE
        },
    }


def _physical(
    label: str,
) -> tuple[
    dict[str, tuple[dict[str, object], ...]],
    dict[str, tuple[dict[str, object], ...]],
    dict[str, tuple[dict[str, object], ...]],
]:
    parent: dict[str, tuple[dict[str, object], ...]] = {}
    child: dict[str, tuple[dict[str, object], ...]] = {}
    oracle: dict[str, tuple[dict[str, object], ...]] = {}
    for table in shadow.I5_TABLE_ORDER:

        def entry(key: str, role: str, *, table_name: str = table) -> dict[str, object]:
            content = f"{table_name}:{key}:{role}".encode()
            return {
                "artifact": {
                    "bytes": len(content),
                    "path": f"silver/fixture/{table_name}/{key}-{role}.parquet",
                    "sha256": hashlib.sha256(content).hexdigest(),
                },
                "partition_key": key,
                "row_count": 1,
                "schema_digest": (
                    S7_DERIVED_CONTRACTS[table_name].schema_digest
                    if role == "oracle"
                    else I3_V2_CONTRACTS[table_name].schema_digest
                ),
            }

        if table == "universe_daily":
            base = tuple(entry(session.isoformat(), "base") for session in REQUIRED_SCOPE[:-1])
            delta = entry(TARGET.isoformat(), "delta")
            oracle_entries = []
            for session in REQUIRED_SCOPE:
                oracle_entry = entry(session.isoformat(), "oracle")
                oracle_entry.pop("partition_key")
                oracle_entries.append(oracle_entry)
            parent[table] = base
            child[table] = (*base, delta)
            oracle[table] = tuple(oracle_entries)
        else:
            prefix_key = _digest(f"{label}-{table}-base-segment")
            suffix_key = _digest(f"{label}-{table}-delta-segment")
            base = entry(prefix_key, "base")
            delta = entry(suffix_key, "delta")
            oracle_entry = entry(suffix_key, "oracle")
            oracle_entry.pop("partition_key")
            parent[table] = (base,)
            child[table] = (base, delta)
            oracle[table] = (oracle_entry,)
    return parent, child, oracle


def _side(
    label: str,
    *,
    rows: dict[str, dict[str, tuple[dict[str, object], ...]]] | None = None,
    physical: dict[str, tuple[dict[str, object], ...]],
) -> shadow._ResolvedSide:
    source_rows = (rows or _rows())["universe_daily"]
    source_digest = shadow._source_lineage_digest(source_rows)
    return shadow._ResolvedSide(
        release_id=_digest(f"{label}-release"),
        rows=rows or _rows(),
        physical=physical,
        source_binding_digest=source_digest,
        schema_bundle_digest=_digest("schema-bundle"),
        transform_semantics_digest=_digest(f"{label}-transform"),
        identity_policy_bundle_id=_digest("policy-bundle"),
        calendar_digest=_digest("calendar"),
        checkpoint_id=_digest(f"{label}-checkpoint"),
        run_receipt_id=_digest(f"{label}-run-receipt"),
        manifest_sha256=_digest(f"{label}-manifest"),
    )


def _execute(tmp_path: Path, **kwargs: object) -> shadow.ShadowRunResult:
    parent, child, oracle_physical = _physical("normal")
    incremental = _side("incremental", physical=child)
    oracle = _side("oracle", physical=oracle_physical)
    values = {
        "incremental": incremental,
        "oracle": oracle,
        "parent_physical": parent,
        "common_parent_release_id": _digest("parent-release"),
        "comparison_sessions": REQUIRED_SCOPE,
        "receipt_available_session": AVAILABLE,
        "resource_policy": _policy(),
    }
    values.update(kwargs)
    return shadow._execute_i5_shadow_fixture(tmp_path, **values)


def _production_rebuild_fixture(
    tmp_path: Path,
    result: shadow.ShadowRunResult,
) -> tuple[shadow.ShadowRunSpec, shadow._ProductionInputs, tuple[dict[str, object], ...]]:
    parent, child, oracle_physical = _physical("production-rebuild")
    incremental = replace(
        _side("incremental-production", physical=child),
        replayed_bytes_floor=100,
    )
    oracle = replace(
        _side("oracle-production", physical=oracle_physical),
        replayed_bytes_floor=100,
    )
    inputs = shadow._ProductionInputs(
        incremental=incremental,
        oracle=oracle,
        loaded_incremental=None,
        loaded_parent=None,
        parent_physical=parent,
        parent_reader_digest=_digest("production-parent-reader"),
        chain_resolution_milliseconds=1,
    )
    production_spec = replace(
        result.run_spec,
        authority=shadow.I5_PRODUCTION_AUTHORITY,
        incremental_completion_artifact=ArtifactPin(
            "manifests/silver/incremental/i3/completions/exact.json",
            _digest("production-incremental-completion"),
            1,
        ),
        incremental_deep_attestation_artifact=ArtifactPin(
            "manifests/silver/incremental/i3/deep-attestations/exact.json",
            _digest("production-deep-attestation"),
            1,
        ),
        full_oracle_completion_artifact=ArtifactPin(
            "manifests/silver/identity/s7-streaming-full-execution-completions/"
            f"plan_id={_digest('production-plan')}/"
            f"approval_id={_digest('production-approval')}/manifest.json",
            _digest("production-full-completion"),
            1,
        ),
    )
    documents = tuple(
        shadow._closed_json(
            (tmp_path / comparison.details_artifact.path).read_bytes(),
            label="stored comparison details",
        )
        for comparison in result.completion.receipt.comparisons
    )
    return production_spec, inputs, documents


def _write_exact(root: Path, relative: str, content: bytes) -> ArtifactPin:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return ArtifactPin(relative, hashlib.sha256(content).hexdigest(), len(content))


def _full_oracle_universe_row() -> dict[str, object]:
    row: dict[str, object] = {}
    for field in S7_DERIVED_CONTRACTS["universe_daily"].arrow_schema:
        if field.nullable:
            row[field.name] = None
        elif pa.types.is_string(field.type):
            row[field.name] = "fixture"
        elif pa.types.is_boolean(field.type):
            row[field.name] = False
        elif pa.types.is_integer(field.type):
            row[field.name] = 0
        elif pa.types.is_date(field.type):
            row[field.name] = TARGET
        elif pa.types.is_list(field.type):
            row[field.name] = []
        else:  # pragma: no cover - closed legacy contract types
            raise AssertionError(field.type)
    row.update(
        {
            "session_year": TARGET.year,
            "session_date": TARGET,
            "ticker": "AAA",
            "observed_cik_normalized": "0000000001",
            "observed_composite_figi": "BBG000000001",
            "observed_share_class_figi": "BBG000000011",
            "selected_source_record_id": _digest("full-source"),
            "source_selection_status": "selected_unique",
            "source_version_count": 1,
        }
    )
    return row


def _full_oracle_fixture(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    missing_unselected: bool = False,
) -> ArtifactPin:
    source_binding_id = _digest("full-source-binding")
    plan_id = _digest("full-plan")
    approval_id = _digest("full-approval")
    approved_at = datetime(2026, 8, 7, 12, tzinfo=UTC)
    captured_at = approved_at + timedelta(minutes=1)
    completed_at = captured_at + timedelta(minutes=1)
    membership_sessions = (CHALLENGE, TARGET) if missing_unselected else (TARGET,)
    binding = SimpleNamespace(
        source_binding_id=source_binding_id,
        cutoff_session=TARGET,
        membership_artifacts=tuple(
            SimpleNamespace(session_date=session) for session in membership_sessions
        ),
        registry_pins=(
            SimpleNamespace(registry_name="identity_adjudication", release_id=_digest("registry")),
        ),
        calendar_artifact_id=_digest("calendar-id"),
        calendar_artifact_sha256=_digest("calendar-sha"),
        row_count=len(membership_sessions),
        session_count=len(membership_sessions),
    )
    plan = {
        "plan_id": plan_id,
        "resource_caps": shadow.full_runtime.StreamingResourceCaps(
            source_bytes_cap=1024**3,
            output_bytes_cap=1024**3,
            tmp_bytes_cap=1024**3,
            wall_clock_seconds_cap=3600,
            session_count_cap=10,
            row_count_cap=10,
            per_session_row_cap=10,
            batch_row_cap=10,
        ).to_dict(),
        "source_binding_id": source_binding_id,
        "runtime_binding": {"runtime_file_set_digest": _digest("runtime")},
    }
    approval = SimpleNamespace(
        approval_id=approval_id,
        plan_id=plan_id,
        approved_at_utc=approved_at,
    )
    monkeypatch.setattr(
        shadow.full_runtime,
        "_load_execution_controls",
        lambda *_args, **_kwargs: {
            "approval": approval,
            "binding": binding,
            "plan": plan,
        },
    )
    monkeypatch.setattr(
        shadow.full_runtime,
        "_calendar_availability",
        lambda *_args, **_kwargs: {"source_available_session": AVAILABLE.isoformat()},
    )

    candidate_id = stable_digest(
        {
            "adapter_version": shadow.full_runtime.PRODUCTION_ADAPTER_VERSION,
            "approval_id": approval_id,
            "engine_version": shadow.full_runtime.STREAMING_POLICY_VERSION,
            "plan_id": plan_id,
            "source_binding_id": source_binding_id,
        }
    )
    official_result: dict[str, str] = {}
    monkeypatch.setattr(
        shadow.full_runtime,
        "_verify_completion_and_candidate",
        lambda *_args, **_kwargs: SimpleNamespace(
            approval_id=approval_id,
            candidate_id=candidate_id,
            completion_id=official_result["completion_id"],
            plan_id=plan_id,
            session_count=binding.session_count,
            source_row_count=binding.row_count,
        ),
    )
    candidate_root = shadow.full_runtime._candidate_path(candidate_id)
    intent_payload = {
        "approval_id": approval_id,
        "artifact_type": "s7_streaming_four_table_full_run_intent",
        "candidate_id": candidate_id,
        "capabilities": dict(shadow.full_runtime._FALSE_CAPABILITIES),
        "captured_at_utc": shadow.full_runtime._utc_text(captured_at),
        "intent_version": shadow.full_runtime.STREAMING_INTENT_VERSION,
        "plan_id": plan_id,
        "source_binding_id": source_binding_id,
        "state": "authorized_awaiting_execution",
    }
    intent = {**intent_payload, "intent_id": stable_digest(intent_payload)}
    intent_pin = _write_exact(
        root,
        "manifests/silver/identity/s7-streaming-full-run-intents/"
        f"plan_id={plan_id}/approval_id={approval_id}/manifest.json",
        shadow._canonical_json_bytes(intent),
    )

    outputs: dict[str, object] = {}
    counts: dict[str, int] = {}
    for table_name in ("asset_master", "ticker_alias", "issuer_master"):
        schema = S7_DERIVED_CONTRACTS[table_name].arrow_schema
        table = pa.Table.from_pylist([], schema=schema)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        content = sink.getvalue().to_pybytes()
        relative = f"data/{table_name}.parquet"
        _write_exact(root, f"{candidate_root}/{relative}", content)
        outputs[table_name] = {
            "bytes": len(content),
            "path": relative,
            "row_count": 0,
            "schema_digest": S7_DERIVED_CONTRACTS[table_name].schema_digest,
            "sha256": hashlib.sha256(content).hexdigest(),
        }
        counts[table_name] = 0

    universe_schema = S7_DERIVED_CONTRACTS["universe_daily"].arrow_schema
    universe = pa.Table.from_pylist([_full_oracle_universe_row()], schema=universe_schema)
    sink = pa.BufferOutputStream()
    pq.write_table(universe, sink)
    universe_content = sink.getvalue().to_pybytes()
    universe_relative = f"data/universe_daily/session_date={TARGET.isoformat()}/part-00000.parquet"
    _write_exact(root, f"{candidate_root}/{universe_relative}", universe_content)
    outputs["universe_daily"] = [
        {
            "bytes": len(universe_content),
            "path": universe_relative,
            "row_count": 1,
            "schema_digest": S7_DERIVED_CONTRACTS["universe_daily"].schema_digest,
            "sha256": hashlib.sha256(universe_content).hexdigest(),
        }
    ]
    counts["universe_daily"] = 1

    qa = {
        "artifact_type": "s7_streaming_four_table_full_qa",
        "bounded_collision_examples": [],
        "bounded_share_class_conflict_examples": [],
        "critical_failure_count": 0,
        "gate_b_relation_share_class_conflict_rows": 0,
        "gate_b_relation_share_class_mismatch_rows": 0,
        "identity_quality_forced_liquidation_rows": 0,
        "inactive_or_delisted_inferred_from_identity_quality_rows": 0,
        "missing_eligible_alias_rows": 0,
        "multi_registry_composite_override_collision_alias_rows": 0,
        "multi_registry_composite_override_collision_eligible_rows": 0,
        "multi_registry_composite_override_collision_resolved_rows": 0,
        "multi_registry_composite_override_collision_rows": 0,
        "publish_authorized": False,
        "reference_inventory_unattempted_rows": 0,
        "session_count": 1,
        "share_class_correction_before_unique_composite_rows": 0,
        "source_membership_omission_or_duplication_rows": 0,
        "source_membership_rows": 1,
        "source_membership_streaming_lineage_digest": _digest("full-lineage"),
        "state": shadow.full_runtime.STREAMING_STATE,
        "table_row_counts": counts,
        "ticker_alias_rows": 0,
        "transition_automatic_return_stitching_rows": 0,
        "unapproved_canonical_override_rows": 0,
        "unadjudicated_gate_b_share_class_conflict_eligible_rows": 0,
        "unadjudicated_gate_b_share_class_conflict_rows": 0,
        "unknown_or_unapproved_foreign_identity_eligible_rows": 0,
        "unresolved_rows": 0,
    }
    qa_content = shadow._canonical_json_bytes(qa)
    _write_exact(root, f"{candidate_root}/qa/qa.json", qa_content)
    outputs["qa"] = {
        "bytes": len(qa_content),
        "path": "qa/qa.json",
        "sha256": hashlib.sha256(qa_content).hexdigest(),
    }

    candidate_payload = {
        "adapter_version": shadow.full_runtime.PRODUCTION_ADAPTER_VERSION,
        "approval_id": approval_id,
        "artifact_type": "s7_streaming_four_table_full_candidate",
        "candidate_id": candidate_id,
        "candidate_version": shadow.full_runtime.STREAMING_CANDIDATE_VERSION,
        "capabilities": dict(shadow.full_runtime._FALSE_CAPABILITIES),
        "contract_pins": shadow.full_runtime._contract_pins(),
        "intent": intent_pin.to_dict(),
        "outputs": outputs,
        "plan_id": plan_id,
        "policy_version": shadow.full_runtime.STREAMING_POLICY_VERSION,
        "source_binding_id": source_binding_id,
        "state": shadow.full_runtime.STREAMING_STATE,
        "table_row_counts": counts,
    }
    candidate = {**candidate_payload, "manifest_id": stable_digest(candidate_payload)}
    candidate_pin = _write_exact(
        root,
        f"{candidate_root}/manifest.json",
        shadow._canonical_json_bytes(candidate),
    )
    completion_payload = {
        "approval_id": approval_id,
        "artifact_type": "s7_streaming_four_table_full_execution_completion",
        "candidate_id": candidate_id,
        "candidate_manifest": candidate_pin.to_dict(),
        "capabilities": dict(shadow.full_runtime._FALSE_CAPABILITIES),
        "complete": True,
        "completed_at_utc": shadow.full_runtime._utc_text(completed_at),
        "completion_state": shadow.full_runtime.STREAMING_STATE,
        "completion_version": shadow.full_runtime.STREAMING_COMPLETION_VERSION,
        "plan_id": plan_id,
        "raw_collision_rows": 0,
        "source_binding_id": source_binding_id,
        "source_row_count": 1,
        "table_row_counts": counts,
    }
    completion = {**completion_payload, "completion_id": stable_digest(completion_payload)}
    official_result["completion_id"] = completion["completion_id"]
    return _write_exact(
        root,
        shadow.full_runtime._completion_path(plan_id, approval_id),
        shadow._canonical_json_bytes(completion),
    )


def test_exact_full_oracle_candidate_replays_intent_outputs_and_qa(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completion_pin = _full_oracle_fixture(tmp_path, monkeypatch)
    meter = shadow._ReadMeter(tmp_path)

    oracle, binding = shadow._full_oracle_side(
        tmp_path,
        completion_pin=completion_pin,
        comparison_sessions=(TARGET,),
        receipt_available_session=AVAILABLE,
        meter=meter,
    )

    assert oracle.release_id == stable_digest(
        {
            "adapter_version": shadow.full_runtime.PRODUCTION_ADAPTER_VERSION,
            "approval_id": _digest("full-approval"),
            "engine_version": shadow.full_runtime.STREAMING_POLICY_VERSION,
            "plan_id": _digest("full-plan"),
            "source_binding_id": _digest("full-source-binding"),
        }
    )
    assert binding.cutoff_session == TARGET
    assert tuple(oracle.rows["universe_daily"]) == (TARGET.isoformat(),)
    assert len(oracle.rows["universe_daily"][TARGET.isoformat()]) == 1
    assert oracle.source_binding_digest == shadow._source_lineage_digest(
        oracle.rows["universe_daily"]
    )
    assert meter.bytes > completion_pin.bytes


def test_exact_full_oracle_rejects_tampered_candidate_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completion_pin = _full_oracle_fixture(tmp_path, monkeypatch)
    completion = shadow._closed_json(
        (tmp_path / completion_pin.path).read_bytes(),
        label="Full completion fixture",
    )
    candidate_pin = ArtifactPin(**completion["candidate_manifest"])
    candidate = shadow._closed_json(
        (tmp_path / candidate_pin.path).read_bytes(),
        label="Full candidate fixture",
    )
    output = candidate["outputs"]["universe_daily"][0]
    output_path = (
        tmp_path / shadow.full_runtime._candidate_path(candidate["candidate_id"]) / output["path"]
    )
    output_path.write_bytes(output_path.read_bytes() + b"tampered")

    with pytest.raises(shadow.I5ShadowRuntimeError, match="exact pin differs"):
        shadow._full_oracle_side(
            tmp_path,
            completion_pin=completion_pin,
            comparison_sessions=(TARGET,),
            receipt_available_session=AVAILABLE,
            meter=shadow._ReadMeter(tmp_path),
        )


def test_full_official_replay_rejects_missing_unselected_partition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completion_pin = _full_oracle_fixture(
        tmp_path,
        monkeypatch,
        missing_unselected=True,
    )

    def reject_missing_partition(*_args: object, **_kwargs: object) -> object:
        raise shadow.full_runtime.S7StreamingMaterializationError(
            "candidate file set differs: missing unselected partition"
        )

    monkeypatch.setattr(
        shadow.full_runtime,
        "_verify_completion_and_candidate",
        reject_missing_partition,
    )
    with pytest.raises(shadow.I5FullOracleSeamError, match="official Full tree replay failed"):
        shadow._full_oracle_side(
            tmp_path,
            completion_pin=completion_pin,
            comparison_sessions=(TARGET,),
            receipt_available_session=AVAILABLE,
            meter=shadow._ReadMeter(tmp_path),
        )


def test_fixture_shadow_writes_exact_gate_b_free_awaiting_review_receipt(
    tmp_path: Path,
) -> None:
    result = _execute(tmp_path)

    assert result.completion.authority == shadow.I5_FIXTURE_AUTHORITY
    assert result.completion.logical_payload()["state"] == "awaiting_review"
    assert result.completion.logical_payload()["gate_b_authorized"] is False
    assert result.completion.logical_payload()["publish_authorized"] is False
    assert tuple(item.projection for item in result.completion.receipt.comparisons) == tuple(
        EquivalenceProjection
    )
    assert tuple(item.scenario for item in result.completion.receipt.failure_recovery) == tuple(
        FailureScenario
    )
    assert all(
        item.parent_reader_before_digest == item.parent_reader_after_digest
        and item.unpublished_visible_count == 0
        and item.deleted_artifact_count == 0
        for item in result.completion.receipt.failure_recovery
    )
    observation = result.completion.receipt.resource_observation
    expected_written = (
        result.run_spec_artifact.bytes
        + result.completion_artifact.bytes
        + sum(item.details_artifact.bytes for item in result.completion.receipt.comparisons)
        + sum(item.details_artifact.bytes for item in result.completion.receipt.failure_recovery)
    )
    assert observation.write_bytes == expected_written
    assert observation.read_bytes > 0
    assert observation.wall_clock_seconds >= 1
    assert observation.peak_rss_bytes > 0
    assert observation.chain_resolution_milliseconds == 1
    validate_shadow_equivalence(
        result.run_spec.lifecycle_spec,
        result.completion.receipt,
        availability_cutoff_session=AVAILABLE,
        artifact_reader=lambda relative: (tmp_path / relative).read_bytes(),
    )
    loaded = shadow.load_i5_shadow_completion_exact(
        tmp_path, result.completion_artifact, production=False
    )
    assert loaded == result.completion
    assert not list(tmp_path.glob("**/*gate-b*"))
    assert not list(tmp_path.glob("**/*pointer*"))


def test_duplicate_run_reuses_exact_completion_bytes(tmp_path: Path) -> None:
    first = _execute(tmp_path)
    before = (tmp_path / first.completion_artifact.path).read_bytes()
    second = _execute(tmp_path)

    assert second.idempotent is True
    assert second.completion_artifact == first.completion_artifact
    assert (tmp_path / second.completion_artifact.path).read_bytes() == before


def test_fixture_completion_cannot_be_promoted_to_production(tmp_path: Path) -> None:
    result = _execute(tmp_path)
    with pytest.raises(shadow.I5ShadowRuntimeError, match="semantics differ"):
        shadow.load_i5_shadow_completion_exact(
            tmp_path,
            result.completion_artifact,
            production=True,
        )


def test_canonical_difference_fails_before_completion(tmp_path: Path) -> None:
    parent, child, oracle_physical = _physical("difference")
    incremental = _side("incremental", physical=child)
    oracle_rows = _rows()
    oracle_rows["universe_daily"][TARGET.isoformat()][0]["asset_id"] = _digest("different-asset")
    oracle = _side("oracle", rows=oracle_rows, physical=oracle_physical)

    with pytest.raises(shadow.I5ShadowRuntimeError, match="canonical shadow equivalence"):
        _execute(
            tmp_path,
            incremental=incremental,
            oracle=oracle,
            parent_physical=parent,
        )
    assert not list(tmp_path.glob("**/completion.json"))


def test_physical_prefix_mutation_fails_clean_append_gate(tmp_path: Path) -> None:
    parent, child, oracle_physical = _physical("prefix")
    mutated = {key: tuple(value) for key, value in child.items()}
    bad = dict(mutated["asset_master"][0])
    bad["row_count"] = 2
    mutated["asset_master"] = (bad, mutated["asset_master"][1])

    with pytest.raises(shadow.I5ShadowRuntimeError, match="physical clean-append"):
        _execute(
            tmp_path,
            incremental=_side("incremental", physical=mutated),
            oracle=_side("oracle", physical=oracle_physical),
            parent_physical=parent,
        )


def test_source_lineage_mismatch_is_explicit_oracle_seam(tmp_path: Path) -> None:
    parent, child, oracle_physical = _physical("source")
    oracle_rows = _rows()
    oracle_rows["universe_daily"][TARGET.isoformat()][0]["selected_source_record_id"] = _digest(
        "wrong-source"
    )
    with pytest.raises(shadow.I5FullOracleSeamError, match="source lineage differs"):
        shadow._execute_i5_shadow_fixture(
            tmp_path,
            incremental=_side("incremental", physical=child),
            oracle=_side("oracle", rows=oracle_rows, physical=oracle_physical),
            parent_physical=parent,
            common_parent_release_id=_digest("parent-release"),
            comparison_sessions=REQUIRED_SCOPE,
            receipt_available_session=AVAILABLE,
            resource_policy=_policy(),
        )


def test_completion_and_details_tampering_fail_exact_replay(tmp_path: Path) -> None:
    result = _execute(tmp_path)
    comparison = result.completion.receipt.comparisons[0]
    details = tmp_path / comparison.details_artifact.path
    details.write_bytes(details.read_bytes() + b" ")

    with pytest.raises(Exception, match=r"artifact|bytes|SHA|pin"):
        shadow.load_i5_shadow_completion_exact(
            tmp_path, result.completion_artifact, production=False
        )


def test_rehashed_but_semantically_forged_details_fail_replay(tmp_path: Path) -> None:
    result = _execute(tmp_path)
    comparison = result.completion.receipt.comparisons[0]
    details_path = tmp_path / comparison.details_artifact.path
    details = shadow._closed_json(details_path.read_bytes(), label="comparison details")
    details["tables"][0]["schema_digest"] = _digest("forged-schema")
    details_body = dict(details)
    details_body.pop("details_id")
    details["details_id"] = stable_digest(details_body)
    details_content = shadow._canonical_json_bytes(details)
    details_path.write_bytes(details_content)

    receipt = result.completion.receipt.to_dict()
    comparison_body = receipt["comparisons"][0]
    comparison_body["details_artifact"] = {
        "bytes": len(details_content),
        "path": comparison.details_artifact.path,
        "sha256": hashlib.sha256(details_content).hexdigest(),
    }
    receipt_body = dict(receipt)
    receipt_body.pop("receipt_id")
    receipt["receipt_id"] = stable_digest(receipt_body)

    completion = result.completion.to_dict()
    completion["receipt"] = receipt
    completion_body = dict(completion)
    completion_body.pop("completion_id")
    completion["completion_id"] = stable_digest(completion_body)
    completion_content = shadow._canonical_json_bytes(completion)
    completion_path = tmp_path / result.completion_artifact.path
    completion_path.write_bytes(completion_content)
    forged_pin = ArtifactPin(
        path=result.completion_artifact.path,
        sha256=hashlib.sha256(completion_content).hexdigest(),
        bytes=len(completion_content),
    )

    with pytest.raises(shadow.I5ShadowRuntimeError, match="table authority differs"):
        shadow.load_i5_shadow_completion_exact(tmp_path, forged_pin, production=False)


def test_rehashed_but_forged_failure_evidence_fails_replay(tmp_path: Path) -> None:
    result = _execute(tmp_path)
    failure = result.completion.receipt.failure_recovery[0]
    details_path = tmp_path / failure.details_artifact.path
    details = shadow._closed_json(details_path.read_bytes(), label="failure details")
    details["outcome"]["production_deep_loader_invoked"] = True
    details_body = dict(details)
    details_body.pop("details_id")
    details["details_id"] = stable_digest(details_body)
    details_content = shadow._canonical_json_bytes(details)
    details_path.write_bytes(details_content)

    receipt = result.completion.receipt.to_dict()
    receipt["failure_recovery"][0]["details_artifact"] = {
        "bytes": len(details_content),
        "path": failure.details_artifact.path,
        "sha256": hashlib.sha256(details_content).hexdigest(),
    }
    receipt_body = dict(receipt)
    receipt_body.pop("receipt_id")
    receipt["receipt_id"] = stable_digest(receipt_body)

    completion = result.completion.to_dict()
    completion["receipt"] = receipt
    completion_body = dict(completion)
    completion_body.pop("completion_id")
    completion["completion_id"] = stable_digest(completion_body)
    completion_content = shadow._canonical_json_bytes(completion)
    completion_path = tmp_path / result.completion_artifact.path
    completion_path.write_bytes(completion_content)
    forged_pin = ArtifactPin(
        path=result.completion_artifact.path,
        sha256=hashlib.sha256(completion_content).hexdigest(),
        bytes=len(completion_content),
    )

    with pytest.raises(shadow.I5ShadowRuntimeError, match="outcome semantics differ"):
        shadow.load_i5_shadow_completion_exact(tmp_path, forged_pin, production=False)


def test_foreign_no_clobber_completion_blocks_retry(tmp_path: Path) -> None:
    result = _execute(tmp_path)
    completion = tmp_path / result.completion_artifact.path
    completion.write_text("{}\n", encoding="utf-8")

    with pytest.raises(shadow.I5ShadowRuntimeError, match="completion"):
        _execute(tmp_path)


def test_write_resource_gate_fails_before_completion(tmp_path: Path) -> None:
    with pytest.raises(shadow.I5ShadowRuntimeError, match="write-byte resource gate"):
        _execute(tmp_path, resource_policy=_policy(max_write_bytes=1))
    assert not list(tmp_path.glob("**/completion.json"))


def test_production_loader_rejects_underreported_producer_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _execute(tmp_path)
    spec, inputs, _documents = _production_rebuild_fixture(tmp_path, result)
    observation = replace(result.completion.receipt.resource_observation, read_bytes=0)
    receipt = replace(result.completion.receipt, resource_observation=observation)
    monkeypatch.setattr(
        shadow,
        "_process_io_snapshot",
        lambda **_kwargs: _io_counter(),
    )
    monkeypatch.setattr(shadow, "_load_production_inputs", lambda *_args, **_kwargs: inputs)
    monkeypatch.setattr(shadow, "_build_run_spec", lambda *_args, **_kwargs: spec)

    with pytest.raises(shadow.I5ProductionReadMeterSeamError, match="underreport"):
        shadow._verify_production_completion_against_producers(
            tmp_path,
            spec=spec,
            receipt=receipt,
            meter=shadow._ReadMeter(tmp_path),
        )


def test_production_loader_rejects_underreported_producer_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _execute(tmp_path)
    spec, inputs, _documents = _production_rebuild_fixture(tmp_path, result)
    observation = replace(
        result.completion.receipt.resource_observation,
        read_bytes=10_000,
        write_bytes=0,
    )
    receipt = replace(result.completion.receipt, resource_observation=observation)
    samples = iter((_io_counter(), _io_counter(wchar=1, syscw=1)))
    monkeypatch.setattr(
        shadow,
        "_process_io_snapshot",
        lambda **_kwargs: next(samples),
    )
    monkeypatch.setattr(shadow, "_load_production_inputs", lambda *_args, **_kwargs: inputs)
    monkeypatch.setattr(shadow, "_build_run_spec", lambda *_args, **_kwargs: spec)

    with pytest.raises(shadow.I5ProductionReadMeterSeamError, match=r"write.*underreport"):
        shadow._verify_production_completion_against_producers(
            tmp_path,
            spec=spec,
            receipt=receipt,
            meter=shadow._ReadMeter(tmp_path),
        )


def test_production_interrupted_runner_evidence_is_required_and_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _execute(tmp_path)
    spec, inputs, documents = _production_rebuild_fixture(tmp_path, result)
    monkeypatch.setattr(
        shadow,
        "_process_io_snapshot",
        lambda **_kwargs: _io_counter(),
    )
    monkeypatch.setattr(shadow, "_load_production_inputs", lambda *_args, **_kwargs: inputs)
    monkeypatch.setattr(shadow, "_build_run_spec", lambda *_args, **_kwargs: spec)
    monkeypatch.setattr(
        shadow,
        "_comparison_receipts",
        lambda *_args, **_kwargs: (result.completion.receipt.comparisons, documents),
    )

    with pytest.raises(
        shadow.I5ProductionFailureExerciseSeamError,
        match="interrupted-run evidence unavailable",
    ):
        shadow._verify_production_completion_against_producers(
            tmp_path,
            spec=spec,
            receipt=result.completion.receipt,
            meter=shadow._ReadMeter(tmp_path),
        )


def test_production_interrupted_runner_replays_exact_i3_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from test_silver_s7_5_i3_production import _interrupted_retry_delta_fixture

    _run_spec, run_spec_pin, _parent = _interrupted_retry_delta_fixture(
        tmp_path,
        monkeypatch,
    )
    with pytest.raises(shadow.i3_runtime.I3ProductionInterruptedRetryPending):
        shadow.i3_runtime.exercise_i3_production_interrupted_retry(
            tmp_path,
            run_spec_pin,
        )
    shadow.i3_runtime._ACTIVE_INTERRUPTION_CAPABILITIES.clear()
    recovered = shadow.i3_runtime.exercise_i3_production_interrupted_retry(
        tmp_path,
        run_spec_pin,
    )
    deep = shadow.load_i3_production_deep_attestation_exact(
        recovered.stage_result.deep_attestation_pin,
        lambda relative: (tmp_path / relative).read_bytes(),
    )
    loaded = replace(recovered.stage_result.loaded, deep_attestation=deep)

    outcome = shadow._production_interrupted_retry_outcome(
        tmp_path,
        loaded=loaded,
        completion_artifact=recovered.stage_result.completion_pin,
        deep_attestation_artifact=recovered.stage_result.deep_attestation_pin,
    )

    assert outcome["fail_after"] == "failed_receipt_durable_before_completion"
    assert outcome["interrupted_retry_receipt_id"] == recovered.receipt.receipt_id
    assert outcome["failed_receipt_id"] == recovered.receipt.failed_receipt_id
    assert outcome["recovery_completion_id"] == recovered.receipt.completion_id
    assert outcome["recovery_deep_attestation_id"] == recovered.receipt.deep_attestation_id
    assert outcome["parent_reader_before_digest"] == outcome["parent_reader_after_digest"]
    assert outcome["deleted_artifact_count"] == 0
    assert outcome["unpublished_visible_count"] == 0
    assert outcome["reused_exact_replay"] is True


def test_production_interrupted_runner_rejects_nonreplayed_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from test_silver_s7_5_i3_production import _interrupted_retry_delta_fixture

    _run_spec, run_spec_pin, _parent = _interrupted_retry_delta_fixture(
        tmp_path,
        monkeypatch,
    )
    with pytest.raises(shadow.i3_runtime.I3ProductionInterruptedRetryPending):
        shadow.i3_runtime.exercise_i3_production_interrupted_retry(
            tmp_path,
            run_spec_pin,
        )
    recovered = shadow.i3_runtime.exercise_i3_production_interrupted_retry(
        tmp_path,
        run_spec_pin,
    )
    deep = shadow.load_i3_production_deep_attestation_exact(
        recovered.stage_result.deep_attestation_pin,
        lambda relative: (tmp_path / relative).read_bytes(),
    )
    loaded = replace(recovered.stage_result.loaded, deep_attestation=deep)
    monkeypatch.setattr(
        shadow.i3_runtime,
        "exercise_i3_production_interrupted_retry",
        lambda *_args, **_kwargs: replace(recovered, reused=False),
    )

    with pytest.raises(
        shadow.I5ProductionFailureExerciseSeamError,
        match="differs from the exact I3 DELTA",
    ):
        shadow._production_interrupted_retry_outcome(
            tmp_path,
            loaded=loaded,
            completion_artifact=recovered.stage_result.completion_pin,
            deep_attestation_artifact=recovered.stage_result.deep_attestation_pin,
        )


def test_process_io_audit_uses_one_baseline_through_duplicate_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = _io_counter(
        rchar=100,
        wchar=200,
        syscr=3,
        syscw=4,
        read_bytes=25,
        write_bytes=50,
    )
    samples = iter(
        (
            _io_counter(
                rchar=160,
                wchar=240,
                syscr=5,
                syscw=7,
                read_bytes=45,
                write_bytes=90,
            ),
            _io_counter(
                rchar=310,
                wchar=470,
                syscr=11,
                syscw=15,
                read_bytes=125,
                write_bytes=300,
            ),
        )
    )
    monkeypatch.setattr(shadow, "_process_io_snapshot", lambda **_kwargs: next(samples))
    meter = shadow._ReadMeter(tmp_path)
    audit = shadow._ProcessIOAudit(baseline)

    first = audit.sample(meter)
    duplicate = audit.sample(meter)

    assert first.read_bytes == 60
    assert first.write_bytes == 40
    assert duplicate.read_bytes == 210
    assert duplicate.write_bytes == 270
    assert duplicate.write_syscalls == 11
    assert duplicate.physical_write_bytes == 250
    assert meter.bytes == 210
    assert audit.write_bytes == 270


def test_disk_monitor_reserves_full_sqlite_peak_and_rejects_low_floor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(shadow, "_disk_free", lambda _root: 1_000)
    monitor = shadow._DiskFloorMonitor(tmp_path, floor_bytes=500)
    reserve = shadow._official_full_sqlite_reserve_bytes(
        SimpleNamespace(tmp_bytes_cap=600)  # type: ignore[arg-type]
    )

    assert monitor.sample("entry") == 1_000
    with pytest.raises(shadow.I5ShadowRuntimeError, match="disk floor"):
        monitor.sample("before_official_full_replay", reserve_bytes=reserve)
    assert monitor.minimum == 400


def test_checkpoint_corruption_rejects_in_memory_deep_claim_without_exact_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkpoint_content = b'{"checkpoint_id":"child"}\n'
    checkpoint_pin = _write_exact(
        tmp_path,
        "manifests/silver/incremental/i3/checkpoints/child.json",
        checkpoint_content,
    )

    @dataclass(frozen=True)
    class Checkpoint:
        def exact_pin(self, *, path: str) -> ArtifactPin:
            assert path == checkpoint_pin.path
            return checkpoint_pin

    @dataclass(frozen=True)
    class OutputSet:
        checkpoint_artifact: ArtifactPin
        checkpoint_id: str

        @property
        def output_set_id(self) -> str:
            return _digest("child-output-set")

    @dataclass(frozen=True)
    class Receipt:
        output_set: OutputSet

        @property
        def receipt_id(self) -> str:
            return _digest("child-receipt")

    @dataclass(frozen=True)
    class Completion:
        receipt_id: str
        output_set_id: str
        checkpoint_id: str
        artifact: ArtifactPin

        def exact_pin(self, *, path: str) -> ArtifactPin:
            assert path == self.artifact.path
            return self.artifact

    @dataclass(frozen=True)
    class Deep:
        completion_artifact: ArtifactPin
        output_set_id: str
        checkpoint_artifact: ArtifactPin
        checkpoint_id: str

    @dataclass(frozen=True)
    class Loaded:
        completion: Completion
        receipt: Receipt
        deep_attestation: Deep
        checkpoint: Checkpoint

    completion_pin = ArtifactPin(
        "manifests/i3/completion.json",
        _digest("completion"),
        1,
    )
    output_set = OutputSet(checkpoint_pin, "child")
    loaded = Loaded(
        completion=Completion(
            receipt_id=_digest("child-receipt"),
            output_set_id=output_set.output_set_id,
            checkpoint_id="child",
            artifact=completion_pin,
        ),
        receipt=Receipt(output_set),
        deep_attestation=Deep(
            completion_artifact=completion_pin,
            output_set_id=output_set.output_set_id,
            checkpoint_artifact=checkpoint_pin,
            checkpoint_id="child",
        ),
        checkpoint=Checkpoint(),
    )
    deep_calls: list[ArtifactPin] = []

    def reject_tampered_deep(
        _root: Path,
        _completion_pin: ArtifactPin,
        _deep_pin: ArtifactPin,
        tampered: Loaded,
    ) -> None:
        forged = tampered.receipt.output_set.checkpoint_artifact
        deep_calls.append(forged)
        assert forged.path == checkpoint_pin.path
        assert forged.sha256 != checkpoint_pin.sha256
        raise shadow.i3_runtime.I3ProductionStageError("tampered child checkpoint")

    monkeypatch.setattr(shadow.i3_runtime, "_verify_deep_attestation", reject_tampered_deep)
    with pytest.raises(shadow.I5ShadowRuntimeError, match="missing or unsafe"):
        shadow._exercise_production_checkpoint_corruption(
            tmp_path,
            loaded=loaded,  # type: ignore[arg-type]
            completion_pin=completion_pin,
            deep_attestation_pin=ArtifactPin(
                "manifests/i3/deep.json",
                _digest("deep"),
                1,
            ),
            meter=shadow._ReadMeter(tmp_path),
        )
    assert deep_calls == []


def test_checkpoint_corruption_replays_real_i3_completion_receipt_and_deep_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    staged = stage_i3_production_base(
        tmp_path,
        _write_run_spec(tmp_path, run_spec),
        materializer=CompactBaseMigrationMaterializer(legacy, s4),
    )
    loaded = shadow.verify_i3_production_deep_attestation(
        tmp_path,
        staged.completion_pin,
        staged.deep_attestation_pin,
        expected_kind=shadow.I3ProductionRunKind.BASE,
    )
    output_set = loaded.receipt.output_set
    assert output_set is not None

    outcome = shadow._exercise_production_checkpoint_corruption(
        tmp_path,
        loaded=loaded,
        completion_pin=staged.completion_pin,
        deep_attestation_pin=staged.deep_attestation_pin,
        meter=shadow._ReadMeter(tmp_path),
    )

    assert outcome["actual_child_checkpoint_path"] == output_set.checkpoint_artifact.path
    assert outcome["actual_child_checkpoint_sha256"] == output_set.checkpoint_artifact.sha256
    assert outcome["deep_chain_loader_invoked"] is True


def test_config_rejects_discovery_paths_and_scope_without_target() -> None:
    good = ArtifactPin("manifests/i3/completion.json", _digest("pin"), 1)
    for unsafe in (
        "manifests/latest/result.json",
        "manifests/i3/latest.json",
        "manifests/i3/completion-*.json",
        "manifests/fixtures/i3/completion.json",
    ):
        with pytest.raises(shadow.I5ShadowRuntimeError, match="not production authority"):
            shadow.ShadowRunConfig(
                incremental_completion_artifact=replace(good, path=unsafe),
                incremental_deep_attestation_artifact=good,
                full_oracle_completion_artifact=good,
                comparison_sessions=(TARGET,),
                receipt_available_session=AVAILABLE,
                resource_policy=_policy(),
            )
    with pytest.raises(shadow.I5ShadowRuntimeError, match="module-owned challenge set"):
        shadow.ShadowRunConfig(
            incremental_completion_artifact=good,
            incremental_deep_attestation_artifact=good,
            full_oracle_completion_artifact=good,
            comparison_sessions=(CHALLENGE,),
            receipt_available_session=AVAILABLE,
            resource_policy=_policy(),
        )
    with pytest.raises(shadow.I5ShadowRuntimeError, match="module-owned challenge set"):
        shadow.ShadowRunConfig(
            incremental_completion_artifact=good,
            incremental_deep_attestation_artifact=good,
            full_oracle_completion_artifact=good,
            comparison_sessions=(TARGET,),
            receipt_available_session=AVAILABLE,
            resource_policy=_policy(),
        )


def test_missing_full_completion_is_p0_and_never_accepts_a_digest(tmp_path: Path) -> None:
    pin = ArtifactPin(
        "manifests/silver/identity/s7-streaming-full-execution-completions/"
        f"plan_id={_digest('plan')}/approval_id={_digest('approval')}/manifest.json",
        _digest("missing-full"),
        1,
    )
    meter = shadow._ReadMeter(tmp_path)
    with pytest.raises(shadow.I5FullOracleSeamError, match="P0 full-oracle seam"):
        shadow._full_oracle_side(
            tmp_path,
            completion_pin=pin,
            comparison_sessions=(TARGET,),
            meter=meter,
        )


def test_parquet_replay_checks_bytes_schema_and_row_count(tmp_path: Path) -> None:
    schema = pa.schema([pa.field("value", pa.int64(), nullable=False)])
    table = pa.Table.from_pylist([{"value": 1}], schema=schema)
    relative = "silver/fixture/exact.parquet"
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    pq.write_table(table, path)
    content = path.read_bytes()
    pin = ArtifactPin(relative, hashlib.sha256(content).hexdigest(), len(content))
    meter = shadow._ReadMeter(tmp_path)

    assert shadow._read_parquet_exact(
        tmp_path,
        artifact=pin,
        table_name="fixture",
        expected_schema=schema,
        expected_rows=1,
        meter=meter,
    ).equals(table)
    with pytest.raises(shadow.I5ShadowRuntimeError, match="row count differs"):
        shadow._read_parquet_exact(
            tmp_path,
            artifact=pin,
            table_name="fixture",
            expected_schema=schema,
            expected_rows=2,
            meter=meter,
        )
    with pytest.raises(shadow.I5ShadowRuntimeError, match="schema or row count differs"):
        shadow._read_parquet_exact(
            tmp_path,
            artifact=pin,
            table_name="fixture",
            expected_schema=pa.schema([pa.field("value", pa.string())]),
            expected_rows=1,
            meter=meter,
        )


def test_full_oracle_receipt_requires_contract_schema_and_row_count() -> None:
    expected_schema = _digest("oracle-schema")
    receipt = {
        "bytes": 10,
        "path": "data/asset_master.parquet",
        "row_count": 2,
        "schema_digest": expected_schema,
        "sha256": _digest("oracle-bytes"),
    }
    parsed = shadow._oracle_output_receipt(
        receipt,
        candidate_root=f"silver/oracle/candidate_id={_digest('oracle')}",
        label="oracle fixture",
        expected_schema_digest=expected_schema,
    )
    assert parsed["row_count"] == 2
    assert parsed["schema_digest"] == expected_schema

    with pytest.raises(shadow.I5FullOracleSeamError, match="schema digest differs"):
        shadow._oracle_output_receipt(
            {**receipt, "schema_digest": _digest("wrong-schema")},
            candidate_root=f"silver/oracle/candidate_id={_digest('oracle')}",
            label="oracle fixture",
            expected_schema_digest=expected_schema,
        )
    with pytest.raises(shadow.I5FullOracleSeamError, match="row-count fields differ"):
        shadow._oracle_output_receipt(
            {key: value for key, value in receipt.items() if key != "row_count"},
            candidate_root=f"silver/oracle/candidate_id={_digest('oracle')}",
            label="oracle fixture",
            expected_schema_digest=expected_schema,
        )


def test_challenge_sessions_are_closed_exact_scope(tmp_path: Path) -> None:
    result = _execute(tmp_path)
    assert result.run_spec.comparison_sessions == REQUIRED_SCOPE
    details = result.completion.receipt.comparisons[0].details_artifact
    body = shadow._closed_json((tmp_path / details.path).read_bytes(), label="comparison details")
    universe = next(item for item in body["tables"] if item["table_name"] == "universe_daily")
    assert [item["partition_key"] for item in universe["partitions"]] == [
        session.isoformat() for session in REQUIRED_SCOPE
    ]
