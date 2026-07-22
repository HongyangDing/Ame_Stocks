from __future__ import annotations

import hashlib
import inspect
import json
import tracemalloc
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.cli import silver_identity_materialization_streaming as stream_cli
from ame_stocks_api.silver import identity_materialization_streaming as stream
from ame_stocks_api.silver.asset_contract import UNIVERSE_SOURCE_DAILY_CONTRACT
from ame_stocks_api.silver.calendar_artifact import (
    build_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.identity_materialization_streaming import (
    DISK_HARD_FLOOR_BYTES,
    REGISTRY_ORDER,
    S7_STANDING_AUTHORIZATION_TEXT,
    S7_STANDING_REAFFIRMATION_TEXT,
    ExactFilePin,
    FrozenRegistryProjectionAdapter,
    GateBReferencePin,
    GateCCompletionPin,
    ResolutionProjection,
    S7StreamingMaterializationError,
    S7StreamingSourceBinding,
    SessionArtifactPin,
    StreamingResourceCaps,
    execute_streaming_bounded_profile_preview,
    execute_streaming_full_candidate,
    prepare_streaming_approval_request,
    prepare_streaming_bounded_profile_preview_plan,
    prepare_streaming_full_plan,
    record_standing_streaming_approval,
    record_standing_streaming_profile_approval,
    record_standing_v4_contract_approval,
    store_production_streaming_source_binding_document,
    store_streaming_source_binding,
)
from ame_stocks_api.silver.identity_registry_workflow import RegistryReleasePin
from ame_stocks_api.silver.identity_resolution import canonical_asset_id

_SESSIONS = (date(2024, 1, 12), date(2024, 1, 16), date(2024, 1, 17))
_CLOCK = datetime(2024, 1, 12, 14, 0, tzinfo=UTC)


def _write_bytes(root: Path, relative: str, content: bytes) -> ExactFilePin:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return ExactFilePin(relative, hashlib.sha256(content).hexdigest(), len(content))


def _write_json(root: Path, relative: str, value: object) -> ExactFilePin:
    return _write_bytes(root, relative, stream._canonical_bytes(value))


def _compact_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _runtime_binding() -> dict[str, object]:
    files = [
        {
            "bytes": 1,
            "git_blob": "1" * 40,
            "git_mode": "100644",
            "path": relative,
            "sha256": stable_digest({"runtime": relative}),
        }
        for relative in stream._RUNTIME_SOURCE_PATHS
    ]
    return {
        "binding_version": "s7_streaming_full_runtime_git_binding_v1",
        "exact_checkout_clean": True,
        "repository_commit": "2" * 40,
        "repository_tree": "3" * 40,
        "runtime_file_set_digest": stable_digest(files),
        "runtime_files": files,
        "runtime_versions": {"pyarrow": pa.__version__, "python": "3.12.0"},
    }


def _default_value(field: pa.Field) -> object:
    if field.nullable:
        return None
    if pa.types.is_string(field.type):
        return "fixture"
    if pa.types.is_date32(field.type):
        return _SESSIONS[0]
    if pa.types.is_timestamp(field.type):
        return datetime(2024, 1, 1, tzinfo=UTC)
    if pa.types.is_boolean(field.type):
        return False
    if pa.types.is_int64(field.type):
        return 1
    if pa.types.is_list(field.type):
        return []
    raise AssertionError(f"no fixture default for {field}")


def _source_row(
    session: date,
    ticker: str,
    composite: str,
    *,
    share: str,
    active: bool = True,
) -> dict[str, object]:
    row = {
        field.name: _default_value(field) for field in UNIVERSE_SOURCE_DAILY_CONTRACT.arrow_schema
    }
    source_id = stable_digest(
        {"composite": composite, "session": session.isoformat(), "ticker": ticker}
    )
    row.update(
        {
            "session_year": session.year,
            "session_date": session,
            "ticker": ticker,
            "active_on_date": active,
            "type_code": "CS",
            "name": f"{ticker} Corp",
            "market": "stocks",
            "locale": "us",
            "primary_exchange_mic": "XNYS",
            "currency_name": "usd",
            "cik": "1",
            "composite_figi": composite,
            "share_class_figi": share,
            "identity_link_status": "multi_identifier_evidence_pending_s7",
            "selected_source_record_id": source_id,
            "source_version_count": 1,
            "selection_status": "singleton",
            "reference_time_scope": "provider_historical_date_membership_snapshot_v1",
            "metadata_time_scope": (
                "metadata_as_returned_at_source_capture_not_historical_vintage_v1"
            ),
            "source_available_session": session,
            "source_availability_quality": (
                "reconstructed_historical_snapshot_without_archived_vintage"
            ),
        }
    )
    return row


class _FakeRelease:
    def __init__(self, pin: RegistryReleasePin) -> None:
        self.registry_name = pin.registry_name
        self.release_id = pin.release_id
        self.manifest_pin = pin
        self.release_available_session = pin.release_available_session
        self.decision_rows: dict[str, dict[str, object]] = {}

    def decision_ids_for_exact_source_row(self, *_: object, **__: object) -> tuple[str, ...]:
        return ()


class _FakeRegistrySet:
    def __init__(self, pins: tuple[RegistryReleasePin, ...]) -> None:
        self.releases = tuple(_FakeRelease(pin) for pin in pins)

    def by_name(self, name: str) -> _FakeRelease:
        return self.releases[REGISTRY_ORDER.index(name)]

    def composite_matches(self, *_: object, **__: object) -> tuple[tuple[str, str], ...]:
        return ()


class _DirectAdapter:
    adapter_version = "fixture-direct-projection-v1"
    production_ready = False

    def resolve_session(self, source: pa.Table, **_: object) -> tuple[ResolutionProjection, ...]:
        output = []
        for row in source.to_pylist():
            composite = row["composite_figi"]
            share = row["share_class_figi"]
            eligible = composite != "BBG000000099"
            asset = canonical_asset_id(composite) if eligible else None
            output.append(
                ResolutionProjection(
                    selected_source_record_id=row["selected_source_record_id"],
                    observed_composite_market_code=("US" if eligible else "GB"),
                    observed_asset_id=canonical_asset_id(composite),
                    canonical_composite_figi=(composite if eligible else None),
                    canonical_composite_market_code=("US" if eligible else None),
                    canonical_share_class_figi=(share if eligible else None),
                    canonical_cik_normalized="0000000001",
                    asset_id=asset,
                    share_class_id=(stream._share_class_id(share) if eligible else None),
                    issuer_id=stream._issuer_id("0000000001"),
                    identity_resolution_status=(
                        "resolved_identity" if eligible else "unresolved_non_us_composite"
                    ),
                    identity_resolution_method=(
                        "direct_gate_b_us" if eligible else "gate_b_non_us_no_override"
                    ),
                    identity_disposition=("direct_observation" if eligible else "unresolved"),
                    identity_case_id=None,
                    identity_case_available_session=None,
                    identity_adjudication_id=None,
                    cross_market_scope_id=None,
                    cross_market_adjudication_id=None,
                    cross_market_adjudication_available_session=None,
                    cross_market_classification_status=("known_us" if eligible else "known_non_us"),
                    identity_case_resolution_role=None,
                    adjudication_available_session=None,
                    backtest_identity_eligible=eligible,
                    current_reference_factor_eligible=False,
                    security_type_scope="ordinary_equity",
                    identity_evidence_available_session=_SESSIONS[0],
                    provider_composite_override_id=None,
                    provider_composite_override_available_session=None,
                    share_class_adjudication_id=None,
                    share_class_adjudication_available_session=None,
                    asset_transition_ids=(),
                    composite_registry_match_count=0,
                    composite_registry_collision=False,
                )
            )
        return tuple(output)


def _fixture(
    root: Path,
    *,
    rows_by_session: tuple[tuple[dict[str, object], ...], ...] | None = None,
    gate_b_overrides: dict[str, dict[str, object]] | None = None,
) -> tuple[S7StreamingSourceBinding, _FakeRegistrySet, dict[str, object]]:
    calendar = build_xnys_calendar_artifact(_SESSIONS[0], _SESSIONS[-1])
    write_xnys_calendar_artifact(root, calendar)
    runtime = _runtime_binding()
    approval_receipts = []
    for table_name in stream.TABLE_ORDER:
        approval_receipts.append(
            record_standing_v4_contract_approval(
                root,
                table_name=table_name,
                calendar_artifact_id=calendar.calendar_artifact_id,
                calendar_artifact_sha256=calendar.sha256,
                authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
                reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
                approved_by="joe",
                runtime_probe=lambda: runtime,
                now=lambda: _CLOCK,
            ).receipt
        )
    if rows_by_session is None:
        rows_by_session = (
            (_source_row(_SESSIONS[0], "AAA", "BBG000000001", share="BBG000000011"),),
            (_source_row(_SESSIONS[1], "BBB", "BBG000000002", share="BBG000000022"),),
            (
                _source_row(
                    _SESSIONS[2],
                    "AAA",
                    "BBG000000001",
                    share="BBG000000011",
                    active=False,
                ),
            ),
        )
    membership = []
    composites: set[str] = set()
    shares_by_composite: dict[str, str] = {}
    for session, rows in zip(_SESSIONS, rows_by_session, strict=True):
        table = pa.Table.from_pylist(
            [dict(row) for row in rows], schema=UNIVERSE_SOURCE_DAILY_CONTRACT.arrow_schema
        )
        relative = f"silver/test/s4/session_date={session.isoformat()}/part.parquet"
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
        pin = ExactFilePin(relative, stream.sha256_file(path), path.stat().st_size)
        membership.append(SessionArtifactPin(session, table.num_rows, pin))
        composites.update(str(row["composite_figi"]) for row in rows)
        for row in rows:
            shares_by_composite.setdefault(
                str(row["composite_figi"]),
                str(row["share_class_figi"]),
            )
    s4_manifest = _write_json(root, "manifests/test/s4.json", {"fixture": "s4"})
    gate_b_id = stable_digest({"fixture": "gate-b"})
    gate_b_manifest = _write_json(
        root,
        "manifests/test/gate-b.json",
        {"candidate_id": gate_b_id, "state": "awaiting_review"},
    )
    gate_rows = []
    for composite in sorted(composites):
        gate_row = {
            "classification": ("known_non_us" if composite == "BBG000000099" else "known_us"),
            "composite_figi": composite,
            "relation_share_class_conflict": False,
            "selected_market_code": ("GB" if composite == "BBG000000099" else "US"),
            "selected_share_class_figi": shares_by_composite[composite],
            "source_available_session": _SESSIONS[0],
        }
        if gate_b_overrides is not None:
            gate_row.update(gate_b_overrides.get(composite, {}))
        gate_rows.append(gate_row)
    gate_b_path = root / "silver/test/gate-b.parquet"
    gate_b_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(gate_rows), gate_b_path)
    gate_b_data = ExactFilePin(
        "silver/test/gate-b.parquet",
        stream.sha256_file(gate_b_path),
        gate_b_path.stat().st_size,
    )
    gate_c_id = stable_digest({"fixture": "gate-c"})
    completion_id = stable_digest({"fixture": "gate-c-completion"})
    gate_c_manifest = _write_json(
        root,
        "manifests/test/gate-c.json",
        {"candidate_id": gate_c_id, "state": "awaiting_review"},
    )
    gate_c_completion = _write_json(
        root,
        "manifests/test/gate-c-completion.json",
        {"completion_id": completion_id, "completion_state": "awaiting_review"},
    )
    identity_case_preview_id = stable_digest({"fixture": "identity-case-preview"})
    identity_case_preview = _write_json(
        root,
        "manifests/test/identity-case-preview.json",
        {
            "preview_artifact_id": identity_case_preview_id,
            "result": {
                "preview_manifest_available_session": _SESSIONS[0].isoformat(),
            },
        },
    )
    gate_c_qa = _write_json(root, "manifests/test/gate-c-qa.json", {"critical_failure_count": 0})
    registry_pins = []
    for name in REGISTRY_ORDER:
        release_id = stable_digest({"fixture": "registry", "name": name})
        pin = _write_json(root, f"manifests/test/registry-{name}.json", {"name": name})
        registry_pins.append(
            RegistryReleasePin(
                registry_name=name,
                release_id=release_id,
                manifest_path=pin.path,
                manifest_sha256=pin.sha256,
                manifest_bytes=pin.bytes,
                release_available_session=_SESSIONS[0],
            )
        )
    binding = S7StreamingSourceBinding(
        mode="fixture",
        cutoff_session=_SESSIONS[-1],
        s4_release_set_manifest=s4_manifest,
        membership_artifacts=tuple(membership),
        gate_b=GateBReferencePin(
            candidate_id=gate_b_id,
            candidate_state="awaiting_review",
            reference_version="fixture-v1",
            closed=True,
            manifest=gate_b_manifest,
            data=gate_b_data,
        ),
        gate_c=GateCCompletionPin(
            candidate_id=gate_c_id,
            completion_id=completion_id,
            completion_state="awaiting_review",
            complete=True,
            candidate_manifest=gate_c_manifest,
            completion_manifest=gate_c_completion,
            identity_case_preview_id=identity_case_preview_id,
            identity_case_preview_manifest=identity_case_preview,
            identity_case_preview_available_session=_SESSIONS[0],
            qa=gate_c_qa,
        ),
        registry_pins=tuple(registry_pins),
        contract_approvals=tuple(approval_receipts),
        runtime_binding=runtime,
        calendar_artifact_id=calendar.calendar_artifact_id,
        calendar_artifact_sha256=calendar.sha256,
    )
    return binding, _FakeRegistrySet(tuple(registry_pins)), runtime


def _resource_caps() -> StreamingResourceCaps:
    return StreamingResourceCaps(
        source_bytes_cap=10**9,
        output_bytes_cap=10**9,
        tmp_bytes_cap=10**9,
        wall_clock_seconds_cap=3600,
        session_count_cap=100,
        row_count_cap=1_000_000,
        per_session_row_cap=100_000,
        batch_row_cap=10_000,
        disk_free_floor_bytes=DISK_HARD_FLOOR_BYTES,
    )


def _controls(
    root: Path,
    binding: S7StreamingSourceBinding,
    *,
    profile_plan_id: str | None = None,
    profile_approval_id: str | None = None,
) -> tuple[dict[str, object], object]:
    store_streaming_source_binding(root, binding)
    caps = _resource_caps()
    plan, _ = prepare_streaming_full_plan(
        root,
        source_binding_id=binding.source_binding_id,
        resource_caps=caps,
        prepared_by="builder",
        prepared_at_utc=datetime(2024, 1, 12, 13, 55, tzinfo=UTC),
        profile_plan_id=profile_plan_id,
        profile_approval_id=profile_approval_id,
    )
    request, _ = prepare_streaming_approval_request(
        root,
        plan_id=plan["plan_id"],
        requested_by="requester",
        requested_at_utc=datetime(2024, 1, 12, 13, 56, tzinfo=UTC),
    )
    approval, _ = record_standing_streaming_approval(
        root,
        request_id=request.request_id,
        authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
        reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
        approved_by="joe",
        now=lambda: _CLOCK,
    )
    return plan, approval


def _completed_profile(
    root: Path,
    binding: S7StreamingSourceBinding,
    registries: _FakeRegistrySet,
    runtime: dict[str, object],
    *,
    checkpoint_hook: object = None,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    store_streaming_source_binding(root, binding)
    plan, _ = prepare_streaming_bounded_profile_preview_plan(
        root,
        source_binding_id=binding.source_binding_id,
        full_resource_caps=_resource_caps(),
        sample_session_cap=3,
        prepared_by="profile-builder",
        prepared_at_utc=datetime(2024, 1, 12, 13, 40, tzinfo=UTC),
    )
    approval, _ = record_standing_streaming_profile_approval(
        root,
        plan_id=plan["plan_id"],
        authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
        reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
        approved_by="joe",
        now=lambda: datetime(2024, 1, 12, 13, 45, tzinfo=UTC),
    )
    completion = stream._execute_streaming_bounded_profile_preview_fixture(
        root,
        plan_id=plan["plan_id"],
        approval_id=approval["approval_id"],
        registry_loader=lambda *_args, **_kwargs: registries,
        runtime_probe=lambda: runtime,
        now=lambda: datetime(2024, 1, 12, 13, 50, tzinfo=UTC),
        monotonic=lambda: 0.0,
        rss_probe=lambda: 1024,
        disk_free_probe=lambda _: DISK_HARD_FLOOR_BYTES + 10**9,
        checkpoint_hook=checkpoint_hook,
    )
    return plan, approval, completion


def _execute(
    root: Path,
    binding: S7StreamingSourceBinding,
    registries: _FakeRegistrySet,
    runtime: dict[str, object],
    *,
    checkpoint_hook: object = None,
    adapter: object | None = None,
    disk_free_probe: object | None = None,
):
    plan, approval = _controls(root, binding)

    def loader(*_args: object, **_kwargs: object) -> _FakeRegistrySet:
        return registries

    return stream._execute_streaming_full_candidate_fixture(
        root,
        plan_id=plan["plan_id"],
        approval_id=approval.approval_id,
        adapter=adapter or _DirectAdapter(),
        registry_loader=loader,
        runtime_probe=lambda: runtime,
        now=lambda: datetime(2024, 1, 12, 14, 5, tzinfo=UTC),
        monotonic=lambda: 0.0,
        rss_probe=lambda: 1024,
        disk_free_probe=(
            disk_free_probe
            if disk_free_probe is not None
            else lambda _: DISK_HARD_FLOOR_BYTES + 10**9
        ),
        checkpoint_hook=checkpoint_hook,
    )


def test_two_pass_real_parquet_preserves_inactive_and_gaps(tmp_path: Path) -> None:
    binding, registries, runtime = _fixture(tmp_path)
    source_columns = stream._source_binding_columns(binding)
    assert source_columns["source_identity_case_candidate_manifest_id"] == (
        binding.gate_c.identity_case_preview_id
    )
    assert source_columns["source_identity_case_candidate_manifest_sha256"] == (
        binding.gate_c.identity_case_preview_manifest.sha256
    )
    assert source_columns["source_identity_market_consistency_candidate_manifest_id"] == (
        binding.gate_c.candidate_id
    )
    assert source_columns["source_identity_market_consistency_candidate_manifest_sha256"] == (
        binding.gate_c.candidate_manifest.sha256
    )
    assert binding.gate_c.identity_case_preview_id != binding.gate_b.candidate_id
    assert S7StreamingSourceBinding.from_dict(binding.to_dict()) == binding
    result = _execute(tmp_path, binding, registries, runtime)

    assert result.state == "awaiting_review"
    assert result.source_row_count == 3
    assert result.table_row_counts["universe_daily"] == 3
    assert result.table_row_counts["ticker_alias"] == 3
    universe = pq.read_table(
        tmp_path / result.candidate_path / "data/universe_daily",
        partitioning=None,
    ).to_pylist()
    assert [row["active_on_date"] for row in universe if row["ticker"] == "AAA"] == [
        True,
        False,
    ]
    assert all(row["identity_quality_liquidation_signal"] is False for row in universe)
    aliases = pq.read_table(
        tmp_path / result.candidate_path / "data/ticker_alias.parquet"
    ).to_pylist()
    assert [(row["ticker"], row["valid_from_session"]) for row in aliases] == [
        ("AAA", _SESSIONS[0]),
        ("AAA", _SESSIONS[2]),
        ("BBB", _SESSIONS[1]),
    ]


def test_frozen_production_adapter_runs_real_parquet_fixture(tmp_path: Path) -> None:
    binding, registries, runtime = _fixture(tmp_path)
    result = _execute(
        tmp_path,
        binding,
        registries,
        runtime,
        adapter=FrozenRegistryProjectionAdapter(),
    )

    assert result.source_row_count == 3
    universe = pq.read_table(
        tmp_path / result.candidate_path / "data/universe_daily",
        partitioning=None,
    ).to_pylist()
    assert all(
        row["identity_resolution_method"] == "source_composite_figi_exact" for row in universe
    )
    assert all(row["backtest_identity_eligible"] is True for row in universe)


def test_gate_b_share_conflict_preserves_matching_self_share_eligibility(
    tmp_path: Path,
) -> None:
    composite = "BBG000000001"
    binding, registries, _ = _fixture(
        tmp_path,
        gate_b_overrides={
            composite: {
                "relation_share_class_conflict": True,
                "selected_share_class_figi": "BBG000000011",
            }
        },
    )
    gate_b = stream._load_gate_b_reference(tmp_path, binding.gate_b)
    source = _source_row(_SESSIONS[0], "AAA", composite, share="BBG000000011")

    projection = stream._frozen_registry_projection(
        source,
        gate_b_by_composite=gate_b,
        registries=registries,
        binding=binding,
    )
    row = stream._build_and_validate_universe_row(
        source,
        projection,
        gate_b=gate_b,
        registries=registries,
        binding=binding,
    )

    assert row["canonical_composite_figi"] == composite
    assert row["canonical_share_class_figi"] == "BBG000000011"
    assert row["identity_resolution_status"] == "resolved_strong"
    assert row["identity_resolution_method"] == "source_composite_figi_exact"
    assert row["backtest_identity_eligible"] is True


def test_unadjudicated_gate_b_share_conflict_keeps_asset_but_has_no_alias(
    tmp_path: Path,
) -> None:
    composite = "BBG000000001"
    binding, registries, runtime = _fixture(
        tmp_path,
        gate_b_overrides={
            composite: {
                "relation_share_class_conflict": True,
                "selected_share_class_figi": "BBG000000012",
            }
        },
    )
    result = _execute(
        tmp_path,
        binding,
        registries,
        runtime,
        adapter=FrozenRegistryProjectionAdapter(),
    )
    universe = pq.read_table(
        tmp_path / result.candidate_path / "data/universe_daily",
        partitioning=None,
    ).to_pylist()
    conflicted = [row for row in universe if row["ticker"] == "AAA"]

    assert len(conflicted) == 2
    assert all(row["canonical_composite_figi"] == composite for row in conflicted)
    assert all(row["asset_id"] == canonical_asset_id(composite) for row in conflicted)
    assert all(row["canonical_share_class_figi"] is None for row in conflicted)
    assert all(row["share_class_id"] is None for row in conflicted)
    assert all(row["identity_resolution_status"] == "resolved_conflicted" for row in conflicted)
    assert all(
        row["identity_resolution_method"] == "source_composite_figi_exact" for row in conflicted
    )
    assert all(row["backtest_identity_eligible"] is False for row in conflicted)
    assert all(row["ticker_alias_id"] is None for row in conflicted)
    assert all(row["identity_quality_liquidation_signal"] is False for row in conflicted)

    qa = json.loads((tmp_path / result.candidate_path / "qa/qa.json").read_text())
    assert qa["gate_b_relation_share_class_conflict_rows"] == 2
    assert qa["gate_b_relation_share_class_mismatch_rows"] == 2
    assert qa["unadjudicated_gate_b_share_class_conflict_rows"] == 2
    assert qa["unadjudicated_gate_b_share_class_conflict_eligible_rows"] == 0
    assert len(qa["bounded_share_class_conflict_examples"]) == 2
    assert qa["critical_failure_count"] == 0


def test_missing_observed_share_under_gate_b_conflict_is_ineligible(
    tmp_path: Path,
) -> None:
    composite = "BBG000000001"
    binding, registries, _ = _fixture(
        tmp_path,
        gate_b_overrides={
            composite: {
                "relation_share_class_conflict": True,
                "selected_share_class_figi": "BBG000000011",
            }
        },
    )
    gate_b = stream._load_gate_b_reference(tmp_path, binding.gate_b)
    source = _source_row(_SESSIONS[0], "AAA", composite, share="BBG000000011")
    source["share_class_figi"] = None

    projection = stream._frozen_registry_projection(
        source,
        gate_b_by_composite=gate_b,
        registries=registries,
        binding=binding,
    )
    row = stream._build_and_validate_universe_row(
        source,
        projection,
        gate_b=gate_b,
        registries=registries,
        binding=binding,
    )

    assert row["canonical_composite_figi"] == composite
    assert row["asset_id"] == canonical_asset_id(composite)
    assert row["canonical_share_class_figi"] is None
    assert row["identity_resolution_status"] == "resolved_conflicted"
    assert row["backtest_identity_eligible"] is False
    assert row["ticker_alias_id"] is None
    assert row["identity_quality_liquidation_signal"] is False


def test_exact_share_adjudication_resolves_gate_b_relation_conflict(
    tmp_path: Path,
) -> None:
    composite = "BBG000000001"
    observed_share = "BBG000000099"
    canonical_share = "BBG000000011"
    rows = (
        (_source_row(_SESSIONS[0], "AAA", composite, share=observed_share),),
        (_source_row(_SESSIONS[1], "BBB", "BBG000000002", share="BBG000000022"),),
        (_source_row(_SESSIONS[2], "CCC", "BBG000000003", share="BBG000000033"),),
    )
    binding, registries, _ = _fixture(
        tmp_path,
        rows_by_session=rows,
        gate_b_overrides={
            composite: {
                "relation_share_class_conflict": True,
                "selected_share_class_figi": canonical_share,
            }
        },
    )
    decision_id = "c" * 64
    decision = {
        "adjudication_available_session": _SESSIONS[0],
        "canonical_share_class_figi": canonical_share,
        "canonical_share_class_id": stream._share_class_id(canonical_share),
        "required_unique_canonical_composite_figi": composite,
    }
    share_release = registries.by_name("share_class_adjudication")
    share_release.decision_ids_for_exact_source_row = lambda *_args, **_kwargs: (decision_id,)
    share_release.require_exact_source_row = lambda *_args, **_kwargs: decision
    gate_b = stream._load_gate_b_reference(tmp_path, binding.gate_b)
    source = rows[0][0]

    projection = stream._frozen_registry_projection(
        source,
        gate_b_by_composite=gate_b,
        registries=registries,
        binding=binding,
    )
    row = stream._build_and_validate_universe_row(
        source,
        projection,
        gate_b=gate_b,
        registries=registries,
        binding=binding,
    )

    assert row["canonical_composite_figi"] == composite
    assert row["asset_id"] == canonical_asset_id(composite)
    assert row["observed_share_class_figi"] == observed_share
    assert row["canonical_share_class_figi"] == canonical_share
    assert row["share_class_adjudication_id"] == decision_id
    assert row["backtest_identity_eligible"] is True


def test_non_us_composite_cannot_be_promoted_by_share_adjudication(
    tmp_path: Path,
) -> None:
    composite = "BBG000000099"
    observed_share = "BBG000000099"
    rows = (
        (_source_row(_SESSIONS[0], "FOREIGN", composite, share=observed_share),),
        (_source_row(_SESSIONS[1], "BBB", "BBG000000002", share="BBG000000022"),),
        (_source_row(_SESSIONS[2], "CCC", "BBG000000003", share="BBG000000033"),),
    )
    binding, registries, _ = _fixture(tmp_path, rows_by_session=rows)
    decision_id = "d" * 64
    share_release = registries.by_name("share_class_adjudication")
    share_release.decision_ids_for_exact_source_row = lambda *_args, **_kwargs: (decision_id,)
    gate_b = stream._load_gate_b_reference(tmp_path, binding.gate_b)

    with pytest.raises(
        S7StreamingMaterializationError,
        match="ShareClass decision preceded unique Composite resolution",
    ):
        stream._frozen_registry_projection(
            rows[0][0],
            gate_b_by_composite=gate_b,
            registries=registries,
            binding=binding,
        )


def test_standing_approval_fixed_slot_replays_first_runtime_receipt(tmp_path: Path) -> None:
    binding, _, _ = _fixture(tmp_path)
    store_streaming_source_binding(tmp_path, binding)
    caps = StreamingResourceCaps(
        source_bytes_cap=10**9,
        output_bytes_cap=10**9,
        tmp_bytes_cap=10**9,
        wall_clock_seconds_cap=3600,
        session_count_cap=10,
        row_count_cap=10,
        per_session_row_cap=10,
        batch_row_cap=10,
        disk_free_floor_bytes=DISK_HARD_FLOOR_BYTES,
    )
    plan, _ = prepare_streaming_full_plan(
        tmp_path,
        source_binding_id=binding.source_binding_id,
        resource_caps=caps,
        prepared_by="builder",
        prepared_at_utc=datetime(2024, 1, 12, 13, 55, tzinfo=UTC),
    )
    request, _ = prepare_streaming_approval_request(
        tmp_path,
        plan_id=plan["plan_id"],
        requested_by="requester",
        requested_at_utc=datetime(2024, 1, 12, 13, 56, tzinfo=UTC),
    )
    first, first_control = record_standing_streaming_approval(
        tmp_path,
        request_id=request.request_id,
        authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
        reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
        approved_by="first",
        now=lambda: _CLOCK,
    )
    replay, replay_control = record_standing_streaming_approval(
        tmp_path,
        request_id=request.request_id,
        authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
        reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
        approved_by="different-retry-actor",
        now=lambda: datetime(2024, 1, 16, 14, 0, tzinfo=UTC),
    )

    assert replay == first
    assert replay_control == first_control
    assert replay.approved_by == "first"
    assert replay.approved_at_utc == _CLOCK


def test_plan_and_request_fixed_slots_replay_and_reject_actor_fork(tmp_path: Path) -> None:
    binding, _, _ = _fixture(tmp_path)
    store_streaming_source_binding(tmp_path, binding)
    caps = StreamingResourceCaps(
        source_bytes_cap=10**9,
        output_bytes_cap=10**9,
        tmp_bytes_cap=10**9,
        wall_clock_seconds_cap=3600,
        session_count_cap=10,
        row_count_cap=10,
        per_session_row_cap=10,
        batch_row_cap=10,
        disk_free_floor_bytes=DISK_HARD_FLOOR_BYTES,
    )
    first_plan, first_plan_control = prepare_streaming_full_plan(
        tmp_path,
        source_binding_id=binding.source_binding_id,
        resource_caps=caps,
        prepared_by="builder",
        prepared_at_utc=datetime(2024, 1, 12, 13, 55, tzinfo=UTC),
    )
    replay_plan, replay_plan_control = prepare_streaming_full_plan(
        tmp_path,
        source_binding_id=binding.source_binding_id,
        resource_caps=caps,
        prepared_by="builder",
        prepared_at_utc=datetime(2024, 1, 16, 13, 55, tzinfo=UTC),
    )
    assert replay_plan == first_plan
    assert replay_plan_control == first_plan_control
    with pytest.raises(S7StreamingMaterializationError, match="plan slot actor differs"):
        prepare_streaming_full_plan(
            tmp_path,
            source_binding_id=binding.source_binding_id,
            resource_caps=caps,
            prepared_by="forked-builder",
            prepared_at_utc=datetime(2024, 1, 16, 13, 55, tzinfo=UTC),
        )

    first_request, first_request_control = prepare_streaming_approval_request(
        tmp_path,
        plan_id=first_plan["plan_id"],
        requested_by="requester",
        requested_at_utc=datetime(2024, 1, 12, 13, 56, tzinfo=UTC),
    )
    replay_request, replay_request_control = prepare_streaming_approval_request(
        tmp_path,
        plan_id=first_plan["plan_id"],
        requested_by="requester",
        requested_at_utc=datetime(2024, 1, 16, 13, 56, tzinfo=UTC),
    )
    assert replay_request == first_request
    assert replay_request_control == first_request_control
    with pytest.raises(S7StreamingMaterializationError, match="request slot actor differs"):
        prepare_streaming_approval_request(
            tmp_path,
            plan_id=first_plan["plan_id"],
            requested_by="forked-requester",
            requested_at_utc=datetime(2024, 1, 16, 13, 56, tzinfo=UTC),
        )


def test_production_execution_api_has_no_injection_hooks(tmp_path: Path) -> None:
    assert set(inspect.signature(execute_streaming_full_candidate).parameters) == {
        "data_root",
        "plan_id",
        "approval_id",
    }
    assert set(inspect.signature(execute_streaming_bounded_profile_preview).parameters) == {
        "data_root",
        "plan_id",
        "approval_id",
    }
    with pytest.raises(TypeError, match="unexpected keyword argument 'adapter'"):
        execute_streaming_full_candidate(
            tmp_path,
            plan_id="1" * 64,
            approval_id="2" * 64,
            adapter=_DirectAdapter(),
        )  # type: ignore[call-arg]


def test_bounded_profile_replays_and_full_plan_binds_exact_completion(
    tmp_path: Path,
) -> None:
    binding, registries, runtime = _fixture(tmp_path)
    profile_plan, profile_approval, first = _completed_profile(
        tmp_path, binding, registries, runtime
    )
    replay = stream._execute_streaming_bounded_profile_preview_fixture(
        tmp_path,
        plan_id=profile_plan["plan_id"],
        approval_id=profile_approval["approval_id"],
        registry_loader=lambda *_args, **_kwargs: registries,
        runtime_probe=lambda: runtime,
        now=lambda: datetime(2024, 1, 16, 14, 0, tzinfo=UTC),
        monotonic=lambda: 0.0,
        rss_probe=lambda: 1024,
        disk_free_probe=lambda _: DISK_HARD_FLOOR_BYTES + 10**9,
    )

    assert replay == first
    metrics = first["metrics"]
    assert metrics["critical_failure_count"] == 0
    assert metrics["sample_source_compressed_bytes"] > 0
    assert metrics["sample_output_bytes"] > 0
    assert metrics["sample_peak_staging_to_output_ratio_ppm"] >= 1_000_000
    full_plan, _ = prepare_streaming_full_plan(
        tmp_path,
        source_binding_id=binding.source_binding_id,
        resource_caps=_resource_caps(),
        prepared_by="builder-after-profile",
        prepared_at_utc=datetime(2024, 1, 12, 13, 55, tzinfo=UTC),
        profile_plan_id=profile_plan["plan_id"],
        profile_approval_id=profile_approval["approval_id"],
    )
    evidence = full_plan["bounded_profile_evidence"]
    assert evidence["completion_id"] == first["completion_id"]
    assert evidence["metrics_digest"] == stable_digest(first["metrics"])


def test_profile_fixed_slots_replay_first_receipts_and_reject_plan_actor_fork(
    tmp_path: Path,
) -> None:
    binding, _, _ = _fixture(tmp_path)
    store_streaming_source_binding(tmp_path, binding)
    first_plan, first_control = prepare_streaming_bounded_profile_preview_plan(
        tmp_path,
        source_binding_id=binding.source_binding_id,
        full_resource_caps=_resource_caps(),
        sample_session_cap=2,
        prepared_by="profile-builder",
        prepared_at_utc=datetime(2024, 1, 12, 13, 40, tzinfo=UTC),
    )
    replay_plan, replay_control = prepare_streaming_bounded_profile_preview_plan(
        tmp_path,
        source_binding_id=binding.source_binding_id,
        full_resource_caps=_resource_caps(),
        sample_session_cap=2,
        prepared_by="profile-builder",
        prepared_at_utc=datetime(2024, 1, 16, 13, 40, tzinfo=UTC),
    )
    assert replay_plan == first_plan
    assert replay_control == first_control
    with pytest.raises(S7StreamingMaterializationError, match="profile plan slot actor differs"):
        prepare_streaming_bounded_profile_preview_plan(
            tmp_path,
            source_binding_id=binding.source_binding_id,
            full_resource_caps=_resource_caps(),
            sample_session_cap=2,
            prepared_by="forked-profile-builder",
            prepared_at_utc=datetime(2024, 1, 16, 13, 40, tzinfo=UTC),
        )
    first_approval, first_approval_control = record_standing_streaming_profile_approval(
        tmp_path,
        plan_id=first_plan["plan_id"],
        authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
        reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
        approved_by="first",
        now=lambda: datetime(2024, 1, 12, 13, 45, tzinfo=UTC),
    )
    replay_approval, replay_approval_control = record_standing_streaming_profile_approval(
        tmp_path,
        plan_id=first_plan["plan_id"],
        authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
        reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
        approved_by="different-retry-actor",
        now=lambda: datetime(2024, 1, 16, 14, 0, tzinfo=UTC),
    )
    assert replay_approval == first_approval
    assert replay_approval_control == first_approval_control
    assert replay_approval["approved_by"] == "first"


def test_profile_intent_precedes_source_read_and_tamper_retry_fails_closed(
    tmp_path: Path,
) -> None:
    binding, registries, runtime = _fixture(tmp_path)
    store_streaming_source_binding(tmp_path, binding)
    plan, _ = prepare_streaming_bounded_profile_preview_plan(
        tmp_path,
        source_binding_id=binding.source_binding_id,
        full_resource_caps=_resource_caps(),
        sample_session_cap=1,
        prepared_by="profile-builder",
        prepared_at_utc=datetime(2024, 1, 12, 13, 40, tzinfo=UTC),
    )
    approval, _ = record_standing_streaming_profile_approval(
        tmp_path,
        plan_id=plan["plan_id"],
        authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
        reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
        approved_by="joe",
        now=lambda: datetime(2024, 1, 12, 13, 45, tzinfo=UTC),
    )
    sampled = SessionArtifactPin.from_dict(plan["sample_artifacts"][0]).artifact
    intent_path = tmp_path / stream._profile_intent_path(plan["plan_id"], approval["approval_id"])

    def tamper_after_intent(stage: str) -> None:
        assert stage == "intent_durable"
        assert intent_path.is_file()
        with (tmp_path / sampled.path).open("ab") as handle:
            handle.write(b"tamper")

    arguments = {
        "plan_id": plan["plan_id"],
        "approval_id": approval["approval_id"],
        "registry_loader": lambda *_args, **_kwargs: registries,
        "runtime_probe": lambda: runtime,
        "now": lambda: datetime(2024, 1, 12, 13, 50, tzinfo=UTC),
        "monotonic": lambda: 0.0,
        "rss_probe": lambda: 1024,
        "disk_free_probe": lambda _: DISK_HARD_FLOOR_BYTES + 10**9,
    }
    with pytest.raises(S7StreamingMaterializationError, match="exact source pin differs"):
        stream._execute_streaming_bounded_profile_preview_fixture(
            tmp_path, **arguments, checkpoint_hook=tamper_after_intent
        )
    first_intent = intent_path.read_bytes()
    with pytest.raises(S7StreamingMaterializationError, match="exact source pin differs"):
        stream._execute_streaming_bounded_profile_preview_fixture(tmp_path, **arguments)
    assert intent_path.read_bytes() == first_intent


def test_profile_rejects_full_projection_even_when_bounded_sample_fits(
    tmp_path: Path,
) -> None:
    binding, registries, runtime = _fixture(tmp_path)
    store_streaming_source_binding(tmp_path, binding)
    caps = replace(_resource_caps(), output_bytes_cap=200_000)
    plan, _ = prepare_streaming_bounded_profile_preview_plan(
        tmp_path,
        source_binding_id=binding.source_binding_id,
        full_resource_caps=caps,
        sample_session_cap=1,
        prepared_by="profile-builder",
        prepared_at_utc=datetime(2024, 1, 12, 13, 40, tzinfo=UTC),
    )
    approval, _ = record_standing_streaming_profile_approval(
        tmp_path,
        plan_id=plan["plan_id"],
        authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
        reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
        approved_by="joe",
        now=lambda: datetime(2024, 1, 12, 13, 45, tzinfo=UTC),
    )
    with pytest.raises(S7StreamingMaterializationError, match="projection breaches"):
        stream._execute_streaming_bounded_profile_preview_fixture(
            tmp_path,
            plan_id=plan["plan_id"],
            approval_id=approval["approval_id"],
            registry_loader=lambda *_args, **_kwargs: registries,
            runtime_probe=lambda: runtime,
            now=lambda: datetime(2024, 1, 12, 13, 50, tzinfo=UTC),
            monotonic=lambda: 0.0,
            rss_probe=lambda: 1024,
            disk_free_probe=lambda _: DISK_HARD_FLOOR_BYTES + 10**9,
        )
    completion = tmp_path / stream._profile_completion_path(
        plan["plan_id"], approval["approval_id"]
    )
    assert not completion.exists()
    with pytest.raises(S7StreamingMaterializationError, match="staging requires explicit review"):
        stream._execute_streaming_bounded_profile_preview_fixture(
            tmp_path,
            plan_id=plan["plan_id"],
            approval_id=approval["approval_id"],
            registry_loader=lambda *_args, **_kwargs: registries,
            runtime_probe=lambda: runtime,
            now=lambda: datetime(2024, 1, 12, 13, 50, tzinfo=UTC),
            monotonic=lambda: 0.0,
            rss_probe=lambda: 1024,
            disk_free_probe=lambda _: DISK_HARD_FLOOR_BYTES + 10**9,
        )


def test_profile_candidate_tamper_is_detected_on_replay(tmp_path: Path) -> None:
    binding, registries, runtime = _fixture(tmp_path)
    plan, approval, completion = _completed_profile(tmp_path, binding, registries, runtime)
    candidate = ExactFilePin.from_dict(completion["candidate"])
    with (tmp_path / candidate.path).open("ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(S7StreamingMaterializationError, match="exact source pin differs"):
        stream._execute_streaming_bounded_profile_preview_fixture(
            tmp_path,
            plan_id=plan["plan_id"],
            approval_id=approval["approval_id"],
            registry_loader=lambda *_args, **_kwargs: registries,
            runtime_probe=lambda: runtime,
            now=lambda: datetime(2024, 1, 16, 14, 0, tzinfo=UTC),
            monotonic=lambda: 0.0,
            rss_probe=lambda: 1024,
            disk_free_probe=lambda _: DISK_HARD_FLOOR_BYTES + 10**9,
        )


def test_profile_checks_disk_floor_after_intent_before_source_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binding, registries, runtime = _fixture(tmp_path)
    store_streaming_source_binding(tmp_path, binding)
    plan, _ = prepare_streaming_bounded_profile_preview_plan(
        tmp_path,
        source_binding_id=binding.source_binding_id,
        full_resource_caps=_resource_caps(),
        sample_session_cap=1,
        prepared_by="profile-builder",
        prepared_at_utc=datetime(2024, 1, 12, 13, 40, tzinfo=UTC),
    )
    approval, _ = record_standing_streaming_profile_approval(
        tmp_path,
        plan_id=plan["plan_id"],
        authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
        reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
        approved_by="joe",
        now=lambda: datetime(2024, 1, 12, 13, 45, tzinfo=UTC),
    )
    source_read = False

    def unexpected_source_read(*_args: object, **_kwargs: object) -> None:
        nonlocal source_read
        source_read = True
        raise AssertionError("source read reached")

    monkeypatch.setattr(stream, "_load_verified_execution_sources", unexpected_source_read)
    with pytest.raises(S7StreamingMaterializationError, match="disk floor breached"):
        stream._execute_streaming_bounded_profile_preview_fixture(
            tmp_path,
            plan_id=plan["plan_id"],
            approval_id=approval["approval_id"],
            registry_loader=lambda *_args, **_kwargs: registries,
            runtime_probe=lambda: runtime,
            now=lambda: datetime(2024, 1, 12, 13, 50, tzinfo=UTC),
            monotonic=lambda: 0.0,
            rss_probe=lambda: 1024,
            disk_free_probe=lambda _: DISK_HARD_FLOOR_BYTES - 1,
        )
    assert not source_read
    intent_path = stream._profile_intent_path(plan["plan_id"], approval["approval_id"])
    assert (tmp_path / intent_path).is_file()
    assert not (
        tmp_path
        / stream._profile_candidate_path(
            stable_digest(
                {
                    "adapter_version": stream.PRODUCTION_ADAPTER_VERSION,
                    "approval_id": approval["approval_id"],
                    "plan_id": plan["plan_id"],
                    "policy_version": stream.PROFILE_POLICY_VERSION,
                }
            )
        )
    ).exists()


def test_profile_candidate_without_completion_recovers_without_rerun(tmp_path: Path) -> None:
    binding, registries, runtime = _fixture(tmp_path)

    def crash_after_candidate(stage: str) -> None:
        if stage == "candidate_durable":
            raise RuntimeError("crash after candidate")

    with pytest.raises(RuntimeError, match="crash after candidate"):
        _completed_profile(
            tmp_path,
            binding,
            registries,
            runtime,
            checkpoint_hook=crash_after_candidate,
        )
    plan, _ = prepare_streaming_bounded_profile_preview_plan(
        tmp_path,
        source_binding_id=binding.source_binding_id,
        full_resource_caps=_resource_caps(),
        sample_session_cap=3,
        prepared_by="profile-builder",
        prepared_at_utc=datetime(2024, 1, 12, 13, 40, tzinfo=UTC),
    )
    approval, _ = record_standing_streaming_profile_approval(
        tmp_path,
        plan_id=plan["plan_id"],
        authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
        reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
        approved_by="joe",
        now=lambda: datetime(2024, 1, 12, 13, 45, tzinfo=UTC),
    )
    recovered = stream._execute_streaming_bounded_profile_preview_fixture(
        tmp_path,
        plan_id=plan["plan_id"],
        approval_id=approval["approval_id"],
        registry_loader=lambda *_args, **_kwargs: registries,
        runtime_probe=lambda: runtime,
        now=lambda: datetime(2024, 1, 16, 14, 0, tzinfo=UTC),
        monotonic=lambda: 0.0,
        rss_probe=lambda: 1024,
        disk_free_probe=lambda _: DISK_HARD_FLOOR_BYTES + 2 * 10**9,
    )
    assert recovered["complete"] is True
    assert recovered["candidate"]["path"] == stream._profile_candidate_path(
        recovered["candidate_id"]
    )


def test_production_binding_entrypoint_rejects_fixture_document(tmp_path: Path) -> None:
    binding, _, _ = _fixture(tmp_path)
    with pytest.raises(S7StreamingMaterializationError, match="caller-authored"):
        store_production_streaming_source_binding_document(tmp_path, binding.to_dict())


def _official_gate_b_replay_fixture(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    manifest_content: bytes,
) -> GateBReferencePin:
    candidate_id = stable_digest({"fixture": "official-gate-b"})
    manifest = _write_bytes(root, "manifests/test/official-gate-b.json", manifest_content)
    data = _write_bytes(root, "silver/test/official-gate-b.parquet", b"fixture-parquet")

    def verify_market_candidate(
        data_root: Path,
        *,
        candidate_path: str,
        candidate_id: str,
        candidate_sha256: str,
        require_production_approval: bool,
    ) -> SimpleNamespace:
        assert data_root == root
        assert candidate_path == manifest.path
        assert candidate_id == stable_digest({"fixture": "official-gate-b"})
        assert candidate_sha256 == manifest.sha256
        assert require_production_approval is True
        return SimpleNamespace(
            candidate_id=candidate_id,
            data_path=data.path,
            manifest_path=manifest.path,
        )

    monkeypatch.setattr(stream, "verify_market_classification_candidate", verify_market_candidate)
    return GateBReferencePin(
        candidate_id=candidate_id,
        candidate_state=stream.STREAMING_STATE,
        reference_version=stream.PRODUCTION_GATE_B_REFERENCE_VERSION,
        closed=True,
        manifest=manifest,
        data=data,
    )


def test_official_gate_b_production_replay_accepts_compact_no_lf_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate_id = stable_digest({"fixture": "official-gate-b"})
    document = {"candidate_id": candidate_id, "state": stream.STREAMING_STATE}
    content = _compact_json_bytes(document)
    requested = _official_gate_b_replay_fixture(tmp_path, monkeypatch, content)

    replayed = stream._replay_official_gate_b(tmp_path, requested)

    assert replayed.candidate_id == candidate_id
    assert replayed.reference_version == stream.PRODUCTION_GATE_B_REFERENCE_VERSION
    assert replayed.manifest == requested.manifest
    assert replayed.data == requested.data
    assert not content.endswith(b"\n")
    with pytest.raises(S7StreamingMaterializationError, match="not canonical JSON"):
        stream._load_canonical_json(content, "LF control")


@pytest.mark.parametrize(
    ("variant", "expected_error"),
    [
        ("newline", "not compact canonical JSON"),
        ("pretty", "not compact canonical JSON"),
        ("duplicate", "invalid JSON"),
        ("tail_tamper", "invalid JSON"),
    ],
)
def test_official_gate_b_production_replay_rejects_non_producer_json_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    variant: str,
    expected_error: str,
) -> None:
    candidate_id = stable_digest({"fixture": "official-gate-b"})
    document = {"candidate_id": candidate_id, "state": stream.STREAMING_STATE}
    compact = _compact_json_bytes(document)
    variants = {
        "newline": compact + b"\n",
        "pretty": json.dumps(document, indent=2, sort_keys=True).encode("utf-8"),
        "duplicate": (
            f'{{"candidate_id":"{candidate_id}","state":"{stream.STREAMING_STATE}",'
            f'"state":"{stream.STREAMING_STATE}"}}'
        ).encode(),
        "tail_tamper": compact + b"#",
    }
    requested = _official_gate_b_replay_fixture(
        tmp_path,
        monkeypatch,
        variants[variant],
    )

    with pytest.raises(S7StreamingMaterializationError, match=expected_error):
        stream._replay_official_gate_b(tmp_path, requested)


def test_production_gate_b_pin_loader_uses_compact_dialect(tmp_path: Path) -> None:
    candidate_id = stable_digest({"fixture": "production-gate-b-pin"})
    manifest = _write_bytes(
        tmp_path,
        "manifests/test/production-gate-b-pin.json",
        _compact_json_bytes({"candidate_id": candidate_id, "state": stream.STREAMING_STATE}),
    )
    data_path = tmp_path / "silver/test/production-gate-b-pin.parquet"
    data_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "classification": "us_composite",
                    "composite_figi": "BBG000000001",
                    "reference_version": stream.PRODUCTION_GATE_B_REFERENCE_VERSION,
                    "relation_share_class_conflict": False,
                    "selected_market_code": "US",
                    "selected_share_class_figi": "BBG000000011",
                    "source_available_session": _SESSIONS[0],
                }
            ]
        ),
        data_path,
    )
    data = ExactFilePin(
        "silver/test/production-gate-b-pin.parquet",
        stream.sha256_file(data_path),
        data_path.stat().st_size,
    )
    pin = GateBReferencePin(
        candidate_id=candidate_id,
        candidate_state=stream.STREAMING_STATE,
        reference_version=stream.PRODUCTION_GATE_B_REFERENCE_VERSION,
        closed=True,
        manifest=manifest,
        data=data,
    )

    loaded = stream._load_gate_b_reference(tmp_path, pin)

    assert loaded["BBG000000001"] == {
        "classification": "us_composite",
        "relation_share_class_conflict": False,
        "selected_market_code": "US",
        "selected_share_class_figi": "BBG000000011",
        "source_available_session": _SESSIONS[0],
    }


def test_gate_c_identity_case_preview_is_exactly_replayed(tmp_path: Path) -> None:
    preview_id = stable_digest({"fixture": "official-identity-case-preview"})
    preview = _write_json(
        tmp_path,
        "manifests/test/official-identity-case-preview.json",
        {
            "preview_artifact_id": preview_id,
            "result": {
                "preview_manifest_available_session": _SESSIONS[0].isoformat(),
            },
        },
    )
    candidate = {
        "registry_loader_source_refs": {
            "detector_preview": {
                "bytes": preview.bytes,
                "path": preview.path,
                "preview_artifact_id": preview_id,
                "sha256": preview.sha256,
            }
        }
    }

    loaded_id, loaded_pin, loaded_session = stream._load_gate_c_identity_case_preview(
        tmp_path,
        candidate,
    )
    assert (loaded_id, loaded_pin, loaded_session) == (preview_id, preview, _SESSIONS[0])

    tampered = json.loads(json.dumps(candidate))
    tampered["registry_loader_source_refs"]["detector_preview"]["sha256"] = "0" * 64
    with pytest.raises(S7StreamingMaterializationError, match="receipt differs"):
        stream._load_gate_c_identity_case_preview(tmp_path, tampered)

    wrong_embedded_id = json.loads(json.dumps(candidate))
    wrong_embedded_id["registry_loader_source_refs"]["detector_preview"][
        "preview_artifact_id"
    ] = "1" * 64
    with pytest.raises(S7StreamingMaterializationError, match="embedded ID differs"):
        stream._load_gate_c_identity_case_preview(tmp_path, wrong_embedded_id)


def test_production_cli_has_no_publish_or_adapter_input_and_freezes_full_adapter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    parser = stream_cli.build_parser()
    commands = next(
        action.choices
        for action in parser._actions
        if isinstance(action.choices, dict) and "execute-full" in action.choices
    )
    assert all("publish" not in command for command in commands)
    help_text = parser.format_help()
    assert "--adapter" not in help_text
    observed: dict[str, object] = {}

    def execute(root: Path, **kwargs: object) -> SimpleNamespace:
        observed.update(kwargs)
        assert root == tmp_path
        return SimpleNamespace(
            approval_id="1" * 64,
            candidate_id="2" * 64,
            candidate_path="silver/candidate",
            completion_id="3" * 64,
            completion_path="manifests/completion.json",
            idempotent=False,
            plan_id="4" * 64,
            raw_collision_rows=0,
            session_count=1,
            source_row_count=1,
            state="awaiting_review",
            table_row_counts={"universe_daily": 1},
        )

    monkeypatch.setattr(stream_cli, "execute_streaming_full_candidate", execute)
    assert (
        stream_cli.main(
            [
                "execute-full",
                "--data-root",
                str(tmp_path),
                "--plan-id",
                "4" * 64,
                "--approval-id",
                "1" * 64,
            ]
        )
        == 0
    )
    assert "adapter" not in observed
    assert "requested_at_utc" not in observed
    assert "source_rows" not in observed
    assert json.loads(capsys.readouterr().out)["state"] == "awaiting_review"


def test_full_completion_is_written_only_after_final_caps_and_fast_path_rechecks(
    tmp_path: Path,
) -> None:
    binding, registries, runtime = _fixture(tmp_path)
    candidate_parent = tmp_path / "silver/identity/s7-streaming-full-candidates"

    def fail_after_candidate(_: Path) -> int:
        return (
            DISK_HARD_FLOOR_BYTES - 1
            if candidate_parent.exists() and any(candidate_parent.iterdir())
            else DISK_HARD_FLOOR_BYTES + 10**9
        )

    with pytest.raises(S7StreamingMaterializationError, match="disk floor breached"):
        _execute(
            tmp_path,
            binding,
            registries,
            runtime,
            disk_free_probe=fail_after_candidate,
        )
    assert candidate_parent.is_dir()
    assert not list(
        tmp_path.glob(
            "manifests/silver/identity/s7-streaming-full-execution-completions/**/manifest.json"
        )
    )
    result = _execute(tmp_path, binding, registries, runtime)
    assert (tmp_path / result.completion_path).is_file()
    with pytest.raises(S7StreamingMaterializationError, match="disk floor breached"):
        _execute(
            tmp_path,
            binding,
            registries,
            runtime,
            disk_free_probe=lambda _: DISK_HARD_FLOOR_BYTES - 1,
        )


def test_full_candidate_publish_is_atomic_no_clobber(tmp_path: Path) -> None:
    binding, registries, runtime = _fixture(tmp_path)
    foreign_target: Path | None = None

    def install_foreign_target(stage: str, _: date | None) -> None:
        nonlocal foreign_target
        if stage != "before_candidate_publish":
            return
        staging_parent = tmp_path / "tmp/silver-s7-streaming-full"
        staging = next(staging_parent.iterdir())
        foreign_target = tmp_path / "silver/identity/s7-streaming-full-candidates" / staging.name
        foreign_target.mkdir(parents=True)

    with pytest.raises(S7StreamingMaterializationError, match="exclusive candidate publish"):
        _execute(
            tmp_path,
            binding,
            registries,
            runtime,
            checkpoint_hook=install_foreign_target,
        )
    assert foreign_target is not None and foreign_target.is_dir()
    assert not any(foreign_target.iterdir())
    assert list((tmp_path / "tmp/silver-s7-streaming-full").iterdir())


def test_frozen_adapter_composite_and_share_collisions_remain_ineligible(
    tmp_path: Path,
) -> None:
    binding, registries, _ = _fixture(tmp_path)
    source = _source_row(_SESSIONS[0], "AAA", "BBG000000001", share="BBG000000011")
    gate_b = stream._load_gate_b_reference(tmp_path, binding.gate_b)
    registries.composite_matches = lambda *_args, **_kwargs: (
        ("identity_adjudication", "a" * 64),
        ("provider_composite_override", "b" * 64),
    )
    composite_projection = stream._frozen_registry_projection(
        source,
        gate_b_by_composite=gate_b,
        registries=registries,
        binding=binding,
    )
    composite_row = stream._build_and_validate_universe_row(
        source,
        composite_projection,
        gate_b=gate_b,
        registries=registries,
        binding=binding,
    )
    assert composite_row["identity_resolution_status"] == "unresolved_registry_collision"
    assert composite_row["composite_registry_match_count"] == 2
    assert composite_row["composite_registry_collision"] is True
    assert composite_row["asset_id"] is None
    assert composite_row["canonical_composite_figi"] is None
    assert composite_row["backtest_identity_eligible"] is False
    assert composite_row["identity_quality_liquidation_signal"] is False

    registries.composite_matches = lambda *_args, **_kwargs: ()
    share_release = registries.by_name("share_class_adjudication")
    share_release.decision_ids_for_exact_source_row = lambda *_args, **_kwargs: (
        "c" * 64,
        "d" * 64,
    )
    share_projection = stream._frozen_registry_projection(
        source,
        gate_b_by_composite=gate_b,
        registries=registries,
        binding=binding,
    )
    share_row = stream._build_and_validate_universe_row(
        source,
        share_projection,
        gate_b=gate_b,
        registries=registries,
        binding=binding,
    )
    assert share_row["identity_resolution_status"] == "resolved_conflicted"
    assert share_row["composite_registry_collision"] is False
    assert share_row["canonical_composite_figi"] is None
    assert share_row["canonical_share_class_figi"] is None
    assert share_row["asset_id"] is None
    assert share_row["ticker_alias_id"] is None
    assert share_row["backtest_identity_eligible"] is False
    assert share_row["identity_quality_liquidation_signal"] is False


def test_interruption_leaves_staging_and_restart_fails_closed(tmp_path: Path) -> None:
    binding, registries, runtime = _fixture(tmp_path)

    def crash(stage: str, _: date | None) -> None:
        if stage == "pass1_session_committed":
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected crash"):
        _execute(
            tmp_path,
            binding,
            registries,
            runtime,
            checkpoint_hook=crash,
        )
    with pytest.raises(S7StreamingMaterializationError, match="staging requires explicit review"):
        _execute(tmp_path, binding, registries, runtime)


def test_many_session_alias_churn_and_aggregates_remain_bounded(tmp_path: Path) -> None:
    binding, registries, runtime = _fixture(tmp_path)
    result = _execute(tmp_path, binding, registries, runtime)
    template = pq.read_table(
        tmp_path
        / result.candidate_path
        / "data/universe_daily/session_date=2024-01-12/part-00000.parquet"
    ).to_pylist()[0]
    session_count = 20_000
    sessions = tuple(date(2030, 1, 1) + timedelta(days=index) for index in range(session_count))
    spill = stream._AliasSpill.create(tmp_path / "stress/aliases.sqlite3")
    open_aliases: dict[str, object] = {}
    assets: dict[str, object] = {}
    issuers: dict[str, object] = {}

    tracemalloc.start()
    for index, session in enumerate(sessions):
        if index % 2:
            current = open_aliases.pop("AAA", None)
            if current is not None:
                spill.append(stream._close_alias(current, sessions=sessions))
            continue
        row = dict(template)
        row["session_date"] = session
        row["session_year"] = session.year
        row["selected_source_record_id"] = stable_digest({"stress": index})
        stream._update_alias_interval(
            spill,
            open_aliases,
            row=row,
            session_index=index,
            sessions=sessions,
        )
        stream._update_aggregates(assets, issuers, row=row, source_name="AAA Corp")
        assert len(open_aliases) <= 1
    current = open_aliases.pop("AAA", None)
    if current is not None:
        spill.append(stream._close_alias(current, sessions=sessions))
    spill.commit()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    spill.close()

    assert spill.row_count == session_count // 2
    assert peak < 25 * 1024 * 1024
    aggregate = next(iter(assets.values()))
    assert aggregate.row_count == session_count // 2
    assert not hasattr(aggregate, "sessions")
    assert not hasattr(aggregate, "source_record_ids")
    assert len(aggregate.tickers) == 1
    assert len(next(iter(issuers.values())).tickers) == 1
