from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import identity_materialization_streaming as stream
from ame_stocks_api.silver import incremental_i5_shadow_runtime as i5
from ame_stocks_api.silver.asset_contract import UNIVERSE_SOURCE_DAILY_CONTRACT
from ame_stocks_api.silver.asset_incremental_contract import S4SessionPartitionReceipt
from ame_stocks_api.silver.incremental_contract import ArtifactPin

_TARGET = date(2026, 7, 10)
_AVAILABLE = date(2026, 7, 29)


def _digest(label: str) -> str:
    return stable_digest({"s7-5-full-oracle-extension-test": label})


def _artifact(label: str, path: str, *, bytes_: int = 101) -> ArtifactPin:
    return ArtifactPin(path=path, sha256=_digest(f"{label}-sha"), bytes=bytes_)


def _exact(pin: ArtifactPin) -> stream.ExactFilePin:
    return stream.ExactFilePin(path=pin.path, sha256=pin.sha256, bytes=pin.bytes)


def _authority_fixture() -> tuple[
    stream.S75IncrementalSessionExtension,
    SimpleNamespace,
    SimpleNamespace,
    S4SessionPartitionReceipt,
]:
    base_id = _digest("base-binding")
    base_artifact = _artifact(
        "base-binding",
        "manifests/silver/identity/s7-streaming-full-source-bindings/"
        f"source_binding_id={base_id}/manifest.json",
    )
    i2_receipt_id = _digest("i2-receipt")
    i2_artifact = _artifact(
        "i2-receipt",
        "manifests/silver/incremental/s4/assets/session_year=2026/"
        "session_date=2026-07-10/run-receipt.json",
    )
    i2_source_binding_id = _digest("i2-source-binding")
    membership_artifact = _artifact(
        "membership",
        "silver/assets/universe_source_daily/session_year=2026/"
        "session_date=2026-07-10/part.parquet",
        bytes_=211,
    )
    membership = S4SessionPartitionReceipt(
        table_name="universe_source_daily",
        session_date=_TARGET,
        artifact=membership_artifact,
        row_count=17,
        contract_id=UNIVERSE_SOURCE_DAILY_CONTRACT.contract_id,
        schema_digest=UNIVERSE_SOURCE_DAILY_CONTRACT.schema_digest,
        source_binding_id=i2_source_binding_id,
        row_funnel_digest=_digest("membership-funnel"),
        qa_result_set_digest=_digest("membership-qa"),
    )
    parent_frontier_id = _digest("parent-frontier")
    extension = stream.S75IncrementalSessionExtension(
        base_source_binding_id=base_id,
        base_source_binding_manifest=_exact(base_artifact),
        i2_run_receipt_id=i2_receipt_id,
        i2_run_receipt=_exact(i2_artifact),
        i2_source_binding_id=i2_source_binding_id,
        i2_parent_frontier_id=parent_frontier_id,
        receipt_available_session=_AVAILABLE,
        membership_artifact=stream.SessionArtifactPin(
            session_date=_TARGET,
            row_count=membership.row_count,
            artifact=_exact(membership_artifact),
        ),
        membership_partition_receipt_id=membership.partition_receipt_id,
        membership_contract_id=membership.contract_id,
        membership_schema_digest=membership.schema_digest,
    )
    i2_run = SimpleNamespace(
        receipt=SimpleNamespace(
            receipt_id=i2_receipt_id,
            session_date=_TARGET,
            source_binding_id=i2_source_binding_id,
            parent_frontier_id=parent_frontier_id,
            receipt_available_session=_AVAILABLE,
            partition_receipts=(membership,),
        ),
        receipt_artifact=i2_artifact,
    )
    delta_inputs = SimpleNamespace(
        source_binding=SimpleNamespace(source_binding_id=base_id),
        source_binding_artifact=base_artifact,
        i2_run=i2_run,
    )
    run_spec = SimpleNamespace(
        i2_receipts=(SimpleNamespace(receipt_id=i2_receipt_id, artifact=i2_artifact),)
    )
    return extension, delta_inputs, run_spec, membership


def test_extension_round_trip_and_exact_i2_loader(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    extension, delta_inputs, _run_spec, _membership = _authority_fixture()
    assert stream.S75IncrementalSessionExtension.from_dict(extension.to_dict()) == extension
    monkeypatch.setattr(
        stream,
        "load_completed_s4_asset_session_run",
        lambda _root, _session: delta_inputs.i2_run,
    )

    rebuilt = stream._load_s75_incremental_session_extension(
        tmp_path,
        base=delta_inputs.source_binding,
        base_pin=extension.base_source_binding_manifest,
        session_date=_TARGET,
    )

    assert rebuilt == extension


def test_extension_loader_rejects_tampered_membership_contract(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    extension, delta_inputs, _run_spec, membership = _authority_fixture()
    tampered = replace(membership, contract_id=_digest("wrong-contract"))
    original_receipt = delta_inputs.i2_run.receipt
    bad_run = SimpleNamespace(
        receipt=SimpleNamespace(
            receipt_id=original_receipt.receipt_id,
            session_date=original_receipt.session_date,
            source_binding_id=original_receipt.source_binding_id,
            parent_frontier_id=original_receipt.parent_frontier_id,
            receipt_available_session=original_receipt.receipt_available_session,
            partition_receipts=(tampered,),
        ),
        receipt_artifact=delta_inputs.i2_run.receipt_artifact,
    )
    monkeypatch.setattr(
        stream,
        "load_completed_s4_asset_session_run",
        lambda _root, _session: bad_run,
    )

    with pytest.raises(stream.S7StreamingMaterializationError, match="contract differs"):
        stream._load_s75_incremental_session_extension(
            tmp_path,
            base=delta_inputs.source_binding,
            base_pin=extension.base_source_binding_manifest,
            session_date=_TARGET,
        )


def test_bounded_profile_sample_uses_base_rows_and_drops_extension_authority() -> None:
    extension, _delta_inputs, _run_spec, _membership = _authority_fixture()
    base_membership = replace(
        extension.membership_artifact,
        session_date=date(2026, 7, 9),
        artifact=stream.ExactFilePin(
            path=(
                "silver/assets/universe_source_daily/session_year=2026/"
                "session_date=2026-07-09/part.parquet"
            ),
            sha256=_digest("base-membership-sha"),
            bytes=199,
        ),
    )

    @dataclass(frozen=True)
    class Binding:
        mode: str
        membership_artifacts: tuple[stream.SessionArtifactPin, ...]
        incremental_session_extension: stream.S75IncrementalSessionExtension | None
        transition_profile_anchor_binding: SimpleNamespace

    original = Binding(
        mode="production",
        membership_artifacts=(base_membership, extension.membership_artifact),
        incremental_session_extension=extension,
        transition_profile_anchor_binding=SimpleNamespace(mandatory_sessions=()),
    )
    population = stream._profile_sample_population(original)  # type: ignore[arg-type]
    sampled = stream._bounded_profile_sample_binding(
        original,  # type: ignore[arg-type]
        population,
    )

    assert population == (base_membership,)
    assert sampled.mode == "fixture"
    assert sampled.membership_artifacts == (base_membership,)
    assert sampled.incremental_session_extension is None


def test_historical_full_oracle_runtime_replays_old_git_without_replace_refs(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()

    def git(*arguments: str) -> str:
        result = subprocess.run(
            ("git", "-C", str(repository), *arguments),
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    git("init")
    git("config", "user.email", "s75-test@example.invalid")
    git("config", "user.name", "S7.5 Test")
    runtime_path = repository / "tracked_runtime.py"
    runtime_path.write_bytes(b"RECORDED = True\n")
    git("add", "tracked_runtime.py")
    git("commit", "-m", "record runtime")
    recorded_commit = git("rev-parse", "HEAD")
    monkeypatch.setattr(stream, "_repository_root", lambda: repository)
    monkeypatch.setattr(stream, "_RUNTIME_SOURCE_PATHS", ("tracked_runtime.py",))
    recorded = stream._repository_runtime_binding()

    runtime_path.write_bytes(b"RECORDED = False\n")
    git("add", "tracked_runtime.py")
    git("commit", "-m", "change runtime")
    replacement_commit = git("rev-parse", "HEAD")
    execution = stream._repository_runtime_binding()
    git("replace", recorded_commit, replacement_commit)

    stream._verify_recorded_streaming_runtime_binding(recorded)
    binding = SimpleNamespace(
        mode="production",
        incremental_session_extension=object(),
        runtime_binding=recorded,
    )

    stream._verify_bound_execution_runtime(
        binding,
        execution_runtime_binding=execution,
        runtime_probe=lambda: execution,
    )
    with pytest.raises(stream.S7StreamingMaterializationError, match="execution runtime differs"):
        stream._verify_bound_execution_runtime(
            binding,
            execution_runtime_binding=execution,
            runtime_probe=lambda: recorded,
        )

    tampered = dict(recorded)
    files = [dict(value) for value in recorded["runtime_files"]]
    files[0]["sha256"] = hashlib.sha256(b"RECORDED = False\n").hexdigest()
    files[0]["bytes"] = len(b"RECORDED = False\n")
    tampered["runtime_files"] = files
    tampered["runtime_file_set_digest"] = stable_digest(files)
    with pytest.raises(stream.S7StreamingMaterializationError, match="mode/blob/bytes differ"):
        stream._verify_recorded_streaming_runtime_binding(tampered)


def test_nonextended_full_execution_still_requires_current_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = {"runtime": "current"}
    monkeypatch.setattr(stream, "_validate_runtime_binding", lambda _value: None)
    binding = SimpleNamespace(
        mode="production",
        incremental_session_extension=None,
        runtime_binding=current,
    )
    monkeypatch.setattr(
        stream,
        "_verify_recorded_streaming_runtime_binding",
        lambda _value: (_ for _ in ()).throw(AssertionError("historical replay was selected")),
    )
    stream._verify_bound_execution_runtime(
        binding,
        execution_runtime_binding=None,
        runtime_probe=lambda: current,
    )
    with pytest.raises(stream.S7StreamingMaterializationError, match="runtime Git/source"):
        stream._verify_bound_execution_runtime(
            binding,
            execution_runtime_binding=None,
            runtime_probe=lambda: {"runtime": "different"},
        )


def test_extended_profile_plan_binds_executor_runtime_and_changes_slot_id(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    extension, _delta_inputs, _run_spec, _membership = _authority_fixture()
    source_runtime = {"runtime": "historical-source"}
    execution_runtime = {"runtime": "current-executor"}
    membership = extension.membership_artifact
    base_membership = replace(
        membership,
        session_date=date(2026, 7, 9),
        artifact=stream.ExactFilePin(
            path=(
                "silver/assets/universe_source_daily/session_year=2026/"
                "session_date=2026-07-09/part.parquet"
            ),
            sha256=_digest("profile-base-membership-sha"),
            bytes=97,
        ),
    )
    transition = SimpleNamespace(mandatory_sessions=(), to_dict=lambda: {"anchor": "fixed"})
    binding = SimpleNamespace(
        contract_approvals=(),
        incremental_session_extension=extension,
        membership_artifacts=(base_membership, membership),
        mode="production",
        runtime_binding=source_runtime,
        source_binding_id=_digest("extended-binding"),
        transition_profile_anchor_binding=transition,
    )
    binding_receipt = stream.ExactFilePin(
        path=(
            "manifests/silver/identity/s7-streaming-full-source-bindings/"
            f"source_binding_id={binding.source_binding_id}/manifest.json"
        ),
        sha256=_digest("extended-binding-pin"),
        bytes=123,
    )
    monkeypatch.setattr(
        stream, "_load_source_binding", lambda _root, _id: (binding, binding_receipt)
    )
    monkeypatch.setattr(stream, "_trusted_contract_approvals", lambda _root, _binding: ())
    monkeypatch.setattr(stream, "_validate_binding_against_caps", lambda *_args: None)
    monkeypatch.setattr(stream, "_repository_runtime_binding", lambda: execution_runtime)
    caps = stream.StreamingResourceCaps(
        source_bytes_cap=1,
        row_count_cap=1,
        session_count_cap=1,
        per_session_row_cap=1,
        output_bytes_cap=1,
        tmp_bytes_cap=1,
        rss_bytes_cap=1,
        disk_free_floor_bytes=stream.DISK_HARD_FLOOR_BYTES,
        wall_clock_seconds_cap=1,
        batch_row_cap=1,
        worker_count=1,
    )

    plan, _ = stream.prepare_streaming_bounded_profile_preview_plan(
        tmp_path,
        source_binding_id=binding.source_binding_id,
        full_resource_caps=caps,
        sample_session_cap=1,
        prepared_by="s75-test",
        prepared_at_utc=datetime(2026, 8, 9, tzinfo=UTC),
    )

    assert plan["runtime_binding"] == source_runtime
    assert plan["execution_runtime_binding"] == execution_runtime
    legacy_slot = dict(plan)
    legacy_slot.pop("plan_id")
    legacy_slot.pop("prepared_at_utc")
    legacy_slot.pop("prepared_by")
    legacy_slot.pop("execution_runtime_binding")
    legacy_slot["artifact_type"] = "s7_streaming_size_profile_plan_slot"
    assert stable_digest(legacy_slot) != plan["plan_id"]


@pytest.mark.parametrize(
    "field,value_label",
    (
        ("base_source_binding_id", "wrong-base"),
        ("i2_parent_frontier_id", "wrong-frontier"),
        ("membership_partition_receipt_id", "wrong-partition"),
    ),
)
def test_i5_full_oracle_rejects_joint_authority_mismatch(
    field: str,
    value_label: str,
) -> None:
    extension, delta_inputs, run_spec, _membership = _authority_fixture()
    i5._verify_full_oracle_input_closure(
        delta_inputs=delta_inputs,
        oracle_binding=SimpleNamespace(incremental_session_extension=extension),
        run_spec=run_spec,
    )
    tampered = replace(extension, **{field: _digest(value_label)})

    with pytest.raises(i5.I5FullOracleSeamError, match="exact base plus I2 inputs"):
        i5._verify_full_oracle_input_closure(
            delta_inputs=delta_inputs,
            oracle_binding=SimpleNamespace(incremental_session_extension=tampered),
            run_spec=run_spec,
        )


def test_missing_gate_b_fallback_is_scoped_and_ineligible() -> None:
    extension, _delta_inputs, _run_spec, _membership = _authority_fixture()
    binding = SimpleNamespace(
        incremental_session_extension=extension,
        registry_pins=(SimpleNamespace(release_available_session=_AVAILABLE),),
        cutoff_session=_AVAILABLE,
    )
    source = {
        "active_on_date": True,
        "composite_figi": "BBG000KMY6N2",
        "selected_source_record_id": _digest("missing-gate-b-source"),
        "session_date": _TARGET,
        "source_available_session": _TARGET,
    }

    projection = stream._s75_incremental_full_projection(
        source,
        gate_b_by_composite={},
        registries=SimpleNamespace(),
        binding=binding,
    )

    assert projection.identity_resolution_status == "unresolved"
    assert projection.identity_resolution_method == ("cross_market_composite_pending_unresolved")
    assert projection.backtest_identity_eligible is False
    assert projection.canonical_composite_figi is None
    assert stream._is_s75_pending_extension_projection(source, projection, binding)

    with pytest.raises(stream.S7StreamingMaterializationError, match="scope differs"):
        stream._s75_incremental_full_projection(
            {**source, "session_date": date(2026, 7, 9)},
            gate_b_by_composite={},
            registries=SimpleNamespace(),
            binding=binding,
        )


def test_i5_requires_all_sixteen_fail_closed_gate_b_rows() -> None:
    extension, _delta_inputs, _run_spec, _membership = _authority_fixture()
    binding = SimpleNamespace(incremental_session_extension=extension)
    row = {
        "asset_id": None,
        "backtest_identity_eligible": False,
        "canonical_composite_figi": None,
        "cross_market_classification_status": "not_classified",
        "current_reference_factor_eligible": False,
        "identity_disposition": "pending_cross_market_review",
        "identity_resolution_method": "cross_market_composite_pending_unresolved",
        "identity_resolution_status": "unresolved",
        "ticker_alias_id": None,
    }
    rows = {
        "universe_daily": {
            _TARGET.isoformat(): tuple(
                {**row, "selected_source_record_id": _digest(f"pending-{index}")}
                for index in range(stream.S75_GATE_B_UNATTEMPTED_SOURCE_ROW_COUNT)
            )
        }
    }

    assert i5._expected_reference_unattempted_rows(binding, rows) == (
        stream.S75_GATE_B_UNATTEMPTED_SOURCE_ROW_COUNT
    )
    rows["universe_daily"][_TARGET.isoformat()] = rows["universe_daily"][_TARGET.isoformat()][:-1]
    with pytest.raises(i5.I5FullOracleSeamError, match="extension scope differs"):
        i5._expected_reference_unattempted_rows(binding, rows)
