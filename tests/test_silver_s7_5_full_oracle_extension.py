from __future__ import annotations

from dataclasses import replace
from datetime import date
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
