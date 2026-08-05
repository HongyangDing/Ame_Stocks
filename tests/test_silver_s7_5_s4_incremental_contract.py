from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.asset_incremental import (
    S4_ASSET_INCREMENTAL_PARQUET_WRITER_POLICY,
    S4_ASSET_INCREMENTAL_TRANSFORM_SEMANTICS_DIGEST,
    S4_BASE_TERMINAL_PARTITION_SET_RULE_VERSION,
    run_s4_asset_session_incremental,
)
from ame_stocks_api.silver.asset_incremental_contract import (
    S4AssetIncrementalContractError,
    S4BaseFrontier,
    S4ParentFrontierPin,
    S4ParentKind,
    S4ReferenceBinding,
    S4SessionPartitionReceipt,
    S4SessionRunReceipt,
    S4SessionRunSpec,
    S4SessionSourceBinding,
)
from ame_stocks_api.silver.contracts import (
    SourceInventory,
    SourceInventoryItem,
    SourceLayer,
    UpstreamManifestRef,
    arrow_schema_digest,
)
from ame_stocks_api.silver.incremental_contract import ArtifactPin

SESSION = date(2026, 5, 11)
AVAILABLE = date(2026, 7, 13)
TABLES = (
    "asset_observation_daily",
    "asset_observation_version",
    "universe_source_daily",
)
REPO_ROOT = Path(__file__).resolve().parents[1]
I2_CONTRACT_ID = "400a788ff4a6cc173b7814ac3b81b5609b75302d3f455bfcfe9b1e4a17b905a0"
I2_CANDIDATE_SHA256 = "43d1f8e9030cd36daa6d2423c87cb5f98eaa3a025e26541644dbad4e42986a2c"
I2_CANDIDATE_PATH = (
    REPO_ROOT / "docs/silver/contracts/control/s7_5_s4_session_incremental_bundle-v1.candidate.json"
)


def _digest(label: str) -> str:
    return stable_digest(label)


def _pin(label: str, *, suffix: str = ".json") -> ArtifactPin:
    return ArtifactPin(
        path=f"fixtures/{label}{suffix}",
        sha256=_digest(f"bytes:{label}"),
        bytes=100 + len(label),
    )


def _inventory(*, git_commit: str = "a" * 40) -> SourceInventory:
    return SourceInventory(
        source_dataset="assets",
        source_layer=SourceLayer.BRONZE,
        git_commit=git_commit,
        upstream_manifests=(
            UpstreamManifestRef(path="manifests/massive/assets/a.json", sha256=_digest("ma")),
            UpstreamManifestRef(path="manifests/massive/assets/i.json", sha256=_digest("mi")),
        ),
        artifacts=(
            SourceInventoryItem(
                path="bronze/massive/assets/request_id=a/page-00000.json.gz",
                sha256=_digest("pa"),
                bytes=10,
                row_count=2,
                media_type="application/gzip+json",
            ),
            SourceInventoryItem(
                path="bronze/massive/assets/request_id=i/page-00000.json.gz",
                sha256=_digest("pi"),
                bytes=11,
                row_count=1,
                media_type="application/gzip+json",
            ),
        ),
    )


def _source(*, git_commit: str = "a" * 40) -> S4SessionSourceBinding:
    return S4SessionSourceBinding(
        session_date=SESSION,
        inventory=_inventory(git_commit=git_commit),
        active_request_id=_digest("active-request"),
        inactive_request_id=_digest("inactive-request"),
        pair_capture_completed_at_utc=datetime(2026, 7, 11, 14, 4, tzinfo=UTC),
        pair_available_session=AVAILABLE,
        page_count=2,
        declared_row_count=3,
    )


def _reference() -> S4ReferenceBinding:
    return S4ReferenceBinding(
        ticker_types=("CS", "ETF"),
        exchange_mics=("XNAS", "XNYS"),
        dependency_pins=(_pin("exchange-release"), _pin("ticker-type-release")),
    )


def _base() -> S4BaseFrontier:
    release_set_id = _digest("s4-base-release-set")
    return S4BaseFrontier(
        base_release_set_id=release_set_id,
        base_release_set_artifact=ArtifactPin(
            path=(
                "manifests/silver/release-sets/assets/"
                f"release_set_id={release_set_id}/manifest.json"
            ),
            sha256=_digest("s4-base-release-set-marker"),
            bytes=1_000,
        ),
        terminal_session=date(2026, 5, 8),
        terminal_partition_set_digest=_digest("terminal-partitions"),
        calendar_artifact_id=_digest("calendar"),
        reference_binding_id=_reference().binding_id,
        contract_ids_by_table={table: _digest(f"contract:{table}") for table in TABLES},
        schema_digests_by_table={table: _digest(f"schema:{table}") for table in TABLES},
        transform_semantics_digest=_digest("transform"),
        parquet_writer_policy={
            "compression": "zstd",
            "version": "2.6",
            "write_statistics": True,
        },
        release_available_session=date(2026, 7, 10),
    )


def _parent() -> S4ParentFrontierPin:
    base = _base()
    return S4ParentFrontierPin(
        parent_kind=S4ParentKind.BASE_RELEASE,
        terminal_session=base.terminal_session,
        terminal_receipt_id=base.frontier_id,
        artifact=_pin("base-frontier"),
    )


def _spec() -> S4SessionRunSpec:
    return S4SessionRunSpec(
        parent_frontier=_parent(),
        source_binding=_source(),
        reference_binding=_reference(),
        calendar_artifact_id=_digest("calendar"),
        calendar_artifact=_pin("calendar"),
        contract_ids_by_table={table: _digest(f"contract:{table}") for table in TABLES},
        schema_digests_by_table={table: _digest(f"schema:{table}") for table in TABLES},
        transform_semantics_digest=_digest("transform"),
        parquet_writer_policy={
            "compression": "zstd",
            "version": "2.6",
            "write_statistics": True,
        },
        receipt_available_session=AVAILABLE,
        writer_git_commit="a" * 40,
    )


def _receipt(spec: S4SessionRunSpec) -> S4SessionRunReceipt:
    partitions = tuple(
        S4SessionPartitionReceipt(
            table_name=table,
            session_date=SESSION,
            artifact=_pin(table, suffix=".parquet"),
            row_count=0 if table == "asset_observation_version" else 2,
            contract_id=spec.contract_ids_by_table[table],
            schema_digest=spec.schema_digests_by_table[table],
            source_binding_id=spec.source_binding.source_binding_id,
            row_funnel_digest=_digest(f"funnel:{table}"),
            qa_result_set_digest=_digest(f"qa:{table}"),
        )
        for table in TABLES
    )
    combined_qa = stable_digest(
        {
            "table_qa_result_set_digests": {
                item.table_name: item.qa_result_set_digest for item in partitions
            }
        }
    )
    return S4SessionRunReceipt(
        run_spec_id=spec.run_spec_id,
        run_spec_artifact=_pin("run-spec"),
        parent_frontier_id=spec.parent_frontier.parent_frontier_id,
        session_date=SESSION,
        source_binding_id=spec.source_binding.source_binding_id,
        pair_available_session=AVAILABLE,
        receipt_available_session=AVAILABLE,
        partition_receipts=partitions,
        qa_details_artifact=_pin("qa-details"),
        qa_result_set_digest=combined_qa,
    )


def test_s4_incremental_contracts_round_trip_and_allow_empty_version_partition() -> None:
    release_set_id = _digest("base")
    base = S4BaseFrontier(
        base_release_set_id=release_set_id,
        base_release_set_artifact=ArtifactPin(
            path=(
                "manifests/silver/release-sets/assets/"
                f"release_set_id={release_set_id}/manifest.json"
            ),
            sha256=_digest("base-marker"),
            bytes=1_000,
        ),
        terminal_session=date(2026, 5, 8),
        terminal_partition_set_digest=_digest("partitions"),
        calendar_artifact_id=_digest("calendar"),
        reference_binding_id=_reference().binding_id,
        contract_ids_by_table={table: _digest(f"contract:{table}") for table in TABLES},
        schema_digests_by_table={table: _digest(f"schema:{table}") for table in TABLES},
        transform_semantics_digest=_digest("transform"),
        parquet_writer_policy={
            "compression": "zstd",
            "version": "2.6",
            "write_statistics": True,
        },
        release_available_session=date(2026, 7, 10),
    )
    assert S4BaseFrontier.from_dict(base.to_dict()) == base

    spec = _spec()
    assert S4SessionRunSpec.from_dict(spec.to_dict()) == spec
    receipt = _receipt(spec)
    assert S4SessionRunReceipt.from_dict(receipt.to_dict()) == receipt
    version = next(
        item
        for item in receipt.partition_receipts
        if item.table_name == "asset_observation_version"
    )
    assert version.row_count == 0


def test_i2_candidate_hashes_and_code_semantics_reproduce() -> None:
    content = I2_CANDIDATE_PATH.read_bytes()
    candidate = json.loads(content)
    logical = candidate["logical_contract"]
    assert hashlib.sha256(content).hexdigest() == I2_CANDIDATE_SHA256
    assert candidate["contract_id"] == I2_CONTRACT_ID == stable_digest(logical)
    assert logical["transform_semantics_digest"] == (
        S4_ASSET_INCREMENTAL_TRANSFORM_SEMANTICS_DIGEST
    )
    assert logical["parquet_writer_policy"] == dict(S4_ASSET_INCREMENTAL_PARQUET_WRITER_POLICY)
    for table, contract in ASSET_CONTRACTS.items():
        frozen = logical["output_tables"][table]
        assert frozen["contract_id"] == contract.contract_id
        assert frozen["schema_digest"] == arrow_schema_digest(contract.arrow_schema)
    assert set(_spec().to_dict()) == set(logical["control_objects"]["run_spec_fields"])
    assert set(_receipt(_spec()).to_dict()) == set(logical["control_objects"]["run_receipt_fields"])
    assert set(_base().to_dict()) == set(logical["control_objects"]["base_frontier_fields"])
    assert (
        logical["calendar_and_frontier"]["base_frontier_rule_version"]
        == (_base().to_dict()["rule_version"])
    )
    assert (
        logical["calendar_and_frontier"]["base_frontier_bootstrap"][
            "terminal_partition_set_rule_version"
        ]
        == S4_BASE_TERMINAL_PARTITION_SET_RULE_VERSION
    )
    assert (
        logical["calendar_and_frontier"]["base_frontier_bootstrap"][
            "historical_s4_data_parquet_read"
        ]
        is False
    )
    assert (
        "inventory_serialization_order_is_irrelevant"
        in logical["source_selection"]["upstream_manifest_order_rule"]
    )
    assert "s4_base_frontier_bootstrap_adapter" in logical["control_objects"]["nested_values"]
    assert "transform_fn" not in inspect.signature(run_s4_asset_session_incremental).parameters


def test_runtime_git_provenance_does_not_change_logical_source_or_run_ids() -> None:
    left_source = _source(git_commit="a" * 40)
    right_source = _source(git_commit="b" * 40)
    assert left_source.source_binding_id == right_source.source_binding_id
    assert left_source.to_dict() != right_source.to_dict()

    left_spec = _spec()
    right_spec = replace(
        left_spec,
        source_binding=right_source,
        writer_git_commit="b" * 40,
    )
    assert left_spec.run_spec_id == right_spec.run_spec_id
    assert left_spec.to_dict() != right_spec.to_dict()

    left_receipt = _receipt(left_spec)
    right_receipt = replace(
        left_receipt,
        run_spec_artifact=_pin("run-spec-from-new-writer"),
    )
    assert left_receipt.receipt_id == right_receipt.receipt_id
    assert left_receipt.to_dict() != right_receipt.to_dict()

    right_parent = replace(
        left_spec.parent_frontier,
        artifact=_pin("same-parent-receipt-from-new-writer"),
    )
    assert left_spec.parent_frontier.parent_frontier_id == right_parent.parent_frontier_id
    child_from_new_parent_bytes = replace(left_spec, parent_frontier=right_parent)
    assert left_spec.run_spec_id == child_from_new_parent_bytes.run_spec_id


def test_run_spec_requires_exact_three_table_contract_and_schema_maps() -> None:
    spec = _spec()
    with pytest.raises(S4AssetIncrementalContractError, match="exactly the three"):
        replace(
            spec,
            contract_ids_by_table={
                key: value
                for key, value in spec.contract_ids_by_table.items()
                if key != "asset_observation_version"
            },
        )


def test_reference_binding_rejects_unsorted_values_and_dependency_paths() -> None:
    with pytest.raises(S4AssetIncrementalContractError, match="sorted and unique"):
        S4ReferenceBinding(
            ticker_types=("ETF", "CS"),
            exchange_mics=("XNAS",),
            dependency_pins=(_pin("exchange"), _pin("ticker")),
        )
    with pytest.raises(S4AssetIncrementalContractError, match="sorted with unique paths"):
        S4ReferenceBinding(
            ticker_types=("CS",),
            exchange_mics=("XNAS",),
            dependency_pins=(_pin("ticker"), _pin("exchange")),
        )


def test_round_trip_rejects_tampered_ids_and_missing_partition() -> None:
    spec = _spec()
    document = spec.to_dict()
    document["run_spec_id"] = _digest("forged")
    with pytest.raises(S4AssetIncrementalContractError, match="does not reproduce"):
        S4SessionRunSpec.from_dict(document)

    receipt = _receipt(spec)
    with pytest.raises(S4AssetIncrementalContractError, match="exactly three"):
        replace(receipt, partition_receipts=receipt.partition_receipts[:-1])
