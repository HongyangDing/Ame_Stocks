from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa

from ame_stocks_api.silver.contracts import ArrowType, QASeverity, QAStatus, TableContract
from ame_stocks_api.silver.identity_market_inventory_contract import (
    COMPOSITE_FIGI_INVENTORY_CONTRACT,
    COMPOSITE_FIGI_INVENTORY_CONTRACT_ID,
    COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
    COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
)

_ROOT = Path(__file__).resolve().parents[1]
_CANDIDATE = (
    _ROOT / "docs/silver/contracts/identity/composite_figi_inventory.schema-v1.candidate.json"
)
_RESOURCE = (
    _ROOT / "backend/ame_stocks_api/silver/schema_resources/composite_figi_inventory.schema-v1.json"
)
_OLD_CONTRACT_IDS = {
    "asset_master.schema-v1.json": (
        "959c5f7bf464eed59fd32a7008349f60ebcfd3cf9e892c9c3d7f00080eae2149"
    ),
    "asset_observation_daily.schema-v1.json": (
        "dd916b8528b9ce1a341e6b8ad897ae80e40d5df118b8e102e4ea1f1ea6e9c045"
    ),
    "asset_observation_version.schema-v1.json": (
        "14ce114f5911f7e4d1c15e58f0f42a8307066d6517e859d6233fa23c199616fc"
    ),
    "condition_code_data_type_bridge.schema-v1.json": (
        "a088a7ab0c562a9fbb90fb0a242be598b7d983d004af27973dd22666d16960dd"
    ),
    "condition_code_dim.schema-v1.json": (
        "de48f79738b2ed8d65c04a49c9f889ace84b69a4df7771051f67d30acd153192"
    ),
    "exchange_dim.schema-v1.json": (
        "1803d28f2b4b6088e32d27d06c7102111e4f141b6645a1059829732442f0e479"
    ),
    "identity_adjudication.schema-v1.json": (
        "6423cc01b952498cc78d55e93a349d7afe408bd30003e4f7be59f211102f2d5e"
    ),
    "identity_cross_market_adjudication.schema-v1.json": (
        "ae91c7b1bfc27bde82e5f5a39afdc5a3c2c9929d075486cb081836b6798e14e8"
    ),
    "issuer_master.schema-v1.json": (
        "2faa8d4d2e10e4a065b10b9ae851e53ac517db7e69af4fd59d5f6edc677aa408"
    ),
    "ticker_alias.schema-v1.json": (
        "39dbf6ef89ed4c2d466fa0be2e47d2840a90f1a97f6a47670af05df3e15513ce"
    ),
    "ticker_change_event.schema-v1.json": (
        "48a46dfd810b95137125b336917c23343da2aace5a6a71d99129b4d10f2e59b1"
    ),
    "ticker_event_request_status.schema-v1.json": (
        "5890117915e8ffc585c2faa1b9f4a9909a75f068bdad50a5e6bd64f78cf1df02"
    ),
    "ticker_overview_safe.schema-v1.json": (
        "f4e873e6595fee0a66362a0d39b3f7c36176b95354ecad93453613f7ac84ca3c"
    ),
    "ticker_type_dim.schema-v1.json": (
        "b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d"
    ),
    "universe_daily.schema-v1.json": (
        "38cd59c4e4b04de8444ba99ed93e6fd8c7a78aec24f01205d7df7494bcfd33d3"
    ),
    "universe_source_daily.schema-v1.json": (
        "9711320ee9227df347224b7cd17a41fe10a352fddf089cd72b758bde7a7f0c58"
    ),
}


def test_candidate_and_packaged_resource_are_byte_identical_and_pinned() -> None:
    candidate_payload = _CANDIDATE.read_bytes()
    resource_payload = _RESOURCE.read_bytes()

    assert candidate_payload == resource_payload
    assert hashlib.sha256(candidate_payload).hexdigest() == (
        COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256
    )
    assert TableContract.from_dict(json.loads(candidate_payload)) == (
        COMPOSITE_FIGI_INVENTORY_CONTRACT
    )


def test_composite_inventory_contract_freezes_exact_shape_and_nonnull_list_items() -> None:
    contract = COMPOSITE_FIGI_INVENTORY_CONTRACT

    assert (contract.domain, contract.table, contract.schema_version) == (
        "identity",
        "composite_figi_inventory",
        1,
    )
    assert contract.contract_id == COMPOSITE_FIGI_INVENTORY_CONTRACT_ID
    assert contract.schema_digest == COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST
    assert tuple(column.name for column in contract.columns) == (
        "observed_composite_figi",
        "observed_share_class_figis",
        "share_class_conflict",
        "first_session",
        "last_session",
        "active_row_count",
        "inactive_row_count",
        "session_count",
        "ticker_count",
        "provider_locale_count",
        "provider_market_count",
        "primary_exchange_count",
        "parent_table_count",
        "source_release_count",
        "source_record_lineage_digest",
    )
    assert tuple(column.arrow_type for column in contract.columns) == (
        ArrowType.STRING,
        ArrowType.LIST_STRING,
        ArrowType.BOOLEAN,
        ArrowType.DATE32,
        ArrowType.DATE32,
        ArrowType.INT64,
        ArrowType.INT64,
        ArrowType.INT64,
        ArrowType.INT64,
        ArrowType.INT64,
        ArrowType.INT64,
        ArrowType.INT64,
        ArrowType.INT64,
        ArrowType.INT64,
        ArrowType.STRING,
    )
    assert all(not column.nullable for column in contract.columns)
    assert contract.primary_key == ("observed_composite_figi",)
    assert contract.partition_by == ()
    assert contract.sort_by == ("observed_composite_figi",)
    assert contract.source_datasets == (
        "asset_observation_daily",
        "universe_source_daily",
    )

    list_field = contract.arrow_schema.field("observed_share_class_figis")
    assert not list_field.nullable
    assert pa.types.is_list(list_field.type)
    assert list_field.type.value_type == pa.string()
    assert not list_field.type.value_field.nullable
    assert str(list_field.type) == "list<item: string not null>"


def test_composite_inventory_contract_freezes_exact_qa_surface() -> None:
    rules = {rule.check_id: rule for rule in COMPOSITE_FIGI_INVENTORY_CONTRACT.qa_rules}
    critical = (
        "schema_exact",
        "source_binding_invalid",
        "source_artifact_integrity_invalid",
        "source_count_mismatch",
        "session_spine_mismatch",
        "authority_source_record_duplicate",
        "requested_provider_active_mismatch",
        "universe_selected_source_record_duplicate",
        "universe_parent_missing",
        "universe_projection_mismatch",
        "inventory_authority_leak",
        "invalid_composite_in_output",
        "primary_key_duplicate",
        "output_sort_invalid",
        "lineage_digest_invalid",
        "aggregate_invariant_invalid",
        "resource_cap_exceeded",
    )
    high = (
        "malformed_composite_rows",
        "malformed_share_class_rows",
        "share_class_conflict_groups",
    )
    medium = (
        "composite_figi_null_rows",
        "valid_composite_missing_share_class_rows",
    )

    assert tuple(rules) == (*critical, *high, *medium)
    assert all(
        rules[check_id].severity is QASeverity.CRITICAL
        and rules[check_id].failure_status is QAStatus.FAILED
        for check_id in critical
    )
    assert all(
        rules[check_id].severity is QASeverity.HIGH
        and rules[check_id].failure_status is QAStatus.WARNING
        for check_id in high
    )
    assert all(
        rules[check_id].severity is QASeverity.MEDIUM
        and rules[check_id].failure_status is QAStatus.WARNING
        for check_id in medium
    )
    assert all(rule.threshold_expression == "numerator eq 0" for rule in rules.values())


def test_list_string_extension_does_not_change_any_preexisting_contract_id() -> None:
    resource_root = _RESOURCE.parent

    observed = {
        name: TableContract.from_dict(
            json.loads((resource_root / name).read_text(encoding="utf-8"))
        ).contract_id
        for name in _OLD_CONTRACT_IDS
    }

    assert observed == _OLD_CONTRACT_IDS
