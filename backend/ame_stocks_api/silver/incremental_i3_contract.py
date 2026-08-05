"""Pinned native-v2 physical contracts for the S7.5 I3 incremental path.

The published S7 v1 resources are immutable equivalence oracles.  I3 therefore
describes each v2 table as an exact, checksummed overlay on its corresponding
v1 contract instead of copying and silently editing the historical resource.
The overlay removes legacy identity keys where required and adds stable segment
or row-version lineage while retaining every unaffected canonical column.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from importlib.resources import files
from types import MappingProxyType
from typing import Final

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import (
    ColumnSpec,
    QARule,
    SilverContractError,
    TableContract,
)
from ame_stocks_api.silver.identity_resolution_contract import (
    S7_DERIVED_CONTRACTS,
    S7_RESOURCE_SHA256_BY_TABLE,
)

I3_V2_TABLE_ORDER: Final = (
    "asset_master",
    "ticker_alias",
    "issuer_master",
    "universe_daily",
)
I3_V2_OVERLAY_SCHEMA_VERSION: Final = 1
I3_V2_SCHEMA_BUNDLE_DIGEST: Final = (
    "22ffa9d2b96b7c9a26f1766d58fcb6d2cb4e8c3f89599a26d3440e95592cf579"
)

_OVERLAY_RESOURCE_BY_TABLE: Final = MappingProxyType(
    {
        "asset_master": "asset_master.schema-v2.incremental-overlay.json",
        "ticker_alias": "ticker_alias.schema-v2.incremental-overlay.json",
        "issuer_master": "issuer_master.schema-v2.incremental-overlay.json",
        "universe_daily": "universe_daily.schema-v2.incremental-overlay.json",
    }
)
I3_V2_CONTRACT_ID_BY_TABLE: Final[Mapping[str, str]] = MappingProxyType(
    {
        "asset_master": "7fda2d1dd54244426b517fe69142bcc5bbd3abc917501509435a9f8e3b44577a",
        "ticker_alias": "808a1df16e9a10a4d433f078c314837083fb778526b2e938479816c257a6a9b4",
        "issuer_master": "28a708f7cd89c3836b28bde17e51d6be07a7a15220f2cab67736dc4558b35238",
        "universe_daily": "d9b0728c9d99fadced24473985fd07920964365f434f2ef5b0e9841030cedf87",
    }
)
I3_V2_RESOURCE_SHA256_BY_TABLE: Final[Mapping[str, str]] = MappingProxyType(
    {
        "asset_master": "478d8d611ed1f696235550af32844c577056b30e7040b8033e51d4690f9e19ff",
        "ticker_alias": "96041d73bb2180f0753501a3fc7ad18848b9a0d82d02fca3aff4d68507b972a3",
        "issuer_master": "29ee2af8eca6e4e50214c62597872f07252e96642749bbfcd8752adbdc7b7a4f",
        "universe_daily": "786d20eff5cca3cfde120626572cd31e6386eaf88493fb3b801d355998173bcc",
    }
)


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SilverContractError(f"duplicate I3 overlay key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise SilverContractError(f"non-finite I3 overlay JSON value: {value}")


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise SilverContractError(f"{label} must be an array")
    return value


def _string_array(value: object, label: str) -> tuple[str, ...]:
    items = _array(value, label)
    if any(not isinstance(item, str) for item in items):
        raise SilverContractError(f"{label} must contain strings")
    return tuple(items)


def _load_overlay_contract(table_name: str) -> TableContract:
    resource_name = _OVERLAY_RESOURCE_BY_TABLE[table_name]
    resource = files("ame_stocks_api.silver").joinpath(f"schema_resources/{resource_name}")
    content = resource.read_bytes()
    observed_sha = hashlib.sha256(content).hexdigest()
    if observed_sha != I3_V2_RESOURCE_SHA256_BY_TABLE[table_name]:  # pragma: no cover
        raise RuntimeError(f"packaged {resource_name} differs from the pinned I3 resource")
    try:
        document = json.loads(
            content,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:  # pragma: no cover
        raise RuntimeError(f"packaged {resource_name} is not strict JSON") from exc
    if not isinstance(document, dict):  # pragma: no cover
        raise RuntimeError(f"packaged {resource_name} is not an object")
    expected_keys = {
        "added_columns",
        "artifact_type",
        "base_contract_id",
        "base_resource_sha256",
        "contract_id",
        "description",
        "domain",
        "grain",
        "overlay_schema_version",
        "partition_by",
        "primary_key",
        "qa_rules",
        "removed_columns",
        "schema_version",
        "sort_by",
        "source_datasets",
        "table",
    }
    if set(document) != expected_keys:
        raise RuntimeError(f"packaged {resource_name} has unexpected fields")
    if (
        document["artifact_type"] != "s7_5_i3_v2_table_contract_overlay"
        or document["overlay_schema_version"] != I3_V2_OVERLAY_SCHEMA_VERSION
        or document["table"] != table_name
    ):
        raise RuntimeError(f"packaged {resource_name} has incompatible identity")
    base = S7_DERIVED_CONTRACTS[table_name]
    if (
        document["base_contract_id"] != base.contract_id
        or document["base_resource_sha256"] != S7_RESOURCE_SHA256_BY_TABLE[table_name]
    ):
        raise RuntimeError(f"packaged {resource_name} changed its immutable v1 base")
    removed = _string_array(document["removed_columns"], "removed columns")
    if removed != tuple(sorted(set(removed))):
        raise RuntimeError(f"packaged {resource_name} removed columns are not sorted and unique")
    base_names = {item.name for item in base.columns}
    if not set(removed).issubset(base_names):
        raise RuntimeError(f"packaged {resource_name} removes an unknown v1 column")
    added = tuple(
        ColumnSpec.from_dict(item) for item in _array(document["added_columns"], "added columns")
    )
    added_names = [item.name for item in added]
    retained_names = base_names - set(removed)
    if len(set(added_names)) != len(added_names) or retained_names.intersection(added_names):
        raise RuntimeError(f"packaged {resource_name} added columns collide")
    contract = TableContract(
        domain=str(document["domain"]),
        table=table_name,
        schema_version=int(document["schema_version"]),
        description=str(document["description"]),
        grain=str(document["grain"]),
        columns=tuple(item for item in base.columns if item.name not in removed) + added,
        primary_key=_string_array(document["primary_key"], "primary key"),
        partition_by=_string_array(document["partition_by"], "partition by"),
        sort_by=_string_array(document["sort_by"], "sort by"),
        source_datasets=_string_array(document["source_datasets"], "source datasets"),
        qa_rules=tuple(QARule.from_dict(item) for item in _array(document["qa_rules"], "QA rules")),
    )
    if (
        document["contract_id"] != contract.contract_id
        or contract.contract_id != I3_V2_CONTRACT_ID_BY_TABLE[table_name]
    ):
        raise RuntimeError(f"packaged {resource_name} contract identity differs")
    return contract


I3_V2_CONTRACTS: Final[Mapping[str, TableContract]] = MappingProxyType(
    {table: _load_overlay_contract(table) for table in I3_V2_TABLE_ORDER}
)

_observed_bundle_digest = stable_digest(
    [
        {
            "contract_id": I3_V2_CONTRACTS[table].contract_id,
            "resource_sha256": I3_V2_RESOURCE_SHA256_BY_TABLE[table],
            "schema_digest": I3_V2_CONTRACTS[table].schema_digest,
            "table": table,
        }
        for table in I3_V2_TABLE_ORDER
    ]
)
if _observed_bundle_digest != I3_V2_SCHEMA_BUNDLE_DIGEST:  # pragma: no cover
    raise RuntimeError("packaged I3 v2 schema bundle digest differs")


__all__ = [
    "I3_V2_CONTRACTS",
    "I3_V2_CONTRACT_ID_BY_TABLE",
    "I3_V2_OVERLAY_SCHEMA_VERSION",
    "I3_V2_RESOURCE_SHA256_BY_TABLE",
    "I3_V2_SCHEMA_BUNDLE_DIGEST",
    "I3_V2_TABLE_ORDER",
]
