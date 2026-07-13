"""Frozen Silver contracts for the Massive condition-code reference dataset."""

from __future__ import annotations

import json
from importlib.resources import files

from ame_stocks_api.silver.contracts import TableContract

CONDITION_CODE_DIM_CONTRACT_ID = "de48f79738b2ed8d65c04a49c9f889ace84b69a4df7771051f67d30acd153192"
CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT_ID = (
    "a088a7ab0c562a9fbb90fb0a242be598b7d983d004af27973dd22666d16960dd"
)


def _load_contract(resource_name: str, expected_id: str) -> TableContract:
    resource = files("ame_stocks_api.silver").joinpath(f"schema_resources/{resource_name}")
    contract = TableContract.from_dict(json.loads(resource.read_text(encoding="utf-8")))
    if contract.contract_id != expected_id:  # pragma: no cover - import guard
        raise RuntimeError(f"packaged {contract.table} contract differs from review")
    return contract


CONDITION_CODE_DIM_CONTRACT = _load_contract(
    "condition_code_dim.schema-v1.json",
    CONDITION_CODE_DIM_CONTRACT_ID,
)
CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT = _load_contract(
    "condition_code_data_type_bridge.schema-v1.json",
    CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT_ID,
)

CONDITION_CODE_CONTRACTS = {
    CONDITION_CODE_DIM_CONTRACT.table: CONDITION_CODE_DIM_CONTRACT,
    CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT.table: (CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT),
}

__all__ = [
    "CONDITION_CODE_CONTRACTS",
    "CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT",
    "CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT_ID",
    "CONDITION_CODE_DIM_CONTRACT",
    "CONDITION_CODE_DIM_CONTRACT_ID",
]
