"""Frozen Silver contracts for the approved Massive Assets source tables."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib.resources import files
from types import MappingProxyType
from typing import Final

from ame_stocks_api.silver.contracts import TableContract

ASSET_OBSERVATION_DAILY_CONTRACT_ID = (
    "dd916b8528b9ce1a341e6b8ad897ae80e40d5df118b8e102e4ea1f1ea6e9c045"
)
ASSET_OBSERVATION_VERSION_CONTRACT_ID = (
    "14ce114f5911f7e4d1c15e58f0f42a8307066d6517e859d6233fa23c199616fc"
)
UNIVERSE_SOURCE_DAILY_CONTRACT_ID = (
    "9711320ee9227df347224b7cd17a41fe10a352fddf089cd72b758bde7a7f0c58"
)


def _load_contract(resource_name: str, expected_id: str) -> TableContract:
    resource = files("ame_stocks_api.silver").joinpath(f"schema_resources/{resource_name}")
    contract = TableContract.from_dict(json.loads(resource.read_text(encoding="utf-8")))
    if contract.contract_id != expected_id:  # pragma: no cover - import guard
        raise RuntimeError(f"packaged {contract.table} contract differs from user approval")
    return contract


ASSET_OBSERVATION_DAILY_CONTRACT: Final = _load_contract(
    "asset_observation_daily.schema-v1.json",
    ASSET_OBSERVATION_DAILY_CONTRACT_ID,
)
ASSET_OBSERVATION_VERSION_CONTRACT: Final = _load_contract(
    "asset_observation_version.schema-v1.json",
    ASSET_OBSERVATION_VERSION_CONTRACT_ID,
)
UNIVERSE_SOURCE_DAILY_CONTRACT: Final = _load_contract(
    "universe_source_daily.schema-v1.json",
    UNIVERSE_SOURCE_DAILY_CONTRACT_ID,
)

ASSET_CONTRACTS: Final[Mapping[str, TableContract]] = MappingProxyType(
    {
        ASSET_OBSERVATION_DAILY_CONTRACT.table: ASSET_OBSERVATION_DAILY_CONTRACT,
        ASSET_OBSERVATION_VERSION_CONTRACT.table: ASSET_OBSERVATION_VERSION_CONTRACT,
        UNIVERSE_SOURCE_DAILY_CONTRACT.table: UNIVERSE_SOURCE_DAILY_CONTRACT,
    }
)

__all__ = [
    "ASSET_CONTRACTS",
    "ASSET_OBSERVATION_DAILY_CONTRACT",
    "ASSET_OBSERVATION_DAILY_CONTRACT_ID",
    "ASSET_OBSERVATION_VERSION_CONTRACT",
    "ASSET_OBSERVATION_VERSION_CONTRACT_ID",
    "UNIVERSE_SOURCE_DAILY_CONTRACT",
    "UNIVERSE_SOURCE_DAILY_CONTRACT_ID",
]
