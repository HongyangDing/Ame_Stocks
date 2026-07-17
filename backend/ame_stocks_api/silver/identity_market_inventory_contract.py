"""Frozen Gate-A contract for the full-history observed Composite FIGI inventory."""

from __future__ import annotations

import hashlib
import json
from importlib.resources import files
from typing import Final

from ame_stocks_api.silver.contracts import TableContract

COMPOSITE_FIGI_INVENTORY_CONTRACT_ID: Final = (
    "66ac429ccc2f76bbb2a474679e83a9cf68a0f52a52c662c76905e2e4221241e8"
)
COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST: Final = (
    "cc7a98521e72d88f840ec489ee290cf824d2b882ad8b479826bc7290f5e1f3e9"
)
COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256: Final = (
    "007277ee45a6b2f7b5ef10ac30a514aef8f70a8ee75585a7a992c11d9711b78d"
)
COMPOSITE_FIGI_INVENTORY_RESOURCE_NAME: Final = (
    "schema_resources/composite_figi_inventory.schema-v1.json"
)


def _load_contract() -> TableContract:
    resource = files("ame_stocks_api.silver").joinpath(COMPOSITE_FIGI_INVENTORY_RESOURCE_NAME)
    payload = resource.read_bytes()
    if hashlib.sha256(payload).hexdigest() != COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256:
        raise RuntimeError("packaged composite_figi_inventory resource bytes differ")
    contract = TableContract.from_dict(json.loads(payload))
    if contract.contract_id != COMPOSITE_FIGI_INVENTORY_CONTRACT_ID:
        raise RuntimeError("packaged composite_figi_inventory contract ID differs")
    if contract.schema_digest != COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST:
        raise RuntimeError("packaged composite_figi_inventory Arrow schema differs")
    return contract


COMPOSITE_FIGI_INVENTORY_CONTRACT: Final = _load_contract()

__all__ = [
    "COMPOSITE_FIGI_INVENTORY_CONTRACT",
    "COMPOSITE_FIGI_INVENTORY_CONTRACT_ID",
    "COMPOSITE_FIGI_INVENTORY_RESOURCE_NAME",
    "COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256",
    "COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST",
]
