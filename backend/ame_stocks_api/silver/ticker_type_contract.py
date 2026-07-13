"""The exact user-approved Silver contract for the Massive ticker-type dictionary."""

from __future__ import annotations

import json
from importlib.resources import files

from ame_stocks_api.silver.contracts import TableContract

TICKER_TYPE_DIM_CONTRACT_ID = "b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d"
_CONTRACT_RESOURCE = "schema_resources/ticker_type_dim.schema-v1.json"


def _load_contract() -> TableContract:
    resource = files("ame_stocks_api.silver").joinpath(_CONTRACT_RESOURCE)
    contract = TableContract.from_dict(json.loads(resource.read_text(encoding="utf-8")))
    if contract.contract_id != TICKER_TYPE_DIM_CONTRACT_ID:  # pragma: no cover
        raise RuntimeError("the packaged ticker_type_dim contract differs from user approval")
    return contract


TICKER_TYPE_DIM_CONTRACT = _load_contract()

__all__ = ["TICKER_TYPE_DIM_CONTRACT", "TICKER_TYPE_DIM_CONTRACT_ID"]
