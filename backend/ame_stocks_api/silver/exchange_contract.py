"""The exact user-approved Silver contract for the Massive exchange dictionary."""

from __future__ import annotations

import json
from importlib.resources import files

from ame_stocks_api.silver.contracts import TableContract

EXCHANGE_DIM_CONTRACT_ID = (
    "1803d28f2b4b6088e32d27d06c7102111e4f141b6645a1059829732442f0e479"
)
_CONTRACT_RESOURCE = "schema_resources/exchange_dim.schema-v1.json"


def _load_contract() -> TableContract:
    resource = files("ame_stocks_api.silver").joinpath(_CONTRACT_RESOURCE)
    contract = TableContract.from_dict(json.loads(resource.read_text(encoding="utf-8")))
    if contract.contract_id != EXCHANGE_DIM_CONTRACT_ID:  # pragma: no cover - import guard
        raise RuntimeError("the packaged exchange_dim contract differs from user approval")
    return contract


EXCHANGE_DIM_CONTRACT = _load_contract()

__all__ = ["EXCHANGE_DIM_CONTRACT", "EXCHANGE_DIM_CONTRACT_ID"]
