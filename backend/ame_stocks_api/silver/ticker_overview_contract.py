"""Frozen S6 contract for allowlisted Ticker Overview lifecycle evidence."""

from __future__ import annotations

import json
from importlib.resources import files
from typing import Final

from ame_stocks_api.silver.contracts import TableContract

TICKER_OVERVIEW_SAFE_CONTRACT_ID = (
    "f4e873e6595fee0a66362a0d39b3f7c36176b95354ecad93453613f7ac84ca3c"
)


def _load_contract() -> TableContract:
    resource = files("ame_stocks_api.silver").joinpath(
        "schema_resources/ticker_overview_safe.schema-v1.json"
    )
    contract = TableContract.from_dict(json.loads(resource.read_text(encoding="utf-8")))
    if contract.contract_id != TICKER_OVERVIEW_SAFE_CONTRACT_ID:  # pragma: no cover
        raise RuntimeError("packaged ticker_overview_safe contract differs from S6 approval")
    return contract


TICKER_OVERVIEW_SAFE_CONTRACT: Final = _load_contract()

__all__ = ["TICKER_OVERVIEW_SAFE_CONTRACT", "TICKER_OVERVIEW_SAFE_CONTRACT_ID"]
