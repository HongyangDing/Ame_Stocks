"""Frozen S5 contracts for formal Massive ticker-event identity evidence."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib.resources import files
from types import MappingProxyType
from typing import Final

from ame_stocks_api.silver.contracts import TableContract

TICKER_EVENT_REQUEST_STATUS_CONTRACT_ID = (
    "5890117915e8ffc585c2faa1b9f4a9909a75f068bdad50a5e6bd64f78cf1df02"
)
TICKER_CHANGE_EVENT_CONTRACT_ID = "48a46dfd810b95137125b336917c23343da2aace5a6a71d99129b4d10f2e59b1"


def _load_contract(resource_name: str, expected_id: str) -> TableContract:
    resource = files("ame_stocks_api.silver").joinpath(f"schema_resources/{resource_name}")
    contract = TableContract.from_dict(json.loads(resource.read_text(encoding="utf-8")))
    if contract.contract_id != expected_id:  # pragma: no cover - import guard
        raise RuntimeError(f"packaged {contract.table} contract differs from S5 approval")
    return contract


TICKER_EVENT_REQUEST_STATUS_CONTRACT: Final = _load_contract(
    "ticker_event_request_status.schema-v1.json",
    TICKER_EVENT_REQUEST_STATUS_CONTRACT_ID,
)
TICKER_CHANGE_EVENT_CONTRACT: Final = _load_contract(
    "ticker_change_event.schema-v1.json",
    TICKER_CHANGE_EVENT_CONTRACT_ID,
)

TICKER_EVENT_CONTRACTS: Final[Mapping[str, TableContract]] = MappingProxyType(
    {
        TICKER_EVENT_REQUEST_STATUS_CONTRACT.table: TICKER_EVENT_REQUEST_STATUS_CONTRACT,
        TICKER_CHANGE_EVENT_CONTRACT.table: TICKER_CHANGE_EVENT_CONTRACT,
    }
)

__all__ = [
    "TICKER_CHANGE_EVENT_CONTRACT",
    "TICKER_CHANGE_EVENT_CONTRACT_ID",
    "TICKER_EVENT_CONTRACTS",
    "TICKER_EVENT_REQUEST_STATUS_CONTRACT",
    "TICKER_EVENT_REQUEST_STATUS_CONTRACT_ID",
]
