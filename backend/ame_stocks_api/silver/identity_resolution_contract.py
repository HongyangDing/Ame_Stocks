"""Frozen S7 contracts for adjudicated, cutoff-bound research identity."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from importlib.resources import files
from types import MappingProxyType
from typing import Final

from ame_stocks_api.silver.contracts import TableContract

IDENTITY_ADJUDICATION_CONTRACT_ID = (
    "6423cc01b952498cc78d55e93a349d7afe408bd30003e4f7be59f211102f2d5e"
)
ASSET_MASTER_CONTRACT_ID = "adbba0d86bd9681e034b0ffda3e380da40b6fc92d280942d856d416a1b53f868"
TICKER_ALIAS_CONTRACT_ID = "384d1e5acf2181f929e29c5e3a5369a796f0ee42cdde7740b7ca3bdfdf8faf3b"
ISSUER_MASTER_CONTRACT_ID = "4951c0ab96fdd91b961cf4234185607e858856fb1b1ad4279b2e84d41fb2eb58"
UNIVERSE_DAILY_CONTRACT_ID = "0555e785b4fb5f9df8832d37f8c08cf5fc487e8573993cf39ae3ffba4ccc45b0"

IDENTITY_ADJUDICATION_RESOURCE_SHA256 = (
    "eb5e9d1746ad2014d7b0e4a9a56ffa29e4f36cf1e1d18d348634a058f0d22231"
)
ASSET_MASTER_RESOURCE_SHA256 = "0a6dd9cb244e60723eeff625b6d82b42fc6fe882fbe0660532807054a4f717f2"
TICKER_ALIAS_RESOURCE_SHA256 = "8ef120892c5748ca51fc1242d143372237c1b5d9b92ac9f4f2585aea48fd5afe"
ISSUER_MASTER_RESOURCE_SHA256 = "6f326ae11885affb5bac37500c2006bdc845f2205d7388e2043b5504d0fb0ec8"
UNIVERSE_DAILY_RESOURCE_SHA256 = "fe8d5760384322419eb28a0f8b3af6f45d52c1cbba18bc5226578fa471766701"


def _load_contract(
    resource_name: str,
    *,
    expected_id: str,
    expected_resource_sha256: str,
) -> TableContract:
    resource = files("ame_stocks_api.silver").joinpath(f"schema_resources/{resource_name}")
    payload = resource.read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected_resource_sha256:  # pragma: no cover
        raise RuntimeError(f"packaged {resource_name} differs byte-for-byte from S7 approval")
    contract = TableContract.from_dict(json.loads(payload))
    if contract.contract_id != expected_id:  # pragma: no cover - import guard
        raise RuntimeError(f"packaged {contract.table} contract differs from S7 approval")
    return contract


IDENTITY_ADJUDICATION_CONTRACT: Final = _load_contract(
    "identity_adjudication.schema-v1.json",
    expected_id=IDENTITY_ADJUDICATION_CONTRACT_ID,
    expected_resource_sha256=IDENTITY_ADJUDICATION_RESOURCE_SHA256,
)
ASSET_MASTER_CONTRACT: Final = _load_contract(
    "asset_master.schema-v1.json",
    expected_id=ASSET_MASTER_CONTRACT_ID,
    expected_resource_sha256=ASSET_MASTER_RESOURCE_SHA256,
)
TICKER_ALIAS_CONTRACT: Final = _load_contract(
    "ticker_alias.schema-v1.json",
    expected_id=TICKER_ALIAS_CONTRACT_ID,
    expected_resource_sha256=TICKER_ALIAS_RESOURCE_SHA256,
)
ISSUER_MASTER_CONTRACT: Final = _load_contract(
    "issuer_master.schema-v1.json",
    expected_id=ISSUER_MASTER_CONTRACT_ID,
    expected_resource_sha256=ISSUER_MASTER_RESOURCE_SHA256,
)
UNIVERSE_DAILY_CONTRACT: Final = _load_contract(
    "universe_daily.schema-v1.json",
    expected_id=UNIVERSE_DAILY_CONTRACT_ID,
    expected_resource_sha256=UNIVERSE_DAILY_RESOURCE_SHA256,
)

S7_ADJUDICATION_CONTRACTS: Final[Mapping[str, TableContract]] = MappingProxyType(
    {IDENTITY_ADJUDICATION_CONTRACT.table: IDENTITY_ADJUDICATION_CONTRACT}
)
S7_DERIVED_CONTRACTS: Final[Mapping[str, TableContract]] = MappingProxyType(
    {
        ASSET_MASTER_CONTRACT.table: ASSET_MASTER_CONTRACT,
        TICKER_ALIAS_CONTRACT.table: TICKER_ALIAS_CONTRACT,
        ISSUER_MASTER_CONTRACT.table: ISSUER_MASTER_CONTRACT,
        UNIVERSE_DAILY_CONTRACT.table: UNIVERSE_DAILY_CONTRACT,
    }
)
S7_CONTRACTS: Final[Mapping[str, TableContract]] = MappingProxyType(
    {**S7_ADJUDICATION_CONTRACTS, **S7_DERIVED_CONTRACTS}
)
S7_RESOURCE_SHA256_BY_TABLE: Final[Mapping[str, str]] = MappingProxyType(
    {
        IDENTITY_ADJUDICATION_CONTRACT.table: IDENTITY_ADJUDICATION_RESOURCE_SHA256,
        ASSET_MASTER_CONTRACT.table: ASSET_MASTER_RESOURCE_SHA256,
        TICKER_ALIAS_CONTRACT.table: TICKER_ALIAS_RESOURCE_SHA256,
        ISSUER_MASTER_CONTRACT.table: ISSUER_MASTER_RESOURCE_SHA256,
        UNIVERSE_DAILY_CONTRACT.table: UNIVERSE_DAILY_RESOURCE_SHA256,
    }
)

__all__ = [
    "ASSET_MASTER_CONTRACT",
    "ASSET_MASTER_CONTRACT_ID",
    "ASSET_MASTER_RESOURCE_SHA256",
    "IDENTITY_ADJUDICATION_CONTRACT",
    "IDENTITY_ADJUDICATION_CONTRACT_ID",
    "IDENTITY_ADJUDICATION_RESOURCE_SHA256",
    "ISSUER_MASTER_CONTRACT",
    "ISSUER_MASTER_CONTRACT_ID",
    "ISSUER_MASTER_RESOURCE_SHA256",
    "S7_ADJUDICATION_CONTRACTS",
    "S7_CONTRACTS",
    "S7_DERIVED_CONTRACTS",
    "S7_RESOURCE_SHA256_BY_TABLE",
    "TICKER_ALIAS_CONTRACT",
    "TICKER_ALIAS_CONTRACT_ID",
    "TICKER_ALIAS_RESOURCE_SHA256",
    "UNIVERSE_DAILY_CONTRACT",
    "UNIVERSE_DAILY_CONTRACT_ID",
    "UNIVERSE_DAILY_RESOURCE_SHA256",
]
