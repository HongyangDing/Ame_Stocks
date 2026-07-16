"""Pinned S7 contract candidates for adjudicated, cutoff-bound research identity."""

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
IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT_ID = (
    "ae91c7b1bfc27bde82e5f5a39afdc5a3c2c9929d075486cb081836b6798e14e8"
)
ASSET_MASTER_CONTRACT_ID = "959c5f7bf464eed59fd32a7008349f60ebcfd3cf9e892c9c3d7f00080eae2149"
TICKER_ALIAS_CONTRACT_ID = "39dbf6ef89ed4c2d466fa0be2e47d2840a90f1a97f6a47670af05df3e15513ce"
ISSUER_MASTER_CONTRACT_ID = "2faa8d4d2e10e4a065b10b9ae851e53ac517db7e69af4fd59d5f6edc677aa408"
UNIVERSE_DAILY_CONTRACT_ID = "38cd59c4e4b04de8444ba99ed93e6fd8c7a78aec24f01205d7df7494bcfd33d3"

IDENTITY_ADJUDICATION_RESOURCE_SHA256 = (
    "eb5e9d1746ad2014d7b0e4a9a56ffa29e4f36cf1e1d18d348634a058f0d22231"
)
IDENTITY_CROSS_MARKET_ADJUDICATION_RESOURCE_SHA256 = (
    "a7308e22c07e8243a8587bfc7eab7ae45b2f232fe9bba310d084916d722f56d0"
)
ASSET_MASTER_RESOURCE_SHA256 = "bfb31004df41c4556e71beb379bb36e07063f36298d329c887be48c005b02fa5"
TICKER_ALIAS_RESOURCE_SHA256 = "8bf758af5c358c79477ff40177aab5f3b7c8d26f7f0882e261f7d844a66a1f95"
ISSUER_MASTER_RESOURCE_SHA256 = "adee0a5457ac32356a0ec9b9a28c692fcebdacc4ba9cccedd1237e8c66b722b7"
UNIVERSE_DAILY_RESOURCE_SHA256 = "c0923508dafa0d56de4be6b8ff43187a581627dd1d64e964cf5f506f5ce8ea0b"


def _load_contract(
    resource_name: str,
    *,
    expected_id: str,
    expected_resource_sha256: str,
) -> TableContract:
    resource = files("ame_stocks_api.silver").joinpath(f"schema_resources/{resource_name}")
    payload = resource.read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected_resource_sha256:  # pragma: no cover
        raise RuntimeError(f"packaged {resource_name} differs from the pinned S7 candidate")
    contract = TableContract.from_dict(json.loads(payload))
    if contract.contract_id != expected_id:  # pragma: no cover - import guard
        raise RuntimeError(f"packaged {contract.table} contract differs from the pinned candidate")
    return contract


IDENTITY_ADJUDICATION_CONTRACT: Final = _load_contract(
    "identity_adjudication.schema-v1.json",
    expected_id=IDENTITY_ADJUDICATION_CONTRACT_ID,
    expected_resource_sha256=IDENTITY_ADJUDICATION_RESOURCE_SHA256,
)
IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT: Final = _load_contract(
    "identity_cross_market_adjudication.schema-v1.json",
    expected_id=IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT_ID,
    expected_resource_sha256=IDENTITY_CROSS_MARKET_ADJUDICATION_RESOURCE_SHA256,
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
    {
        IDENTITY_ADJUDICATION_CONTRACT.table: IDENTITY_ADJUDICATION_CONTRACT,
        IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT.table: (
            IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT
        ),
    }
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
        IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT.table: (
            IDENTITY_CROSS_MARKET_ADJUDICATION_RESOURCE_SHA256
        ),
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
    "IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT",
    "IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT_ID",
    "IDENTITY_CROSS_MARKET_ADJUDICATION_RESOURCE_SHA256",
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
