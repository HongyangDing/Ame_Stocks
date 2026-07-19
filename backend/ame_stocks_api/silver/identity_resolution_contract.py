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
ASSET_MASTER_CONTRACT_ID = "4d85c7cc73ee4b61ca548aec4b64aa6cb05e779d3c3beb1a8f601023d96f8df1"
TICKER_ALIAS_CONTRACT_ID = "796423964d875daa3aa25fc2d14b06dcebd436bb91d42629866f0995dbc2931e"
ISSUER_MASTER_CONTRACT_ID = "0e46c0e939989205b4dcd48f11e3443ec5c3e72b366dcd5684c417bb134d6b70"
UNIVERSE_DAILY_CONTRACT_ID = "bf1ab110844f1d7a572db2d4e14e725b83ca0a99b566c61b1af89aa24d514fbf"

IDENTITY_ADJUDICATION_RESOURCE_SHA256 = (
    "eb5e9d1746ad2014d7b0e4a9a56ffa29e4f36cf1e1d18d348634a058f0d22231"
)
IDENTITY_CROSS_MARKET_ADJUDICATION_RESOURCE_SHA256 = (
    "a7308e22c07e8243a8587bfc7eab7ae45b2f232fe9bba310d084916d722f56d0"
)
ASSET_MASTER_RESOURCE_SHA256 = "cc14c11dca1f449a3c8fcdbe4f0e419a26dc15312b75116db767706699f0b849"
TICKER_ALIAS_RESOURCE_SHA256 = "89c1f05a545ab18100dafc7b3b27210aff38ecceeddb0e1d048419a30b8f83de"
ISSUER_MASTER_RESOURCE_SHA256 = "17108231fa5ab46fd98095b52a06fda88a1ababf9e9e3b3dae8ce66bdd7f8c50"
UNIVERSE_DAILY_RESOURCE_SHA256 = "c83327b1e38defa8f56bc1ea87f011bb360692da327323cae41cc8eeee2d54be"


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
    "asset_master.schema-v1.registry-v4.json",
    expected_id=ASSET_MASTER_CONTRACT_ID,
    expected_resource_sha256=ASSET_MASTER_RESOURCE_SHA256,
)
TICKER_ALIAS_CONTRACT: Final = _load_contract(
    "ticker_alias.schema-v1.registry-v4.json",
    expected_id=TICKER_ALIAS_CONTRACT_ID,
    expected_resource_sha256=TICKER_ALIAS_RESOURCE_SHA256,
)
ISSUER_MASTER_CONTRACT: Final = _load_contract(
    "issuer_master.schema-v1.registry-v4.json",
    expected_id=ISSUER_MASTER_CONTRACT_ID,
    expected_resource_sha256=ISSUER_MASTER_RESOURCE_SHA256,
)
UNIVERSE_DAILY_CONTRACT: Final = _load_contract(
    "universe_daily.schema-v1.registry-v4.json",
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
