from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from ame_stocks_api.silver.contracts import TableContract
from ame_stocks_api.silver.identity_resolution_contract import (
    ASSET_MASTER_CONTRACT,
    ASSET_MASTER_CONTRACT_ID,
    ASSET_MASTER_RESOURCE_SHA256,
    IDENTITY_ADJUDICATION_CONTRACT,
    IDENTITY_ADJUDICATION_CONTRACT_ID,
    IDENTITY_ADJUDICATION_RESOURCE_SHA256,
    IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT,
    IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT_ID,
    IDENTITY_CROSS_MARKET_ADJUDICATION_RESOURCE_SHA256,
    ISSUER_MASTER_CONTRACT,
    ISSUER_MASTER_CONTRACT_ID,
    ISSUER_MASTER_RESOURCE_SHA256,
    S7_ADJUDICATION_CONTRACTS,
    S7_CONTRACTS,
    S7_DERIVED_CONTRACTS,
    S7_RESOURCE_SHA256_BY_TABLE,
    TICKER_ALIAS_CONTRACT,
    TICKER_ALIAS_CONTRACT_ID,
    TICKER_ALIAS_RESOURCE_SHA256,
    UNIVERSE_DAILY_CONTRACT,
    UNIVERSE_DAILY_CONTRACT_ID,
    UNIVERSE_DAILY_RESOURCE_SHA256,
)

_ROOT = Path(__file__).resolve().parents[1]
_RESOURCE_ROOT = _ROOT / "backend/ame_stocks_api/silver/schema_resources"
_CANDIDATES = {
    "identity_adjudication": (
        _ROOT / "docs/silver/contracts/identity/identity_adjudication.schema-v1.candidate.json"
    ),
    "identity_cross_market_adjudication": (
        _ROOT
        / "docs/silver/contracts/identity/"
        "identity_cross_market_adjudication.schema-v1.candidate.json"
    ),
    "asset_master": _ROOT / "docs/silver/contracts/identity/asset_master.schema-v1.candidate.json",
    "ticker_alias": _ROOT / "docs/silver/contracts/identity/ticker_alias.schema-v1.candidate.json",
    "issuer_master": (
        _ROOT / "docs/silver/contracts/identity/issuer_master.schema-v1.candidate.json"
    ),
    "universe_daily": (
        _ROOT / "docs/silver/contracts/reference/universe_daily.schema-v1.candidate.json"
    ),
}
_CONTRACTS = {
    "identity_adjudication": IDENTITY_ADJUDICATION_CONTRACT,
    "identity_cross_market_adjudication": IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT,
    "asset_master": ASSET_MASTER_CONTRACT,
    "ticker_alias": TICKER_ALIAS_CONTRACT,
    "issuer_master": ISSUER_MASTER_CONTRACT,
    "universe_daily": UNIVERSE_DAILY_CONTRACT,
}
_EXPECTED_IDS = {
    "identity_adjudication": IDENTITY_ADJUDICATION_CONTRACT_ID,
    "identity_cross_market_adjudication": (
        IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT_ID
    ),
    "asset_master": ASSET_MASTER_CONTRACT_ID,
    "ticker_alias": TICKER_ALIAS_CONTRACT_ID,
    "issuer_master": ISSUER_MASTER_CONTRACT_ID,
    "universe_daily": UNIVERSE_DAILY_CONTRACT_ID,
}
_EXPECTED_RESOURCE_SHA256 = {
    "identity_adjudication": IDENTITY_ADJUDICATION_RESOURCE_SHA256,
    "identity_cross_market_adjudication": (
        IDENTITY_CROSS_MARKET_ADJUDICATION_RESOURCE_SHA256
    ),
    "asset_master": ASSET_MASTER_RESOURCE_SHA256,
    "ticker_alias": TICKER_ALIAS_RESOURCE_SHA256,
    "issuer_master": ISSUER_MASTER_RESOURCE_SHA256,
    "universe_daily": UNIVERSE_DAILY_RESOURCE_SHA256,
}
_EXPECTED_SCHEMA_DIGESTS = {
    "identity_adjudication": "e5082a8611bedb6913f79da506f1f5cc19c94507b9e27d04edfb88566033575f",
    "identity_cross_market_adjudication": (
        "96fe9108cd246919a9a00855d04d9f4057c439b6043d4d67178beb1c32d7a0fe"
    ),
    "asset_master": "5ef86bbe8e3e0219e795ed9f8c5c9eca35ebc7b16ff21a903901765b3e7d53d3",
    "ticker_alias": "2f857bc07319426e48494901571a570b1abf622c16c9e429ab8185c08af2d743",
    "issuer_master": "dac9dbe43450cf094c8170d8e88db1742fb035052df9d1b78b7ced02cc4282d2",
    "universe_daily": "80902539df5dc822dc43a88cf7325b16f4fdc2c4c6786c78ea93434116e6e25a",
}


def test_pinned_s7_resources_are_byte_identical_to_candidates() -> None:
    for table, candidate_path in _CANDIDATES.items():
        candidate_payload = candidate_path.read_bytes()
        resource_payload = (_RESOURCE_ROOT / f"{table}.schema-v1.json").read_bytes()

        assert resource_payload == candidate_payload
        assert hashlib.sha256(resource_payload).hexdigest() == _EXPECTED_RESOURCE_SHA256[table]
        assert TableContract.from_dict(json.loads(resource_payload)) == _CONTRACTS[table]


def test_s7_loader_freezes_exact_contract_ids_schema_digests_and_shapes() -> None:
    assert {table: contract.contract_id for table, contract in _CONTRACTS.items()} == _EXPECTED_IDS
    assert {
        table: contract.schema_digest for table, contract in _CONTRACTS.items()
    } == _EXPECTED_SCHEMA_DIGESTS
    assert {
        table: (contract.domain, len(contract.columns), len(contract.qa_rules))
        for table, contract in _CONTRACTS.items()
    } == {
        "identity_adjudication": ("identity", 51, 19),
        "identity_cross_market_adjudication": ("identity", 60, 24),
        "asset_master": ("identity", 46, 37),
        "ticker_alias": ("identity", 54, 49),
        "issuer_master": ("identity", 35, 35),
        "universe_daily": ("reference", 59, 55),
    }
    assert all(
        TableContract.from_dict(contract.to_dict()) == contract for contract in _CONTRACTS.values()
    )


def test_s7_contract_registries_are_complete_ordered_and_immutable() -> None:
    assert tuple(S7_CONTRACTS) == (
        "identity_adjudication",
        "identity_cross_market_adjudication",
        "asset_master",
        "ticker_alias",
        "issuer_master",
        "universe_daily",
    )
    assert dict(S7_CONTRACTS) == _CONTRACTS
    assert dict(S7_ADJUDICATION_CONTRACTS) == {
        "identity_adjudication": IDENTITY_ADJUDICATION_CONTRACT,
        "identity_cross_market_adjudication": (
            IDENTITY_CROSS_MARKET_ADJUDICATION_CONTRACT
        ),
    }
    assert dict(S7_DERIVED_CONTRACTS) == {
        table: _CONTRACTS[table]
        for table in ("asset_master", "ticker_alias", "issuer_master", "universe_daily")
    }
    assert dict(S7_RESOURCE_SHA256_BY_TABLE) == _EXPECTED_RESOURCE_SHA256

    with pytest.raises(TypeError):
        S7_CONTRACTS["asset_master"] = IDENTITY_ADJUDICATION_CONTRACT  # type: ignore[index]
    with pytest.raises(TypeError):
        S7_RESOURCE_SHA256_BY_TABLE["asset_master"] = "0" * 64  # type: ignore[index]
