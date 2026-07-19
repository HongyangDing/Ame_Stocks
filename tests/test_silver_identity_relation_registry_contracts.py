from __future__ import annotations

import hashlib
from pathlib import Path

from ame_stocks_api.silver.identity_relation_registry_contract import (
    RELATION_REGISTRY_CONTRACT_IDS,
    RELATION_REGISTRY_CONTRACTS,
    RELATION_REGISTRY_RESOURCE_SHA256,
    RELATION_REGISTRY_SCHEMA_DIGESTS,
    contract_bytes,
    load_pinned_relation_registry_contracts,
)

ROOT = Path(__file__).resolve().parents[1]

EXPECTED = {
    "provider_composite_override": {
        "columns": 52,
        "qa": 21,
        "contract_id": "a090c4ed150b2f59c38b4f01791f70ce655d44e9c3576bd0a13ac7fd9ba32bc5",
        "schema_digest": "a79e9774d9915fc223b2ff2cea2f7c665892abcd175d70abd8bea04cfdc0bd4c",
        "resource_sha": "1e87d4c5d61a973eddd1e2b39e2d6c56f5405a1aedd451597067eaef192506eb",
    },
    "share_class_adjudication": {
        "columns": 51,
        "qa": 24,
        "contract_id": "5918ade4aaca64372cbb9de70297dce042ef39da4fd3186b174c4c687edd2919",
        "schema_digest": "9a2580dbc02fa76658e4a9f7ae4f01efb823e756c52f92b29627abb16c6b1589",
        "resource_sha": "004abaea381e3897d383b3d4e90d9a13336f153f7cd892c2a4bc34101026eabd",
    },
    "asset_transition": {
        "columns": 51,
        "qa": 20,
        "contract_id": "8831443729fe360c3b4265595a2bd74c8a8b9031cb6f6ca30ee0ac4e1beef7ac",
        "schema_digest": "668f9c1d747f5de6dcd62c517a524590d5c45a571f92ce3bad65e8aea9ca5a4e",
        "resource_sha": "7694dc99a5d92ed99e7c6e22dd2625ea0e9029b4a8abda707006ef1892ec3024",
    },
}


def test_contract_candidates_and_packaged_resources_are_byte_identical() -> None:
    for table, contract in RELATION_REGISTRY_CONTRACTS.items():
        candidate = (
            ROOT / "docs/silver/contracts/identity" / f"{table}.schema-v1.candidate.json"
        ).read_bytes()
        resource = (
            ROOT / "backend/ame_stocks_api/silver/schema_resources" / f"{table}.schema-v1.json"
        ).read_bytes()
        assert candidate == resource == contract_bytes(contract)
        assert hashlib.sha256(resource).hexdigest() == EXPECTED[table]["resource_sha"]


def test_contract_ids_schema_digests_shapes_and_loader_are_frozen() -> None:
    assert tuple(RELATION_REGISTRY_CONTRACTS) == (
        "provider_composite_override",
        "share_class_adjudication",
        "asset_transition",
    )
    assert dict(RELATION_REGISTRY_CONTRACT_IDS) == {
        table: values["contract_id"] for table, values in EXPECTED.items()
    }
    assert dict(RELATION_REGISTRY_SCHEMA_DIGESTS) == {
        table: values["schema_digest"] for table, values in EXPECTED.items()
    }
    assert dict(RELATION_REGISTRY_RESOURCE_SHA256) == {
        table: values["resource_sha"] for table, values in EXPECTED.items()
    }
    assert {
        table: (len(contract.columns), len(contract.qa_rules))
        for table, contract in RELATION_REGISTRY_CONTRACTS.items()
    } == {table: (values["columns"], values["qa"]) for table, values in EXPECTED.items()}
    assert dict(load_pinned_relation_registry_contracts()) == dict(RELATION_REGISTRY_CONTRACTS)


def test_registry_responsibilities_and_collision_qa_are_mutually_exclusive() -> None:
    provider = RELATION_REGISTRY_CONTRACTS["provider_composite_override"]
    share_class = RELATION_REGISTRY_CONTRACTS["share_class_adjudication"]
    transition = RELATION_REGISTRY_CONTRACTS["asset_transition"]
    provider_columns = {item.name for item in provider.columns}
    share_columns = {item.name for item in share_class.columns}
    transition_columns = {item.name for item in transition.columns}

    assert {
        "observed_composite_figi",
        "canonical_composite_figi",
        "canonical_asset_id",
        "asset_transition_id",
    }.issubset(provider_columns)
    assert "canonical_share_class_figi" not in provider_columns

    assert {
        "observed_share_class_figi",
        "canonical_share_class_figi",
        "required_unique_canonical_composite_figi",
    }.issubset(share_columns)
    assert "canonical_asset_id" not in share_columns
    assert "asset_transition_id" not in share_columns

    assert {
        "predecessor_asset_id",
        "successor_asset_id",
        "return_stitching_effect",
    }.issubset(transition_columns)
    assert "canonical_override" not in transition_columns
    assert "canonical_share_class_figi" not in transition_columns

    provider_qa = {item.check_id for item in provider.qa_rules}
    assert {
        "multi_registry_composite_override_collision_rows",
        "multi_registry_composite_override_collision_eligible_rows",
        "multi_registry_composite_override_collision_resolved_rows",
        "multi_registry_composite_override_collision_alias_rows",
    }.issubset(provider_qa)
    assert {
        "asset_transition_used_as_identity_override_rows",
        "asset_transition_return_stitching_rows",
        "temporary_security_merged_into_ordinary_asset_rows",
    }.issubset({item.check_id for item in transition.qa_rules})


def test_evidence_availability_is_distinct_from_historical_event_fields() -> None:
    transition = RELATION_REGISTRY_CONTRACTS["asset_transition"]
    descriptions = {item.name: item.description for item in transition.columns}
    assert "not evidence availability" in descriptions["legal_effective_date"]
    assert "never backdated" in descriptions["transition_available_session"]
    for contract in RELATION_REGISTRY_CONTRACTS.values():
        names = {item.name for item in contract.columns}
        assert {
            "candidate_available_session",
            "external_evidence_available_session",
            "approval_available_session",
            "availability_calendar_id",
            "availability_calendar_sha256",
        }.issubset(names)
