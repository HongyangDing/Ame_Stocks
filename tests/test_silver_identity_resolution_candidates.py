from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import QASeverity, QAStatus, TableContract

ARTIFACT_REFS_DIGEST_VERSION = "s7_release_output_groups_v1"
RELEASE_BUNDLE_DIGEST_VERSION = "s7_six_release_receipts_v1"

ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = (
    ROOT / "docs/silver/source-profiles/identity-resolution-s7-2026-07-14.json"
)
PROFILE_SHA256 = "b35e7df2ceb136b7717b0c8faf36e01e83599f3425dae3e05dc76901b083f2d0"

CANDIDATE_PATHS = {
    "identity_adjudication": (
        ROOT
        / "docs/silver/contracts/identity/identity_adjudication.schema-v1.candidate.json"
    ),
    "identity_cross_market_adjudication": (
        ROOT
        / "docs/silver/contracts/identity/"
        "identity_cross_market_adjudication.schema-v1.candidate.json"
    ),
    "asset_master": (
        ROOT / "docs/silver/contracts/identity/asset_master.schema-v1.candidate.json"
    ),
    "ticker_alias": (
        ROOT / "docs/silver/contracts/identity/ticker_alias.schema-v1.candidate.json"
    ),
    "issuer_master": (
        ROOT / "docs/silver/contracts/identity/issuer_master.schema-v1.candidate.json"
    ),
    "universe_daily": (
        ROOT / "docs/silver/contracts/reference/universe_daily.schema-v1.candidate.json"
    ),
}
EXPECTED_CONTRACT_IDS = {
    "identity_adjudication": "6423cc01b952498cc78d55e93a349d7afe408bd30003e4f7be59f211102f2d5e",
    "identity_cross_market_adjudication": (
        "ae91c7b1bfc27bde82e5f5a39afdc5a3c2c9929d075486cb081836b6798e14e8"
    ),
    "asset_master": "959c5f7bf464eed59fd32a7008349f60ebcfd3cf9e892c9c3d7f00080eae2149",
    "ticker_alias": "39dbf6ef89ed4c2d466fa0be2e47d2840a90f1a97f6a47670af05df3e15513ce",
    "issuer_master": "2faa8d4d2e10e4a065b10b9ae851e53ac517db7e69af4fd59d5f6edc677aa408",
    "universe_daily": "38cd59c4e4b04de8444ba99ed93e6fd8c7a78aec24f01205d7df7494bcfd33d3",
}
EXPECTED_SCHEMA_DIGESTS = {
    "identity_adjudication": "e5082a8611bedb6913f79da506f1f5cc19c94507b9e27d04edfb88566033575f",
    "identity_cross_market_adjudication": (
        "96fe9108cd246919a9a00855d04d9f4057c439b6043d4d67178beb1c32d7a0fe"
    ),
    "asset_master": "5ef86bbe8e3e0219e795ed9f8c5c9eca35ebc7b16ff21a903901765b3e7d53d3",
    "ticker_alias": "2f857bc07319426e48494901571a570b1abf622c16c9e429ab8185c08af2d743",
    "issuer_master": "dac9dbe43450cf094c8170d8e88db1742fb035052df9d1b78b7ced02cc4282d2",
    "universe_daily": "80902539df5dc822dc43a88cf7325b16f4fdc2c4c6786c78ea93434116e6e25a",
}
EXPECTED_FILE_SHA256 = {
    "identity_adjudication": "eb5e9d1746ad2014d7b0e4a9a56ffa29e4f36cf1e1d18d348634a058f0d22231",
    "identity_cross_market_adjudication": (
        "a7308e22c07e8243a8587bfc7eab7ae45b2f232fe9bba310d084916d722f56d0"
    ),
    "asset_master": "bfb31004df41c4556e71beb379bb36e07063f36298d329c887be48c005b02fa5",
    "ticker_alias": "8bf758af5c358c79477ff40177aab5f3b7c8d26f7f0882e261f7d844a66a1f95",
    "issuer_master": "adee0a5457ac32356a0ec9b9a28c692fcebdacc4ba9cccedd1237e8c66b722b7",
    "universe_daily": "c0923508dafa0d56de4be6b8ff43187a581627dd1d64e964cf5f506f5ce8ea0b",
}

EXPECTED_SIX_UPSTREAM = (
    "asset_observation_daily",
    "asset_observation_version",
    "universe_source_daily",
    "ticker_event_request_status",
    "ticker_change_event",
    "ticker_overview_safe",
)
EXPECTED_ADJUDICATION_UPSTREAM = (
    *EXPECTED_SIX_UPSTREAM,
    "identity_case_candidate_manifest",
    "identity_external_evidence_manifest",
    "identity_adjudication_plan",
)
EXPECTED_CROSS_MARKET_ADJUDICATION_UPSTREAM = (
    *EXPECTED_SIX_UPSTREAM,
    "identity_case_candidate_manifest",
    "identity_market_consistency_candidate_manifest",
    "identity_cross_market_external_evidence_manifest",
    "identity_cross_market_adjudication_plan",
)
EXPECTED_DERIVED_UPSTREAM = (
    *EXPECTED_SIX_UPSTREAM,
    "identity_case_candidate_manifest",
    "identity_adjudication",
    "identity_market_consistency_candidate_manifest",
    "identity_cross_market_adjudication",
)
EXPECTED_BINDING_COLUMNS = {
    "identity_resolution_cutoff_session",
    "source_s4_release_set_id",
    "source_s5_status_release_id",
    "source_s5_event_release_id",
    "source_s6_overview_release_id",
    "source_identity_case_candidate_manifest_id",
    "source_identity_case_candidate_manifest_sha256",
    "source_identity_adjudication_release_id",
    "source_identity_adjudication_release_available_session",
    "source_identity_market_consistency_candidate_manifest_id",
    "source_identity_market_consistency_candidate_manifest_sha256",
    "source_identity_cross_market_adjudication_release_id",
    "source_identity_cross_market_adjudication_release_available_session",
}


def artifact_refs_digest(receipts: Sequence[Mapping[str, object]]) -> str:
    return stable_digest(
        {
            "artifact_refs_digest_version": ARTIFACT_REFS_DIGEST_VERSION,
            "release_output_groups": [
                {
                    "artifact_count": receipt["artifact_count"],
                    "outputs_digest": receipt["outputs_digest"],
                    "table": receipt["table"],
                }
                for receipt in receipts
            ],
        }
    )


def release_bundle_digest(receipts: Sequence[Mapping[str, object]]) -> str:
    return stable_digest(
        {
            "release_bundle_digest_version": RELEASE_BUNDLE_DIGEST_VERSION,
            "release_receipts": list(receipts),
        }
    )


def load_contract(name: str) -> TableContract:
    return TableContract.from_dict(
        json.loads(CANDIDATE_PATHS[name].read_text(encoding="utf-8"))
    )


def test_s7_profile_is_machine_readable_and_exactly_reconciled() -> None:
    content = PROFILE_PATH.read_bytes()
    profile = json.loads(content)

    assert hashlib.sha256(content).hexdigest() == PROFILE_SHA256
    assert profile["profile_summary_schema_version"] == 1
    assert profile["write_boundary"] == {
        "bronze_or_published_silver_modified": False,
        "profile_artifact_written_to_remote_data_root": False,
        "remote_profile_file_system_outputs": 0,
        "s7_build_or_release_created": False,
    }

    sources = profile["fixed_sources"]
    bundle = profile["fixed_input_bundle"]
    assert len(sources) == 6
    assert sum(source["artifact_count"] for source in sources) == bundle["artifact_count"]
    assert sum(source["row_count"] for source in sources) == bundle["row_count_sum"]
    assert sum(source["stored_bytes"] for source in sources) == bundle["stored_bytes_sum"]
    assert bundle["artifact_count"] == 7_542
    assert bundle["row_count_sum"] == 138_825_855
    assert bundle["stored_bytes_sum"] == 15_944_020_220
    assert bundle["artifact_refs_digest_version"] == ARTIFACT_REFS_DIGEST_VERSION
    assert bundle["release_bundle_digest_version"] == RELEASE_BUNDLE_DIGEST_VERSION
    assert artifact_refs_digest(sources) == bundle["artifact_refs_digest"]
    assert release_bundle_digest(sources) == bundle["release_bundle_digest"]

    binding = profile["source_release_binding"]
    binding_payload = {key: value for key, value in binding.items() if key != "binding_id"}
    assert stable_digest(binding_payload) == binding["binding_id"]
    assert binding["binding_id"] == (
        "49f3d20725f2609b43d6736df78993b2975c9f1b71947af93190dc0658366c64"
    )
    assert set(binding["release_ids_by_dataset"]) == set(EXPECTED_SIX_UPSTREAM)
    assert set(binding["release_manifest_sha256_by_dataset"]) == set(
        EXPECTED_SIX_UPSTREAM
    )
    sources_by_table = {source["table"]: source for source in sources}
    assert binding["release_ids_by_dataset"] == {
        table: sources_by_table[table]["release_id"] for table in EXPECTED_SIX_UPSTREAM
    }
    assert binding["release_manifest_sha256_by_dataset"] == {
        table: sources_by_table[table]["release_manifest_sha256"]
        for table in EXPECTED_SIX_UPSTREAM
    }
    assert binding["artifact_refs_digest"] == bundle["artifact_refs_digest"]
    assert binding["artifact_refs_digest_version"] == (
        bundle["artifact_refs_digest_version"]
    )
    assert binding["release_bundle_digest"] == bundle["release_bundle_digest"]
    assert binding["release_bundle_digest_version"] == (
        bundle["release_bundle_digest_version"]
    )
    s4_receipt = profile["s4_release_set_receipt"]
    assert s4_receipt["release_set_id"] == binding["s4_release_set_id"]
    assert s4_receipt["sha256"] == binding["s4_release_set_manifest_sha256"]
    assert s4_receipt["member_release_ids_by_table"] == {
        table: sources_by_table[table]["release_id"]
        for table in EXPECTED_SIX_UPSTREAM[:3]
    }

    provenance = profile["profile_provenance"]
    scripts = {item["path"]: item for item in provenance["scripts"]}
    assert set(scripts) == {
        "scripts/s7_integrity_receipt.py",
        "scripts/s7_small_profile.py",
        "scripts/s7_universe_profile.py",
        "scripts/s7_type_anchor_profile.py",
    }
    for path, receipt in scripts.items():
        assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == receipt["sha256"]
        assert len(receipt["stdout_sha256_repeat"]) == 64

    fact_payload = {
        key: profile[key] for key in profile["deterministic_fact_digest_scope"]
    }
    assert stable_digest(fact_payload) == profile["deterministic_fact_digest"]
    assert profile["deterministic_fact_digest"] == (
        "42141c3998e3ae3270b9fdf4994363edb06a3c7adb7eed6b26a161264593c04d"
    )
    controls = profile["control_file_receipts"]
    lifecycle = controls["ticker_overview_lifecycle_plan"]
    assert lifecycle["bytes"] == 12_910_337
    assert lifecycle["row_count"] == 30_739
    assert lifecycle["sha256"] == (
        "ce8e6c457ab6e7110d26d8d4afa38186b91509f6f02d4a3177352471779876a0"
    )
    assert lifecycle["bound_build_id"] == sources_by_table[
        "ticker_overview_safe"
    ]["build_id"]
    assert lifecycle["bound_release_id"] == sources_by_table[
        "ticker_overview_safe"
    ]["release_id"]
    assert lifecycle["bound_release_manifest_sha256"] == sources_by_table[
        "ticker_overview_safe"
    ]["release_manifest_sha256"]
    assert lifecycle["bound_coverage_receipt_sha256"] == (
        "b771d67e3c0d6139a31766c2b2ffb431292d1d896a4e593a7c100fcaec552ae7"
    )
    assert lifecycle["bound_lifecycle_inventory"]["inventory_id"] == (
        "b566cd78a7d65d9d986edbb3d538b567b03dd1b6efe898b3df994c35f5668076"
    )
    assert lifecycle["bound_overview_inventory"]["inventory_id"] == (
        "5503057d5e575e3827bf53599ee342f7ad6d2d8328cf20a127b08ec5c1fc8c03"
    )
    assert controls["s6_pending_quarantine"]["row_count"] == 169
    assert controls["s6_pending_quarantine"]["sha256"] == (
        "b12b8bae3b154f31a2e7ca46010db4dcdef72004d6151e443729116f74fe9b05"
    )
    assert controls["s6_pending_quarantine"]["bound_build_id"] == lifecycle[
        "bound_build_id"
    ]
    assert controls["s6_pending_quarantine"]["bound_release_id"] == lifecycle[
        "bound_release_id"
    ]
    integrity_stdout_payload = {
        "bundle_integrity": {
            "artifact_refs_digest": bundle["artifact_refs_digest"],
            "artifact_refs_digest_version": bundle["artifact_refs_digest_version"],
            "release_bundle_digest": bundle["release_bundle_digest"],
            "release_bundle_digest_version": bundle["release_bundle_digest_version"],
            "verified_artifact_count": bundle["artifact_count"],
        },
        "control_files": controls,
        "release_receipts": sources,
        "s4_release_set": profile["s4_release_set_receipt"],
        "write_boundary": {
            "bronze_or_published_silver_modified": False,
            "file_system_outputs": 0,
        },
    }
    reconstructed_stdout = (
        json.dumps(integrity_stdout_payload, indent=2, sort_keys=True) + "\n"
    ).encode()
    assert hashlib.sha256(reconstructed_stdout).hexdigest() == profile[
        "source_integrity"
    ]["integrity_receipt_stdout_sha256"]
    assert profile["source_integrity"] == {
        "exact_six_release_manifest_and_artifact_trust_chain": "passed",
        "integrity_receipt_script_sha256": (
            "90063edb9b56ec329ef7262975effeb72708d1209fd22fe37de8d2d3686eca74"
        ),
        "integrity_receipt_stdout_sha256": (
            "4a629fa338371c627efecb2aab4fa644cca301ea50f051778f0f51f5a380b069"
        ),
        "s4_release_set_marker_and_s6_control_lineage": "passed",
        "verified_artifact_count": 7_542,
        "verified_release_count": 6,
        "verified_s6_source_binding_count": 30_739,
    }

    universe = profile["s4_universe_full_profile"]
    assert universe["active_rows"] + universe["inactive_rows"] == universe["row_count"]
    assert sum(universe["active_anchor_rows"].values()) == universe["active_rows"]
    assert sum(universe["all_anchor_rows"].values()) == universe["row_count"]
    assert universe["active_rows_with_composite_figi"] == (
        universe["active_anchor_rows"]["share_class_figi"]
        + universe["active_anchor_rows"]["composite_figi_only"]
    )
    assert universe["active_share_class_without_composite_rows"] == 0
    assert sum(item["row_count"] for item in profile["active_anchor_by_type"]) == (
        universe["active_rows"]
    )
    for item in profile["active_anchor_by_type"]:
        assert item["row_count"] == sum(
            item[name]
            for name in (
                "share_class_figi_rows",
                "composite_figi_only_rows",
                "no_security_figi_rows",
            )
        )

    s5 = profile["s5_profile"]
    assert s5["complete_request_rows"] + s5["http_404_request_rows"] == s5["request_rows"]
    assert s5["accepted_event_rows"] + s5["event_quarantine_rows"] == s5["raw_event_rows"]

    s6 = profile["s6_profile"]
    assert sum(s6["identity_match_basis"].values()) == s6["accepted_rows"]
    assert s6["accepted_rows"] + s6["pending_quarantine"]["row_count"] == (
        s6["source_lifecycle_rows"]
    )
    assert sum(
        s6["pending_quarantine"][name]
        for name in ("share_class_figi", "composite_figi", "cik")
    ) == s6["pending_quarantine"]["row_count"]


def test_s7_candidate_contracts_are_valid_and_digest_frozen() -> None:
    contracts = {name: load_contract(name) for name in CANDIDATE_PATHS}

    assert {name: contract.contract_id for name, contract in contracts.items()} == (
        EXPECTED_CONTRACT_IDS
    )
    assert {name: contract.schema_digest for name, contract in contracts.items()} == (
        EXPECTED_SCHEMA_DIGESTS
    )
    assert {
        name: hashlib.sha256(path.read_bytes()).hexdigest()
        for name, path in CANDIDATE_PATHS.items()
    } == EXPECTED_FILE_SHA256
    assert all(
        TableContract.from_dict(contract.to_dict()) == contract
        for contract in contracts.values()
    )
    assert {
        name: (contract.domain, contract.table, contract.schema_version)
        for name, contract in contracts.items()
    } == {
        "identity_adjudication": ("identity", "identity_adjudication", 1),
        "identity_cross_market_adjudication": (
            "identity",
            "identity_cross_market_adjudication",
            1,
        ),
        "asset_master": ("identity", "asset_master", 1),
        "ticker_alias": ("identity", "ticker_alias", 1),
        "issuer_master": ("identity", "issuer_master", 1),
        "universe_daily": ("reference", "universe_daily", 1),
    }
    assert {
        name: (len(contract.columns), len(contract.qa_rules))
        for name, contract in contracts.items()
    } == {
        "identity_adjudication": (51, 19),
        "identity_cross_market_adjudication": (60, 24),
        "asset_master": (46, 37),
        "ticker_alias": (54, 49),
        "issuer_master": (35, 35),
        "universe_daily": (59, 55),
    }
    assert contracts["identity_adjudication"].source_datasets == (
        EXPECTED_ADJUDICATION_UPSTREAM
    )
    assert contracts["identity_cross_market_adjudication"].source_datasets == (
        EXPECTED_CROSS_MARKET_ADJUDICATION_UPSTREAM
    )
    assert all(
        contracts[name].source_datasets == EXPECTED_DERIVED_UPSTREAM
        for name in ("asset_master", "ticker_alias", "issuer_master", "universe_daily")
    )
    assert all(
        {column.name for column in contract.columns if not column.nullable}
        >= EXPECTED_BINDING_COLUMNS
        for name, contract in contracts.items()
        if name
        not in {"identity_adjudication", "identity_cross_market_adjudication"}
    )
    assert "identity_adjudication" not in contracts[
        "identity_adjudication"
    ].source_datasets
    assert "identity_cross_market_adjudication" not in contracts[
        "identity_cross_market_adjudication"
    ].source_datasets
    assert all(
        {"asset_master", "ticker_alias", "issuer_master", "universe_daily"}.isdisjoint(
            contracts[name].source_datasets
        )
        for name in ("asset_master", "ticker_alias", "issuer_master", "universe_daily")
    )


def test_s7_deterministic_identity_fixed_vectors() -> None:
    asset_payload = {
        "namespace": "ame_stocks.identity.asset",
        "rule_version": "ame_stocks_asset_id_from_composite_figi_v1",
        "anchor_type": "composite_figi",
        "anchor_value": "BBG000B9XRY4",
    }
    share_payload = {
        "namespace": "ame_stocks.identity.share_class",
        "rule_version": "ame_stocks_share_class_id_from_share_class_figi_v1",
        "anchor_type": "share_class_figi",
        "anchor_value": "BBG001S5N8V8",
    }
    issuer_payload = {
        "namespace": "ame_stocks.identity.issuer",
        "rule_version": "ame_stocks_issuer_id_from_normalized_cik_v1",
        "anchor_type": "cik_normalized",
        "anchor_value": "0000320193",
    }
    asset_id = stable_digest(asset_payload)
    share_class_id = stable_digest(share_payload)
    issuer_id = stable_digest(issuer_payload)

    assert asset_id == "423f8da3d1b7dcae53aa997d845cd269fe8ed3ab188dc3e7e982d18c8650ce08"
    assert share_class_id == (
        "858ec64e0790912a3298b0c2ac62023f3d807bfcde8f7e143d179c3db8915012"
    )
    assert issuer_id == "cd178adefcd4e3b564cafee98411e18c87bd843d91eeb502a4ac2604dfee7940"

    episode_source_record_set_digest = stable_digest(["s4-b-001", "s4-b-002"])
    assert episode_source_record_set_digest == (
        "c198348b6aec22c308729c9b042557bd68c6a39f5825acd7f352d785e963c49c"
    )
    identity_case_payload = {
        "namespace": "ame_stocks.identity.provider_figi_bounce_case",
        "rule_version": "s7_provider_figi_bounce_case_id_v1",
        "six_release_binding_id": (
            "49f3d20725f2609b43d6736df78993b2975c9f1b71947af93190dc0658366c64"
        ),
        "detector_rule_version": "s7_provider_figi_bounce_detector_v1",
        "ticker": "AAPL",
        "left_outer_composite_figi": "BBG000B9XRY4",
        "middle_observed_composite_figi": "BBG000BPH459",
        "right_outer_composite_figi": "BBG000B9XRY4",
        "left_outer_source_record_id": "s4-a-left",
        "right_outer_source_record_id": "s4-a-right",
        "episode_valid_from_session": "2024-01-03",
        "episode_valid_through_session": "2024-01-04",
        "episode_source_record_set_digest": episode_source_record_set_digest,
    }
    identity_case_id = stable_digest(identity_case_payload)
    assert identity_case_id == (
        "8cd333b4fb72b62e4534ddb316d2ebf30f3cc6d852e19ea778375c13b7daa46e"
    )
    adjudication_series_payload = {
        "namespace": "ame_stocks.identity.adjudication_series",
        "rule_version": "s7_identity_adjudication_series_id_v1",
        "identity_case_id": identity_case_id,
    }
    adjudication_series_id = stable_digest(adjudication_series_payload)
    assert adjudication_series_id == (
        "2a5a3791079973fbf1efa307feed29165225c74afe3dfe745a64072a1646c5dd"
    )
    evidence_digest = stable_digest(
        [
            {
                "dataset": "asset_observation_daily",
                "release_id": "r-s4",
                "source_record_id": "s4-a-left",
            },
            {
                "dataset": "asset_observation_daily",
                "release_id": "r-s4",
                "source_record_id": "s4-a-right",
            },
        ]
    )
    assert evidence_digest == (
        "94c74efd73502eb2f645f8a2a84f1c19b9a89a5087d398249b7c9f14a03de06e"
    )
    asset_b_id = stable_digest(
        {
            "namespace": "ame_stocks.identity.asset",
            "rule_version": "ame_stocks_asset_id_from_composite_figi_v1",
            "anchor_type": "composite_figi",
            "anchor_value": "BBG000BPH459",
        }
    )
    adjudication_base = {
        "namespace": "ame_stocks.identity.adjudication",
        "rule_version": "s7_identity_adjudication_id_v1",
        "adjudication_series_id": adjudication_series_id,
        "decision_version": 1,
        "supersedes_identity_adjudication_id": None,
        "identity_case_id": identity_case_id,
        "evidence_digest": evidence_digest,
    }
    genuine_adjudication_id = stable_digest(
        {
            **adjudication_base,
            "canonical_asset_id": asset_b_id,
            "canonical_composite_figi": "BBG000BPH459",
            "disposition": "confirmed_genuine_transition",
            "reason_code": "corroborated_security_transition",
            "reason_detail": (
                "Bounded S4 episode and corroborating identity fields support a "
                "genuine transition."
            ),
        }
    )
    contamination_adjudication_id = stable_digest(
        {
            **adjudication_base,
            "canonical_asset_id": asset_id,
            "canonical_composite_figi": "BBG000B9XRY4",
            "disposition": "confirmed_provider_contamination",
            "reason_code": "provider_figi_episode_contamination",
            "reason_detail": (
                "Bounded S4 episode lacks a paired identity transition and the "
                "outer canonical anchor is independently supported."
            ),
        }
    )
    withdrawn_adjudication_id = stable_digest(
        {
            **adjudication_base,
            "decision_version": 2,
            "supersedes_identity_adjudication_id": contamination_adjudication_id,
            "canonical_asset_id": None,
            "canonical_composite_figi": None,
            "disposition": "adjudicated_unresolved",
            "reason_code": "withdraw_prior_identity_mapping",
            "reason_detail": (
                "New evidence invalidates the prior mapping; retain the episode as "
                "unresolved pending further review."
            ),
        }
    )
    assert genuine_adjudication_id == (
        "4b06f7fb805fbda6176cbc41df5feb7d1607998a6dc5dd303a357163409f5d9d"
    )
    assert contamination_adjudication_id == (
        "d897dffb0aa7cb874c3abb723e9ae95d8ff1ceebdf2a8fb61b9badff958db8bf"
    )
    assert withdrawn_adjudication_id == (
        "970cbf5d97c646df84ec31715de710707646f428658dc20ad9fd2fcf162a2777"
    )
    assert len({genuine_adjudication_id, contamination_adjudication_id}) == 2

    alias_payload = {
        "namespace": "ame_stocks.identity.ticker_alias",
        "rule_version": "ame_stocks_ticker_alias_id_from_observed_and_canonical_interval_v3",
        "adjudication_available_session": None,
        "asset_id": asset_id,
        "canonical_cik_normalized": "0000320193",
        "canonical_composite_figi": "BBG000B9XRY4",
        "canonical_composite_market_code": "US",
        "canonical_share_class_figi": "BBG001S5N8V8",
        "cross_market_adjudication_available_session": None,
        "cross_market_adjudication_id": None,
        "cross_market_scope_id": None,
        "identity_adjudication_id": None,
        "identity_case_available_session": None,
        "identity_case_id": None,
        "identity_case_resolution_role": None,
        "identity_disposition": "observed_consistent",
        "identity_resolution_cutoff_session": "2026-07-15",
        "issuer_id": issuer_id,
        "observed_cik_normalized": "0000320193",
        "observed_composite_figi": "BBG000B9XRY4",
        "observed_composite_market_code": "US",
        "observed_share_class_figi": "BBG001S5N8V8",
        "share_class_id": share_class_id,
        "source_identity_adjudication_release_available_session": "2026-07-15",
        "source_identity_adjudication_release_id": "e" * 64,
        "source_identity_case_candidate_manifest_id": "f" * 64,
        "source_identity_case_candidate_manifest_sha256": "1" * 64,
        "source_identity_cross_market_adjudication_release_available_session": (
            "2026-07-15"
        ),
        "source_identity_cross_market_adjudication_release_id": "2" * 64,
        "source_identity_market_consistency_candidate_manifest_id": "3" * 64,
        "source_identity_market_consistency_candidate_manifest_sha256": "4" * 64,
        "ticker": "AAPL",
        "valid_from_session": "2024-01-02",
    }
    assert stable_digest(alias_payload) == (
        "8b66d0321988dbbe089d0b50ee69e6228c818db7bcf9b507b166c8dd252ee043"
    )
    alias_payload["canonical_cik_normalized"] = None
    alias_payload["canonical_share_class_figi"] = None
    alias_payload["share_class_id"] = None
    alias_payload["issuer_id"] = None
    alias_payload["observed_cik_normalized"] = None
    alias_payload["observed_share_class_figi"] = None
    assert stable_digest(alias_payload) == (
        "8e1ddf720c957dea524b7aa8c2ac295f06fb2f7413e8fed3528a810bfcda84f4"
    )


def test_asset_master_uses_composite_asset_and_share_class_parent_layers() -> None:
    contract = load_contract("asset_master")
    columns = {column.name: column for column in contract.columns}
    rules = {rule.check_id: rule for rule in contract.qa_rules}

    assert contract.primary_key == ("asset_id",)
    assert contract.partition_by == ()
    assert "canonical_composite_figi" in columns
    assert columns["canonical_composite_figi"].nullable is False
    assert columns["canonical_identity_basis"].nullable is False
    assert columns["observed_composite_figi_count"].nullable is False
    assert columns["adjudicated_override_evidence_row_count"].nullable is False
    assert columns["identity_adjudication_count"].nullable is False
    assert columns["genuine_transition_adjudication_count"].nullable is False
    assert columns["provider_contamination_adjudication_count"].nullable is False
    assert columns["cross_market_override_evidence_row_count"].nullable is False
    assert columns["cross_market_adjudication_count"].nullable is False
    assert columns["first_direct_observed_session"].nullable is True
    assert columns["last_direct_observed_session"].nullable is True
    assert columns["first_canonical_membership_session"].nullable is True
    assert columns["identity_resolution_cutoff_session"].nullable is False
    assert columns[
        "source_identity_adjudication_release_available_session"
    ].nullable is False
    assert columns["share_class_id_rule_version"].nullable is False
    assert columns["share_class_id"].nullable is True
    assert columns["canonical_share_class_figi"].nullable is True
    assert "U.S. tradable-security" in contract.grain
    assert "provider-observed FIGI" in columns["canonical_composite_figi"].description
    assert "may legitimately parent multiple" in columns["canonical_share_class_figi"].description
    assert rules["noncomposite_derived_asset_id_rows"].severity is QASeverity.CRITICAL
    assert rules["state_domain_invalid_rows"].severity is QASeverity.CRITICAL
    assert rules["state_matrix_invalid_rows"].severity is QASeverity.CRITICAL
    assert (
        rules["evidence_count_recomputation_mismatch_rows"].severity
        is QASeverity.CRITICAL
    )
    assert (
        rules["identity_evidence_availability_recomputation_mismatch_rows"].severity
        is QASeverity.CRITICAL
    )
    assert rules["security_level_conflict_rows"].severity is QASeverity.HIGH
    assert rules["security_level_conflict_rows"].failure_status is QAStatus.WARNING
    assert rules["row_funnel_unreconciled"].severity is QASeverity.CRITICAL
    assert "retained unresolved universe rows" in rules["row_funnel_unreconciled"].description
    assert rules["unapproved_canonical_identity_override_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["contaminated_hierarchy_evidence_leak_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["unadjudicated_suspected_episode_created_asset_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["approved_adjudication_bypassed_conflict_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["unapproved_cross_market_created_asset_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["cross_market_target_evidence_invalid_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["identity_resolution_cutoff_invalid_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["s5_event_created_asset_rows"].severity is QASeverity.CRITICAL
    assert "No S5-only event identity" in rules[
        "s5_event_created_asset_rows"
    ].description
    assert rules["unattached_s5_event_identity_rows"].severity is QASeverity.HIGH
    assert rules["unattached_s5_event_identity_rows"].failure_status is QAStatus.WARNING
    assert "no approved supersession source" in rules["supersession_invalid_rows"].description
    assert "ticker_event_request_status" in rules[
        "evidence_count_recomputation_mismatch_rows"
    ].description
    assert "cross-market" in rules[
        "identity_evidence_availability_recomputation_mismatch_rows"
    ].description


def test_identity_adjudication_is_protected_append_only_upstream_registry() -> None:
    contract = load_contract("identity_adjudication")
    columns = {column.name: column for column in contract.columns}
    rules = {rule.check_id: rule for rule in contract.qa_rules}

    assert contract.primary_key == ("identity_adjudication_id",)
    assert contract.source_datasets == EXPECTED_ADJUDICATION_UPSTREAM
    assert columns["observed_composite_figi"].nullable is False
    assert columns["canonical_composite_figi"].nullable is True
    assert columns["canonical_asset_id"].nullable is True
    assert columns["episode_source_record_set_digest"].nullable is False
    assert columns["episode_source_record_count"].nullable is False
    assert columns["identity_case_id"].nullable is False
    assert columns["identity_case_available_session"].nullable is False
    assert columns["source_identity_case_candidate_manifest_id"].nullable is False
    assert columns["source_identity_case_candidate_manifest_sha256"].nullable is False
    assert columns["external_evidence_record_count"].nullable is False
    assert columns["source_external_evidence_manifest_id"].nullable is True
    assert columns["source_external_evidence_manifest_sha256"].nullable is True
    assert columns["approval_receipt_sha256"].nullable is False
    assert columns["approval_available_session"].nullable is False
    assert columns["adjudication_available_session"].nullable is False
    assert columns["availability_calendar_sha256"].nullable is False
    assert columns["outcome_or_backtest_evidence_used"].nullable is False
    assert "published before" in contract.description
    assert rules["adjudication_version_chain_invalid_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["adjudication_state_matrix_invalid_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["adjudication_scope_overlap_rows"].severity is QASeverity.CRITICAL
    assert rules["adjudication_terminal_head_selection_invalid_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["adjudication_approval_binding_conflict_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["external_evidence_manifest_binding_invalid_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["canonical_override_target_unanchored_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["outcome_or_backtest_evidence_rows"].severity is QASeverity.CRITICAL
    assert rules["provider_figi_bounce_cases_without_reason_counts"].severity is (
        QASeverity.HIGH
    )


def test_cross_market_adjudication_is_exact_scope_and_cannot_mutate_membership() -> None:
    contract = load_contract("identity_cross_market_adjudication")
    columns = {column.name: column for column in contract.columns}
    rules = {rule.check_id: rule for rule in contract.qa_rules}

    assert contract.primary_key == ("cross_market_adjudication_id",)
    assert contract.source_datasets == EXPECTED_CROSS_MARKET_ADJUDICATION_UPSTREAM
    for name in (
        "provider_id",
        "provider_locale",
        "observed_ticker",
        "share_class_figi",
        "observed_foreign_composite_figi",
        "cross_market_subject_id",
        "valid_from_session",
        "valid_through_session",
        "scoped_source_record_ids_json",
        "source_s4_release_set_id",
        "source_external_evidence_manifest_id",
        "approval_receipt_id",
        "adjudication_available_session",
        "identity_quality_liquidation_signal",
    ):
        assert columns[name].nullable is False
    assert columns["canonical_us_composite_figi"].nullable is True
    assert columns["canonical_composite_market_code"].nullable is True
    assert columns["canonical_asset_id"].nullable is True
    assert rules["cross_market_subject_id_recomputation_mismatch_rows"].severity is (
        QASeverity.CRITICAL
    )
    for check_id in (
        "cross_market_scope_overlap_rows",
        "canonical_target_evidence_invalid_rows",
        "cross_market_approval_binding_invalid_rows",
        "identity_quality_membership_mutation_rows",
        "identity_quality_forced_liquidation_signal_rows",
    ):
        assert rules[check_id].severity is QASeverity.CRITICAL


def test_ticker_alias_is_observed_half_open_interval_evidence() -> None:
    contract = load_contract("ticker_alias")
    columns = {column.name: column for column in contract.columns}
    rules = {rule.check_id: rule for rule in contract.qa_rules}

    assert contract.primary_key == ("ticker_alias_id",)
    assert contract.partition_by == ()
    assert columns["valid_from_session"].nullable is False
    assert columns["valid_through_session"].nullable is False
    assert columns["valid_to_session_exclusive"].nullable is True
    assert columns["ticker_alias_id_rule_version"].nullable is False
    assert columns["observed_composite_figi"].nullable is False
    assert columns["observed_asset_id"].nullable is False
    assert columns["canonical_composite_figi"].nullable is False
    assert columns["observed_share_class_figi"].nullable is True
    assert columns["observed_cik_normalized"].nullable is True
    assert columns["canonical_share_class_figi"].nullable is True
    assert columns["canonical_cik_normalized"].nullable is True
    assert columns["identity_disposition"].nullable is False
    assert columns["identity_adjudication_id"].nullable is True
    assert columns["cross_market_scope_id"].nullable is True
    assert columns["cross_market_adjudication_id"].nullable is True
    assert columns["identity_case_resolution_role"].nullable is True
    assert columns["identity_case_available_session"].nullable is True
    assert columns["identity_resolution_cutoff_session"].nullable is False
    assert columns["share_class_id"].nullable is True
    assert "observed/canonical Composite FIGI" in contract.grain
    assert "be the sole source" in rules["event_defined_interval_rows"].description
    assert rules["interval_internal_gap_rows"].severity is QASeverity.CRITICAL
    assert rules["interval_source_count_mismatch_rows"].severity is QASeverity.CRITICAL
    assert rules["ticker_interval_overlap_rows"].severity is QASeverity.CRITICAL
    assert rules["alias_universe_bidirectional_mismatch_rows"].severity is QASeverity.CRITICAL
    assert "Every emitted ticker_alias row" in rules["eligibility_invalid_rows"].description
    assert (
        rules["evidence_count_recomputation_mismatch_rows"].severity
        is QASeverity.CRITICAL
    )
    assert (
        rules["identity_evidence_availability_recomputation_mismatch_rows"].severity
        is QASeverity.CRITICAL
    )
    assert rules["weak_noncomposite_candidate_rows"].severity is QASeverity.HIGH
    assert "request-status rows" in rules[
        "evidence_count_recomputation_mismatch_rows"
    ].description
    assert "event_date_quality=ordinary_calendar_date" in rules[
        "evidence_count_recomputation_mismatch_rows"
    ].description
    assert rules["event_interval_association_ambiguous_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["unassociated_s5_ticker_event_rows"].severity is QASeverity.HIGH
    assert rules["unassociated_s5_ticker_event_rows"].failure_status is (
        QAStatus.WARNING
    )
    assert rules["unapproved_canonical_identity_override_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["unapproved_cross_market_composite_alias_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["cross_market_override_foreign_locale_leak_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["correct_us_observation_overridden_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["unresolved_suspected_episode_alias_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["adjudicated_unresolved_episode_alias_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["canonical_hierarchy_without_independent_support_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["adjudication_bypassed_nonfigi_conflict_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["cutoff_bound_registry_selection_invalid_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert "both registry releases" in rules[
        "identity_evidence_availability_recomputation_mismatch_rows"
    ].description


def test_issuer_master_keeps_cik_out_of_asset_resolution() -> None:
    contract = load_contract("issuer_master")
    columns = {column.name: column for column in contract.columns}
    rules = {rule.check_id: rule for rule in contract.qa_rules}

    assert contract.primary_key == ("issuer_id",)
    assert columns["cik_normalized"].nullable is False
    assert columns["reference_name"].nullable is True
    assert "not labeled a legal issuer name" in columns["reference_name"].description
    assert columns["sic_code_current_reference"].nullable is True
    assert columns["excluded_contamination_evidence_row_count"].nullable is False
    assert columns["excluded_cross_market_contamination_evidence_row_count"].nullable is False
    assert columns["identity_resolution_cutoff_session"].nullable is False
    assert columns[
        "source_identity_adjudication_release_available_session"
    ].nullable is False
    assert "never an asset identity key" in contract.description
    assert rules["asset_id_derived_from_cik_rows"].severity is QASeverity.CRITICAL
    assert columns["source_s5_status_release_id"].nullable is False
    assert columns["source_identity_adjudication_release_id"].nullable is False
    assert rules["state_domain_invalid_rows"].severity is QASeverity.CRITICAL
    assert (
        rules["evidence_count_recomputation_mismatch_rows"].severity
        is QASeverity.CRITICAL
    )
    assert (
        rules["reference_availability_recomputation_mismatch_rows"].severity
        is QASeverity.CRITICAL
    )
    assert rules["multiple_reference_sic_rows"].severity is QASeverity.HIGH
    assert rules["row_funnel_unreconciled"].severity is QASeverity.CRITICAL
    assert "exactly one issuer row" in rules["row_funnel_unreconciled"].description
    assert rules["s5_event_created_issuer_rows"].severity is QASeverity.CRITICAL
    assert "No S5-only event evidence" in rules[
        "s5_event_created_issuer_rows"
    ].description
    assert rules["unattached_s5_event_issuer_rows"].severity is QASeverity.HIGH
    assert rules["unattached_s5_event_issuer_rows"].failure_status is QAStatus.WARNING
    assert "no approved supersession source" in rules["supersession_invalid_rows"].description
    assert "request-status rows" in rules[
        "evidence_count_recomputation_mismatch_rows"
    ].description
    assert "cross-market decision" in rules[
        "reference_availability_recomputation_mismatch_rows"
    ].description
    assert rules["adjudication_changed_issuer_identity_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["contaminated_issuer_evidence_leak_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["contamination_only_cik_created_issuer_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules[
        "contamination_exclusion_count_recomputation_mismatch_rows"
    ].severity is QASeverity.CRITICAL
    assert rules["cross_market_contaminated_issuer_evidence_leak_rows"].severity is (
        QASeverity.CRITICAL
    )


def test_universe_daily_is_active_left_preserving_and_fail_closed() -> None:
    contract = load_contract("universe_daily")
    columns = {column.name: column for column in contract.columns}
    rules = {rule.check_id: rule for rule in contract.qa_rules}

    assert contract.primary_key == ("session_date", "ticker")
    assert contract.partition_by == ("session_year", "session_date")
    assert columns["active_on_date"].nullable is False
    assert columns["asset_id"].nullable is True
    assert columns["share_class_id"].nullable is True
    assert columns["ticker_alias_id"].nullable is True
    assert columns["observed_composite_figi"].nullable is True
    assert columns["observed_asset_id"].nullable is True
    assert columns["canonical_composite_figi"].nullable is True
    assert columns["observed_share_class_figi"].nullable is True
    assert columns["observed_cik_normalized"].nullable is True
    assert columns["canonical_share_class_figi"].nullable is True
    assert columns["canonical_cik_normalized"].nullable is True
    assert columns["identity_disposition"].nullable is False
    assert columns["identity_case_id"].nullable is True
    assert columns["identity_case_available_session"].nullable is True
    assert columns["identity_adjudication_id"].nullable is True
    assert columns["cross_market_scope_id"].nullable is True
    assert columns["cross_market_adjudication_id"].nullable is True
    assert columns["cross_market_classification_status"].nullable is False
    assert columns["identity_case_resolution_role"].nullable is True
    assert columns["adjudication_available_session"].nullable is True
    assert columns["identity_resolution_cutoff_session"].nullable is False
    assert columns["position_continuity_status"].nullable is False
    assert columns["identity_quality_liquidation_signal"].nullable is False
    assert columns["membership_source_available_session"].nullable is False
    assert columns["membership_source_availability_quality"].nullable is False
    assert columns["identity_evidence_available_session"].nullable is False
    assert columns["current_reference_factor_eligible"].nullable is False
    assert "active-only" in contract.description
    assert "preserving unresolved rows" in contract.description
    assert rules["active_left_funnel_unreconciled"].severity is QASeverity.CRITICAL
    assert rules["nonresolved_row_eligible_rows"].severity is QASeverity.CRITICAL
    assert rules["state_domain_invalid_rows"].severity is QASeverity.CRITICAL
    assert rules["state_matrix_invalid_rows"].severity is QASeverity.CRITICAL
    assert rules["alias_universe_bidirectional_mismatch_rows"].severity is QASeverity.CRITICAL
    assert (
        rules["identity_evidence_availability_recomputation_mismatch_rows"].severity
        is QASeverity.CRITICAL
    )
    assert rules["foreign_asset_eligibility_mismatch_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert "approved cross-market row" in rules[
        "resolved_row_incomplete_rows"
    ].description
    assert rules["active_common_stock_without_eligible_identity_rows"].severity is (
        QASeverity.HIGH
    )
    assert rules["s6_pending_quarantine_unresolved_rows"].severity is QASeverity.HIGH
    assert rules["s5_event_created_resolution_rows"].severity is QASeverity.CRITICAL
    assert "No universe row may gain or change" in rules[
        "s5_event_created_resolution_rows"
    ].description
    assert rules["suspected_provider_figi_bounce_rows"].severity is QASeverity.HIGH
    assert rules["suspected_provider_figi_bounce_rows"].failure_status is (
        QAStatus.WARNING
    )
    assert rules["unapproved_canonical_identity_override_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["suspected_provider_contamination_eligible_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["us_locale_non_us_composite_figi_rows"].severity is QASeverity.HIGH
    assert rules["us_locale_non_us_composite_figi_rows"].failure_status is (
        QAStatus.WARNING
    )
    assert rules["unapproved_cross_market_composite_eligible_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules[
        "inverse_bounce_misclassified_as_genuine_transition_rows"
    ].severity is QASeverity.CRITICAL
    assert rules["cross_market_override_outside_us_locale_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["contaminated_hierarchy_evidence_leak_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["identity_resolution_cutoff_invalid_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["registry_release_availability_invalid_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["identity_quality_continuity_matrix_invalid_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["identity_quality_membership_mutation_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert rules["identity_quality_forced_liquidation_signal_rows"].severity is (
        QASeverity.CRITICAL
    )
    assert "both registry releases" in rules[
        "identity_evidence_availability_recomputation_mismatch_rows"
    ].description


def test_provider_figi_bounce_fixed_contract_decision_vectors() -> None:
    """Freeze schema-level outcomes without implementing an S7 transform fixture."""

    canonical_a = "a" * 64
    canonical_b = "b" * 64
    case_id = "8cd333b4fb72b62e4534ddb316d2ebf30f3cc6d852e19ea778375c13b7daa46e"
    genuine_adjudication = (
        "4b06f7fb805fbda6176cbc41df5feb7d1607998a6dc5dd303a357163409f5d9d"
    )
    contamination_adjudication = (
        "d897dffb0aa7cb874c3abb723e9ae95d8ff1ceebdf2a8fb61b9badff958db8bf"
    )
    withdrawn_adjudication = (
        "970cbf5d97c646df84ec31715de710707646f428658dc20ad9fd2fcf162a2777"
    )
    candidate_manifest_id = "e" * 64
    candidate_manifest_sha256 = "f" * 64
    registry_release_available_session = "2026-07-16"
    cutoff_session = "2026-07-16"
    cases = {
        "confirmed_genuine_transition": {
            "active_on_date": True,
            "observed_composite_figi": "BBG00000000B",
            "observed_share_class_figi": "BBG0000000SB",
            "observed_cik_normalized": "0000000002",
            "canonical_composite_figi": "BBG00000000B",
            "canonical_share_class_figi": "BBG0000000SB",
            "canonical_cik_normalized": "0000000002",
            "asset_id": canonical_b,
            "identity_resolution_status": "resolved_strong",
            "identity_resolution_method": "approved_genuine_transition",
            "identity_disposition": "confirmed_genuine_transition",
            "identity_case_id": case_id,
            "identity_case_available_session": "2026-07-14",
            "source_identity_case_candidate_manifest_id": candidate_manifest_id,
            "source_identity_case_candidate_manifest_sha256": candidate_manifest_sha256,
            "identity_adjudication_id": genuine_adjudication,
            "adjudication_available_session": "2026-07-15",
            "source_identity_adjudication_release_available_session": (
                registry_release_available_session
            ),
            "identity_resolution_cutoff_session": cutoff_session,
            "backtest_identity_eligible": True,
            "ticker_alias_id": stable_digest({"scenario": "genuine"}),
            "position_continuity_status": "resolved_identity",
            "identity_quality_liquidation_signal": False,
        },
        "confirmed_provider_contamination": {
            "active_on_date": True,
            "observed_composite_figi": "BBG00000000B",
            "observed_share_class_figi": "BBG0000000SB",
            "observed_cik_normalized": "0000000002",
            "canonical_composite_figi": "BBG00000000A",
            "canonical_share_class_figi": "BBG0000000SA",
            "canonical_cik_normalized": "0000000001",
            "asset_id": canonical_a,
            "identity_resolution_status": "resolved_approved_override",
            "identity_resolution_method": "approved_provider_contamination_override",
            "identity_disposition": "confirmed_provider_contamination",
            "identity_case_id": case_id,
            "identity_case_available_session": "2026-07-14",
            "source_identity_case_candidate_manifest_id": candidate_manifest_id,
            "source_identity_case_candidate_manifest_sha256": candidate_manifest_sha256,
            "identity_adjudication_id": contamination_adjudication,
            "adjudication_available_session": "2026-07-15",
            "source_identity_adjudication_release_available_session": (
                registry_release_available_session
            ),
            "identity_resolution_cutoff_session": cutoff_session,
            "backtest_identity_eligible": True,
            "ticker_alias_id": stable_digest({"scenario": "contamination"}),
            "position_continuity_status": "resolved_identity",
            "identity_quality_liquidation_signal": False,
        },
        "pending_unresolved": {
            "active_on_date": True,
            "observed_composite_figi": "BBG00000000B",
            "observed_share_class_figi": "BBG0000000SB",
            "observed_cik_normalized": "0000000002",
            "canonical_composite_figi": None,
            "canonical_share_class_figi": None,
            "canonical_cik_normalized": None,
            "asset_id": None,
            "identity_resolution_status": "unresolved",
            "identity_resolution_method": "provider_figi_bounce_pending_unresolved",
            "identity_disposition": "pending_unresolved",
            "identity_case_id": case_id,
            "identity_case_available_session": "2026-07-14",
            "source_identity_case_candidate_manifest_id": candidate_manifest_id,
            "source_identity_case_candidate_manifest_sha256": candidate_manifest_sha256,
            "identity_adjudication_id": None,
            "adjudication_available_session": None,
            "source_identity_adjudication_release_available_session": (
                registry_release_available_session
            ),
            "identity_resolution_cutoff_session": cutoff_session,
            "backtest_identity_eligible": False,
            "ticker_alias_id": None,
            "position_continuity_status": (
                "identity_uncertain_no_new_trade_no_forced_exit_run_incomplete"
            ),
            "identity_quality_liquidation_signal": False,
        },
        "adjudicated_unresolved_withdrawal": {
            "active_on_date": True,
            "observed_composite_figi": "BBG00000000B",
            "observed_share_class_figi": "BBG0000000SB",
            "observed_cik_normalized": "0000000002",
            "canonical_composite_figi": None,
            "canonical_share_class_figi": None,
            "canonical_cik_normalized": None,
            "asset_id": None,
            "identity_resolution_status": "unresolved",
            "identity_resolution_method": "provider_figi_bounce_adjudicated_unresolved",
            "identity_disposition": "adjudicated_unresolved",
            "identity_case_id": case_id,
            "identity_case_available_session": "2026-07-14",
            "source_identity_case_candidate_manifest_id": candidate_manifest_id,
            "source_identity_case_candidate_manifest_sha256": candidate_manifest_sha256,
            "identity_adjudication_id": withdrawn_adjudication,
            "adjudication_available_session": "2026-07-18",
            "source_identity_adjudication_release_available_session": "2026-07-20",
            "identity_resolution_cutoff_session": "2026-07-20",
            "backtest_identity_eligible": False,
            "ticker_alias_id": None,
            "position_continuity_status": (
                "identity_uncertain_no_new_trade_no_forced_exit_run_incomplete"
            ),
            "identity_quality_liquidation_signal": False,
        },
        "confirmed_but_relationship_conflicted": {
            "active_on_date": True,
            "observed_composite_figi": "BBG00000000B",
            "observed_share_class_figi": "BBG0000000SB",
            "observed_cik_normalized": "0000000002",
            "canonical_composite_figi": "BBG00000000A",
            "canonical_share_class_figi": None,
            "canonical_cik_normalized": None,
            "asset_id": canonical_a,
            "identity_resolution_status": "resolved_conflicted",
            "identity_resolution_method": "approved_provider_contamination_override",
            "identity_disposition": "confirmed_provider_contamination",
            "identity_case_id": case_id,
            "identity_case_available_session": "2026-07-14",
            "source_identity_case_candidate_manifest_id": candidate_manifest_id,
            "source_identity_case_candidate_manifest_sha256": candidate_manifest_sha256,
            "identity_adjudication_id": contamination_adjudication,
            "adjudication_available_session": "2026-07-15",
            "source_identity_adjudication_release_available_session": (
                registry_release_available_session
            ),
            "identity_resolution_cutoff_session": cutoff_session,
            "backtest_identity_eligible": False,
            "ticker_alias_id": None,
            "position_continuity_status": (
                "identity_uncertain_no_new_trade_no_forced_exit_run_incomplete"
            ),
            "identity_quality_liquidation_signal": False,
        },
    }

    genuine = cases["confirmed_genuine_transition"]
    assert genuine["observed_composite_figi"] == genuine["canonical_composite_figi"]
    assert genuine["asset_id"] == canonical_b
    assert genuine["identity_adjudication_id"] == genuine_adjudication
    assert genuine["identity_resolution_method"] == "approved_genuine_transition"
    assert genuine["ticker_alias_id"] is not None

    contamination = cases["confirmed_provider_contamination"]
    assert contamination["observed_composite_figi"] != contamination[
        "canonical_composite_figi"
    ]
    assert contamination["observed_share_class_figi"] != contamination[
        "canonical_share_class_figi"
    ]
    assert contamination["observed_cik_normalized"] != contamination[
        "canonical_cik_normalized"
    ]
    assert contamination["asset_id"] == canonical_a
    assert contamination["identity_adjudication_id"] == contamination_adjudication
    assert contamination["ticker_alias_id"] is not None

    unresolved = cases["pending_unresolved"]
    assert unresolved["active_on_date"] is True
    assert unresolved["observed_composite_figi"] == "BBG00000000B"
    assert unresolved["canonical_composite_figi"] is None
    assert unresolved["asset_id"] is None
    assert unresolved["identity_adjudication_id"] is None
    assert unresolved["backtest_identity_eligible"] is False
    assert unresolved["ticker_alias_id"] is None
    assert unresolved["identity_quality_liquidation_signal"] is False
    assert "no_forced_exit" in unresolved["position_continuity_status"]

    withdrawn = cases["adjudicated_unresolved_withdrawal"]
    assert withdrawn["identity_adjudication_id"] == withdrawn_adjudication
    assert withdrawn["identity_disposition"] == "adjudicated_unresolved"
    assert withdrawn["asset_id"] is None
    assert withdrawn["ticker_alias_id"] is None

    conflicted = cases["confirmed_but_relationship_conflicted"]
    assert conflicted["identity_disposition"] == "confirmed_provider_contamination"
    assert conflicted["identity_resolution_status"] == "resolved_conflicted"
    assert conflicted["asset_id"] == canonical_a
    assert conflicted["ticker_alias_id"] is None
    assert conflicted["backtest_identity_eligible"] is False

    # Detection and registry publication are both cutoff-bound. Before the right-side A
    # makes the case available, B follows ordinary direct-observation rules; after the
    # case is known but before a published decision is available, it fails closed.
    cutoff_vectors = {
        "before_case_available": {
            "cutoff": "2026-07-13",
            "case_available": "2026-07-14",
            "disposition": "observed_consistent",
            "eligible_if_other_gates_pass": True,
        },
        "case_known_registry_not_available": {
            "cutoff": "2026-07-15",
            "case_available": "2026-07-14",
            "adjudication_available": "2026-07-15",
            "registry_release_available": "2026-07-16",
            "disposition": "pending_unresolved",
            "eligible_if_other_gates_pass": False,
        },
        "decision_effective": {
            "cutoff": "2026-07-16",
            "case_available": "2026-07-14",
            "adjudication_available": "2026-07-15",
            "registry_release_available": "2026-07-16",
            "disposition": "confirmed_provider_contamination",
            "eligible_if_other_gates_pass": True,
        },
    }
    assert cutoff_vectors["before_case_available"]["cutoff"] < cutoff_vectors[
        "before_case_available"
    ]["case_available"]
    assert cutoff_vectors["case_known_registry_not_available"]["cutoff"] < (
        cutoff_vectors["case_known_registry_not_available"][
            "registry_release_available"
        ]
    )
    assert cutoff_vectors["case_known_registry_not_available"][
        "eligible_if_other_gates_pass"
    ] is False
    assert cutoff_vectors["decision_effective"]["cutoff"] >= cutoff_vectors[
        "decision_effective"
    ]["registry_release_available"]

    assert all(case["active_on_date"] is True for case in cases.values())
    assert all(
        case["identity_quality_liquidation_signal"] is False
        for case in cases.values()
    )
    assert genuine_adjudication != contamination_adjudication
