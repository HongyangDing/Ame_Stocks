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
PROFILE_SHA256 = "02678e174d70d2801152a4fed67c2e6579f32ed0a2d3922cfc63651df4851545"

CANDIDATE_PATHS = {
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
    "asset_master": "d7a6ef66f72c1048b6556b57910af3afbea4926661f3c6062708937fbc2b4ba6",
    "ticker_alias": "e645573e813c18a82fcea80f3bfef07c547c738fc00cc01419a1f1824a27a47b",
    "issuer_master": "33c146bab2a9aed61a44d8c20e9b301fd1aa116deb5185214df92a4ee69f632d",
    "universe_daily": "915a389ccfa9d8442cd2b7b2a14f782adf68f2bb5c8635ddf90224102750e319",
}
EXPECTED_SCHEMA_DIGESTS = {
    "asset_master": "83415d165ed166cea75fc9103c0ce062bac893bb725cea478759bdf049f138ff",
    "ticker_alias": "e5b021c0e1ae2e956b815b41a1f8ecd8ad9762daecbd6b5d9088654770143793",
    "issuer_master": "0b308f96f3277385e40faf1f42a3aa420df1dd7d9dd85f7a932036fec8527f6e",
    "universe_daily": "c6133821e404b3a35ed7b460035796ffee6a1d69c456c8f380076fa6e9cd2329",
}
EXPECTED_FILE_SHA256 = {
    "asset_master": "ef8b6a9160a20a9f9d7313e978c7588ee26db97fe994139399511d532cf2cce4",
    "ticker_alias": "5da4cedd48bc83d39225e2facaf1c2f05ef5655f3ec2480e5d42067f8daba77f",
    "issuer_master": "4f637d92bf89fb685577be013809649c82967400228648c54272d785ab3d5e6a",
    "universe_daily": "12cf84371b99e23c10b4578760bf8ee6b64a139505bfacf0e6ac521325ca8a84",
}

EXPECTED_SIX_UPSTREAM = (
    "asset_observation_daily",
    "asset_observation_version",
    "universe_source_daily",
    "ticker_event_request_status",
    "ticker_change_event",
    "ticker_overview_safe",
)
EXPECTED_BINDING_COLUMNS = {
    "source_s4_release_set_id",
    "source_s5_status_release_id",
    "source_s5_event_release_id",
    "source_s6_overview_release_id",
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
        "f788483993e6c4536eb15acece4a90ddd4e8e86005763bfe8ad43d84ac7ec3af"
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
        "asset_master": ("identity", "asset_master", 1),
        "ticker_alias": ("identity", "ticker_alias", 1),
        "issuer_master": ("identity", "issuer_master", 1),
        "universe_daily": ("reference", "universe_daily", 1),
    }
    assert {
        name: (len(contract.columns), len(contract.qa_rules))
        for name, contract in contracts.items()
    } == {
        "asset_master": (26, 26),
        "ticker_alias": (29, 33),
        "issuer_master": (24, 27),
        "universe_daily": (32, 33),
    }
    assert all(
        contract.source_datasets == EXPECTED_SIX_UPSTREAM
        for contract in contracts.values()
    )
    assert all(
        {column.name for column in contract.columns if not column.nullable}
        >= EXPECTED_BINDING_COLUMNS
        for contract in contracts.values()
    )
    assert all(
        {"asset_master", "ticker_alias", "issuer_master", "universe_daily"}.isdisjoint(
            contract.source_datasets
        )
        for contract in contracts.values()
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

    alias_payload = {
        "namespace": "ame_stocks.identity.ticker_alias",
        "rule_version": "ame_stocks_ticker_alias_id_from_observed_interval_v1",
        "asset_id": asset_id,
        "ticker": "AAPL",
        "valid_from_session": "2024-01-02",
        "share_class_id": share_class_id,
        "issuer_id": issuer_id,
    }
    assert stable_digest(alias_payload) == (
        "ff8708591441fc3a86ed609d1e025b78f392b9a9f415f268573f2f44224f34f1"
    )
    alias_payload["share_class_id"] = None
    alias_payload["issuer_id"] = None
    assert stable_digest(alias_payload) == (
        "47dbba345b3eabb922f9a5f4cf51448aae8e4cce2f711464576dd51d9222612c"
    )


def test_asset_master_uses_composite_asset_and_share_class_parent_layers() -> None:
    contract = load_contract("asset_master")
    columns = {column.name: column for column in contract.columns}
    rules = {rule.check_id: rule for rule in contract.qa_rules}

    assert contract.primary_key == ("asset_id",)
    assert contract.partition_by == ()
    assert "canonical_composite_figi" in columns
    assert columns["canonical_composite_figi"].nullable is False
    assert columns["share_class_id_rule_version"].nullable is False
    assert columns["share_class_id"].nullable is True
    assert columns["canonical_share_class_figi"].nullable is True
    assert "U.S. tradable-security" in contract.grain
    assert "only v1 permanent" in columns["canonical_composite_figi"].description
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
    assert "exactly one accepted v1 asset row" in rules[
        "row_funnel_unreconciled"
    ].description
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
    assert "transitive accepted-source evidence closure" in rules[
        "identity_evidence_availability_recomputation_mismatch_rows"
    ].description
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
    assert columns["composite_figi"].nullable is False
    assert columns["share_class_id"].nullable is True
    assert "maximal XNYS-consecutive interval" in contract.grain
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
    assert "transitive accepted-source evidence closure" in rules[
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
    assert "never an asset identity key" in contract.description
    assert rules["asset_id_derived_from_cik_rows"].severity is QASeverity.CRITICAL
    assert columns["source_s5_status_release_id"].nullable is False
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
    assert "exactly one v1 issuer row" in rules["row_funnel_unreconciled"].description
    assert rules["s5_event_created_issuer_rows"].severity is QASeverity.CRITICAL
    assert "No S5-only event evidence" in rules[
        "s5_event_created_issuer_rows"
    ].description
    assert rules["unattached_s5_event_issuer_rows"].severity is QASeverity.HIGH
    assert rules["unattached_s5_event_issuer_rows"].failure_status is QAStatus.WARNING
    assert "no approved supersession source" in rules["supersession_invalid_rows"].description
    assert "ticker_event_request_status" in rules[
        "evidence_count_recomputation_mismatch_rows"
    ].description
    assert "transitive accepted-source evidence closure" in rules[
        "reference_availability_recomputation_mismatch_rows"
    ].description


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
    assert "resolved_strong rows" in rules["resolved_row_incomplete_rows"].description
    assert rules["active_common_stock_without_eligible_identity_rows"].severity is (
        QASeverity.HIGH
    )
    assert rules["s6_pending_quarantine_unresolved_rows"].severity is QASeverity.HIGH
    assert rules["s5_event_created_resolution_rows"].severity is QASeverity.CRITICAL
    assert "No universe row may gain or change" in rules[
        "s5_event_created_resolution_rows"
    ].description
    assert "transitive accepted-source evidence closure" in rules[
        "identity_evidence_availability_recomputation_mismatch_rows"
    ].description
