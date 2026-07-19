from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pyarrow as pa

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import ArrowType, QASeverity, QAStatus, TableContract
from ame_stocks_api.silver.identity_exact_group_history_contract import (
    EXACT_GROUP_HISTORY_CAPABILITIES,
    EXACT_GROUP_HISTORY_END_SESSION,
    EXACT_GROUP_HISTORY_FIXED_COMPOSITES,
    EXACT_GROUP_HISTORY_FIXED_GROUPS,
    EXACT_GROUP_HISTORY_FIXED_REVIEW_GROUP_IDS,
    EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST,
    EXACT_GROUP_HISTORY_FIXED_TICKERS,
    EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE,
    EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST,
    EXACT_GROUP_HISTORY_PHYSICAL_SOURCE_TABLES,
    EXACT_GROUP_HISTORY_PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION,
    EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE,
    EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID,
    EXACT_GROUP_HISTORY_S4_RELEASE_SET_MANIFEST_SHA256,
    EXACT_GROUP_HISTORY_S4_SOURCE_ARTIFACT_COUNT,
    EXACT_GROUP_HISTORY_S4_SOURCE_BYTES,
    EXACT_GROUP_HISTORY_S4_SOURCE_ROW_COUNT,
    EXACT_GROUP_HISTORY_START_SESSION,
    EXACT_GROUP_HISTORY_XNYS_SESSION_COUNT,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST,
    exact_group_history_fixed_scope,
    exact_group_history_observed_run_semantics,
    exact_group_history_review_group_id,
)
from ame_stocks_api.silver.identity_provider_evidence import (
    PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION,
)

_ROOT = Path(__file__).resolve().parents[1]
_CANDIDATE = (
    _ROOT / "docs/silver/contracts/identity/"
    "identity_exact_group_history_review_slot.schema-v1.candidate.json"
)
_RESOURCE = (
    _ROOT / "backend/ame_stocks_api/silver/schema_resources/"
    "identity_exact_group_history_review_slot.schema-v1.json"
)


def test_candidate_and_resource_are_byte_identical_content_addressed_contracts() -> None:
    candidate = _CANDIDATE.read_bytes()
    resource = _RESOURCE.read_bytes()

    assert candidate == resource
    assert hashlib.sha256(resource).hexdigest() == (
        IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256
    )
    parsed = TableContract.from_dict(json.loads(resource))
    assert parsed == IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT
    assert parsed.contract_id == IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID
    assert parsed.schema_digest == IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST
    assert IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID == (
        "cdf406e869c06c2942588a043f6e50dd429f1d6a8818d05e4d01a75fb8a92765"
    )
    assert IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST == (
        "3ba74162c4903cef843496acc49d47198b1cc09f0206158b0ae065da38415400"
    )
    assert IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256 == (
        "ae957aeb2b61e7970eadcf2e963b7ae48ff2be6f4582901f1b9d26c7ff31b80c"
    )
    assert IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST == (
        "837d03c92707590d505a5ea683760eb1448073a213abf07af4a6501ad263ce49"
    )


def test_contract_freezes_exact_review_only_slot_shape() -> None:
    contract = IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT
    names = tuple(column.name for column in contract.columns)

    assert (contract.domain, contract.table, contract.schema_version) == (
        "identity",
        "identity_exact_group_history_review_slot",
        1,
    )
    assert contract.primary_key == ("review_group_id", "session_date")
    assert contract.partition_by == ()
    assert contract.sort_by == ("ticker", "session_date")
    assert contract.source_datasets == (
        "composite_figi_inventory",
        "identity_directional_raw_preview_slot",
        "asset_observation_daily",
        "universe_source_daily",
    )
    assert len(names) == 62
    assert names == (
        "review_group_id",
        "review_scope_set_id",
        "provider_id",
        "provider_market",
        "provider_locale",
        "ticker",
        "exact_group_observed_composite_figi",
        "s4_release_set_id",
        "inventory_completion_id",
        "directional_preview_candidate_id",
        "directional_preview_completion_id",
        "session_date",
        "previous_observed_session",
        "previous_observed_session_is_adjacent_xnys",
        "exact_observed_run_id",
        "exact_observed_run_ordinal",
        "observed_session_ordinal_in_run",
        "exact_observed_run_start_session",
        "exact_observed_run_end_session",
        "exact_observed_run_session_count",
        "group_first_observed_session",
        "group_last_observed_session",
        "group_observed_session_count",
        "group_exact_observed_run_count",
        "exact_asset_observation_match_count",
        "exact_asset_observation_attestation_ids_json",
        "exact_group_observed_share_class_figis_json",
        "exact_group_observed_ciks_json",
        "exact_group_observed_primary_exchange_mics_json",
        "exact_group_observed_type_codes_json",
        "exact_group_provider_active_values_json",
        "nonselected_exact_group_asset_observation_count",
        "same_session_exact_group_identity_variant_count",
        "universe_membership_count",
        "membership_status",
        "active_on_date",
        "universe_row_attestation_id",
        "selected_source_record_id",
        "source_version_count",
        "version_group_id",
        "selection_status",
        "selected_parent_asset_match_count",
        "selected_parent_attestation_id",
        "selected_parent_projection_match",
        "selected_parent_matches_exact_group",
        "selected_parent_observed_composite_figi",
        "selected_parent_observed_share_class_figi",
        "selected_parent_observed_cik",
        "selected_parent_observed_primary_exchange_mic",
        "selected_parent_observed_type_code",
        "selected_parent_source_available_session",
        "exact_group_evidence_manifest_id",
        "exact_group_evidence_manifest_path",
        "exact_group_evidence_manifest_sha256",
        "observed_interval_state",
        "registry_evaluation_state",
        "adjudication_eligible",
        "canonical_candidate_eligible",
        "transition_candidate_eligible",
        "exact_override_interval_eligible",
        "full_run_eligible",
        "publication_eligible",
    )
    by_name = {column.name: column for column in contract.columns}
    for name in (
        "exact_asset_observation_attestation_ids_json",
        "exact_group_observed_share_class_figis_json",
        "exact_group_observed_ciks_json",
        "exact_group_observed_primary_exchange_mics_json",
        "exact_group_observed_type_codes_json",
        "exact_group_provider_active_values_json",
    ):
        assert by_name[name].arrow_type is ArrowType.JSON_STRING
        assert not by_name[name].nullable
    assert by_name["session_date"].arrow_type is ArrowType.DATE32
    assert by_name["active_on_date"].arrow_type is ArrowType.BOOLEAN
    assert by_name["previous_observed_session"].nullable
    assert by_name["selected_parent_matches_exact_group"].nullable
    assert all(
        not by_name[name].nullable
        for name in (
            "observed_interval_state",
            "registry_evaluation_state",
            "adjudication_eligible",
            "canonical_candidate_eligible",
            "transition_candidate_eligible",
            "exact_override_interval_eligible",
            "full_run_eligible",
            "publication_eligible",
        )
    )


def test_scope_is_exactly_three_groups_over_the_complete_frozen_s4_release() -> None:
    assert EXACT_GROUP_HISTORY_FIXED_GROUPS == (
        ("SOR", "BBG000KMY6N2"),
        ("XZO", "BBG01XL8FHT0"),
        ("ANABV", "BBG021DMXXT2"),
    )
    assert EXACT_GROUP_HISTORY_FIXED_TICKERS == ("SOR", "XZO", "ANABV")
    assert dict(EXACT_GROUP_HISTORY_FIXED_COMPOSITES) == dict(EXACT_GROUP_HISTORY_FIXED_GROUPS)
    assert dict(EXACT_GROUP_HISTORY_FIXED_REVIEW_GROUP_IDS) == {
        "SOR": "844d92c0d58dabe60608cc2b37e6c69ea007308a4dc69fca07c9c86756a66335",
        "XZO": "31611a44b1102e0622c5ee9d720da591987b44f82fe9e35e7cb3cc14b68c770e",
        "ANABV": "20b28d71fcce26779c50d1d17cd6472fbbd9406b750f4d8e83d09a06c0887718",
    }
    assert (
        date(2016, 7, 11),
        date(2026, 7, 9),
    ) == (EXACT_GROUP_HISTORY_START_SESSION, EXACT_GROUP_HISTORY_END_SESSION)
    assert EXACT_GROUP_HISTORY_XNYS_SESSION_COUNT == 2_513
    assert EXACT_GROUP_HISTORY_PHYSICAL_SOURCE_TABLES == (
        "asset_observation_daily",
        "universe_source_daily",
    )
    assert EXACT_GROUP_HISTORY_S4_SOURCE_ARTIFACT_COUNT == 5_026
    assert EXACT_GROUP_HISTORY_S4_SOURCE_ROW_COUNT == 138_757_511
    assert EXACT_GROUP_HISTORY_S4_SOURCE_BYTES == 15_910_278_169
    assert EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID == (
        "f81c7ee28939db3350fce809326723e911b6d486c6db166d2575fcc92cb2101d"
    )
    assert EXACT_GROUP_HISTORY_S4_RELEASE_SET_MANIFEST_SHA256 == (
        "937eaf4ed502fb2786dafb0dce9ec613bcaccb2cd488812cc5900118238d6c13"
    )
    scope = exact_group_history_fixed_scope()
    assert len(scope) == 3
    assert scope == exact_group_history_fixed_scope()
    assert all(
        item["source_filter_fields"]
        == [
            "provider_id",
            "provider_market",
            "provider_locale",
            "ticker",
            "observed_composite_figi",
        ]
        for item in scope
    )
    assert not any(
        "share_class" in field for item in scope for field in item["source_filter_fields"]
    )
    assert EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST == (
        "b2c88ba3ce02ae0618206da35cf535c03b3dfdbca67edd0474a96165cbae28f2"
    )


def test_observed_run_semantics_and_capability_boundary_are_fail_closed() -> None:
    semantics = exact_group_history_observed_run_semantics()
    contract_names = {
        column.name for column in IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.columns
    }
    forbidden_outputs = {
        "asset_id",
        "canonical_composite_figi",
        "canonical_share_class_figi",
        "disposition",
        "effective_from",
        "effective_to",
        "final_tradability_eligible",
        "forced_liquidation",
        "issuer_id",
        "provider_composite_override_id",
        "share_class_adjudication_id",
        "transition_id",
    }

    assert EXACT_GROUP_HISTORY_PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION == 2
    assert EXACT_GROUP_HISTORY_PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION == (
        PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION
    )
    assert EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE == ("exact_full_release_observed_runs_only")
    assert EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE == "not_evaluated"
    assert set(EXACT_GROUP_HISTORY_CAPABILITIES.values()) == {False}
    assert forbidden_outputs.isdisjoint(contract_names)
    assert semantics == {
        "asset_evidence_rule": (
            "retain_every_exact_group_asset_observation_version_and_attestation"
        ),
        "canonical_identity_rule": "not_generated",
        "effective_interval_rule": "not_inferred_from_observed_runs",
        "membership_rule": "preserve_selected_universe_parent_without_mutation",
        "observed_interval_state": "exact_full_release_observed_runs_only",
        "output_session_rule": "only_sessions_with_exact_group_asset_evidence",
        "registry_evaluation_state": "not_evaluated",
        "run_break_rule": "break_unless_previous_observed_session_is_adjacent_xnys",
        "share_class_filter_rule": "forbidden",
        "tradability_rule": "not_evaluated_no_forced_liquidation_signal",
    }
    assert EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST == (
        "70dfc56002b731b9ddde53c0febaf5b1d75bc1b316387d3f6850ec3cb96f259e"
    )


def test_qa_surface_freezes_critical_gates_and_high_review_metrics() -> None:
    rules = {
        rule.check_id: rule for rule in IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.qa_rules
    }
    critical = (
        "schema_exact",
        "fixed_review_group_scope_invalid",
        "upstream_review_binding_invalid",
        "s4_source_binding_invalid",
        "source_artifact_integrity_invalid",
        "source_scan_count_mismatch",
        "exact_group_scope_leakage_rows",
        "exact_group_asset_omission_rows",
        "exact_group_asset_match_count_invalid_rows",
        "share_class_filter_attempt_rows",
        "duplicate_universe_membership_rows",
        "selected_parent_missing_rows",
        "selected_parent_multiple_rows",
        "selected_parent_projection_mismatch_rows",
        "provider_row_attestation_schema_invalid_rows",
        "row_attestation_replay_invalid_rows",
        "orphan_or_duplicate_attestation_rows",
        "observed_source_row_mutation_rows",
        "observed_run_segmentation_invalid_rows",
        "observed_run_metadata_mismatch_rows",
        "membership_mutation_rows",
        "identity_quality_forced_liquidation_signal_rows",
        "observed_interval_state_invalid_rows",
        "interval_boundary_inference_rows",
        "registry_resolution_attempt_rows",
        "canonical_identity_output_rows",
        "adjudication_or_transition_decision_rows",
        "tradability_decision_rows",
        "capability_true_rows",
        "primary_key_duplicate_excess",
        "output_sort_invalid",
        "output_artifact_readback_invalid",
        "resource_cap_exceeded",
    )
    high = (
        "nonselected_exact_group_asset_versions",
        "exact_group_asset_only_sessions",
        "selected_parent_other_composite_sessions",
        "same_session_exact_group_identity_variant_groups",
        "observed_share_class_change_edges",
        "observed_cik_change_edges",
        "observed_primary_exchange_mic_change_edges",
        "observed_type_code_change_edges",
        "observed_active_status_change_edges",
        "exact_observed_run_gap_edges",
        "multiple_exact_observed_run_groups",
        "release_boundary_touching_groups",
    )

    assert tuple(rules) == (*critical, *high)
    assert all(
        rules[check_id].severity is QASeverity.CRITICAL
        and rules[check_id].failure_status is QAStatus.FAILED
        for check_id in critical
    )
    assert all(
        rules[check_id].severity is QASeverity.HIGH
        and rules[check_id].failure_status is QAStatus.WARNING
        for check_id in high
    )
    assert all(rule.threshold_expression == "numerator eq 0" for rule in rules.values())


def _canonical_json(values: list[object]) -> str:
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _fixture_slot(
    *,
    ticker: str,
    composite: str,
    session: date,
    previous: date | None,
    adjacent: bool | None,
    run_ordinal: int,
    ordinal_in_run: int,
    run_start: date,
    run_end: date,
    run_count: int,
    group_first: date,
    group_last: date,
    group_count: int,
    group_run_count: int,
    asset_versions: int = 1,
    membership: str = "present_active",
    selected_parent_matches: bool | None = True,
    selected_parent_composite: str | None = None,
) -> dict[str, object]:
    group_id = exact_group_history_review_group_id(
        ticker=ticker,
        observed_composite_figi=composite,
    )
    run_id = stable_digest(
        {
            "review_group_id": group_id,
            "run_end": run_end.isoformat(),
            "run_start": run_start.isoformat(),
        }
    )
    asset_attestations = [
        stable_digest({"dataset": "asset_observation_daily", "i": index, "session": str(session)})
        for index in range(asset_versions)
    ]
    present = membership != "absent_source_membership"
    parent_composite = selected_parent_composite or composite
    return {
        "review_group_id": group_id,
        "review_scope_set_id": EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST,
        "provider_id": "massive",
        "provider_market": "stocks",
        "provider_locale": "us",
        "ticker": ticker,
        "exact_group_observed_composite_figi": composite,
        "s4_release_set_id": EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID,
        "inventory_completion_id": "1" * 64,
        "directional_preview_candidate_id": "2" * 64,
        "directional_preview_completion_id": "3" * 64,
        "session_date": session,
        "previous_observed_session": previous,
        "previous_observed_session_is_adjacent_xnys": adjacent,
        "exact_observed_run_id": run_id,
        "exact_observed_run_ordinal": run_ordinal,
        "observed_session_ordinal_in_run": ordinal_in_run,
        "exact_observed_run_start_session": run_start,
        "exact_observed_run_end_session": run_end,
        "exact_observed_run_session_count": run_count,
        "group_first_observed_session": group_first,
        "group_last_observed_session": group_last,
        "group_observed_session_count": group_count,
        "group_exact_observed_run_count": group_run_count,
        "exact_asset_observation_match_count": asset_versions,
        "exact_asset_observation_attestation_ids_json": _canonical_json(asset_attestations),
        "exact_group_observed_share_class_figis_json": _canonical_json(["BBG001FIXTURE"]),
        "exact_group_observed_ciks_json": _canonical_json(["0000000001"]),
        "exact_group_observed_primary_exchange_mics_json": _canonical_json(["XNYS"]),
        "exact_group_observed_type_codes_json": _canonical_json(["CS"]),
        "exact_group_provider_active_values_json": _canonical_json(
            [membership == "present_active"]
        ),
        "nonselected_exact_group_asset_observation_count": (
            asset_versions - 1 if present and selected_parent_matches else asset_versions
        ),
        "same_session_exact_group_identity_variant_count": asset_versions - 1,
        "universe_membership_count": int(present),
        "membership_status": membership,
        "active_on_date": membership == "present_active" if present else None,
        "universe_row_attestation_id": "4" * 64 if present else None,
        "selected_source_record_id": "5" * 64 if present else None,
        "source_version_count": asset_versions if present else None,
        "version_group_id": "6" * 64 if present and asset_versions > 1 else None,
        "selection_status": "selected_latest_available" if present else None,
        "selected_parent_asset_match_count": int(present),
        "selected_parent_attestation_id": "7" * 64 if present else None,
        "selected_parent_projection_match": True if present else None,
        "selected_parent_matches_exact_group": selected_parent_matches if present else None,
        "selected_parent_observed_composite_figi": parent_composite if present else None,
        "selected_parent_observed_share_class_figi": ("BBG001FIXTURE" if present else None),
        "selected_parent_observed_cik": "0000000001" if present else None,
        "selected_parent_observed_primary_exchange_mic": "XNYS" if present else None,
        "selected_parent_observed_type_code": "CS" if present else None,
        "selected_parent_source_available_session": session if present else None,
        "exact_group_evidence_manifest_id": stable_digest({"evidence_group": group_id}),
        "exact_group_evidence_manifest_path": (
            f"evidence/review_group_id={group_id}/manifest.json"
        ),
        "exact_group_evidence_manifest_sha256": "8" * 64,
        "observed_interval_state": "exact_full_release_observed_runs_only",
        "registry_evaluation_state": "not_evaluated",
        "adjudication_eligible": False,
        "canonical_candidate_eligible": False,
        "transition_candidate_eligible": False,
        "exact_override_interval_eligible": False,
        "full_run_eligible": False,
        "publication_eligible": False,
    }


def _in_memory_observed_history_fixture() -> tuple[dict[str, object], ...]:
    sor_first = date(2024, 12, 31)
    sor_second = date(2025, 1, 2)
    sor_gap = date(2025, 1, 6)
    return (
        _fixture_slot(
            ticker="SOR",
            composite="BBG000KMY6N2",
            session=sor_first,
            previous=None,
            adjacent=None,
            run_ordinal=0,
            ordinal_in_run=0,
            run_start=sor_first,
            run_end=sor_second,
            run_count=2,
            group_first=sor_first,
            group_last=sor_gap,
            group_count=3,
            group_run_count=2,
        ),
        _fixture_slot(
            ticker="SOR",
            composite="BBG000KMY6N2",
            session=sor_second,
            previous=sor_first,
            adjacent=True,
            run_ordinal=0,
            ordinal_in_run=1,
            run_start=sor_first,
            run_end=sor_second,
            run_count=2,
            group_first=sor_first,
            group_last=sor_gap,
            group_count=3,
            group_run_count=2,
            asset_versions=2,
        ),
        _fixture_slot(
            ticker="SOR",
            composite="BBG000KMY6N2",
            session=sor_gap,
            previous=sor_second,
            adjacent=False,
            run_ordinal=1,
            ordinal_in_run=0,
            run_start=sor_gap,
            run_end=sor_gap,
            run_count=1,
            group_first=sor_first,
            group_last=sor_gap,
            group_count=3,
            group_run_count=2,
            membership="absent_source_membership",
            selected_parent_matches=None,
        ),
        _fixture_slot(
            ticker="XZO",
            composite="BBG01XL8FHT0",
            session=date(2025, 11, 4),
            previous=None,
            adjacent=None,
            run_ordinal=0,
            ordinal_in_run=0,
            run_start=date(2025, 11, 4),
            run_end=date(2025, 11, 4),
            run_count=1,
            group_first=date(2025, 11, 4),
            group_last=date(2025, 11, 4),
            group_count=1,
            group_run_count=1,
            selected_parent_matches=False,
            selected_parent_composite="BBG01OTHER00",
        ),
        _fixture_slot(
            ticker="ANABV",
            composite="BBG021DMXXT2",
            session=date(2026, 4, 20),
            previous=None,
            adjacent=None,
            run_ordinal=0,
            ordinal_in_run=0,
            run_start=date(2026, 4, 20),
            run_end=date(2026, 4, 20),
            run_count=1,
            group_first=date(2026, 4, 20),
            group_last=date(2026, 4, 20),
            group_count=1,
            group_run_count=1,
            membership="present_inactive",
        ),
    )


def test_pure_memory_fixture_preserves_versions_parent_reconciliation_and_runs() -> None:
    rows = _in_memory_observed_history_fixture()
    table = pa.Table.from_pylist(
        [dict(row) for row in rows],
        schema=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.arrow_schema,
    )
    assert table.num_rows == 5

    duplicate_version = rows[1]
    assert duplicate_version["exact_asset_observation_match_count"] == 2
    assert (
        len(json.loads(str(duplicate_version["exact_asset_observation_attestation_ids_json"]))) == 2
    )
    assert duplicate_version["nonselected_exact_group_asset_observation_count"] == 1

    gap = rows[2]
    assert gap["previous_observed_session_is_adjacent_xnys"] is False
    assert gap["exact_observed_run_ordinal"] == 1
    assert gap["observed_session_ordinal_in_run"] == 0
    assert gap["membership_status"] == "absent_source_membership"
    assert gap["active_on_date"] is None
    assert gap["exact_asset_observation_match_count"] == 1

    other_parent = rows[3]
    assert other_parent["selected_parent_matches_exact_group"] is False
    assert other_parent["selected_parent_observed_composite_figi"] == "BBG01OTHER00"
    assert other_parent["exact_group_observed_composite_figi"] == "BBG01XL8FHT0"

    inactive = rows[4]
    assert inactive["membership_status"] == "present_inactive"
    assert inactive["active_on_date"] is False
    assert all(
        row["observed_interval_state"] == EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE
        for row in rows
    )
    assert all(row["registry_evaluation_state"] == "not_evaluated" for row in rows)
    assert all(
        row[field] is False
        for row in rows
        for field in (
            "adjudication_eligible",
            "canonical_candidate_eligible",
            "transition_candidate_eligible",
            "exact_override_interval_eligible",
            "full_run_eligible",
            "publication_eligible",
        )
    )
    assert not {
        "effective_from",
        "effective_to",
        "canonical_composite_figi",
        "canonical_share_class_figi",
        "final_tradability_eligible",
    } & set().union(*(set(row) for row in rows))
