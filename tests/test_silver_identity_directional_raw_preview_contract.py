from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

from ame_stocks_api.silver.contracts import ArrowType, QASeverity, QAStatus, TableContract
from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
    DIRECTIONAL_RAW_PREVIEW_CAPABILITIES,
    DIRECTIONAL_RAW_PREVIEW_COMPOSITE_CORRECTION_REGISTRIES,
    DIRECTIONAL_RAW_PREVIEW_EXPECTED_PHYSICAL_ARTIFACT_COUNT,
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_ANCHORS,
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS,
    DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
    DIRECTIONAL_RAW_PREVIEW_FIXED_SCOPE_DIGEST,
    DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT,
    DIRECTIONAL_RAW_PREVIEW_FIXED_TICKERS,
    DIRECTIONAL_RAW_PREVIEW_INTERVAL_INFERENCE_STATE,
    DIRECTIONAL_RAW_PREVIEW_PHYSICAL_SOURCE_TABLES,
    DIRECTIONAL_RAW_PREVIEW_PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION,
    DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE,
    DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST,
    DIRECTIONAL_RAW_PREVIEW_RELATION_ONLY_REGISTRY,
    DIRECTIONAL_RAW_PREVIEW_SHARE_CLASS_REGISTRY,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_QA_SEMANTICS_DIGEST,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST,
    directional_raw_preview_fixed_scope,
    directional_raw_preview_registry_exclusivity_semantics,
)
from ame_stocks_api.silver.identity_provider_evidence import (
    PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION,
)

_ROOT = Path(__file__).resolve().parents[1]
_CANDIDATE = (
    _ROOT / "docs/silver/contracts/identity/"
    "identity_directional_raw_preview_slot.schema-v1.candidate.json"
)
_RESOURCE = (
    _ROOT / "backend/ame_stocks_api/silver/schema_resources/"
    "identity_directional_raw_preview_slot.schema-v1.json"
)


def test_candidate_and_resource_are_byte_identical_content_addressed_contracts() -> None:
    candidate = _CANDIDATE.read_bytes()
    resource = _RESOURCE.read_bytes()

    assert candidate == resource
    assert hashlib.sha256(resource).hexdigest() == (
        IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256
    )
    parsed = TableContract.from_dict(json.loads(resource))
    assert parsed == IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT
    assert parsed.contract_id == IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID
    assert parsed.schema_digest == IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST
    assert IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_QA_SEMANTICS_DIGEST == (
        "73aa1e615f5094cb1923e35083cb58536c5f43a5c1ebf1c524d513beaa32ff44"
    )


def test_contract_freezes_review_only_slot_shape() -> None:
    contract = IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT
    names = tuple(column.name for column in contract.columns)

    assert (contract.domain, contract.table, contract.schema_version) == (
        "identity",
        "identity_directional_raw_preview_slot",
        1,
    )
    assert contract.primary_key == ("review_case_id", "session_date")
    assert contract.partition_by == ()
    assert contract.sort_by == ("ticker", "session_date")
    assert contract.source_datasets == (
        "composite_figi_inventory",
        "asset_observation_daily",
        "universe_source_daily",
    )
    assert len(names) == 42
    assert names == (
        "review_case_id",
        "review_scope_set_id",
        "provider_id",
        "provider_market",
        "provider_locale",
        "ticker",
        "inventory_anchor_composite_figi",
        "session_date",
        "session_sequence_ordinal",
        "previous_requested_session",
        "previous_session_is_adjacent_xnys",
        "universe_membership_count",
        "membership_status",
        "active_on_date",
        "universe_row_attestation_id",
        "selected_source_record_id",
        "source_version_count",
        "version_group_id",
        "selection_status",
        "observed_composite_figi",
        "observed_share_class_figi",
        "observed_cik",
        "observed_market",
        "observed_locale",
        "observed_primary_exchange_mic",
        "observed_type_code",
        "universe_source_available_session",
        "asset_observation_match_count",
        "selected_asset_parent_match_count",
        "selected_asset_parent_attestation_id",
        "nonselected_asset_observation_count",
        "asset_observation_attestation_ids_json",
        "selected_parent_projection_match",
        "case_evidence_manifest_id",
        "case_evidence_manifest_path",
        "case_evidence_manifest_sha256",
        "interval_inference_state",
        "registry_evaluation_state",
        "adjudication_eligible",
        "canonical_candidate_eligible",
        "full_run_eligible",
        "publication_eligible",
    )
    by_name = {column.name: column for column in contract.columns}
    assert by_name["asset_observation_attestation_ids_json"].arrow_type is (ArrowType.JSON_STRING)
    assert by_name["session_date"].arrow_type is ArrowType.DATE32
    assert by_name["active_on_date"].arrow_type is ArrowType.BOOLEAN
    assert not by_name["review_case_id"].nullable
    assert not by_name["session_date"].nullable
    assert by_name["observed_composite_figi"].nullable
    assert by_name["selected_parent_projection_match"].nullable
    assert all(
        not by_name[name].nullable
        for name in (
            "interval_inference_state",
            "registry_evaluation_state",
            "adjudication_eligible",
            "canonical_candidate_eligible",
            "full_run_eligible",
            "publication_eligible",
        )
    )


def test_fixed_scope_is_exactly_three_cases_and_eleven_pairs_not_a_cross_product() -> None:
    expected = (
        (
            "SOR",
            (date(2024, 12, 31), date(2025, 1, 2), date(2025, 1, 3)),
        ),
        (
            "XZO",
            (
                date(2025, 11, 4),
                date(2025, 11, 5),
                date(2025, 11, 6),
                date(2025, 11, 7),
            ),
        ),
        (
            "ANABV",
            (
                date(2026, 4, 6),
                date(2026, 4, 7),
                date(2026, 4, 17),
                date(2026, 4, 20),
            ),
        ),
    )
    pairs = {
        (ticker, session)
        for ticker, sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
        for session in sessions
    }
    sessions = {session for _, session in pairs}

    assert expected == DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
    assert DIRECTIONAL_RAW_PREVIEW_FIXED_TICKERS == ("SOR", "XZO", "ANABV")
    assert dict(DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_ANCHORS) == {
        "SOR": "BBG000KMY6N2",
        "XZO": "BBG01XL8FHT0",
        "ANABV": "BBG021DMXXT2",
    }
    assert len(pairs) == DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT == 11
    assert len(sessions) == DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT == 11
    assert len(DIRECTIONAL_RAW_PREVIEW_FIXED_TICKERS) * len(sessions) == 33
    assert DIRECTIONAL_RAW_PREVIEW_EXPECTED_PHYSICAL_ARTIFACT_COUNT == 22
    assert DIRECTIONAL_RAW_PREVIEW_PHYSICAL_SOURCE_TABLES == (
        "asset_observation_daily",
        "universe_source_daily",
    )
    assert directional_raw_preview_fixed_scope() == directional_raw_preview_fixed_scope()
    assert DIRECTIONAL_RAW_PREVIEW_FIXED_SCOPE_DIGEST == (
        "c232e8b7c910d8bb0fe6c82e101c075f5ea1d0ce5845acd8dede4ec2b1ffd6ea"
    )


def test_provider_attestation_and_registry_states_are_fail_closed() -> None:
    contract_names = {
        column.name for column in IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT.columns
    }
    forbidden_identity_outputs = {
        "asset_id",
        "canonical_composite_figi",
        "canonical_share_class_figi",
        "identity_disposition",
        "issuer_id",
        "provider_composite_override_id",
        "share_class_adjudication_id",
        "valid_from_session",
        "valid_through_session",
    }

    assert DIRECTIONAL_RAW_PREVIEW_PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION == 2
    assert DIRECTIONAL_RAW_PREVIEW_PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION == (
        PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION
    )
    assert DIRECTIONAL_RAW_PREVIEW_INTERVAL_INFERENCE_STATE == ("direction_only_not_exact_scope")
    assert DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE == "not_evaluated"
    assert set(DIRECTIONAL_RAW_PREVIEW_CAPABILITIES.values()) == {False}
    assert forbidden_identity_outputs.isdisjoint(contract_names)
    assert DIRECTIONAL_RAW_PREVIEW_COMPOSITE_CORRECTION_REGISTRIES == (
        "identity_adjudication",
        "identity_cross_market_adjudication",
        "provider_composite_override",
    )
    assert DIRECTIONAL_RAW_PREVIEW_SHARE_CLASS_REGISTRY == "share_class_adjudication"
    assert DIRECTIONAL_RAW_PREVIEW_RELATION_ONLY_REGISTRY == "asset_transition"
    semantics = directional_raw_preview_registry_exclusivity_semantics()
    assert semantics["registry_evaluation_state"] == "not_evaluated"
    assert semantics["raw_collision_count_reporting"] == (
        "not_evaluated_in_directional_raw_preview"
    )
    assert semantics["share_class_application_precondition"] == (
        "unique_canonical_composite_required"
    )
    assert semantics["future_collision_qa_policy"] == {
        "multi_registry_composite_override_collision_alias_rows": ("critical_numerator_eq_0"),
        "multi_registry_composite_override_collision_eligible_rows": ("critical_numerator_eq_0"),
        "multi_registry_composite_override_collision_resolved_rows": ("critical_numerator_eq_0"),
        "multi_registry_composite_override_collision_rows": "high_review_nonblocking",
    }
    assert semantics["future_publish_policy"] == (
        "nonzero_raw_collision_requires_explicit_review_acceptance"
    )
    assert DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST == (
        "d2edbfe9420da8ceca4fe40b6b5a12df381fece7198763dba94658242ceb9d5d"
    )


def test_qa_surface_reports_review_facts_without_claiming_registry_collision_zero() -> None:
    rules = {
        rule.check_id: rule for rule in IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT.qa_rules
    }
    critical = (
        "schema_exact",
        "fixed_review_scope_invalid",
        "inventory_binding_invalid",
        "s4_source_binding_invalid",
        "source_artifact_integrity_invalid",
        "source_scan_count_mismatch",
        "exact_pair_scope_leakage_rows",
        "scoped_source_omission_rows",
        "duplicate_universe_membership_rows",
        "selected_parent_missing_rows",
        "selected_parent_multiple_rows",
        "selected_parent_projection_mismatch_rows",
        "provider_row_attestation_schema_invalid_rows",
        "row_attestation_replay_invalid_rows",
        "orphan_or_duplicate_attestation_rows",
        "observed_source_row_mutation_rows",
        "direction_only_state_invalid_rows",
        "registry_resolution_attempt_rows",
        "registry_collision_evaluation_state_invalid_rows",
        "canonical_identity_output_rows",
        "adjudication_or_transition_decision_rows",
        "capability_true_rows",
        "primary_key_duplicate_excess",
        "output_sort_invalid",
        "output_artifact_readback_invalid",
        "resource_cap_exceeded",
    )
    high_review = (
        "requested_slot_missing_membership_rows",
        "asset_only_scope_rows",
        "nonselected_asset_observation_rows",
        "same_session_identity_variant_groups",
        "directional_composite_change_edges",
        "directional_share_class_change_edges",
        "sampled_gap_edges",
        "inventory_anchor_unobserved_slots",
    )

    assert tuple(rules) == (*critical, *high_review)
    assert all(
        rules[check_id].severity is QASeverity.CRITICAL
        and rules[check_id].failure_status is QAStatus.FAILED
        for check_id in critical
    )
    assert all(
        rules[check_id].severity is QASeverity.HIGH
        and rules[check_id].failure_status is QAStatus.WARNING
        for check_id in high_review
    )
    assert all(rule.threshold_expression == "numerator eq 0" for rule in rules.values())
    assert not any(
        check_id.startswith("multi_registry_composite_override_collision") for check_id in rules
    )
