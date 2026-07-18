"""Frozen review-only contract for the three-case S7 directional raw preview.

This module deliberately exposes no runner, approval, external-evidence capture,
registry application, adjudication, Full materialization, or publication path.  It
only pins the exact eleven logical review pairs, the immutable slot-table contract,
and fail-closed semantics that a later separately approved preview must preserve.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from importlib.resources import files
from types import MappingProxyType
from typing import Final

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import TableContract

IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID: Final = (
    "b475ee2c9745791aae83908c0b6b6380724a34db132b194315ccae1a72ca1366"
)
IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST: Final = (
    "fc9a81955b3fe0c79545902c496cc4320df1b7d91f57c5a91e7498657a6cb1af"
)
IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256: Final = (
    "e9c54a61ed5f65b522ba8362268a44966a6620908182e9059bc519c43086d3f6"
)
IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_QA_SEMANTICS_DIGEST: Final = (
    "73aa1e615f5094cb1923e35083cb58536c5f43a5c1ebf1c524d513beaa32ff44"
)
IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_NAME: Final = (
    "schema_resources/identity_directional_raw_preview_slot.schema-v1.json"
)

DIRECTIONAL_RAW_PREVIEW_PROVIDER_ID: Final = "massive"
DIRECTIONAL_RAW_PREVIEW_PROVIDER_MARKET: Final = "stocks"
DIRECTIONAL_RAW_PREVIEW_PROVIDER_LOCALE: Final = "us"
DIRECTIONAL_RAW_PREVIEW_PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION: Final = 2
DIRECTIONAL_RAW_PREVIEW_INTERVAL_INFERENCE_STATE: Final = "direction_only_not_exact_scope"
DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE: Final = "not_evaluated"

DIRECTIONAL_RAW_PREVIEW_FIXED_TICKERS: Final = ("SOR", "XZO", "ANABV")
DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_ANCHORS: Final = MappingProxyType(
    {
        "SOR": "BBG000KMY6N2",
        "XZO": "BBG01XL8FHT0",
        "ANABV": "BBG021DMXXT2",
    }
)
DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS: Final = (
    (
        "SOR",
        (
            date(2024, 12, 31),
            date(2025, 1, 2),
            date(2025, 1, 3),
        ),
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
DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT: Final = sum(
    len(sessions) for _, sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
)
DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT: Final = len(
    {session for _, sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS for session in sessions}
)
DIRECTIONAL_RAW_PREVIEW_PHYSICAL_SOURCE_TABLES: Final = (
    "asset_observation_daily",
    "universe_source_daily",
)
DIRECTIONAL_RAW_PREVIEW_EXPECTED_PHYSICAL_ARTIFACT_COUNT: Final = (
    DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT
    * len(DIRECTIONAL_RAW_PREVIEW_PHYSICAL_SOURCE_TABLES)
)

DIRECTIONAL_RAW_PREVIEW_COMPOSITE_CORRECTION_REGISTRIES: Final = (
    "identity_adjudication",
    "identity_cross_market_adjudication",
    "provider_composite_override",
)
DIRECTIONAL_RAW_PREVIEW_SHARE_CLASS_REGISTRY: Final = "share_class_adjudication"
DIRECTIONAL_RAW_PREVIEW_RELATION_ONLY_REGISTRY: Final = "asset_transition"

DIRECTIONAL_RAW_PREVIEW_CAPABILITIES: Final = MappingProxyType(
    {
        "adjudication_plan_generation": False,
        "canonical_identity_materialization": False,
        "exact_group_history_read": False,
        "external_evidence_capture": False,
        "full_run": False,
        "preview_execution": False,
        "publication": False,
        "registry_materialization": False,
    }
)


def directional_raw_preview_registry_exclusivity_semantics() -> dict[str, object]:
    """Return the frozen downstream rule without pretending it was evaluated here."""

    return {
        "asset_transition_effect": "relation_only_never_override",
        "composite_correction_registries": list(
            DIRECTIONAL_RAW_PREVIEW_COMPOSITE_CORRECTION_REGISTRIES
        ),
        "composite_multi_match_policy": (
            "preserve_membership_no_canonical_no_alias_backtest_identity_ineligible"
        ),
        "future_collision_qa_policy": {
            "multi_registry_composite_override_collision_alias_rows": ("critical_numerator_eq_0"),
            "multi_registry_composite_override_collision_eligible_rows": (
                "critical_numerator_eq_0"
            ),
            "multi_registry_composite_override_collision_resolved_rows": (
                "critical_numerator_eq_0"
            ),
            "multi_registry_composite_override_collision_rows": "high_review_nonblocking",
        },
        "future_publish_policy": ("nonzero_raw_collision_requires_explicit_review_acceptance"),
        "raw_collision_count_reporting": "not_evaluated_in_directional_raw_preview",
        "registry_evaluation_state": DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE,
        "share_class_application_precondition": "unique_canonical_composite_required",
        "share_class_effect": "share_class_only_never_asset_id",
        "share_class_registry": DIRECTIONAL_RAW_PREVIEW_SHARE_CLASS_REGISTRY,
    }


DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST: Final = stable_digest(
    directional_raw_preview_registry_exclusivity_semantics()
)


def directional_raw_preview_fixed_scope() -> tuple[dict[str, object], ...]:
    """Return fresh serializable copies of the exact logical case/session matrix."""

    return tuple(
        {
            "inventory_anchor_composite_figi": (DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_ANCHORS[ticker]),
            "provider_id": DIRECTIONAL_RAW_PREVIEW_PROVIDER_ID,
            "provider_locale": DIRECTIONAL_RAW_PREVIEW_PROVIDER_LOCALE,
            "provider_market": DIRECTIONAL_RAW_PREVIEW_PROVIDER_MARKET,
            "sessions": [session.isoformat() for session in sessions],
            "ticker": ticker,
        }
        for ticker, sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
    )


DIRECTIONAL_RAW_PREVIEW_FIXED_SCOPE_DIGEST: Final = stable_digest(
    list(directional_raw_preview_fixed_scope())
)


def _load_contract() -> TableContract:
    resource = files("ame_stocks_api.silver").joinpath(
        IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_NAME
    )
    payload = resource.read_bytes()
    if hashlib.sha256(payload).hexdigest() != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256:
        raise RuntimeError("packaged directional raw-preview slot resource bytes differ")
    contract = TableContract.from_dict(json.loads(payload))
    if contract.contract_id != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID:
        raise RuntimeError("packaged directional raw-preview slot contract ID differs")
    if contract.schema_digest != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST:
        raise RuntimeError("packaged directional raw-preview slot Arrow schema differs")
    qa_digest = stable_digest([rule.to_dict() for rule in contract.qa_rules])
    if qa_digest != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_QA_SEMANTICS_DIGEST:
        raise RuntimeError("packaged directional raw-preview slot QA semantics differ")
    return contract


IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT: Final = _load_contract()

__all__ = [
    "DIRECTIONAL_RAW_PREVIEW_CAPABILITIES",
    "DIRECTIONAL_RAW_PREVIEW_COMPOSITE_CORRECTION_REGISTRIES",
    "DIRECTIONAL_RAW_PREVIEW_EXPECTED_PHYSICAL_ARTIFACT_COUNT",
    "DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_ANCHORS",
    "DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS",
    "DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT",
    "DIRECTIONAL_RAW_PREVIEW_FIXED_SCOPE_DIGEST",
    "DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT",
    "DIRECTIONAL_RAW_PREVIEW_FIXED_TICKERS",
    "DIRECTIONAL_RAW_PREVIEW_INTERVAL_INFERENCE_STATE",
    "DIRECTIONAL_RAW_PREVIEW_PHYSICAL_SOURCE_TABLES",
    "DIRECTIONAL_RAW_PREVIEW_PROVIDER_ID",
    "DIRECTIONAL_RAW_PREVIEW_PROVIDER_LOCALE",
    "DIRECTIONAL_RAW_PREVIEW_PROVIDER_MARKET",
    "DIRECTIONAL_RAW_PREVIEW_PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION",
    "DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE",
    "DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST",
    "DIRECTIONAL_RAW_PREVIEW_RELATION_ONLY_REGISTRY",
    "DIRECTIONAL_RAW_PREVIEW_SHARE_CLASS_REGISTRY",
    "IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT",
    "IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID",
    "IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_QA_SEMANTICS_DIGEST",
    "IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_NAME",
    "IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256",
    "IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST",
    "directional_raw_preview_fixed_scope",
    "directional_raw_preview_registry_exclusivity_semantics",
]
