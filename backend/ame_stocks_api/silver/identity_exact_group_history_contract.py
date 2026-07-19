"""Frozen review-only contract for the S7 exact-group full-history review.

The package is deliberately non-executable.  It pins three exact
``(provider, market, locale, ticker, observed Composite FIGI)`` groups and the
shape of a future full-S4-release observed-history candidate.  It does not
authorize a source read and cannot infer an override interval, canonicalize an
identity, adjudicate a case, decide tradability, run Full, or publish.
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

IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID: Final = (
    "cdf406e869c06c2942588a043f6e50dd429f1d6a8818d05e4d01a75fb8a92765"
)
IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST: Final = (
    "3ba74162c4903cef843496acc49d47198b1cc09f0206158b0ae065da38415400"
)
IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256: Final = (
    "ae957aeb2b61e7970eadcf2e963b7ae48ff2be6f4582901f1b9d26c7ff31b80c"
)
IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST: Final = (
    "837d03c92707590d505a5ea683760eb1448073a213abf07af4a6501ad263ce49"
)
IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_NAME: Final = (
    "schema_resources/identity_exact_group_history_review_slot.schema-v1.json"
)

EXACT_GROUP_HISTORY_PROVIDER_ID: Final = "massive"
EXACT_GROUP_HISTORY_PROVIDER_MARKET: Final = "stocks"
EXACT_GROUP_HISTORY_PROVIDER_LOCALE: Final = "us"
EXACT_GROUP_HISTORY_START_SESSION: Final = date(2016, 7, 11)
EXACT_GROUP_HISTORY_END_SESSION: Final = date(2026, 7, 9)
EXACT_GROUP_HISTORY_XNYS_SESSION_COUNT: Final = 2_513
EXACT_GROUP_HISTORY_S4_SOURCE_ARTIFACT_COUNT: Final = 5_026
EXACT_GROUP_HISTORY_S4_SOURCE_ROW_COUNT: Final = 138_757_511
EXACT_GROUP_HISTORY_S4_SOURCE_BYTES: Final = 15_910_278_169
EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID: Final = (
    "f81c7ee28939db3350fce809326723e911b6d486c6db166d2575fcc92cb2101d"
)
EXACT_GROUP_HISTORY_S4_RELEASE_SET_MANIFEST_SHA256: Final = (
    "937eaf4ed502fb2786dafb0dce9ec613bcaccb2cd488812cc5900118238d6c13"
)
EXACT_GROUP_HISTORY_PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION: Final = 2
EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE: Final = "exact_full_release_observed_runs_only"
EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE: Final = "not_evaluated"

EXACT_GROUP_HISTORY_FIXED_GROUPS: Final = (
    ("SOR", "BBG000KMY6N2"),
    ("XZO", "BBG01XL8FHT0"),
    ("ANABV", "BBG021DMXXT2"),
)
EXACT_GROUP_HISTORY_FIXED_TICKERS: Final = tuple(
    ticker for ticker, _ in EXACT_GROUP_HISTORY_FIXED_GROUPS
)
EXACT_GROUP_HISTORY_FIXED_COMPOSITES: Final = MappingProxyType(
    dict(EXACT_GROUP_HISTORY_FIXED_GROUPS)
)
EXACT_GROUP_HISTORY_PHYSICAL_SOURCE_TABLES: Final = (
    "asset_observation_daily",
    "universe_source_daily",
)

EXACT_GROUP_HISTORY_CAPABILITIES: Final = MappingProxyType(
    {
        "adjudication_plan_generation": False,
        "canonical_identity_materialization": False,
        "exact_group_history_execution": False,
        "external_evidence_capture": False,
        "full_run": False,
        "override_interval_generation": False,
        "publication": False,
        "registry_materialization": False,
        "tradability_decision_generation": False,
        "transition_generation": False,
    }
)


def exact_group_history_fixed_scope() -> tuple[dict[str, object], ...]:
    """Return fresh serializable copies of the exact three-group S4 scope."""

    return tuple(
        {
            "end_session": EXACT_GROUP_HISTORY_END_SESSION.isoformat(),
            "observed_composite_figi": composite_figi,
            "provider_id": EXACT_GROUP_HISTORY_PROVIDER_ID,
            "provider_locale": EXACT_GROUP_HISTORY_PROVIDER_LOCALE,
            "provider_market": EXACT_GROUP_HISTORY_PROVIDER_MARKET,
            "source_filter_fields": [
                "provider_id",
                "provider_market",
                "provider_locale",
                "ticker",
                "observed_composite_figi",
            ],
            "start_session": EXACT_GROUP_HISTORY_START_SESSION.isoformat(),
            "ticker": ticker,
        }
        for ticker, composite_figi in EXACT_GROUP_HISTORY_FIXED_GROUPS
    )


def exact_group_history_review_group_id(*, ticker: str, observed_composite_figi: str) -> str:
    """Return the control-plane-compatible ID for one frozen review group."""

    if (ticker, observed_composite_figi) not in EXACT_GROUP_HISTORY_FIXED_GROUPS:
        raise ValueError("review group is outside the frozen exact-group scope")
    return stable_digest(
        {
            "locale": EXACT_GROUP_HISTORY_PROVIDER_LOCALE,
            "market": EXACT_GROUP_HISTORY_PROVIDER_MARKET,
            "observed_composite_figi": observed_composite_figi,
            "provider": EXACT_GROUP_HISTORY_PROVIDER_ID,
            "ticker": ticker,
        }
    )


EXACT_GROUP_HISTORY_FIXED_REVIEW_GROUP_IDS: Final = MappingProxyType(
    {
        ticker: exact_group_history_review_group_id(
            ticker=ticker,
            observed_composite_figi=observed_composite_figi,
        )
        for ticker, observed_composite_figi in EXACT_GROUP_HISTORY_FIXED_GROUPS
    }
)


EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST: Final = stable_digest(
    list(exact_group_history_fixed_scope())
)


def exact_group_history_observed_run_semantics() -> dict[str, object]:
    """Return the frozen evidence/run semantics without authorizing execution."""

    return {
        "asset_evidence_rule": (
            "retain_every_exact_group_asset_observation_version_and_attestation"
        ),
        "canonical_identity_rule": "not_generated",
        "effective_interval_rule": "not_inferred_from_observed_runs",
        "membership_rule": "preserve_selected_universe_parent_without_mutation",
        "observed_interval_state": EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE,
        "output_session_rule": "only_sessions_with_exact_group_asset_evidence",
        "registry_evaluation_state": EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE,
        "run_break_rule": "break_unless_previous_observed_session_is_adjacent_xnys",
        "share_class_filter_rule": "forbidden",
        "tradability_rule": "not_evaluated_no_forced_liquidation_signal",
    }


EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST: Final = stable_digest(
    exact_group_history_observed_run_semantics()
)


def _load_contract() -> TableContract:
    resource = files("ame_stocks_api.silver").joinpath(
        IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_NAME
    )
    payload = resource.read_bytes()
    if hashlib.sha256(payload).hexdigest() != (
        IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256
    ):
        raise RuntimeError("packaged exact-group history review resource bytes differ")
    contract = TableContract.from_dict(json.loads(payload))
    if contract.contract_id != IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID:
        raise RuntimeError("packaged exact-group history review contract ID differs")
    if contract.schema_digest != IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST:
        raise RuntimeError("packaged exact-group history review Arrow schema differs")
    qa_digest = stable_digest([rule.to_dict() for rule in contract.qa_rules])
    if qa_digest != IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST:
        raise RuntimeError("packaged exact-group history review QA semantics differ")
    return contract


IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT: Final = _load_contract()

__all__ = [
    "EXACT_GROUP_HISTORY_CAPABILITIES",
    "EXACT_GROUP_HISTORY_END_SESSION",
    "EXACT_GROUP_HISTORY_FIXED_COMPOSITES",
    "EXACT_GROUP_HISTORY_FIXED_GROUPS",
    "EXACT_GROUP_HISTORY_FIXED_REVIEW_GROUP_IDS",
    "EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST",
    "EXACT_GROUP_HISTORY_FIXED_TICKERS",
    "EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE",
    "EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST",
    "EXACT_GROUP_HISTORY_PHYSICAL_SOURCE_TABLES",
    "EXACT_GROUP_HISTORY_PROVIDER_ID",
    "EXACT_GROUP_HISTORY_PROVIDER_LOCALE",
    "EXACT_GROUP_HISTORY_PROVIDER_MARKET",
    "EXACT_GROUP_HISTORY_PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION",
    "EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE",
    "EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID",
    "EXACT_GROUP_HISTORY_S4_RELEASE_SET_MANIFEST_SHA256",
    "EXACT_GROUP_HISTORY_S4_SOURCE_ARTIFACT_COUNT",
    "EXACT_GROUP_HISTORY_S4_SOURCE_BYTES",
    "EXACT_GROUP_HISTORY_S4_SOURCE_ROW_COUNT",
    "EXACT_GROUP_HISTORY_START_SESSION",
    "EXACT_GROUP_HISTORY_XNYS_SESSION_COUNT",
    "IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT",
    "IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID",
    "IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST",
    "IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_NAME",
    "IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256",
    "IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST",
    "exact_group_history_fixed_scope",
    "exact_group_history_observed_run_semantics",
    "exact_group_history_review_group_id",
]
