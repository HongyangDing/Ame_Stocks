"""Source-bound S7 Gate-C full-history market-consistency scan.

This module joins the exact Gate-A ``universe_source_daily`` history to one
explicitly bound Gate-B Composite-market classification candidate.  It preserves
every provider observation and produces review-only interval diagnostics.  It has
no adjudication, override, canonical-identity, release, or publication capability.
"""

from __future__ import annotations

import ctypes
import errno
import fcntl
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import resource
import shutil
import stat
import subprocess
import sys
import time
from collections import Counter, deque
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

import exchange_calendars as xcals
import pyarrow as pa
import pyarrow.parquet as pq

import ame_stocks_api.artifacts as artifacts_module
import ame_stocks_api.silver.identity_market_consistency as market_consistency_module
import ame_stocks_api.silver.identity_market_inventory_plan as inventory_plan_module
import ame_stocks_api.silver.identity_preview_runner as preview_runner_module
import ame_stocks_api.silver.identity_provider_evidence as provider_evidence_module
import ame_stocks_api.silver.identity_source as identity_source_module
from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
)
from ame_stocks_api.silver.calendar_artifact import (
    CALENDAR_ARTIFACT_RULE_VERSION,
    CALENDAR_NAME,
    XNYSCalendarArtifact,
    XNYSCalendarArtifactError,
    load_xnys_calendar_artifact,
)
from ame_stocks_api.silver.identity_market_inventory_plan import (
    EXTERNAL_EVIDENCE_MANIFEST_ID,
    EXTERNAL_EVIDENCE_MANIFEST_PATH,
    EXTERNAL_EVIDENCE_MANIFEST_SHA256,
    PREVIEW_APPROVAL_ID,
    PREVIEW_ARTIFACT_ID,
    PREVIEW_ARTIFACT_SHA256,
    PREVIEW_CASE_COUNT,
    PREVIEW_CASE_EVIDENCE_SET_DIGEST,
    PREVIEW_COMPLETION_ID,
    PREVIEW_COMPLETION_SHA256,
    PREVIEW_PLAN_ID,
    PREVIEW_SUSPECTED_ROW_COUNT,
)
from ame_stocks_api.silver.identity_preview_runner import S7DetectorPreviewCompletion
from ame_stocks_api.silver.identity_provider_evidence import (
    ProviderEvidenceError,
    ProviderRowAttestation,
    S4BounceProviderEvidenceManifest,
    replay_provider_row_attestations_from_official_bundle,
)
from ame_stocks_api.silver.identity_source import IdentitySourceError, open_identity_source_bundle

GATE_C_RULE_VERSION: Final = "s7_full_market_sequence_gate_c_v1"
GATE_C_PLAN_RULE_VERSION: Final = "s7_full_market_sequence_gate_c_plan_v1"
GATE_C_AUTHORIZATION_REQUEST_RULE_VERSION: Final = (
    "s7_full_market_sequence_gate_c_authorization_request_v1"
)
GATE_C_AUTHORIZATION_RECEIPT_RULE_VERSION: Final = (
    "s7_full_market_sequence_gate_c_authorization_receipt_v1"
)
CANDIDATE_STATE: Final = "awaiting_review"
PLAN_STATE: Final = "draft_awaiting_authorization"
AUTHORIZATION_REQUEST_STATE: Final = "awaiting_authorization"
AUTHORIZATION_RECEIPT_STATE: Final = "authorized_awaiting_execution"
AUTHORIZED_ACTION: Final = (
    "authorize_exact_s7_gate_c_full_sequence_scan_once_to_awaiting_review_without_"
    "adjudication_registry_full_or_publish"
)
AUTHORIZATION_LITERAL: Final = (
    "为什么你就不能自己直接把S7运行完呢，我允许你这么做，只要中间不报错或者明显越界就可以自行继续"  # noqa: RUF001
)
AUTHORIZATION_LITERAL_SHA256: Final = hashlib.sha256(AUTHORIZATION_LITERAL.encode()).hexdigest()
AUTHORIZATION_REAFFIRMATION: Final = "批准"
AUTHORIZATION_REAFFIRMATION_SHA256: Final = hashlib.sha256(
    AUTHORIZATION_REAFFIRMATION.encode()
).hexdigest()
CALENDAR_ARTIFACT_ID: Final = "31cc575ae55542a580ee17e09aa242159bbcaedd0a001fd2184021a541b734bd"
CALENDAR_ARTIFACT_SHA256: Final = "3f026761a9f752d1e00c89c9f72383e7d8c0a7f7dcb2cdf8ef82e5831dfc0da7"
CALENDAR_ENGINE_VERSION: Final = "4.13.2"
PROVIDER: Final = "massive"
UNIVERSE_TABLE: Final = "universe_source_daily"
UNIVERSE_RELEASE_ID: Final = "c7e0d9a75857cbca130ba8873a737411ccb2f11d3e711ee0c0b0d9d0e2f5c614"
UNIVERSE_RELEASE_MANIFEST_SHA256: Final = (
    "6b2c6ca1b612c4c38ddc8e359c1402c177a4f19b0295604d42b78bcd5804596d"
)
INVENTORY_CANDIDATE_ID: Final = "b35dc51b5798db2f8cf7783a1f2953990898bc5dde539107beabe53d85a57044"
INVENTORY_CANDIDATE_SHA256: Final = (
    "11fa38df8aaa07a781e80e80d0844213bf7d859cba3826ef26c693d735697970"
)
INVENTORY_COMPLETION_ID: Final = "4472b730bbf5e77b19253c0f6bfc4b78df3135bc2f46424262fff7f735cdce15"
INVENTORY_COMPLETION_SHA256: Final = (
    "255197634284c23c0b42f17b59398c07d5ab1d9d8c9f82493a363924a240a282"
)
INVENTORY_COMPLETION_PATH: Final = (
    "manifests/silver/identity/composite-inventory-execution-completions/"
    "plan_id=57dcfe2cd7431105e0b664163a75e76a42a023e777055bad935b548f41935eb5/"
    "approval_id=9a0b6f07cd6c1294dc1c086cc26b3d94343624c482fae6d2eca2498075f90d5c/"
    "manifest.json"
)
INVENTORY_SOURCE_ARTIFACT_SET_DIGEST: Final = (
    "cb4a0e7cb73a59edcc74d2a8601c26d167dd1f9eed7b9821010040ddb0abcaaf"
)
INVENTORY_CANDIDATE_PATH: Final = (
    "manifests/silver/identity/composite-inventory-candidates/"
    f"candidate_id={INVENTORY_CANDIDATE_ID}/manifest.json"
)
INVENTORY_SOURCE_ARTIFACT_COUNT: Final = 5_026
INVENTORY_SOURCE_ROW_COUNT: Final = 138_757_511
INVENTORY_SOURCE_BYTES: Final = 15_910_278_169
UNIVERSE_ARTIFACT_COUNT: Final = 2_513
UNIVERSE_ROW_COUNT: Final = 69_376_329
START_SESSION: Final = date(2016, 7, 11)
END_SESSION: Final = date(2026, 7, 9)
LONG_STANDING_MIN_SESSIONS: Final = 20
REVIEWED_FOREIGN_ROW_COUNT: Final = 79
REVIEWED_INVERSE_US_ROW_COUNT: Final = 10
REVIEWED_DIRECT_CASE_COUNT: Final = 9
REVIEWED_INVERSE_CASE_COUNT: Final = 10
PRIOR_PREVIEW_PATH: Final = (
    "manifests/silver/identity-bounce-bounded-previews/"
    f"preview_artifact_id={PREVIEW_ARTIFACT_ID}/manifest.json"
)
PRIOR_PREVIEW_COMPLETION_PATH: Final = (
    "manifests/silver/identity/detector-preview-completions/"
    f"plan_id={PREVIEW_PLAN_ID}/approval_id={PREVIEW_APPROVAL_ID}/manifest.json"
)

UNIVERSE_COLUMNS: Final = (
    "session_date",
    "ticker",
    "active_on_date",
    "market",
    "locale",
    "primary_exchange_mic",
    "composite_figi",
    "share_class_figi",
    "selected_source_record_id",
)
US_PRIMARY_MICS: Final = frozenset({"ARCX", "BATS", "IEXG", "XASE", "XNYS", "XNAS"})
US_CLASSIFICATIONS: Final = frozenset({"us_composite"})
NON_US_CLASSIFICATIONS: Final = frozenset({"non_us_composite"})
UNRESOLVED_CLASSIFICATIONS: Final = frozenset(
    {
        "ambiguous",
        "no_mapping",
        "share_class_conflict",
        "source_unavailable",
        "unresolved_conflicting_market_codes",
        "unresolved_no_exact_current_mapping",
    }
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")


class IdentityMarketSequenceError(RuntimeError):
    """Raised when Gate-C cannot produce a trustworthy review candidate."""


@dataclass(frozen=True, slots=True)
class S7MarketSequencePlan:
    plan_id: str
    plan_path: str
    plan_sha256: str
    intent_captured_at_utc: str
    intent_available_session: str
    request_id: str
    request_path: str
    request_sha256: str
    state: str
    idempotent: bool


@dataclass(frozen=True, slots=True)
class S7MarketSequenceAuthorization:
    authorization_id: str
    authorization_path: str
    authorization_sha256: str
    plan_id: str
    request_id: str
    recorded_at_utc: str
    state: str
    idempotent: bool


@dataclass(frozen=True, slots=True)
class S7MarketSequenceResourceCaps:
    batch_rows: int = 65_536
    max_classification_rows: int = 1_000_000
    max_interval_rows: int = 10_000_000
    max_examples: int = 100
    max_output_bytes: int = 4 * 1024**3
    max_tmp_bytes: int = 6 * 1024**3
    rss_bytes_cap: int = 2 * 1024**3
    disk_free_floor_bytes: int = 40 * 1024**3
    wall_clock_seconds_cap: int = 8 * 60 * 60
    worker_count: int = 1

    def __post_init__(self) -> None:
        for label, value in self.to_dict().items():
            if type(value) is not int or value <= 0:
                raise IdentityMarketSequenceError(f"resource cap {label} must be positive")
        if self.max_examples > 100:
            raise IdentityMarketSequenceError("bounded examples cap cannot exceed 100")
        if self.worker_count != 1:
            raise IdentityMarketSequenceError("Gate-C is fixed to one streaming worker")

    def to_dict(self) -> dict[str, int]:
        return {
            "batch_rows": self.batch_rows,
            "disk_free_floor_bytes": self.disk_free_floor_bytes,
            "max_classification_rows": self.max_classification_rows,
            "max_examples": self.max_examples,
            "max_interval_rows": self.max_interval_rows,
            "max_output_bytes": self.max_output_bytes,
            "max_tmp_bytes": self.max_tmp_bytes,
            "rss_bytes_cap": self.rss_bytes_cap,
            "wall_clock_seconds_cap": self.wall_clock_seconds_cap,
            "worker_count": self.worker_count,
        }


@dataclass(frozen=True, slots=True)
class _SourceExpectations:
    inventory_candidate_id: str = INVENTORY_CANDIDATE_ID
    inventory_candidate_sha256: str = INVENTORY_CANDIDATE_SHA256
    inventory_completion_id: str = INVENTORY_COMPLETION_ID
    inventory_completion_sha256: str = INVENTORY_COMPLETION_SHA256
    inventory_completion_path: str = INVENTORY_COMPLETION_PATH
    source_artifact_set_digest: str = INVENTORY_SOURCE_ARTIFACT_SET_DIGEST
    source_artifact_count: int = INVENTORY_SOURCE_ARTIFACT_COUNT
    source_row_count: int = INVENTORY_SOURCE_ROW_COUNT
    source_bytes: int = INVENTORY_SOURCE_BYTES
    universe_artifact_count: int = UNIVERSE_ARTIFACT_COUNT
    universe_row_count: int = UNIVERSE_ROW_COUNT
    universe_release_id: str = UNIVERSE_RELEASE_ID
    universe_release_manifest_sha256: str = UNIVERSE_RELEASE_MANIFEST_SHA256
    start_session: date = START_SESSION
    end_session: date = END_SESSION


PRODUCTION_EXPECTATIONS: Final = _SourceExpectations()


@dataclass(frozen=True, slots=True)
class S7MarketSequenceCandidate:
    candidate_id: str
    completion_id: str
    completion_path: str
    completion_sha256: str
    manifest_path: str
    interval_data_path: str
    daily_reason_counts_path: str
    qa_path: str
    examples_path: str
    reviewed_evidence_path: str
    source_row_count: int
    interval_row_count: int
    us_locale_non_us_composite_figi_rows: int
    unresolved_rows: int
    long_standing_foreign_rows: int
    reviewed_foreign_row_count: int
    reviewed_case_count: int
    idempotent: bool


@dataclass(frozen=True, slots=True)
class _Classification:
    normalized: str
    raw: str
    market_codes: tuple[str, ...]
    source_available_session: str | None


@dataclass(slots=True)
class _OpenInterval:
    key: tuple[object, ...]
    provider: str
    market: str | None
    locale: str | None
    ticker: str
    composite_figi: str | None
    share_class_figi: str | None
    primary_exchange_mic: str | None
    active_on_date: bool
    classification: str
    classification_reason: str
    market_codes: tuple[str, ...]
    source_available_session: str | None
    proposed_backtest_identity_eligible: bool
    start: date
    end: date
    start_session_index: int
    last_session_index: int
    session_count: int
    first_source_record_id: str
    last_source_record_id: str
    lineage: Any = field(repr=False)


@dataclass(slots=True)
class _MarketRun:
    key: tuple[object, ...]
    classification: str
    locale: str | None
    primary_exchange_mic: str | None
    session_count: int
    last_session_index: int


@dataclass(slots=True)
class _Metrics:
    source_rows: int = 0
    interval_rows: int = 0
    flagged_rows: int = 0
    us_locale_foreign_rows: int = 0
    us_mic_foreign_rows: int = 0
    unresolved_rows: int = 0
    non_us_rows: int = 0
    reference_unattempted_rows: int = 0
    unapproved_foreign_eligible_rows: int = 0
    unresolved_eligible_rows: int = 0
    membership_mutation_rows: int = 0
    forced_liquidation_rows: int = 0
    long_standing_foreign_rows: int = 0
    long_standing_foreign_intervals: int = 0
    inverse_bounce_cases: int = 0
    inverse_bounce_middle_rows: int = 0
    inverse_bounce_misclassified_rows: int = 0
    ordinary_bounce_cases: int = 0


@dataclass(slots=True)
class _ResourceMonitor:
    root: Path
    staging: Path
    caps: S7MarketSequenceResourceCaps
    started: float = field(default_factory=time.monotonic)
    peak_rss_bytes: int = 0
    minimum_disk_free_bytes: int = 2**63 - 1
    maximum_tmp_bytes: int = 0

    def check(self) -> None:
        if time.monotonic() - self.started > self.caps.wall_clock_seconds_cap:
            raise IdentityMarketSequenceError("resource_cap_exceeded: wall clock")
        self.peak_rss_bytes = max(self.peak_rss_bytes, _peak_rss_bytes())
        if self.peak_rss_bytes > self.caps.rss_bytes_cap:
            raise IdentityMarketSequenceError("resource_cap_exceeded: RSS")
        self.minimum_disk_free_bytes = min(
            self.minimum_disk_free_bytes, shutil.disk_usage(self.root).free
        )
        if self.minimum_disk_free_bytes < self.caps.disk_free_floor_bytes:
            raise IdentityMarketSequenceError("resource_cap_exceeded: disk free floor")
        self.maximum_tmp_bytes = max(self.maximum_tmp_bytes, _tree_bytes(self.staging))
        if self.maximum_tmp_bytes > self.caps.max_tmp_bytes:
            raise IdentityMarketSequenceError("resource_cap_exceeded: temporary bytes")


INTERVAL_SCHEMA: Final = pa.schema(
    [
        pa.field("provider", pa.string(), nullable=False),
        pa.field("market", pa.string()),
        pa.field("locale", pa.string()),
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("observed_composite_figi", pa.string()),
        pa.field("observed_share_class_figi", pa.string()),
        pa.field("primary_exchange_mic", pa.string()),
        pa.field("active_on_date", pa.bool_(), nullable=False),
        pa.field("market_classification", pa.string(), nullable=False),
        pa.field("classification_reason", pa.string(), nullable=False),
        pa.field("market_codes", pa.list_(pa.string()), nullable=False),
        pa.field("interval_start_session", pa.date32(), nullable=False),
        pa.field("interval_end_session", pa.date32(), nullable=False),
        pa.field("interval_session_count", pa.int64(), nullable=False),
        pa.field("source_row_count", pa.int64(), nullable=False),
        pa.field("first_source_record_id", pa.string(), nullable=False),
        pa.field("last_source_record_id", pa.string(), nullable=False),
        pa.field("source_record_lineage_digest", pa.string(), nullable=False),
        pa.field("active_slice_meets_long_standing_threshold", pa.bool_(), nullable=False),
        pa.field("foreign_market_identity_legal", pa.bool_(), nullable=False),
        pa.field("membership_preserved", pa.bool_(), nullable=False),
        pa.field("identity_quality_inactive_inferred", pa.bool_(), nullable=False),
        pa.field("proposed_backtest_identity_eligible", pa.bool_(), nullable=False),
        pa.field("liquidation_signal", pa.bool_(), nullable=False),
        pa.field("transition_disposition", pa.string(), nullable=False),
        pa.field("source_available_session", pa.string()),
    ]
)
DAILY_REASON_SCHEMA: Final = pa.schema(
    [
        pa.field("session_date", pa.date32(), nullable=False),
        pa.field("reason_code", pa.string(), nullable=False),
        pa.field("row_count", pa.int64(), nullable=False),
    ]
)
REVIEWED_EVIDENCE_SCHEMA: Final = pa.schema(
    [
        pa.field("provider", pa.string(), nullable=False),
        pa.field("market", pa.string(), nullable=False),
        pa.field("locale", pa.string(), nullable=False),
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("session_date", pa.date32(), nullable=False),
        pa.field("observed_composite_figi", pa.string(), nullable=False),
        pa.field("observed_share_class_figi", pa.string(), nullable=False),
        pa.field("primary_exchange_mic", pa.string(), nullable=False),
        pa.field("active_on_date", pa.bool_(), nullable=False),
        pa.field("selected_source_record_id", pa.string(), nullable=False),
        pa.field("asset_observation_attestation_id", pa.string(), nullable=False),
        pa.field("asset_observation_attestation_json", pa.string(), nullable=False),
        pa.field("asset_observation_full_row_digest", pa.string(), nullable=False),
        pa.field("asset_observation_full_row_json", pa.string(), nullable=False),
        pa.field("asset_observation_artifact_path", pa.string(), nullable=False),
        pa.field("asset_observation_artifact_sha256", pa.string(), nullable=False),
        pa.field("asset_observation_parquet_row_group", pa.int64(), nullable=False),
        pa.field("asset_observation_row_index_in_row_group", pa.int64(), nullable=False),
        pa.field("universe_membership_attestation_id", pa.string(), nullable=False),
        pa.field("universe_membership_attestation_json", pa.string(), nullable=False),
        pa.field("universe_membership_full_row_digest", pa.string(), nullable=False),
        pa.field("universe_membership_full_row_json", pa.string(), nullable=False),
        pa.field("universe_membership_artifact_path", pa.string(), nullable=False),
        pa.field("universe_membership_artifact_sha256", pa.string(), nullable=False),
        pa.field("universe_membership_parquet_row_group", pa.int64(), nullable=False),
        pa.field("universe_membership_row_index_in_row_group", pa.int64(), nullable=False),
        pa.field("related_identity_case_ids", pa.list_(pa.string()), nullable=False),
        pa.field("related_case_resolution_roles", pa.list_(pa.string()), nullable=False),
        pa.field("related_case_bindings_json", pa.string(), nullable=False),
        pa.field("source_snapshot_binding_digest", pa.string(), nullable=False),
        pa.field("source_available_session", pa.string(), nullable=False),
    ]
)


def prepare_market_sequence_plan(
    data_root: Path,
    *,
    classification_candidate_path: str,
    classification_candidate_id: str,
    classification_candidate_sha256: str,
    classification_data_path: str,
    classification_data_sha256: str,
    classification_data_bytes: int,
    classification_data_row_count: int,
    classification_source_available_session: str,
    prepared_by: str,
    resource_caps: S7MarketSequenceResourceCaps | None = None,
) -> S7MarketSequencePlan:
    """Freeze durable Gate-C intent before any source Parquet may be opened."""

    root = _validated_root(data_root)
    caps = resource_caps or S7MarketSequenceResourceCaps()
    actor = _safe_text(prepared_by, "prepared_by")
    for label, value in (
        ("classification candidate ID", classification_candidate_id),
        ("classification candidate SHA-256", classification_candidate_sha256),
        ("classification DATA SHA-256", classification_data_sha256),
    ):
        _digest(value, label)
    _nonnegative_int(classification_data_bytes, "classification DATA bytes")
    _positive_int(classification_data_row_count, "classification DATA rows")
    _relative_path(classification_candidate_path, "classification candidate path")
    _relative_path(classification_data_path, "classification DATA path")
    calendar = _load_bound_calendar(root)
    source_available_session = _xnys_session_text(
        classification_source_available_session,
        "classification source available session",
        calendar=calendar,
    )
    expected_candidate_path = (
        "manifests/silver/identity/openfigi-market-consistency-candidates/"
        f"candidate_id={classification_candidate_id}/manifest.json"
    )
    expected_data_path = (
        "manifests/silver/identity/openfigi-market-consistency-candidates/"
        f"candidate_id={classification_candidate_id}/data/classification.parquet"
    )
    if (
        classification_candidate_path != expected_candidate_path
        or classification_data_path != expected_data_path
    ):
        raise IdentityMarketSequenceError("Gate-B plan paths are not canonical")
    runtime_provenance = _runtime_provenance()
    expected = PRODUCTION_EXPECTATIONS
    calendar_binding = _calendar_binding(calendar)
    gate_a = {
        "candidate_id": expected.inventory_candidate_id,
        "candidate_path": (
            "manifests/silver/identity/composite-inventory-candidates/"
            f"candidate_id={expected.inventory_candidate_id}/manifest.json"
        ),
        "candidate_sha256": expected.inventory_candidate_sha256,
        "completion_id": expected.inventory_completion_id,
        "completion_path": expected.inventory_completion_path,
        "completion_sha256": expected.inventory_completion_sha256,
        "source_artifact_count": expected.source_artifact_count,
        "source_artifact_set_digest": expected.source_artifact_set_digest,
        "source_bytes": expected.source_bytes,
        "source_row_count": expected.source_row_count,
        "universe_artifact_count": expected.universe_artifact_count,
        "universe_row_count": expected.universe_row_count,
    }
    gate_b = {
        "candidate_id": classification_candidate_id,
        "candidate_path": classification_candidate_path,
        "candidate_sha256": classification_candidate_sha256,
        "data": {
            "bytes": classification_data_bytes,
            "path": classification_data_path,
            "row_count": classification_data_row_count,
            "sha256": classification_data_sha256,
        },
    }
    reviewed_source_evidence = _reviewed_source_plan_binding()
    authorization_requirement = {
        "authorized_action": AUTHORIZED_ACTION,
        "accepted_modes": ["current_standing_receipt", "exact_plan_literal"],
        "execution_without_plan_bound_receipt": False,
    }
    preparation_scope_payload = {
        "artifact_type": "s7_full_market_sequence_preparation_scope",
        "authorization_requirement": authorization_requirement,
        "calendar_binding": calendar_binding,
        "capabilities": _fail_closed_capabilities(),
        "classification_source_available_session": source_available_session,
        "gate_a": gate_a,
        "gate_b": gate_b,
        "plan_rule_version": GATE_C_PLAN_RULE_VERSION,
        "resource_caps": caps.to_dict(),
        "reviewed_source_evidence": reviewed_source_evidence,
        "runtime_provenance": runtime_provenance,
    }
    preparation_scope_id = stable_digest(preparation_scope_payload)
    preparation_relative = _preparation_slot_path(preparation_scope_id)
    preparation_path = _safe(root, preparation_relative)
    existing_preparation: dict[str, object] | None = None
    if preparation_path.exists():
        existing_preparation, _ = _load_canonical_json_file(
            preparation_path, "Gate-C fixed preparation slot"
        )
        if (
            existing_preparation.get("scope_id") != preparation_scope_id
            or existing_preparation.get("scope") != preparation_scope_payload
            or existing_preparation.get("first_writer") != actor
        ):
            raise IdentityMarketSequenceError(
                "existing Gate-C preparation slot actor or scope differs"
            )
        captured_at = _parse_utc(
            existing_preparation.get("intent_captured_at_utc"),
            "preparation intent timestamp",
        )
    else:
        captured_at = _utc_now()
    try:
        intent_session, intent_open = calendar.first_open_after(captured_at)
    except XNYSCalendarArtifactError as exc:
        raise IdentityMarketSequenceError(
            "Gate-C preparation intent is outside the bound calendar"
        ) from exc
    payload = {
        "artifact_type": "s7_full_market_sequence_execution_plan",
        "authorization_requirement": authorization_requirement,
        "availability": {
            "classification_source_available_session": source_available_session,
            "intent_available_session": intent_session.isoformat(),
            "intent_controlling_timestamp_utc": captured_at.isoformat(),
            "intent_first_xnys_open_utc": intent_open.isoformat(),
            "semantics": (
                "review candidate is unavailable before both its bound Gate-B evidence "
                "session and the first XNYS open after actual execution completion; no "
                "identity decision is backdated"
            ),
        },
        "calendar_binding": calendar_binding,
        "capabilities": _fail_closed_capabilities(),
        "gate_a": gate_a,
        "gate_b": gate_b,
        "intent_captured_at_utc": captured_at.isoformat(),
        "plan_rule_version": GATE_C_PLAN_RULE_VERSION,
        "preparation_scope": {
            "path": preparation_relative,
            "scope_id": preparation_scope_id,
        },
        "prepared_by": actor,
        "resource_caps": caps.to_dict(),
        "reviewed_source_evidence": reviewed_source_evidence,
        "runtime_provenance": runtime_provenance,
        "state": PLAN_STATE,
    }
    plan_id = stable_digest(payload)
    document = {**payload, "plan_id": plan_id}
    content = _canonical_bytes(document)
    relative = (
        f"manifests/silver/identity/full-market-sequence-plans/plan_id={plan_id}/manifest.json"
    )
    path = _safe(root, relative)
    request_payload = {
        "artifact_type": "s7_full_market_sequence_authorization_request",
        "authorized_action": AUTHORIZED_ACTION,
        "capabilities": _fail_closed_capabilities(),
        "control_binding_digest": _plan_control_binding_digest(document),
        "plan": {
            "path": relative,
            "plan_id": plan_id,
            "sha256": hashlib.sha256(content).hexdigest(),
        },
        "request_rule_version": GATE_C_AUTHORIZATION_REQUEST_RULE_VERSION,
        "resource_caps_digest": stable_digest(caps.to_dict()),
        "state": AUTHORIZATION_REQUEST_STATE,
    }
    request_id = stable_digest(request_payload)
    request_document = {**request_payload, "request_id": request_id}
    request_content = _canonical_bytes(request_document)
    request_relative = (
        "manifests/silver/identity/full-market-sequence-authorization-requests/"
        f"request_id={request_id}/manifest.json"
    )
    request_path = _safe(root, request_relative)
    preparation_payload = {
        "artifact_type": "s7_full_market_sequence_preparation_slot",
        "first_writer": actor,
        "intent_available_session": intent_session.isoformat(),
        "intent_captured_at_utc": captured_at.isoformat(),
        "intent_first_xnys_open_utc": intent_open.isoformat(),
        "plan": {
            "path": relative,
            "plan_id": plan_id,
            "sha256": hashlib.sha256(content).hexdigest(),
        },
        "request": {
            "path": request_relative,
            "request_id": request_id,
            "sha256": hashlib.sha256(request_content).hexdigest(),
        },
        "scope": preparation_scope_payload,
        "scope_id": preparation_scope_id,
        "state": "intent_recorded_plan_request_bound",
    }
    preparation_id = stable_digest(preparation_payload)
    preparation_document = {**preparation_payload, "preparation_id": preparation_id}
    preparation_content = _canonical_bytes(preparation_document)
    if existing_preparation is not None:
        if existing_preparation != preparation_document:
            raise IdentityMarketSequenceError(
                "existing Gate-C preparation slot plan/request binding differs"
            )
        idempotent = True
    else:
        _write_exact(preparation_path, preparation_content)
        idempotent = False
    if path.exists():
        if not path.is_file() or path.is_symlink() or path.read_bytes() != content:
            raise IdentityMarketSequenceError("existing Gate-C plan differs")
    else:
        _write_exact(path, content)
    if request_path.exists():
        if (
            not request_path.is_file()
            or request_path.is_symlink()
            or request_path.read_bytes() != request_content
        ):
            raise IdentityMarketSequenceError("existing Gate-C authorization request differs")
    else:
        _write_exact(request_path, request_content)
    return S7MarketSequencePlan(
        plan_id=plan_id,
        plan_path=relative,
        plan_sha256=hashlib.sha256(content).hexdigest(),
        intent_captured_at_utc=captured_at.isoformat(),
        intent_available_session=intent_session.isoformat(),
        request_id=request_id,
        request_path=request_relative,
        request_sha256=hashlib.sha256(request_content).hexdigest(),
        state=PLAN_STATE,
        idempotent=idempotent,
    )


def authorize_market_sequence_plan_under_standing_grant(
    data_root: Path,
    *,
    plan_path: str,
    plan_id: str,
    plan_sha256: str,
    request_path: str,
    request_id: str,
    request_sha256: str,
    recorded_by: str,
) -> S7MarketSequenceAuthorization:
    """Record current standing authority against one exact Gate-C plan/request."""

    return _record_market_sequence_authorization(
        data_root,
        plan_path=plan_path,
        plan_id=plan_id,
        plan_sha256=plan_sha256,
        request_path=request_path,
        request_id=request_id,
        request_sha256=request_sha256,
        recorded_by=recorded_by,
        authorization_basis={
            "grant": AUTHORIZATION_LITERAL,
            "grant_sha256": AUTHORIZATION_LITERAL_SHA256,
            "kind": "current_standing_authorization",
            "reaffirmation": AUTHORIZATION_REAFFIRMATION,
            "reaffirmation_sha256": AUTHORIZATION_REAFFIRMATION_SHA256,
        },
    )


def exact_market_sequence_approval_literal(
    *,
    plan_id: str,
    plan_sha256: str,
    request_id: str,
    request_sha256: str,
    resource_caps_digest: str,
) -> str:
    for label, value in (
        ("plan ID", plan_id),
        ("plan SHA-256", plan_sha256),
        ("request ID", request_id),
        ("request SHA-256", request_sha256),
        ("resource caps digest", resource_caps_digest),
    ):
        _digest(value, label)
    return _compact_json(
        {
            "authorized_action": AUTHORIZED_ACTION,
            "literal_version": "s7_full_market_sequence_gate_c_exact_approval_v1",
            "plan_id": plan_id,
            "plan_sha256": plan_sha256,
            "request_id": request_id,
            "request_sha256": request_sha256,
            "resource_caps_digest": resource_caps_digest,
        }
    )


def authorize_market_sequence_plan_with_exact_literal(
    data_root: Path,
    *,
    plan_path: str,
    plan_id: str,
    plan_sha256: str,
    request_path: str,
    request_id: str,
    request_sha256: str,
    approval_literal: str,
    approved_by: str,
) -> S7MarketSequenceAuthorization:
    root = _validated_root(data_root)
    plan = _load_and_verify_plan(root, plan_path, plan_id=plan_id, plan_sha256=plan_sha256)
    request = _load_and_verify_authorization_request(
        root,
        request_path=request_path,
        request_id=request_id,
        request_sha256=request_sha256,
        plan=plan,
        plan_path=plan_path,
        plan_id=plan_id,
        plan_sha256=plan_sha256,
    )
    expected = exact_market_sequence_approval_literal(
        plan_id=plan_id,
        plan_sha256=plan_sha256,
        request_id=request_id,
        request_sha256=request_sha256,
        resource_caps_digest=str(request["resource_caps_digest"]),
    )
    if approval_literal != expected:
        raise IdentityMarketSequenceError("Gate-C exact approval literal differs")
    return _record_market_sequence_authorization(
        root,
        plan_path=plan_path,
        plan_id=plan_id,
        plan_sha256=plan_sha256,
        request_path=request_path,
        request_id=request_id,
        request_sha256=request_sha256,
        recorded_by=approved_by,
        authorization_basis={
            "approval_literal": approval_literal,
            "approval_literal_sha256": hashlib.sha256(approval_literal.encode()).hexdigest(),
            "kind": "exact_plan_literal",
        },
        _verified=(plan, request),
    )


def _record_market_sequence_authorization(
    data_root: Path,
    *,
    plan_path: str,
    plan_id: str,
    plan_sha256: str,
    request_path: str,
    request_id: str,
    request_sha256: str,
    recorded_by: str,
    authorization_basis: Mapping[str, object],
    _verified: tuple[Mapping[str, object], Mapping[str, object]] | None = None,
) -> S7MarketSequenceAuthorization:
    root = _validated_root(data_root)
    if _verified is None:
        plan = _load_and_verify_plan(root, plan_path, plan_id=plan_id, plan_sha256=plan_sha256)
        request = _load_and_verify_authorization_request(
            root,
            request_path=request_path,
            request_id=request_id,
            request_sha256=request_sha256,
            plan=plan,
            plan_path=plan_path,
            plan_id=plan_id,
            plan_sha256=plan_sha256,
        )
    else:
        plan, request = _verified
    actor = _safe_text(recorded_by, "authorization recorded_by")
    is_standing = authorization_basis.get("kind") == "current_standing_authorization"
    fixed_relative = _standing_authorization_path(plan_id, request_id) if is_standing else None
    if fixed_relative is not None:
        fixed_path = _safe(root, fixed_relative)
        if fixed_path.exists():
            existing, existing_sha = _load_canonical_json_file(
                fixed_path, "existing Gate-C standing authorization"
            )
            existing_id = _digest(existing.get("authorization_id"), "existing authorization ID")
            verified = _load_and_verify_authorization(
                root,
                authorization_path=fixed_relative,
                authorization_id=existing_id,
                authorization_sha256=existing_sha,
                plan=plan,
                plan_path=plan_path,
                plan_id=plan_id,
                plan_sha256=plan_sha256,
            )
            if (
                verified.get("authorization_basis") != dict(authorization_basis)
                or verified.get("recorded_by") != actor
            ):
                raise IdentityMarketSequenceError(
                    "existing fixed-slot standing authorization differs"
                )
            return S7MarketSequenceAuthorization(
                authorization_id=existing_id,
                authorization_path=fixed_relative,
                authorization_sha256=existing_sha,
                plan_id=plan_id,
                request_id=request_id,
                recorded_at_utc=str(verified["recorded_at_utc"]),
                state=AUTHORIZATION_RECEIPT_STATE,
                idempotent=True,
            )
    recorded_at = _utc_now()
    intent_at = _parse_utc(plan["intent_captured_at_utc"], "plan intent time")
    if recorded_at < intent_at:
        raise IdentityMarketSequenceError("Gate-C authorization predates its plan")
    calendar = _load_bound_calendar(root)
    available_session, first_open = calendar.first_open_after(recorded_at)
    payload = {
        "artifact_type": "s7_full_market_sequence_authorization_receipt",
        "authorization_basis": dict(authorization_basis),
        "authorization_rule_version": GATE_C_AUTHORIZATION_RECEIPT_RULE_VERSION,
        "authorized_action": AUTHORIZED_ACTION,
        "bound_plan_control_digest": _plan_control_binding_digest(plan),
        "capabilities": _fail_closed_capabilities(),
        "plan": {"path": plan_path, "plan_id": plan_id, "sha256": plan_sha256},
        "recorded_at_utc": recorded_at.isoformat(),
        "recorded_available_session": available_session.isoformat(),
        "recorded_by": actor,
        "recorded_first_xnys_open_utc": first_open.isoformat(),
        "request": {
            "path": request_path,
            "request_id": request_id,
            "sha256": request_sha256,
        },
        "request_control_binding_digest": request["control_binding_digest"],
        "runtime_provenance": plan["runtime_provenance"],
        "state": AUTHORIZATION_RECEIPT_STATE,
    }
    authorization_id = stable_digest(payload)
    document = {**payload, "authorization_id": authorization_id}
    content = _canonical_bytes(document)
    relative = fixed_relative or (
        "manifests/silver/identity/full-market-sequence-authorizations/exact/"
        f"authorization_id={authorization_id}/manifest.json"
    )
    path = _safe(root, relative)
    if path.exists():
        if not path.is_file() or path.is_symlink() or path.read_bytes() != content:
            raise IdentityMarketSequenceError("existing Gate-C authorization receipt differs")
        idempotent = True
    else:
        _write_exact(path, content)
        idempotent = False
    return S7MarketSequenceAuthorization(
        authorization_id=authorization_id,
        authorization_path=relative,
        authorization_sha256=hashlib.sha256(content).hexdigest(),
        plan_id=plan_id,
        request_id=request_id,
        recorded_at_utc=recorded_at.isoformat(),
        state=AUTHORIZATION_RECEIPT_STATE,
        idempotent=idempotent,
    )


def run_source_bound_market_sequence(
    data_root: Path,
    *,
    plan_path: str,
    plan_id: str,
    plan_sha256: str,
    authorization_path: str,
    authorization_id: str,
    authorization_sha256: str,
) -> S7MarketSequenceCandidate:
    """Execute one exact durable Gate-C plan and stop at review."""

    root = _validated_root(data_root)
    plan = _load_and_verify_plan(root, plan_path, plan_id=plan_id, plan_sha256=plan_sha256)
    authorization = _load_and_verify_authorization(
        root,
        authorization_path=authorization_path,
        authorization_id=authorization_id,
        authorization_sha256=authorization_sha256,
        plan=plan,
        plan_path=plan_path,
        plan_id=plan_id,
        plan_sha256=plan_sha256,
    )
    caps = S7MarketSequenceResourceCaps(**_mapping(plan["resource_caps"], "resource caps"))
    authorization_recorded_at = str(authorization["recorded_at_utc"])
    gate_a = _mapping(plan["gate_a"], "plan Gate-A binding")
    gate_b = _mapping(plan["gate_b"], "plan Gate-B binding")
    gate_b_data = _mapping(gate_b["data"], "plan Gate-B DATA binding")
    plan_availability = _mapping(plan["availability"], "plan availability")
    completion_relative = _completion_path(plan_id, authorization_id)
    completion_target = _safe(root, completion_relative)
    if completion_target.exists() or completion_target.is_symlink():
        return _candidate_from_completion(
            root,
            completion_relative=completion_relative,
            plan=plan,
            plan_path=plan_path,
            plan_id=plan_id,
            plan_sha256=plan_sha256,
            authorization_path=authorization_path,
            authorization_id=authorization_id,
            authorization_sha256=authorization_sha256,
            caps=caps,
            idempotent=True,
        )

    monitor = _ResourceMonitor(
        root=root,
        staging=_safe(
            root,
            f"tmp/silver-s7-market-sequence/.preflight-plan_id={plan_id}",
        ),
        caps=caps,
    )
    monitor.check()
    inputs = _load_and_verify_inputs(
        root,
        inventory_completion_path=str(gate_a["completion_path"]),
        classification_candidate_path=str(gate_b["candidate_path"]),
        classification_candidate_id=str(gate_b["candidate_id"]),
        classification_candidate_sha256=str(gate_b["candidate_sha256"]),
        classification_data_path=str(gate_b_data["path"]),
        classification_data_sha256=str(gate_b_data["sha256"]),
        classification_data_bytes=int(gate_b_data["bytes"]),
        classification_data_row_count=int(gate_b_data["row_count"]),
        classification_source_available_session=str(
            plan_availability["classification_source_available_session"]
        ),
        caps=caps,
        monitor=monitor,
    )
    monitor.check()
    basis = {
        "algorithm": {
            "lineage_rule": (
                "sha256(utf8(rule_version+'|massive|') followed by chronological "
                "bytes.fromhex(selected_source_record_id) for every interval row)"
            ),
            "long_standing_min_sessions": LONG_STANDING_MIN_SESSIONS,
            "necessary_universe_columns": list(UNIVERSE_COLUMNS),
            "rule_version": GATE_C_RULE_VERSION,
        },
        "gate_a": inputs["gate_a_binding"],
        "gate_b": inputs["gate_b_binding"],
        "plan": {
            "path": plan_path,
            "plan_id": plan_id,
            "sha256": plan_sha256,
        },
        "authorization": {
            "authorization_id": authorization_id,
            "path": authorization_path,
            "sha256": authorization_sha256,
            "request": authorization["request"],
        },
        "registry_loader_source_refs": inputs["registry_loader_source_refs"],
        "resource_caps": caps.to_dict(),
        "reviewed_evidence_set_digest": _reviewed_evidence_digest(inputs["reviewed_evidence_rows"]),
        "source_artifact_set_digest": stable_digest([item for item in inputs["universe_refs"]]),
    }
    candidate_id = stable_digest(basis)
    prefix = (
        f"manifests/silver/identity/full-market-sequence-candidates/candidate_id={candidate_id}"
    )
    manifest_relative = f"{prefix}/manifest.json"
    final_directory = _safe(root, prefix)
    lock_path = _safe(root, f"manifests/silver/locks/s7-market-sequence-{candidate_id}.lock")

    with _exclusive_nonblocking_lock(lock_path):
        if final_directory.is_dir() and not final_directory.is_symlink():
            existing = _candidate_from_existing(
                root,
                manifest_relative,
                candidate_id=candidate_id,
                basis=basis,
                created_at=str(plan["intent_captured_at_utc"]),
                actor=str(plan["prepared_by"]),
                classification_source_available_session=str(
                    plan_availability["classification_source_available_session"]
                ),
                authorization_recorded_at=authorization_recorded_at,
                caps=caps,
                idempotent=True,
            )
            _replay_and_compare_existing(
                root,
                final_manifest_relative=manifest_relative,
                prefix=prefix,
                candidate_id=candidate_id,
                basis=basis,
                universe_refs=inputs["universe_refs"],
                classifications=inputs["classifications"],
                reviewed_evidence_rows=inputs["reviewed_evidence_rows"],
                created_at=str(plan["intent_captured_at_utc"]),
                actor=str(plan["prepared_by"]),
                classification_source_available_session=str(
                    plan_availability["classification_source_available_session"]
                ),
                authorization_recorded_at=authorization_recorded_at,
                caps=caps,
                monitor=monitor,
            )
            return _write_or_load_completion(
                root,
                candidate=existing,
                plan=plan,
                plan_path=plan_path,
                plan_id=plan_id,
                plan_sha256=plan_sha256,
                authorization_path=authorization_path,
                authorization_id=authorization_id,
                authorization_sha256=authorization_sha256,
                idempotent=True,
            )
        if final_directory.exists() or final_directory.is_symlink():
            raise IdentityMarketSequenceError("candidate slot exists but is unsafe")
        staging_parent = _safe(root, "tmp/silver-s7-market-sequence")
        staging_parent.mkdir(parents=True, exist_ok=True)
        staging = staging_parent / f"candidate_id={candidate_id}.staging"
        if staging.exists() or staging.is_symlink():
            raise IdentityMarketSequenceError(
                "prior incomplete Gate-C staging slot requires explicit forensic review"
            )
        staging.mkdir(mode=0o700)
        monitor.staging = staging
        monitor.check()
        try:
            _execute_scan(
                root,
                staging,
                prefix=prefix,
                candidate_id=candidate_id,
                basis=basis,
                universe_refs=inputs["universe_refs"],
                classifications=inputs["classifications"],
                reviewed_evidence_rows=inputs["reviewed_evidence_rows"],
                created_at=str(plan["intent_captured_at_utc"]),
                actor=str(plan["prepared_by"]),
                classification_source_available_session=str(
                    plan_availability["classification_source_available_session"]
                ),
                authorization_recorded_at=authorization_recorded_at,
                caps=caps,
                monitor=monitor,
            )
            final_directory.parent.mkdir(parents=True, exist_ok=True)
            _rename_directory_noreplace(staging, final_directory)
            _fsync_directory(final_directory.parent)
        except Exception:
            # Staging has a unique name and cannot be mistaken for a candidate.  Preserve it
            # for forensic review instead of silently deleting partial output.
            raise
    candidate = _candidate_from_existing(
        root,
        manifest_relative,
        candidate_id=candidate_id,
        basis=basis,
        created_at=str(plan["intent_captured_at_utc"]),
        actor=str(plan["prepared_by"]),
        classification_source_available_session=str(
            plan_availability["classification_source_available_session"]
        ),
        authorization_recorded_at=authorization_recorded_at,
        caps=caps,
        idempotent=False,
    )
    return _write_or_load_completion(
        root,
        candidate=candidate,
        plan=plan,
        plan_path=plan_path,
        plan_id=plan_id,
        plan_sha256=plan_sha256,
        authorization_path=authorization_path,
        authorization_id=authorization_id,
        authorization_sha256=authorization_sha256,
        idempotent=False,
    )


def _load_and_verify_plan(
    root: Path,
    plan_path: str,
    *,
    plan_id: str,
    plan_sha256: str,
) -> dict[str, object]:
    _digest(plan_id, "Gate-C plan ID")
    _digest(plan_sha256, "Gate-C plan SHA-256")
    expected_path = (
        f"manifests/silver/identity/full-market-sequence-plans/plan_id={plan_id}/manifest.json"
    )
    if plan_path != expected_path:
        raise IdentityMarketSequenceError("Gate-C plan path is not canonical")
    document, actual_sha = _load_canonical_json_file(_safe(root, plan_path), "Gate-C plan")
    if actual_sha != plan_sha256 or document.get("plan_id") != plan_id:
        raise IdentityMarketSequenceError("Gate-C plan exact binding differs")
    payload = dict(document)
    payload.pop("plan_id", None)
    if stable_digest(payload) != plan_id:
        raise IdentityMarketSequenceError("Gate-C plan ID recomputation failed")
    if (
        document.get("artifact_type") != "s7_full_market_sequence_execution_plan"
        or document.get("plan_rule_version") != GATE_C_PLAN_RULE_VERSION
        or document.get("state") != PLAN_STATE
    ):
        raise IdentityMarketSequenceError("Gate-C plan type/state differs")
    authorization = _mapping(
        document.get("authorization_requirement"), "plan authorization requirement"
    )
    if authorization != {
        "authorized_action": AUTHORIZED_ACTION,
        "accepted_modes": ["current_standing_receipt", "exact_plan_literal"],
        "execution_without_plan_bound_receipt": False,
    }:
        raise IdentityMarketSequenceError("Gate-C authorization requirement differs")
    if document.get("capabilities") != _fail_closed_capabilities():
        raise IdentityMarketSequenceError("Gate-C plan capabilities are not fail-closed")
    calendar = _load_bound_calendar(root)
    if document.get("calendar_binding") != _calendar_binding(calendar):
        raise IdentityMarketSequenceError("Gate-C calendar binding differs")
    actor = _safe_text(document.get("prepared_by"), "plan prepared_by")
    if actor != document["prepared_by"]:  # pragma: no cover - documents intent
        raise AssertionError("unreachable")
    captured_at = _parse_utc(document.get("intent_captured_at_utc"), "plan intent time")
    availability = _mapping(document.get("availability"), "plan availability")
    expected_session, expected_open = calendar.first_open_after(captured_at)
    if (
        availability.get("intent_controlling_timestamp_utc") != captured_at.isoformat()
        or availability.get("intent_available_session") != expected_session.isoformat()
        or availability.get("intent_first_xnys_open_utc") != expected_open.isoformat()
    ):
        raise IdentityMarketSequenceError("Gate-C intent availability is not reproducible")
    _xnys_session_text(
        availability.get("classification_source_available_session"),
        "classification source available session",
        calendar=calendar,
    )
    expected = PRODUCTION_EXPECTATIONS
    expected_gate_a = {
        "candidate_id": expected.inventory_candidate_id,
        "candidate_path": (
            "manifests/silver/identity/composite-inventory-candidates/"
            f"candidate_id={expected.inventory_candidate_id}/manifest.json"
        ),
        "candidate_sha256": expected.inventory_candidate_sha256,
        "completion_id": expected.inventory_completion_id,
        "completion_path": expected.inventory_completion_path,
        "completion_sha256": expected.inventory_completion_sha256,
        "source_artifact_count": expected.source_artifact_count,
        "source_artifact_set_digest": expected.source_artifact_set_digest,
        "source_bytes": expected.source_bytes,
        "source_row_count": expected.source_row_count,
        "universe_artifact_count": expected.universe_artifact_count,
        "universe_row_count": expected.universe_row_count,
    }
    if document.get("gate_a") != expected_gate_a:
        raise IdentityMarketSequenceError("Gate-C plan Gate-A binding differs")
    gate_b = _mapping(document.get("gate_b"), "plan Gate-B binding")
    if set(gate_b) != {"candidate_id", "candidate_path", "candidate_sha256", "data"}:
        raise IdentityMarketSequenceError("Gate-C plan Gate-B schema differs")
    gate_b_id = _digest(gate_b["candidate_id"], "plan Gate-B candidate ID")
    _digest(gate_b["candidate_sha256"], "plan Gate-B candidate SHA-256")
    if gate_b["candidate_path"] != (
        "manifests/silver/identity/openfigi-market-consistency-candidates/"
        f"candidate_id={gate_b_id}/manifest.json"
    ):
        raise IdentityMarketSequenceError("Gate-C plan Gate-B candidate path differs")
    gate_b_data = _mapping(gate_b["data"], "plan Gate-B DATA")
    if set(gate_b_data) != {"bytes", "path", "row_count", "sha256"}:
        raise IdentityMarketSequenceError("Gate-C plan Gate-B DATA schema differs")
    _nonnegative_int(gate_b_data["bytes"], "plan Gate-B DATA bytes")
    _positive_int(gate_b_data["row_count"], "plan Gate-B DATA rows")
    _digest(gate_b_data["sha256"], "plan Gate-B DATA SHA-256")
    if gate_b_data["path"] != (
        "manifests/silver/identity/openfigi-market-consistency-candidates/"
        f"candidate_id={gate_b_id}/data/classification.parquet"
    ):
        raise IdentityMarketSequenceError("Gate-C plan Gate-B DATA path differs")
    caps = _mapping(document.get("resource_caps"), "plan resource caps")
    S7MarketSequenceResourceCaps(**caps)
    if document.get("reviewed_source_evidence") != _reviewed_source_plan_binding():
        raise IdentityMarketSequenceError("Gate-C reviewed source evidence binding differs")
    if document.get("runtime_provenance") != _runtime_provenance():
        raise IdentityMarketSequenceError("Gate-C runtime provenance differs from plan")
    _verify_preparation_slot(
        root,
        document,
        plan_path=plan_path,
        plan_id=plan_id,
        plan_sha256=plan_sha256,
    )
    return document


def _plan_control_binding_digest(plan: Mapping[str, object]) -> str:
    return stable_digest(
        {
            "authorization_requirement": plan["authorization_requirement"],
            "calendar_binding": plan["calendar_binding"],
            "capabilities": plan["capabilities"],
            "gate_a": plan["gate_a"],
            "gate_b": plan["gate_b"],
            "preparation_scope": plan["preparation_scope"],
            "resource_caps": plan["resource_caps"],
            "reviewed_source_evidence": plan["reviewed_source_evidence"],
            "runtime_provenance": plan["runtime_provenance"],
        }
    )


def _preparation_scope_from_plan(plan: Mapping[str, object]) -> dict[str, object]:
    availability = _mapping(plan.get("availability"), "plan availability")
    return {
        "artifact_type": "s7_full_market_sequence_preparation_scope",
        "authorization_requirement": plan["authorization_requirement"],
        "calendar_binding": plan["calendar_binding"],
        "capabilities": plan["capabilities"],
        "classification_source_available_session": availability[
            "classification_source_available_session"
        ],
        "gate_a": plan["gate_a"],
        "gate_b": plan["gate_b"],
        "plan_rule_version": plan["plan_rule_version"],
        "resource_caps": plan["resource_caps"],
        "reviewed_source_evidence": plan["reviewed_source_evidence"],
        "runtime_provenance": plan["runtime_provenance"],
    }


def _verify_preparation_slot(
    root: Path,
    plan: Mapping[str, object],
    *,
    plan_path: str,
    plan_id: str,
    plan_sha256: str,
) -> dict[str, object]:
    ref = _mapping(plan.get("preparation_scope"), "plan preparation scope")
    if set(ref) != {"path", "scope_id"}:
        raise IdentityMarketSequenceError("Gate-C preparation scope ref schema differs")
    scope = _preparation_scope_from_plan(plan)
    scope_id = stable_digest(scope)
    expected_path = _preparation_slot_path(scope_id)
    if ref != {"path": expected_path, "scope_id": scope_id}:
        raise IdentityMarketSequenceError("Gate-C preparation scope binding differs")
    document, _ = _load_canonical_json_file(
        _safe(root, expected_path), "Gate-C fixed preparation slot"
    )
    payload = dict(document)
    preparation_id = payload.pop("preparation_id", None)
    availability = _mapping(plan.get("availability"), "plan availability")
    request = _mapping(document.get("request"), "preparation request ref")
    if (
        stable_digest(payload) != preparation_id
        or document.get("artifact_type") != "s7_full_market_sequence_preparation_slot"
        or document.get("scope") != scope
        or document.get("scope_id") != scope_id
        or document.get("first_writer") != plan.get("prepared_by")
        or document.get("intent_captured_at_utc") != plan.get("intent_captured_at_utc")
        or document.get("intent_available_session") != availability.get("intent_available_session")
        or document.get("intent_first_xnys_open_utc")
        != availability.get("intent_first_xnys_open_utc")
        or document.get("plan") != {"path": plan_path, "plan_id": plan_id, "sha256": plan_sha256}
        or document.get("state") != "intent_recorded_plan_request_bound"
        or set(request) != {"path", "request_id", "sha256"}
    ):
        raise IdentityMarketSequenceError("Gate-C fixed preparation slot differs")
    request_id = _digest(request["request_id"], "preparation request ID")
    _digest(request["sha256"], "preparation request SHA-256")
    if request["path"] != (
        "manifests/silver/identity/full-market-sequence-authorization-requests/"
        f"request_id={request_id}/manifest.json"
    ):
        raise IdentityMarketSequenceError("Gate-C preparation request path differs")
    return document


def _load_and_verify_authorization_request(
    root: Path,
    *,
    request_path: str,
    request_id: str,
    request_sha256: str,
    plan: Mapping[str, object],
    plan_path: str,
    plan_id: str,
    plan_sha256: str,
) -> dict[str, object]:
    _digest(request_id, "Gate-C request ID")
    _digest(request_sha256, "Gate-C request SHA-256")
    expected_path = (
        "manifests/silver/identity/full-market-sequence-authorization-requests/"
        f"request_id={request_id}/manifest.json"
    )
    if request_path != expected_path:
        raise IdentityMarketSequenceError("Gate-C authorization request path differs")
    document, actual_sha = _load_canonical_json_file(
        _safe(root, request_path), "Gate-C authorization request"
    )
    payload = dict(document)
    embedded_id = payload.pop("request_id", None)
    expected = {
        "artifact_type": "s7_full_market_sequence_authorization_request",
        "authorized_action": AUTHORIZED_ACTION,
        "capabilities": _fail_closed_capabilities(),
        "control_binding_digest": _plan_control_binding_digest(plan),
        "plan": {"path": plan_path, "plan_id": plan_id, "sha256": plan_sha256},
        "request_rule_version": GATE_C_AUTHORIZATION_REQUEST_RULE_VERSION,
        "resource_caps_digest": stable_digest(plan["resource_caps"]),
        "state": AUTHORIZATION_REQUEST_STATE,
    }
    if (
        actual_sha != request_sha256
        or embedded_id != request_id
        or stable_digest(payload) != request_id
        or payload != expected
    ):
        raise IdentityMarketSequenceError("Gate-C authorization request exact binding differs")
    preparation = _verify_preparation_slot(
        root,
        plan,
        plan_path=plan_path,
        plan_id=plan_id,
        plan_sha256=plan_sha256,
    )
    if preparation.get("request") != {
        "path": request_path,
        "request_id": request_id,
        "sha256": request_sha256,
    }:
        raise IdentityMarketSequenceError(
            "Gate-C preparation slot authorization request binding differs"
        )
    return document


def _load_and_verify_authorization(
    root: Path,
    *,
    authorization_path: str,
    authorization_id: str,
    authorization_sha256: str,
    plan: Mapping[str, object],
    plan_path: str,
    plan_id: str,
    plan_sha256: str,
) -> dict[str, object]:
    _digest(authorization_id, "Gate-C authorization ID")
    _digest(authorization_sha256, "Gate-C authorization SHA-256")
    _relative_path(authorization_path, "Gate-C authorization path")
    document, actual_sha = _load_canonical_json_file(
        _safe(root, authorization_path), "Gate-C authorization receipt"
    )
    payload = dict(document)
    embedded_id = payload.pop("authorization_id", None)
    if (
        actual_sha != authorization_sha256
        or embedded_id != authorization_id
        or stable_digest(payload) != authorization_id
        or document.get("artifact_type") != "s7_full_market_sequence_authorization_receipt"
        or document.get("authorization_rule_version") != GATE_C_AUTHORIZATION_RECEIPT_RULE_VERSION
        or document.get("authorized_action") != AUTHORIZED_ACTION
        or document.get("state") != AUTHORIZATION_RECEIPT_STATE
        or document.get("capabilities") != _fail_closed_capabilities()
        or document.get("plan") != {"path": plan_path, "plan_id": plan_id, "sha256": plan_sha256}
        or document.get("bound_plan_control_digest") != _plan_control_binding_digest(plan)
        or document.get("runtime_provenance") != plan["runtime_provenance"]
    ):
        raise IdentityMarketSequenceError("Gate-C authorization exact binding differs")
    request_ref = _mapping(document.get("request"), "authorization request ref")
    if set(request_ref) != {"path", "request_id", "sha256"}:
        raise IdentityMarketSequenceError("Gate-C authorization request ref schema differs")
    request = _load_and_verify_authorization_request(
        root,
        request_path=str(request_ref["path"]),
        request_id=str(request_ref["request_id"]),
        request_sha256=str(request_ref["sha256"]),
        plan=plan,
        plan_path=plan_path,
        plan_id=plan_id,
        plan_sha256=plan_sha256,
    )
    if document.get("request_control_binding_digest") != request["control_binding_digest"]:
        raise IdentityMarketSequenceError("Gate-C authorization request control differs")
    basis = _mapping(document.get("authorization_basis"), "authorization basis")
    if basis == {
        "grant": AUTHORIZATION_LITERAL,
        "grant_sha256": AUTHORIZATION_LITERAL_SHA256,
        "kind": "current_standing_authorization",
        "reaffirmation": AUTHORIZATION_REAFFIRMATION,
        "reaffirmation_sha256": AUTHORIZATION_REAFFIRMATION_SHA256,
    }:
        pass
    elif basis.get("kind") == "exact_plan_literal":
        expected_literal = exact_market_sequence_approval_literal(
            plan_id=plan_id,
            plan_sha256=plan_sha256,
            request_id=str(request_ref["request_id"]),
            request_sha256=str(request_ref["sha256"]),
            resource_caps_digest=str(request["resource_caps_digest"]),
        )
        if basis != {
            "approval_literal": expected_literal,
            "approval_literal_sha256": hashlib.sha256(expected_literal.encode()).hexdigest(),
            "kind": "exact_plan_literal",
        }:
            raise IdentityMarketSequenceError("Gate-C exact authorization basis differs")
    else:
        raise IdentityMarketSequenceError("Gate-C standing authorization basis differs")
    expected_path = (
        _standing_authorization_path(plan_id, str(request_ref["request_id"]))
        if basis.get("kind") == "current_standing_authorization"
        else (
            "manifests/silver/identity/full-market-sequence-authorizations/exact/"
            f"authorization_id={authorization_id}/manifest.json"
        )
    )
    if authorization_path != expected_path:
        raise IdentityMarketSequenceError("Gate-C authorization path differs")
    recorded_at = _parse_utc(document.get("recorded_at_utc"), "authorization recorded time")
    intent_at = _parse_utc(plan["intent_captured_at_utc"], "plan intent time")
    if recorded_at < intent_at:
        raise IdentityMarketSequenceError("Gate-C authorization predates its plan")
    calendar = _load_bound_calendar(root)
    try:
        expected_session, expected_open = calendar.first_open_after(recorded_at)
    except XNYSCalendarArtifactError as exc:
        raise IdentityMarketSequenceError(
            "Gate-C authorization is outside the bound calendar"
        ) from exc
    if (
        document.get("recorded_available_session") != expected_session.isoformat()
        or document.get("recorded_first_xnys_open_utc") != expected_open.isoformat()
        or _safe_text(document.get("recorded_by"), "authorization recorded_by")
        != document["recorded_by"]
    ):
        raise IdentityMarketSequenceError("Gate-C authorization availability differs")
    return document


def _standing_authorization_path(plan_id: str, request_id: str) -> str:
    _digest(plan_id, "standing authorization plan ID")
    _digest(request_id, "standing authorization request ID")
    return (
        "manifests/silver/identity/full-market-sequence-authorizations/standing/"
        f"plan_id={plan_id}/request_id={request_id}/manifest.json"
    )


def _preparation_slot_path(scope_id: str) -> str:
    _digest(scope_id, "Gate-C preparation scope ID")
    return (
        "manifests/silver/identity/full-market-sequence-preparations/"
        f"scope_id={scope_id}/manifest.json"
    )


def _load_and_verify_inputs(
    root: Path,
    *,
    inventory_completion_path: str,
    classification_candidate_path: str,
    classification_candidate_id: str,
    classification_candidate_sha256: str,
    classification_data_path: str,
    classification_data_sha256: str,
    classification_data_bytes: int,
    classification_data_row_count: int,
    classification_source_available_session: str,
    caps: S7MarketSequenceResourceCaps,
    monitor: _ResourceMonitor,
) -> dict[str, Any]:
    monitor.check()
    expected = PRODUCTION_EXPECTATIONS
    inventory_relative = (
        "manifests/silver/identity/composite-inventory-candidates/"
        f"candidate_id={expected.inventory_candidate_id}/manifest.json"
    )
    inventory_path = _safe(root, inventory_relative)
    inventory_document, inventory_sha = _load_canonical_json_file(
        inventory_path, "Gate-A candidate"
    )
    if inventory_sha != expected.inventory_candidate_sha256:
        raise IdentityMarketSequenceError("Gate-A candidate SHA-256 differs")
    if inventory_document.get("candidate_id") != expected.inventory_candidate_id:
        raise IdentityMarketSequenceError("Gate-A candidate ID differs")
    inventory_payload = dict(inventory_document)
    inventory_payload.pop("candidate_id", None)
    inventory_payload.pop("canonical_paths", None)
    if stable_digest(inventory_payload) != expected.inventory_candidate_id:
        raise IdentityMarketSequenceError("Gate-A candidate ID recomputation failed")
    if inventory_document.get("candidate_state") != CANDIDATE_STATE:
        raise IdentityMarketSequenceError("Gate-A candidate is not awaiting_review")

    raw_refs = inventory_document.get("source_artifacts")
    if not isinstance(raw_refs, list):
        raise IdentityMarketSequenceError("Gate-A source_artifacts must be an array")
    refs = [_source_ref(value) for value in raw_refs]
    if len(refs) != expected.source_artifact_count:
        raise IdentityMarketSequenceError("Gate-A source artifact count differs")
    if stable_digest(refs) != expected.source_artifact_set_digest:
        raise IdentityMarketSequenceError("Gate-A source artifact set digest differs")
    if sum(item["row_count"] for item in refs) != expected.source_row_count:
        raise IdentityMarketSequenceError("Gate-A source row count differs")
    if sum(item["bytes"] for item in refs) != expected.source_bytes:
        raise IdentityMarketSequenceError("Gate-A source byte count differs")
    counts = _mapping(inventory_document.get("counts"), "Gate-A counts")
    if (
        counts.get("source_artifact_count") != expected.source_artifact_count
        or counts.get("source_row_count") != expected.source_row_count
        or counts.get("source_bytes") != expected.source_bytes
    ):
        raise IdentityMarketSequenceError("Gate-A manifest counts differ")
    gate_a_outputs = _verify_gate_a_outputs(
        root, inventory_document, expected.inventory_candidate_id
    )
    monitor.check()

    completion_path = _safe(root, inventory_completion_path)
    completion, completion_sha = _load_canonical_json_file(completion_path, "Gate-A completion")
    if (
        completion_sha != expected.inventory_completion_sha256
        or completion.get("completion_id") != expected.inventory_completion_id
    ):
        raise IdentityMarketSequenceError("Gate-A completion exact binding differs")
    completion_payload = dict(completion)
    completion_payload.pop("completion_id", None)
    if stable_digest(completion_payload) != expected.inventory_completion_id:
        raise IdentityMarketSequenceError("Gate-A completion ID recomputation failed")
    _verify_completion_path(inventory_completion_path, completion)
    _verify_gate_a_completion_binding(
        completion,
        inventory_relative=inventory_relative,
        inventory_sha=inventory_sha,
        expected=expected,
    )
    completion_candidate = _mapping(completion.get("candidate"), "completion candidate")
    completion_data = _mapping(completion_candidate.get("data"), "completion DATA ref")
    gate_a_data = gate_a_outputs["data"]
    if any(
        completion_data.get(field) != gate_a_data[field] for field in ("bytes", "path", "sha256")
    ):
        raise IdentityMarketSequenceError("Gate-A candidate/completion DATA binding differs")
    monitor.check()

    universe_refs = [
        item
        for item in refs
        if item["table"] == UNIVERSE_TABLE
        and item["release_id"] == expected.universe_release_id
        and item["release_manifest_sha256"] == expected.universe_release_manifest_sha256
    ]
    universe_refs.sort(key=lambda item: item["session_date"])
    if (
        len(universe_refs) != expected.universe_artifact_count
        or sum(item["row_count"] for item in universe_refs) != expected.universe_row_count
    ):
        raise IdentityMarketSequenceError("exact universe artifact selection differs")
    sessions = [date.fromisoformat(item["session_date"]) for item in universe_refs]
    if (
        len(set(sessions)) != len(sessions)
        or not sessions
        or sessions[0] != expected.start_session
        or sessions[-1] != expected.end_session
    ):
        raise IdentityMarketSequenceError("exact universe session spine differs")
    monitor.check()

    classification_path = _safe(root, classification_candidate_path)
    expected_classification_path = (
        "manifests/silver/identity/openfigi-market-consistency-candidates/"
        f"candidate_id={classification_candidate_id}/manifest.json"
    )
    if classification_candidate_path != expected_classification_path:
        raise IdentityMarketSequenceError("Gate-B candidate path is not canonical")
    classification_document, actual_classification_sha = _load_canonical_json_file(
        classification_path, "Gate-B candidate"
    )
    if (
        actual_classification_sha != classification_candidate_sha256
        or classification_document.get("candidate_id") != classification_candidate_id
        or classification_document.get("state") != CANDIDATE_STATE
        or classification_document.get("source_available_session")
        != classification_source_available_session
    ):
        raise IdentityMarketSequenceError("Gate-B candidate exact binding differs")
    candidate_payload = dict(classification_document)
    manifest_id = candidate_payload.pop("manifest_id", None)
    if stable_digest(candidate_payload) != manifest_id:
        raise IdentityMarketSequenceError("Gate-B manifest ID recomputation failed")
    candidate_basis = _mapping(
        classification_document.get("candidate_basis"), "Gate-B candidate basis"
    )
    if stable_digest(candidate_basis) != classification_candidate_id:
        raise IdentityMarketSequenceError("Gate-B candidate ID recomputation failed")
    expected_inventory_binding = {
        "candidate": {
            "bytes": inventory_path.stat().st_size,
            "candidate_id": expected.inventory_candidate_id,
            "path": inventory_relative,
            "sha256": inventory_sha,
        },
        "completion": {
            "bytes": completion_path.stat().st_size,
            "completion_id": expected.inventory_completion_id,
            "path": inventory_completion_path,
            "sha256": completion_sha,
        },
        "data": gate_a_data,
        "mode": "production",
    }
    if (
        candidate_basis.get("inventory_binding") != expected_inventory_binding
        or classification_document.get("inventory_binding") != expected_inventory_binding
    ):
        raise IdentityMarketSequenceError("Gate-B inventory binding differs from exact Gate-A")
    manifest_data = _mapping(classification_document.get("data"), "Gate-B DATA receipt")
    expected_data_receipt = {
        "bytes": classification_data_bytes,
        "path": classification_data_path,
        "row_count": classification_data_row_count,
        "sha256": classification_data_sha256,
    }
    expected_data_path = (
        "manifests/silver/identity/openfigi-market-consistency-candidates/"
        f"candidate_id={classification_candidate_id}/data/classification.parquet"
    )
    if classification_data_path != expected_data_path:
        raise IdentityMarketSequenceError("Gate-B DATA path is not canonical")
    if manifest_data != expected_data_receipt:
        raise IdentityMarketSequenceError("Gate-B DATA receipt differs from exact arguments")
    try:
        verified_gate_b = market_consistency_module.verify_market_classification_candidate(
            root,
            candidate_path=classification_candidate_path,
            candidate_id=classification_candidate_id,
            candidate_sha256=classification_candidate_sha256,
            require_production_approval=True,
        )
    except market_consistency_module.IdentityMarketConsistencyError as exc:
        raise IdentityMarketSequenceError(
            "Gate-B official candidate replay verification failed"
        ) from exc
    if (
        verified_gate_b.candidate_id != classification_candidate_id
        or verified_gate_b.manifest_path != classification_candidate_path
        or verified_gate_b.data_path != classification_data_path
    ):
        raise IdentityMarketSequenceError("Gate-B official verifier result binding differs")
    monitor.check()
    data_path = _verify_receipt(root, expected_data_receipt, parquet=True)
    if classification_data_row_count > caps.max_classification_rows:
        raise IdentityMarketSequenceError("resource_cap_exceeded: classification rows")
    classifications = _load_classifications(data_path, classification_data_row_count)
    _verify_receipt(root, expected_data_receipt, parquet=True)
    if any(
        item.source_available_session != classification_source_available_session
        for item in classifications.values()
    ):
        raise IdentityMarketSequenceError(
            "Gate-B row availability differs from the bound candidate availability"
        )
    monitor.check()
    reviewed_evidence_rows, reviewed_evidence_binding = _load_reviewed_case_evidence(root)
    monitor.check()

    return {
        "classifications": classifications,
        "gate_a_binding": {
            "candidate_id": expected.inventory_candidate_id,
            "candidate_path": inventory_relative,
            "candidate_sha256": inventory_sha,
            "completion_id": expected.inventory_completion_id,
            "completion_path": inventory_completion_path,
            "completion_sha256": completion_sha,
            "source_artifact_count": expected.source_artifact_count,
            "source_artifact_set_digest": expected.source_artifact_set_digest,
            "source_bytes": expected.source_bytes,
            "source_row_count": expected.source_row_count,
        },
        "gate_b_binding": {
            "candidate_id": classification_candidate_id,
            "candidate_path": classification_candidate_path,
            "candidate_sha256": classification_candidate_sha256,
            "candidate_bytes": classification_path.stat().st_size,
            "data": expected_data_receipt,
            "manifest_id": manifest_id,
        },
        "registry_loader_source_refs": {
            "gate_a_candidate": {
                "bytes": inventory_path.stat().st_size,
                "candidate_id": expected.inventory_candidate_id,
                "path": inventory_relative,
                "sha256": inventory_sha,
            },
            "gate_a_completion": {
                "bytes": completion_path.stat().st_size,
                "completion_id": expected.inventory_completion_id,
                "path": inventory_completion_path,
                "sha256": completion_sha,
            },
            "gate_b_candidate": {
                "bytes": classification_path.stat().st_size,
                "candidate_id": classification_candidate_id,
                "path": classification_candidate_path,
                "sha256": actual_classification_sha,
            },
            "gate_b_data": expected_data_receipt,
            **reviewed_evidence_binding,
        },
        "reviewed_evidence_rows": reviewed_evidence_rows,
        "universe_refs": universe_refs,
    }


def _reviewed_source_plan_binding() -> dict[str, object]:
    return {
        "detector_preview": {
            "case_count": PREVIEW_CASE_COUNT,
            "case_evidence_set_digest": PREVIEW_CASE_EVIDENCE_SET_DIGEST,
            "path": PRIOR_PREVIEW_PATH,
            "preview_artifact_id": PREVIEW_ARTIFACT_ID,
            "sha256": PREVIEW_ARTIFACT_SHA256,
            "suspected_row_count": PREVIEW_SUSPECTED_ROW_COUNT,
        },
        "detector_preview_completion": {
            "completion_id": PREVIEW_COMPLETION_ID,
            "path": PRIOR_PREVIEW_COMPLETION_PATH,
            "sha256": PREVIEW_COMPLETION_SHA256,
        },
        "external_evidence_manifest": {
            "manifest_id": EXTERNAL_EVIDENCE_MANIFEST_ID,
            "path": EXTERNAL_EVIDENCE_MANIFEST_PATH,
            "sha256": EXTERNAL_EVIDENCE_MANIFEST_SHA256,
        },
        "expected_reviewed_counts": {
            "confirmed_non_us_observation_rows": REVIEWED_FOREIGN_ROW_COUNT,
            "correct_us_inverse_observation_rows": REVIEWED_INVERSE_US_ROW_COUNT,
            "direct_case_count": REVIEWED_DIRECT_CASE_COUNT,
            "group_count": 9,
            "identity_case_count": PREVIEW_CASE_COUNT,
            "inverse_case_count": REVIEWED_INVERSE_CASE_COUNT,
        },
        "output_semantics": (
            "bounded exact S4 asset_observation_daily and universe_source_daily row "
            "snapshots for the 79 reviewed foreign observations, with all 19 exact "
            "detector case IDs and raw/semantic roles; no canonical override"
        ),
    }


def _load_reviewed_case_evidence(
    root: Path,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Replay the exact 79 reviewed S4 observations and all 19 case relations."""

    calendar = _load_bound_calendar(root)
    external_path = _repository_root() / EXTERNAL_EVIDENCE_MANIFEST_PATH
    external_content = _read_exact_bytes(
        external_path,
        EXTERNAL_EVIDENCE_MANIFEST_SHA256,
        "cross-market external evidence manifest",
    )
    external = _decode_json_object(external_content, "cross-market external evidence manifest")
    external_payload = dict(external)
    manifest_id = external_payload.pop("manifest_id", None)
    if (
        manifest_id != EXTERNAL_EVIDENCE_MANIFEST_ID
        or stable_digest(external_payload) != EXTERNAL_EVIDENCE_MANIFEST_ID
        or external.get("manifest_type") != "identity_cross_market_external_evidence"
        or external.get("manifest_status") != "candidate_not_approved"
        or external.get("detector_preview_binding")
        != {
            "completion_id": PREVIEW_COMPLETION_ID,
            "identity_case_count": PREVIEW_CASE_COUNT,
            "preview_id": PREVIEW_ARTIFACT_ID,
            "preview_rewritten": False,
            "suspected_row_count": PREVIEW_SUSPECTED_ROW_COUNT,
        }
        or external.get("review_summary")
        != {
            "confirmed_non_us_observation_rows": REVIEWED_FOREIGN_ROW_COUNT,
            "correct_us_inverse_observation_rows": REVIEWED_INVERSE_US_ROW_COUNT,
            "group_count": 9,
            "identity_case_count": PREVIEW_CASE_COUNT,
        }
    ):
        raise IdentityMarketSequenceError("cross-market external evidence binding differs")
    groups_raw = external.get("groups")
    if not isinstance(groups_raw, list) or len(groups_raw) != 9:
        raise IdentityMarketSequenceError("cross-market reviewed group set differs")
    groups: dict[tuple[str, str], dict[str, object]] = {}
    for raw in groups_raw:
        group = _mapping(raw, "cross-market reviewed group")
        ticker = _safe_text(group.get("ticker"), "reviewed group ticker")
        foreign = _figi(group.get("observed_foreign_composite_figi"), "reviewed foreign FIGI")
        canonical = _figi(group.get("canonical_us_composite_figi"), "reviewed US FIGI")
        share_class = _figi(group.get("share_class_figi"), "reviewed Share Class FIGI")
        expected_rows = _positive_int(
            group.get("current_preview_foreign_observation_rows"),
            "reviewed foreign observation rows",
        )
        start = date.fromisoformat(_date_text(group.get("valid_from_session"), "valid_from"))
        end = date.fromisoformat(_date_text(group.get("valid_through_session"), "valid_through"))
        if (
            group.get("provider_id") != PROVIDER
            or group.get("provider_market") != "stocks"
            or group.get("provider_locale") != "us"
            or group.get("canonical_composite_market_code") != "US"
            or start > end
        ):
            raise IdentityMarketSequenceError("cross-market reviewed group scope differs")
        key = (ticker, foreign)
        if key in groups:
            raise IdentityMarketSequenceError("cross-market reviewed group repeats")
        groups[key] = {
            "canonical": canonical,
            "expected_rows": expected_rows,
            "share_class": share_class,
            "start": start,
            "end": end,
        }

    completion_path = _safe(root, PRIOR_PREVIEW_COMPLETION_PATH)
    completion_content = _read_exact_bytes(
        completion_path,
        PREVIEW_COMPLETION_SHA256,
        "detector preview completion",
    )
    try:
        completion = S7DetectorPreviewCompletion.from_dict(
            _decode_json_object(completion_content, "detector preview completion")
        )
    except Exception as exc:
        raise IdentityMarketSequenceError("detector preview completion is invalid") from exc
    if (
        completion.content != completion_content
        or completion.completion_id != PREVIEW_COMPLETION_ID
        or completion.preview_artifact_id != PREVIEW_ARTIFACT_ID
        or completion.preview_artifact_path != PRIOR_PREVIEW_PATH
        or completion.preview_artifact_sha256 != PREVIEW_ARTIFACT_SHA256
        or completion.case_count != PREVIEW_CASE_COUNT
        or completion.suspected_provider_figi_bounce_rows != PREVIEW_SUSPECTED_ROW_COUNT
        or completion.case_evidence_set_digest != PREVIEW_CASE_EVIDENCE_SET_DIGEST
        or completion.calendar_artifact_id != calendar.calendar_artifact_id
        or completion.calendar_artifact_sha256 != calendar.sha256
    ):
        raise IdentityMarketSequenceError("detector preview completion lineage differs")
    preview_path = _safe(root, PRIOR_PREVIEW_PATH)
    preview_content = _read_exact_bytes(
        preview_path,
        PREVIEW_ARTIFACT_SHA256,
        "detector preview artifact",
    )

    manifests: list[
        tuple[dict[str, object], S4BounceProviderEvidenceManifest, dict[str, object]]
    ] = []
    attestation_by_id: dict[str, ProviderRowAttestation] = {}
    case_refs: list[dict[str, object]] = []
    for ref in completion.case_evidence:
        content = _read_exact_bytes(
            _safe(root, ref.path), ref.sha256, f"case evidence {ref.identity_case_id}"
        )
        if len(content) != ref.bytes:
            raise IdentityMarketSequenceError("case evidence byte count differs")
        try:
            manifest = S4BounceProviderEvidenceManifest.from_dict(
                _decode_json_object(content, f"case evidence {ref.identity_case_id}")
            )
        except ProviderEvidenceError as exc:
            raise IdentityMarketSequenceError("case evidence manifest is invalid") from exc
        if (
            manifest.content != content
            or manifest.manifest_id != ref.manifest_id
            or manifest.relative_path != ref.path
            or manifest.plan_id != PREVIEW_PLAN_ID
            or manifest.approval_id != PREVIEW_APPROVAL_ID
            or manifest.preview_artifact_id != PREVIEW_ARTIFACT_ID
            or manifest.preview_artifact_sha256 != PREVIEW_ARTIFACT_SHA256
            or manifest.availability_calendar_id != calendar.calendar_artifact_id
            or manifest.availability_calendar_sha256 != calendar.sha256
            or manifest.case_snapshot.get("identity_case_id") != ref.identity_case_id
        ):
            raise IdentityMarketSequenceError("case evidence trust chain differs")
        case_ref = ref.to_dict()
        case_refs.append(case_ref)
        manifests.append((case_ref, manifest, dict(manifest.case_snapshot)))
        for attestation in manifest.row_attestations:
            previous = attestation_by_id.get(attestation.row_attestation_id)
            if previous is not None and previous.to_dict() != attestation.to_dict():
                raise IdentityMarketSequenceError("case evidence repeats a divergent attestation")
            attestation_by_id[attestation.row_attestation_id] = attestation

    try:
        bundle = open_identity_source_bundle(root)
        replayed = replay_provider_row_attestations_from_official_bundle(
            tuple(attestation_by_id[key] for key in sorted(attestation_by_id)),
            bundle=bundle,
            calendar=calendar,
        )
    except (IdentitySourceError, ProviderEvidenceError, OSError) as exc:
        raise IdentityMarketSequenceError("reviewed S4 row physical replay failed") from exc
    replayed_by_id = {item.row_attestation_id: item for item in replayed}
    if set(replayed_by_id) != set(attestation_by_id) or any(
        replayed_by_id[key].to_dict() != item.to_dict() for key, item in attestation_by_id.items()
    ):
        raise IdentityMarketSequenceError("reviewed S4 row replay differs from attestation")

    rows_by_source: dict[str, dict[str, object]] = {}
    relations_by_source: dict[str, list[dict[str, object]]] = {}
    semantic_roles: Counter[str] = Counter()
    represented_case_ids: set[str] = set()
    for case_ref, manifest, snapshot in manifests:
        ticker = _safe_text(snapshot.get("ticker"), "case ticker")
        middle = [item for item in manifest.usages if item.case_role == "middle"]
        middle_figis = {item.observed_composite_figi for item in middle}
        matching_groups = [
            (foreign, group)
            for (group_ticker, foreign), group in groups.items()
            if group_ticker == ticker
            and middle_figis.intersection({foreign, str(group["canonical"])})
        ]
        if len(matching_groups) != 1:
            raise IdentityMarketSequenceError("case cannot bind exactly one reviewed group")
        foreign, group = matching_groups[0]
        canonical = str(group["canonical"])
        if middle_figis == {foreign}:
            semantic_role = "contaminated_middle_episode"
        elif middle_figis == {canonical}:
            semantic_role = "inverse_middle_is_canonical_us"
        else:
            raise IdentityMarketSequenceError("case middle is neither direct nor inverse")
        semantic_roles[semantic_role] += 1
        represented_case_ids.add(str(case_ref["identity_case_id"]))
        by_attestation = {item.row_attestation_id: item for item in manifest.row_attestations}
        for usage in manifest.usages:
            if usage.observed_composite_figi != foreign:
                continue
            asset = by_attestation[usage.asset_observation_attestation_id]
            universe = by_attestation[usage.universe_membership_attestation_id]
            _validate_reviewed_foreign_pair(
                asset,
                universe,
                ticker=ticker,
                foreign=foreign,
                group=group,
            )
            source_id = usage.source_record_id
            row = _reviewed_foreign_row(asset, universe)
            previous = rows_by_source.get(source_id)
            if previous is not None and previous != row:
                raise IdentityMarketSequenceError("reviewed source row snapshot diverges")
            rows_by_source[source_id] = row
            relations_by_source.setdefault(source_id, []).append(
                {
                    "case_evidence_manifest_id": case_ref["manifest_id"],
                    "case_evidence_manifest_path": case_ref["path"],
                    "case_evidence_manifest_sha256": case_ref["sha256"],
                    "evidence_case_role": usage.case_role,
                    "evidence_role_ordinal": usage.role_ordinal,
                    "identity_case_id": usage.identity_case_id,
                    "identity_case_resolution_role": semantic_role,
                    "usage_id": usage.usage_id,
                }
            )

    if (
        len(rows_by_source) != REVIEWED_FOREIGN_ROW_COUNT
        or len(represented_case_ids) != PREVIEW_CASE_COUNT
        or semantic_roles
        != Counter(
            {
                "contaminated_middle_episode": REVIEWED_DIRECT_CASE_COUNT,
                "inverse_middle_is_canonical_us": REVIEWED_INVERSE_CASE_COUNT,
            }
        )
    ):
        raise IdentityMarketSequenceError("reviewed row/case count reconciliation differs")
    actual_group_counts = Counter(
        (str(row["ticker"]), str(row["observed_composite_figi"])) for row in rows_by_source.values()
    )
    if any(
        actual_group_counts[key] != int(group["expected_rows"]) for key, group in groups.items()
    ):
        raise IdentityMarketSequenceError("reviewed foreign group row counts differ")

    output: list[dict[str, object]] = []
    for source_id in sorted(rows_by_source):
        row = rows_by_source[source_id]
        relations = sorted(
            relations_by_source[source_id],
            key=lambda item: (
                str(item["identity_case_id"]),
                str(item["evidence_case_role"]),
                int(item["evidence_role_ordinal"]),
            ),
        )
        case_ids = sorted({str(item["identity_case_id"]) for item in relations})
        roles = sorted({str(item["identity_case_resolution_role"]) for item in relations})
        binding_payload = {
            "asset_observation_attestation_id": row["asset_observation_attestation_id"],
            "related_cases": relations,
            "selected_source_record_id": source_id,
            "universe_membership_attestation_id": row["universe_membership_attestation_id"],
        }
        output.append(
            {
                **row,
                "related_case_bindings_json": _compact_json(binding_payload),
                "related_case_resolution_roles": roles,
                "related_identity_case_ids": case_ids,
                "source_snapshot_binding_digest": stable_digest(binding_payload),
            }
        )
    output.sort(
        key=lambda item: (item["session_date"], item["ticker"], item["selected_source_record_id"])
    )
    case_refs.sort(key=lambda item: str(item["identity_case_id"]))
    return output, {
        "detector_preview": {
            "bytes": len(preview_content),
            "path": PRIOR_PREVIEW_PATH,
            "preview_artifact_id": PREVIEW_ARTIFACT_ID,
            "sha256": PREVIEW_ARTIFACT_SHA256,
        },
        "detector_preview_completion": {
            "bytes": len(completion_content),
            "completion_id": PREVIEW_COMPLETION_ID,
            "path": PRIOR_PREVIEW_COMPLETION_PATH,
            "sha256": PREVIEW_COMPLETION_SHA256,
        },
        "reviewed_case_evidence": {
            "case_count": len(case_refs),
            "case_evidence_set_digest": PREVIEW_CASE_EVIDENCE_SET_DIGEST,
            "manifests": case_refs,
            "reviewed_foreign_row_count": len(output),
            "reviewed_source_row_set_digest": _reviewed_evidence_digest(output),
        },
        "reviewed_external_evidence": {
            "bytes": len(external_content),
            "manifest_id": EXTERNAL_EVIDENCE_MANIFEST_ID,
            "path": EXTERNAL_EVIDENCE_MANIFEST_PATH,
            "sha256": EXTERNAL_EVIDENCE_MANIFEST_SHA256,
            "storage_scope": "bound_clean_git_checkout",
        },
    }


def _validate_reviewed_foreign_pair(
    asset: ProviderRowAttestation,
    universe: ProviderRowAttestation,
    *,
    ticker: str,
    foreign: str,
    group: Mapping[str, object],
) -> None:
    left = asset.full_row_snapshot
    right = universe.full_row_snapshot
    session = date.fromisoformat(str(right.get("session_date")))
    if (
        asset.dataset != "asset_observation_daily"
        or universe.dataset != "universe_source_daily"
        or asset.source_record_id != universe.source_record_id
        or left.get("source_record_id") != asset.source_record_id
        or right.get("selected_source_record_id") != universe.source_record_id
        or left.get("ticker") != ticker
        or right.get("ticker") != ticker
        or left.get("composite_figi") != foreign
        or right.get("composite_figi") != foreign
        or left.get("share_class_figi") != group["share_class"]
        or right.get("share_class_figi") != group["share_class"]
        or left.get("locale") != "us"
        or right.get("locale") != "us"
        or left.get("market") != "stocks"
        or right.get("market") != "stocks"
        or not (group["start"] <= session <= group["end"])
    ):
        raise IdentityMarketSequenceError("reviewed foreign S4 row scope differs")


def _reviewed_foreign_row(
    asset: ProviderRowAttestation,
    universe: ProviderRowAttestation,
) -> dict[str, object]:
    row = universe.full_row_snapshot
    return {
        "active_on_date": bool(row["active_on_date"]),
        "asset_observation_artifact_path": asset.silver_artifact_path,
        "asset_observation_artifact_sha256": asset.silver_artifact_sha256,
        "asset_observation_attestation_id": asset.row_attestation_id,
        "asset_observation_attestation_json": _compact_json(asset.to_dict()),
        "asset_observation_full_row_digest": asset.full_row_digest,
        "asset_observation_full_row_json": _compact_json(dict(asset.full_row_snapshot)),
        "asset_observation_parquet_row_group": asset.parquet_row_group,
        "asset_observation_row_index_in_row_group": asset.row_index_in_row_group,
        "locale": str(row["locale"]),
        "market": str(row["market"]),
        "observed_composite_figi": str(row["composite_figi"]),
        "observed_share_class_figi": str(row["share_class_figi"]),
        "primary_exchange_mic": str(row["primary_exchange_mic"]),
        "provider": PROVIDER,
        "selected_source_record_id": universe.source_record_id,
        "session_date": date.fromisoformat(str(row["session_date"])),
        "source_available_session": universe.source_available_session.isoformat(),
        "ticker": str(row["ticker"]),
        "universe_membership_artifact_path": universe.silver_artifact_path,
        "universe_membership_artifact_sha256": universe.silver_artifact_sha256,
        "universe_membership_attestation_id": universe.row_attestation_id,
        "universe_membership_attestation_json": _compact_json(universe.to_dict()),
        "universe_membership_full_row_digest": universe.full_row_digest,
        "universe_membership_full_row_json": _compact_json(dict(universe.full_row_snapshot)),
        "universe_membership_parquet_row_group": universe.parquet_row_group,
        "universe_membership_row_index_in_row_group": universe.row_index_in_row_group,
    }


def _reviewed_evidence_digest(rows: Sequence[Mapping[str, object]]) -> str:
    normalized = []
    for value in rows:
        row = dict(value)
        session = row.get("session_date")
        if isinstance(session, date):
            row["session_date"] = session.isoformat()
        normalized.append(row)
    return stable_digest(normalized)


def _execute_scan(
    root: Path,
    staging: Path,
    *,
    prefix: str,
    candidate_id: str,
    basis: Mapping[str, object],
    universe_refs: Sequence[Mapping[str, object]],
    classifications: Mapping[str, _Classification],
    reviewed_evidence_rows: Sequence[Mapping[str, object]],
    created_at: str,
    actor: str,
    classification_source_available_session: str,
    authorization_recorded_at: str,
    caps: S7MarketSequenceResourceCaps,
    monitor: _ResourceMonitor,
) -> S7MarketSequenceCandidate:
    interval_path = staging / "data/sequence-intervals.parquet"
    interval_path.parent.mkdir(parents=True)
    daily_path = staging / "qa/daily-reason-counts.parquet"
    daily_path.parent.mkdir(parents=True)
    interval_writer = pq.ParquetWriter(
        interval_path, INTERVAL_SCHEMA, compression="zstd", use_dictionary=True
    )
    daily_writer = pq.ParquetWriter(
        daily_path, DAILY_REASON_SCHEMA, compression="zstd", use_dictionary=True
    )
    metrics = _Metrics()
    open_intervals: dict[str, _OpenInterval] = {}
    market_runs: dict[str, _MarketRun] = {}
    recent_intervals: dict[str, deque[dict[str, object]]] = {}
    interval_buffer: list[dict[str, object]] = []
    examples_by_reason: dict[str, list[dict[str, object]]] = {}
    global_reasons: Counter[str] = Counter()

    def flush_intervals() -> None:
        if interval_buffer:
            interval_writer.write_table(pa.Table.from_pylist(interval_buffer, INTERVAL_SCHEMA))
            interval_buffer.clear()
            monitor.check()

    def close_interval(item: _OpenInterval) -> None:
        long_foreign = (
            item.classification == "known_non_us"
            and _us_context(item.locale, item.primary_exchange_mic)
            and item.session_count >= LONG_STANDING_MIN_SESSIONS
        )
        interval_buffer.append(
            {
                "provider": item.provider,
                "market": item.market,
                "locale": item.locale,
                "ticker": item.ticker,
                "observed_composite_figi": item.composite_figi,
                "observed_share_class_figi": item.share_class_figi,
                "primary_exchange_mic": item.primary_exchange_mic,
                "active_on_date": item.active_on_date,
                "market_classification": item.classification,
                "classification_reason": item.classification_reason,
                "market_codes": list(item.market_codes),
                "interval_start_session": item.start,
                "interval_end_session": item.end,
                "interval_session_count": item.session_count,
                "source_row_count": item.session_count,
                "first_source_record_id": item.first_source_record_id,
                "last_source_record_id": item.last_source_record_id,
                "source_record_lineage_digest": item.lineage.hexdigest(),
                "active_slice_meets_long_standing_threshold": long_foreign,
                "foreign_market_identity_legal": (
                    item.classification == "known_non_us"
                    and not _us_context(item.locale, item.primary_exchange_mic)
                ),
                "membership_preserved": True,
                "identity_quality_inactive_inferred": False,
                "proposed_backtest_identity_eligible": (item.proposed_backtest_identity_eligible),
                "liquidation_signal": False,
                "transition_disposition": "not_evaluated_no_transition_adjudication",
                "source_available_session": item.source_available_session,
            }
        )
        metrics.interval_rows += 1
        if metrics.interval_rows > caps.max_interval_rows:
            raise IdentityMarketSequenceError("resource_cap_exceeded: interval rows")
        history = recent_intervals.setdefault(item.ticker, deque(maxlen=3))
        summary = {
            "classification": item.classification,
            "composite": item.composite_figi,
            "end_session_index": item.last_session_index,
            "locale": item.locale,
            "mic": item.primary_exchange_mic,
            "share_class": item.share_class_figi,
            "session_count": item.session_count,
            "start_session_index": item.start_session_index,
            "transition_disposition": "not_evaluated_no_transition_adjudication",
        }
        if history:
            previous = history[-1]
            same_identity = all(
                previous[field] == summary[field]
                for field in ("classification", "composite", "locale", "mic", "share_class")
            )
            if same_identity and int(previous["end_session_index"]) + 1 == int(
                summary["start_session_index"]
            ):
                previous["end_session_index"] = summary["end_session_index"]
                previous["session_count"] = int(previous["session_count"]) + int(
                    summary["session_count"]
                )
                if len(interval_buffer) >= 4_096:
                    flush_intervals()
                return
        history.append(summary)
        if len(history) == 3:
            left, middle, right = history
            same_outer = (
                left["composite"] == right["composite"]
                and left["share_class"] == middle["share_class"] == right["share_class"]
                and left["locale"] == middle["locale"] == right["locale"]
                and left["mic"] == middle["mic"] == right["mic"]
            )
            strictly_adjacent = int(left["end_session_index"]) + 1 == int(
                middle["start_session_index"]
            ) and int(middle["end_session_index"]) + 1 == int(right["start_session_index"])
            if (
                same_outer
                and strictly_adjacent
                and [row["classification"] for row in history]
                == [
                    "known_non_us",
                    "known_us",
                    "known_non_us",
                ]
            ):
                metrics.inverse_bounce_cases += 1
                middle_rows = int(middle["session_count"])
                metrics.inverse_bounce_middle_rows += middle_rows
                if middle["transition_disposition"] == "confirmed_genuine_transition":
                    metrics.inverse_bounce_misclassified_rows += middle_rows
            if (
                same_outer
                and strictly_adjacent
                and [row["classification"] for row in history]
                == [
                    "known_us",
                    "known_non_us",
                    "known_us",
                ]
            ):
                metrics.ordinary_bounce_cases += 1
        if len(interval_buffer) >= 4_096:
            flush_intervals()

    def close_market_run(item: _MarketRun) -> None:
        if (
            item.classification == "known_non_us"
            and _us_context(item.locale, item.primary_exchange_mic)
            and item.session_count >= LONG_STANDING_MIN_SESSIONS
        ):
            metrics.long_standing_foreign_intervals += 1
            metrics.long_standing_foreign_rows += item.session_count

    try:
        for session_index, ref in enumerate(universe_refs):
            session = date.fromisoformat(str(ref["session_date"]))
            daily_reasons: Counter[str] = Counter()
            seen: set[str] = set()
            with _open_verified_source_parquet(root, ref) as parquet:
                for batch in parquet.iter_batches(
                    columns=list(UNIVERSE_COLUMNS), batch_size=caps.batch_rows
                ):
                    values = batch.to_pydict()
                    for row_index in range(batch.num_rows):
                        row = {name: values[name][row_index] for name in UNIVERSE_COLUMNS}
                        parsed = _parse_universe_row(row, session=session)
                        ticker = parsed["ticker"]
                        assert isinstance(ticker, str)
                        if ticker in seen:
                            raise IdentityMarketSequenceError(
                                f"universe duplicate same-session ticker: {session} {ticker}"
                            )
                        seen.add(ticker)
                        metrics.source_rows += 1
                        classification, reason, eligible, reference_missing = _classify_row(
                            parsed, classifications
                        )
                        if reference_missing:
                            metrics.reference_unattempted_rows += 1
                        flagged = classification != "known_us" and not (
                            classification == "known_non_us"
                            and not _us_context(parsed["locale"], parsed["primary_exchange_mic"])
                        )
                        if flagged:
                            metrics.flagged_rows += 1
                            metrics.unresolved_rows += int(classification == "unresolved")
                            daily_reasons[reason] += 1
                            global_reasons[reason] += 1
                            reason_examples = examples_by_reason.setdefault(reason, [])
                            example_total = sum(len(items) for items in examples_by_reason.values())
                            per_reason_cap = max(1, min(10, caps.max_examples))
                            if (
                                len(reason_examples) < per_reason_cap
                                and example_total < caps.max_examples
                            ):
                                reason_examples.append(
                                    {
                                        "active_on_date": parsed["active_on_date"],
                                        "classification_reason": reason,
                                        "locale": parsed["locale"],
                                        "market": parsed["market"],
                                        "market_classification": classification,
                                        "observed_composite_figi": parsed["composite_figi"],
                                        "observed_share_class_figi": parsed["share_class_figi"],
                                        "primary_exchange_mic": parsed["primary_exchange_mic"],
                                        "proposed_backtest_identity_eligible": eligible,
                                        "provider": PROVIDER,
                                        "session_date": session.isoformat(),
                                        "ticker": ticker,
                                    }
                                )
                        if (
                            classification == "known_non_us"
                            and str(parsed["locale"] or "").casefold() == "us"
                        ):
                            metrics.us_locale_foreign_rows += 1
                        if classification == "known_non_us":
                            metrics.non_us_rows += 1
                        if (
                            classification == "known_non_us"
                            and str(parsed["primary_exchange_mic"] or "").upper() in US_PRIMARY_MICS
                        ):
                            metrics.us_mic_foreign_rows += 1
                        if classification == "known_non_us" and eligible:
                            metrics.unapproved_foreign_eligible_rows += 1
                        if classification == "unresolved" and eligible:
                            metrics.unresolved_eligible_rows += 1

                        key = (
                            PROVIDER,
                            parsed["market"],
                            parsed["locale"],
                            ticker,
                            parsed["composite_figi"],
                            parsed["share_class_figi"],
                            parsed["primary_exchange_mic"],
                            parsed["active_on_date"],
                            classification,
                            reason,
                            eligible,
                        )
                        market_key = (
                            PROVIDER,
                            parsed["market"],
                            parsed["locale"],
                            ticker,
                            parsed["composite_figi"],
                            parsed["share_class_figi"],
                            parsed["primary_exchange_mic"],
                            classification,
                        )
                        market_run = market_runs.get(ticker)
                        if market_run is not None and (
                            market_run.key != market_key
                            or market_run.last_session_index != session_index - 1
                        ):
                            close_market_run(market_run)
                            market_run = None
                        if market_run is None:
                            market_runs[ticker] = _MarketRun(
                                key=market_key,
                                classification=classification,
                                locale=parsed["locale"],
                                primary_exchange_mic=parsed["primary_exchange_mic"],
                                session_count=1,
                                last_session_index=session_index,
                            )
                        else:
                            market_run.session_count += 1
                            market_run.last_session_index = session_index
                        current = open_intervals.get(ticker)
                        source_id = str(parsed["selected_source_record_id"])
                        if current is not None and (
                            current.key != key or current.last_session_index != session_index - 1
                        ):
                            close_interval(current)
                            current = None
                        if current is None:
                            classification_ref = classifications.get(
                                str(parsed["composite_figi"] or "")
                            )
                            lineage = hashlib.sha256(f"{GATE_C_RULE_VERSION}|{PROVIDER}|".encode())
                            lineage.update(bytes.fromhex(source_id))
                            current = _OpenInterval(
                                key=key,
                                provider=PROVIDER,
                                market=parsed["market"],
                                locale=parsed["locale"],
                                ticker=ticker,
                                composite_figi=parsed["composite_figi"],
                                share_class_figi=parsed["share_class_figi"],
                                primary_exchange_mic=parsed["primary_exchange_mic"],
                                active_on_date=parsed["active_on_date"],
                                classification=classification,
                                classification_reason=reason,
                                market_codes=(
                                    ()
                                    if classification_ref is None
                                    else classification_ref.market_codes
                                ),
                                source_available_session=(
                                    None
                                    if classification_ref is None
                                    else classification_ref.source_available_session
                                ),
                                proposed_backtest_identity_eligible=eligible,
                                start=session,
                                end=session,
                                start_session_index=session_index,
                                last_session_index=session_index,
                                session_count=1,
                                first_source_record_id=source_id,
                                last_source_record_id=source_id,
                                lineage=lineage,
                            )
                            open_intervals[ticker] = current
                        else:
                            current.end = session
                            current.last_session_index = session_index
                            current.session_count += 1
                            current.last_source_record_id = source_id
                            current.lineage.update(bytes.fromhex(source_id))
            for ticker in tuple(open_intervals):
                if ticker not in seen:
                    close_interval(open_intervals.pop(ticker))
                    recent_intervals.pop(ticker, None)
                    close_market_run(market_runs.pop(ticker))
            if daily_reasons:
                daily_writer.write_table(
                    pa.Table.from_pylist(
                        [
                            {
                                "session_date": session,
                                "reason_code": reason,
                                "row_count": count,
                            }
                            for reason, count in sorted(daily_reasons.items())
                        ],
                        DAILY_REASON_SCHEMA,
                    )
                )
            monitor.check()
        for ticker in sorted(open_intervals):
            close_interval(open_intervals[ticker])
        for ticker in sorted(market_runs):
            close_market_run(market_runs[ticker])
        flush_intervals()
    finally:
        interval_writer.close()
        daily_writer.close()

    if metrics.source_rows != PRODUCTION_EXPECTATIONS.universe_row_count:
        raise IdentityMarketSequenceError("streamed universe row count differs")
    if metrics.reference_unattempted_rows:
        raise IdentityMarketSequenceError(
            "reference_inventory_unattempted_rows is nonzero; Gate-B is incomplete"
        )
    if (
        metrics.unapproved_foreign_eligible_rows
        or metrics.unresolved_eligible_rows
        or metrics.membership_mutation_rows
        or metrics.forced_liquidation_rows
    ):
        raise IdentityMarketSequenceError("critical Gate-C identity invariant failed")

    interval_receipt = _staged_parquet_receipt(
        interval_path, f"{prefix}/data/sequence-intervals.parquet", metrics.interval_rows
    )
    daily_receipt = _staged_parquet_receipt(
        daily_path,
        f"{prefix}/qa/daily-reason-counts.parquet",
        pq.ParquetFile(daily_path).metadata.num_rows,
    )
    reviewed_evidence_path = staging / "evidence/reviewed-foreign-s4-source-rows.parquet"
    reviewed_evidence_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(list(reviewed_evidence_rows), schema=REVIEWED_EVIDENCE_SCHEMA),
        reviewed_evidence_path,
        compression="zstd",
        use_dictionary=True,
    )
    reviewed_evidence_receipt = _staged_parquet_receipt(
        reviewed_evidence_path,
        f"{prefix}/evidence/reviewed-foreign-s4-source-rows.parquet",
        len(reviewed_evidence_rows),
    )
    examples = [
        example for reason in sorted(examples_by_reason) for example in examples_by_reason[reason]
    ]
    example_document = {
        "artifact_type": "s7_full_market_sequence_bounded_examples",
        "candidate_id": candidate_id,
        "example_cap": caps.max_examples,
        "examples": examples,
        "reason_counts": dict(sorted(global_reasons.items())),
        "schema_version": 1,
    }
    examples_path = staging / "examples/bounded-examples.json"
    examples_path.parent.mkdir(parents=True)
    _write_exact(examples_path, _canonical_bytes(example_document))
    example_receipt = _staged_receipt(examples_path, f"{prefix}/examples/bounded-examples.json")
    qa_document = _qa_document(
        candidate_id,
        metrics,
        example_receipt["path"],
        reason_counts=global_reasons,
    )
    qa_path = staging / "qa/qa.json"
    _write_exact(qa_path, _canonical_bytes(qa_document))
    qa_receipt = _staged_receipt(qa_path, f"{prefix}/qa/qa.json")
    monitor.check()
    output_bytes = sum(
        int(receipt["bytes"])
        for receipt in (
            interval_receipt,
            daily_receipt,
            reviewed_evidence_receipt,
            example_receipt,
            qa_receipt,
        )
    )
    if output_bytes > caps.max_output_bytes:
        raise IdentityMarketSequenceError("resource_cap_exceeded: output bytes")
    completed_at = _utc_now()
    authorization_at = _parse_utc(
        authorization_recorded_at,
        "authorization recorded timestamp",
    )
    if completed_at < authorization_at:
        raise IdentityMarketSequenceError("candidate completion predates Gate-C authorization")
    calendar = _load_bound_calendar(root)
    next_session, next_open = calendar.first_open_after(completed_at)
    evidence_session = date.fromisoformat(classification_source_available_session)
    candidate_available_session = max(evidence_session, next_session)
    manifest_payload = {
        "artifact_type": "s7_full_market_sequence_candidate",
        "candidate_basis": dict(basis),
        "candidate_id": candidate_id,
        "capabilities": _candidate_capabilities(),
        "counts": {
            "interval_row_count": metrics.interval_rows,
            "long_standing_foreign_sequence_count": metrics.long_standing_foreign_intervals,
            "long_standing_foreign_row_count": metrics.long_standing_foreign_rows,
            "reviewed_case_count": PREVIEW_CASE_COUNT,
            "reviewed_foreign_source_row_count": len(reviewed_evidence_rows),
            "source_row_count": metrics.source_rows,
            "unresolved_row_count": metrics.unresolved_rows,
            "us_locale_non_us_composite_figi_rows": metrics.us_locale_foreign_rows,
        },
        "created_at_utc": created_at,
        "created_by": actor,
        "availability": {
            "candidate_available_session": candidate_available_session.isoformat(),
            "classification_source_available_session": evidence_session.isoformat(),
            "execution_completed_at_utc": completed_at.isoformat(),
            "first_xnys_open_after_completion_utc": next_open.isoformat(),
            "rule": (
                "max(bound Gate-B source availability, first XNYS open after actual "
                "Gate-C completion)"
            ),
        },
        "outputs": {
            "bounded_examples": example_receipt,
            "daily_reason_counts": daily_receipt,
            "interval_data": interval_receipt,
            "qa": qa_receipt,
            "reviewed_foreign_source_evidence": reviewed_evidence_receipt,
        },
        "registry_loader_source_refs": dict(basis["registry_loader_source_refs"]),
        "resource_measurements": {
            "maximum_tmp_bytes": monitor.maximum_tmp_bytes,
            "minimum_disk_free_bytes": monitor.minimum_disk_free_bytes,
            "output_bytes": output_bytes,
            "peak_rss_bytes": monitor.peak_rss_bytes,
            "wall_clock_seconds": float(time.monotonic() - monitor.started),
        },
        "schema_version": 1,
        "state": CANDIDATE_STATE,
    }
    manifest_document = {
        **manifest_payload,
        "manifest_id": stable_digest(manifest_payload),
    }
    manifest_path = staging / "manifest.json"
    _write_exact(manifest_path, _canonical_bytes(manifest_document))
    _validate_staging(staging, manifest_document)
    return S7MarketSequenceCandidate(
        candidate_id=candidate_id,
        completion_id="",
        completion_path="",
        completion_sha256="",
        manifest_path=f"{prefix}/manifest.json",
        interval_data_path=str(interval_receipt["path"]),
        daily_reason_counts_path=str(daily_receipt["path"]),
        qa_path=str(qa_receipt["path"]),
        examples_path=str(example_receipt["path"]),
        reviewed_evidence_path=str(reviewed_evidence_receipt["path"]),
        source_row_count=metrics.source_rows,
        interval_row_count=metrics.interval_rows,
        us_locale_non_us_composite_figi_rows=metrics.us_locale_foreign_rows,
        unresolved_rows=metrics.unresolved_rows,
        long_standing_foreign_rows=metrics.long_standing_foreign_rows,
        reviewed_foreign_row_count=len(reviewed_evidence_rows),
        reviewed_case_count=PREVIEW_CASE_COUNT,
        idempotent=False,
    )


def _replay_and_compare_existing(
    root: Path,
    *,
    final_manifest_relative: str,
    prefix: str,
    candidate_id: str,
    basis: Mapping[str, object],
    universe_refs: Sequence[Mapping[str, object]],
    classifications: Mapping[str, _Classification],
    reviewed_evidence_rows: Sequence[Mapping[str, object]],
    created_at: str,
    actor: str,
    classification_source_available_session: str,
    authorization_recorded_at: str,
    caps: S7MarketSequenceResourceCaps,
    monitor: _ResourceMonitor,
) -> None:
    """Fully replay immutable inputs before accepting an existing candidate."""

    staging_parent = _safe(root, "tmp/silver-s7-market-sequence")
    staging_parent.mkdir(parents=True, exist_ok=True)
    staging = staging_parent / f"candidate_id={candidate_id}.replay"
    if staging.exists() or staging.is_symlink():
        raise IdentityMarketSequenceError(
            "prior incomplete Gate-C replay slot requires explicit forensic review"
        )
    staging.mkdir(mode=0o700)
    monitor.staging = staging
    monitor.check()
    try:
        _execute_scan(
            root,
            staging,
            prefix=prefix,
            candidate_id=candidate_id,
            basis=basis,
            universe_refs=universe_refs,
            classifications=classifications,
            reviewed_evidence_rows=reviewed_evidence_rows,
            created_at=created_at,
            actor=actor,
            classification_source_available_session=(classification_source_available_session),
            authorization_recorded_at=authorization_recorded_at,
            caps=caps,
            monitor=monitor,
        )
        replay, _ = _load_canonical_json_file(staging / "manifest.json", "Gate-C replay")
        existing, _ = _load_canonical_json_file(
            _safe(root, final_manifest_relative), "existing Gate-C candidate"
        )
        if replay.get("outputs") != existing.get("outputs"):
            raise IdentityMarketSequenceError(
                "idempotent full replay output receipts differ from existing candidate"
            )
        if replay.get("counts") != existing.get("counts"):
            raise IdentityMarketSequenceError(
                "idempotent full replay counts differ from existing candidate"
            )
        shutil.rmtree(staging)
        _fsync_directory(staging_parent)
    except Exception:
        # Preserve any failed replay tree for forensic review.
        raise


def _classify_row(
    row: Mapping[str, object], classifications: Mapping[str, _Classification]
) -> tuple[str, str, bool, bool]:
    composite = row["composite_figi"]
    if composite is None:
        return "unresolved", "observed_composite_figi_null", False, False
    if not isinstance(composite, str) or _FIGI.fullmatch(composite) is None:
        return "unresolved", "observed_composite_figi_invalid", False, False
    reference = classifications.get(composite)
    if reference is None:
        return "unresolved", "classification_missing_for_valid_composite", False, True
    if reference.normalized == "known_us":
        return "known_us", "openfigi_us_composite", True, False
    if reference.normalized == "known_non_us":
        if str(row["locale"] or "").casefold() == "us":
            return "known_non_us", "us_locale_non_us_composite_figi", False, False
        if str(row["primary_exchange_mic"] or "").upper() in US_PRIMARY_MICS:
            return "known_non_us", "us_primary_mic_non_us_composite_figi", False, False
        return "known_non_us", "foreign_locale_non_us_composite_legal", False, False
    return "unresolved", f"market_classification_{reference.raw}", False, False


def _load_classifications(path: Path, expected_rows: int) -> dict[str, _Classification]:
    parquet = pq.ParquetFile(path)
    names = set(parquet.schema_arrow.names)
    figi_column = _choose_column(names, ("composite_figi", "observed_composite_figi"), "FIGI")
    status_column = _choose_column(
        names,
        ("classification", "market_classification", "classification_status", "attempt_status"),
        "classification status",
    )
    columns = [figi_column, status_column]
    market_codes_column = "market_codes" if "market_codes" in names else None
    availability_column = (
        "source_available_session" if "source_available_session" in names else None
    )
    columns.extend(item for item in (market_codes_column, availability_column) if item is not None)
    output: dict[str, _Classification] = {}
    rows = 0
    for batch in parquet.iter_batches(columns=columns, batch_size=65_536):
        values = batch.to_pydict()
        for index in range(batch.num_rows):
            rows += 1
            figi = values[figi_column][index]
            if not isinstance(figi, str) or _FIGI.fullmatch(figi) is None:
                raise IdentityMarketSequenceError("Gate-B classification FIGI is invalid")
            if figi in output:
                raise IdentityMarketSequenceError("Gate-B classification repeats a Composite FIGI")
            raw = values[status_column][index]
            if not isinstance(raw, str):
                raise IdentityMarketSequenceError("Gate-B classification status is invalid")
            normalized = _normalize_classification(raw)
            market_codes_raw = (
                [] if market_codes_column is None else values[market_codes_column][index]
            )
            if market_codes_raw is None:
                market_codes_raw = []
            if not isinstance(market_codes_raw, list) or any(
                not isinstance(value, str) for value in market_codes_raw
            ):
                raise IdentityMarketSequenceError("Gate-B market_codes are invalid")
            market_codes = tuple(sorted(set(market_codes_raw)))
            availability = (
                None if availability_column is None else values[availability_column][index]
            )
            if availability is not None and not isinstance(availability, str):
                if isinstance(availability, date):
                    availability = availability.isoformat()
                else:
                    raise IdentityMarketSequenceError("Gate-B availability is invalid")
            output[figi] = _Classification(
                normalized=normalized,
                raw=raw,
                market_codes=market_codes,
                source_available_session=availability,
            )
    if rows != expected_rows or rows != parquet.metadata.num_rows:
        raise IdentityMarketSequenceError("Gate-B classification row count differs")
    return output


def _normalize_classification(value: str) -> str:
    if value in US_CLASSIFICATIONS:
        return "known_us"
    if value in NON_US_CLASSIFICATIONS:
        return "known_non_us"
    if value in UNRESOLVED_CLASSIFICATIONS or value.startswith("unresolved_"):
        return "unresolved"
    raise IdentityMarketSequenceError(f"unsupported Gate-B classification: {value}")


@contextmanager
def _open_verified_source_parquet(
    root: Path, ref: Mapping[str, object]
) -> Iterator[pq.ParquetFile]:
    path = _safe(root, str(ref["path"]))
    if f"session_date={ref['session_date']}" not in Path(str(ref["path"])).parts:
        raise IdentityMarketSequenceError(
            f"universe artifact path/session binding differs: {ref['path']}"
        )
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
        visible = os.stat(path, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(visible.st_mode)
            or (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino)
        ):
            raise IdentityMarketSequenceError(
                f"universe artifact is missing or unsafe: {ref['path']}"
            )
        signature = _stat_signature(opened)
        if opened.st_size != ref["bytes"]:
            raise IdentityMarketSequenceError(f"universe artifact receipt differs: {ref['path']}")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = None
            digest = hashlib.sha256()
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
            if digest.hexdigest() != ref["sha256"]:
                raise IdentityMarketSequenceError(
                    f"universe artifact receipt differs: {ref['path']}"
                )
            handle.seek(0)
            if _stat_signature(os.fstat(handle.fileno())) != signature:
                raise IdentityMarketSequenceError(
                    f"universe artifact changed during verification: {ref['path']}"
                )
            parquet = pq.ParquetFile(handle)
            if parquet.metadata.num_rows != ref["row_count"]:
                raise IdentityMarketSequenceError(
                    f"universe Parquet row count differs: {ref['path']}"
                )
            missing = set(UNIVERSE_COLUMNS) - set(parquet.schema_arrow.names)
            if missing:
                raise IdentityMarketSequenceError(
                    f"universe Parquet required columns missing: {sorted(missing)}"
                )
            yield parquet
            if _stat_signature(os.fstat(handle.fileno())) != signature:
                raise IdentityMarketSequenceError(
                    f"universe artifact changed during scan: {ref['path']}"
                )
            final_visible = os.stat(path, follow_symlinks=False)
            if not stat.S_ISREG(final_visible.st_mode) or (
                final_visible.st_dev,
                final_visible.st_ino,
            ) != (opened.st_dev, opened.st_ino):
                raise IdentityMarketSequenceError(
                    f"universe artifact path changed during scan: {ref['path']}"
                )
    except IdentityMarketSequenceError:
        raise
    except OSError as exc:
        raise IdentityMarketSequenceError(
            f"universe artifact is missing or unsafe: {ref['path']}"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _stat_signature(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _parse_universe_row(row: Mapping[str, object], *, session: date) -> dict[str, Any]:
    row_session = row["session_date"]
    if isinstance(row_session, datetime):
        row_session = row_session.date()
    if row_session != session:
        raise IdentityMarketSequenceError("universe row session differs from artifact session")
    ticker = row["ticker"]
    if not isinstance(ticker, str) or not ticker or ticker.strip() != ticker:
        raise IdentityMarketSequenceError("universe ticker is invalid")
    active = row["active_on_date"]
    if type(active) is not bool:
        raise IdentityMarketSequenceError("active_on_date must be a native bool")
    source_id = row["selected_source_record_id"]
    if not isinstance(source_id, str) or _DIGEST.fullmatch(source_id) is None:
        raise IdentityMarketSequenceError("selected_source_record_id is invalid")
    output: dict[str, Any] = {
        "active_on_date": active,
        "selected_source_record_id": source_id,
        "ticker": ticker,
    }
    for field_name in (
        "market",
        "locale",
        "primary_exchange_mic",
        "composite_figi",
        "share_class_figi",
    ):
        value = row[field_name]
        if value is not None and not isinstance(value, str):
            raise IdentityMarketSequenceError(f"{field_name} must be text or null")
        output[field_name] = value
    return output


def _qa_document(
    candidate_id: str,
    metrics: _Metrics,
    examples_path: object,
    *,
    reason_counts: Mapping[str, int],
) -> dict[str, object]:
    unresolved_reason_counts = {
        reason: count
        for reason, count in sorted(reason_counts.items())
        if reason.startswith("market_classification_")
        or reason.startswith("observed_composite_figi_")
    }
    checks = [
        _qa_check(
            "us_locale_non_us_composite_figi_rows",
            "high",
            metrics.us_locale_foreign_rows,
            metrics.source_rows,
            warning=True,
            examples_path=examples_path,
            reason_counts={"us_locale_non_us_composite_figi": metrics.us_locale_foreign_rows},
        ),
        _qa_check(
            "market_classification_unresolved_rows",
            "high",
            metrics.unresolved_rows,
            metrics.source_rows,
            warning=True,
            examples_path=examples_path,
            reason_counts=unresolved_reason_counts,
        ),
        _qa_check(
            "long_standing_non_us_composite_figi_rows",
            "high",
            metrics.long_standing_foreign_rows,
            metrics.source_rows,
            warning=True,
            examples_path=examples_path,
            reason_counts={
                "continuous_non_us_sequence_at_least_threshold": (
                    metrics.long_standing_foreign_rows
                )
            },
        ),
        _qa_check(
            "reference_inventory_unattempted_rows",
            "critical",
            metrics.reference_unattempted_rows,
            metrics.source_rows,
        ),
        _qa_check(
            "unapproved_cross_market_composite_eligible_rows",
            "critical",
            metrics.unapproved_foreign_eligible_rows,
            metrics.non_us_rows,
        ),
        _qa_check(
            "unresolved_market_identity_eligible_rows",
            "critical",
            metrics.unresolved_eligible_rows,
            metrics.unresolved_rows,
        ),
        _qa_check(
            "inverse_bounce_misclassified_as_genuine_transition_rows",
            "critical",
            metrics.inverse_bounce_misclassified_rows,
            metrics.inverse_bounce_middle_rows,
        ),
        _qa_check(
            "identity_quality_membership_mutation_rows",
            "critical",
            metrics.membership_mutation_rows,
            metrics.source_rows,
        ),
        _qa_check(
            "identity_quality_forced_liquidation_signal_rows",
            "critical",
            metrics.forced_liquidation_rows,
            metrics.source_rows,
        ),
    ]
    return {
        "artifact_type": "s7_full_market_sequence_qa",
        "candidate_id": candidate_id,
        "critical_failure_count": sum(
            int(item["status"] == "failed") for item in checks if item["severity"] == "critical"
        ),
        "diagnostics": {
            "inverse_bounce_detected_case_count": metrics.inverse_bounce_cases,
            "ordinary_bounce_detected_case_count": metrics.ordinary_bounce_cases,
            "us_primary_mic_non_us_composite_figi_rows": metrics.us_mic_foreign_rows,
        },
        "results": checks,
        "schema_version": 1,
    }


def _qa_check(
    check_id: str,
    severity: str,
    numerator: int,
    denominator: int,
    *,
    warning: bool = False,
    examples_path: object | None = None,
    reason_counts: Mapping[str, int] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "check_id": check_id,
        "denominator": denominator,
        "numerator": numerator,
        "severity": severity,
        "status": "warning" if warning and numerator else "failed" if numerator else "passed",
    }
    if examples_path is not None:
        result["bounded_examples_path"] = examples_path
    if reason_counts is not None:
        result["reason_counts"] = dict(reason_counts)
    return result


def _source_ref(value: object) -> dict[str, object]:
    item = _mapping(value, "Gate-A source ref")
    expected = {
        "bytes",
        "path",
        "release_id",
        "release_manifest_sha256",
        "row_count",
        "session_date",
        "sha256",
        "table",
    }
    if set(item) != expected:
        raise IdentityMarketSequenceError("Gate-A source ref schema is not exact")
    _relative_path(item["path"], "source artifact path")
    for label in ("release_id", "release_manifest_sha256", "sha256"):
        _digest(item[label], f"source ref {label}")
    _nonnegative_int(item["bytes"], "source artifact bytes")
    _nonnegative_int(item["row_count"], "source artifact rows")
    _date_text(item["session_date"], "source artifact session")
    if not isinstance(item["table"], str):
        raise IdentityMarketSequenceError("source artifact table must be text")
    return item


def _verify_gate_a_outputs(
    root: Path, document: Mapping[str, object], candidate_id: str
) -> dict[str, dict[str, object]]:
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 3:
        raise IdentityMarketSequenceError("Gate-A output artifact refs differ")
    prefix = (
        f"manifests/silver/identity/composite-inventory-candidates/candidate_id={candidate_id}/"
    )
    output: dict[str, dict[str, object]] = {}
    for value in artifacts:
        item = _mapping(value, "Gate-A output ref")
        role = str(item.get("role"))
        if role in output or role not in {"data", "qa", "bounded_examples"}:
            raise IdentityMarketSequenceError("Gate-A output roles differ")
        relative = _relative_path(item.get("path"), "Gate-A output path")
        receipt = {
            "bytes": _nonnegative_int(item.get("bytes"), "Gate-A output bytes"),
            "path": prefix + relative,
            "sha256": _digest(item.get("sha256"), "Gate-A output SHA-256"),
        }
        if "row_count" in item:
            receipt["row_count"] = _nonnegative_int(item["row_count"], "Gate-A output rows")
        _verify_receipt(root, receipt, parquet=role == "data")
        output[role] = receipt
    if set(output) != {"data", "qa", "bounded_examples"}:
        raise IdentityMarketSequenceError("Gate-A output roles are incomplete")
    return output


def _verify_gate_a_completion_binding(
    completion: Mapping[str, object],
    *,
    inventory_relative: str,
    inventory_sha: str,
    expected: _SourceExpectations,
) -> None:
    candidate = _mapping(completion.get("candidate"), "completion candidate")
    counts = _mapping(completion.get("counts"), "completion counts")
    if (
        candidate.get("candidate_id") != expected.inventory_candidate_id
        or candidate.get("path") != inventory_relative
        or candidate.get("sha256") != inventory_sha
        or candidate.get("state") != CANDIDATE_STATE
        or completion.get("completion_state") != CANDIDATE_STATE
        or completion.get("source_artifact_set_digest") != expected.source_artifact_set_digest
        or counts.get("source_artifact_count") != expected.source_artifact_count
        or counts.get("source_row_count") != expected.source_row_count
        or counts.get("source_bytes") != expected.source_bytes
        or counts.get("reconciliation_row_count") != expected.universe_row_count
    ):
        raise IdentityMarketSequenceError("Gate-A completion/candidate binding differs")


def _verify_completion_path(relative: str, completion: Mapping[str, object]) -> None:
    plan = _digest(completion.get("plan_id"), "Gate-A completion plan ID")
    approval = _digest(completion.get("approval_id"), "Gate-A completion approval ID")
    expected = (
        "manifests/silver/identity/composite-inventory-execution-completions/"
        f"plan_id={plan}/approval_id={approval}/manifest.json"
    )
    if relative != expected:
        raise IdentityMarketSequenceError("Gate-A completion path is not canonical")


def _verify_candidate_availability(
    root: Path,
    manifest: Mapping[str, object],
    *,
    plan_intent_at: str,
    authorization_recorded_at: str,
    classification_source_available_session: str,
) -> None:
    availability = _mapping(manifest.get("availability"), "candidate availability")
    expected_keys = {
        "candidate_available_session",
        "classification_source_available_session",
        "execution_completed_at_utc",
        "first_xnys_open_after_completion_utc",
        "rule",
    }
    if set(availability) != expected_keys:
        raise IdentityMarketSequenceError("candidate availability schema differs")
    intent = _parse_utc(plan_intent_at, "plan intent timestamp")
    completed = _parse_utc(
        availability.get("execution_completed_at_utc"),
        "candidate completion timestamp",
    )
    if completed < intent:
        raise IdentityMarketSequenceError("candidate completion predates durable intent")
    authorization_at = _parse_utc(
        authorization_recorded_at,
        "authorization recorded timestamp",
    )
    if completed < authorization_at:
        raise IdentityMarketSequenceError("candidate completion predates Gate-C authorization")
    calendar = _load_bound_calendar(root)
    try:
        next_session, next_open = calendar.first_open_after(completed)
    except XNYSCalendarArtifactError as exc:
        raise IdentityMarketSequenceError(
            "candidate completion availability is outside the bound calendar"
        ) from exc
    evidence_text = _xnys_session_text(
        classification_source_available_session,
        "classification source available session",
        calendar=calendar,
    )
    evidence_session = date.fromisoformat(evidence_text)
    expected_candidate_session = max(evidence_session, next_session)
    if availability != {
        "candidate_available_session": expected_candidate_session.isoformat(),
        "classification_source_available_session": evidence_session.isoformat(),
        "execution_completed_at_utc": completed.isoformat(),
        "first_xnys_open_after_completion_utc": next_open.isoformat(),
        "rule": (
            "max(bound Gate-B source availability, first XNYS open after actual Gate-C completion)"
        ),
    }:
        raise IdentityMarketSequenceError("candidate availability proof differs")


def _verify_candidate_resources(
    manifest: Mapping[str, object],
    *,
    outputs: Mapping[str, object],
    caps: S7MarketSequenceResourceCaps,
) -> None:
    measurements = _mapping(
        manifest.get("resource_measurements"), "candidate resource measurements"
    )
    if set(measurements) != {
        "maximum_tmp_bytes",
        "minimum_disk_free_bytes",
        "output_bytes",
        "peak_rss_bytes",
        "wall_clock_seconds",
    }:
        raise IdentityMarketSequenceError("candidate resource measurement schema differs")
    maximum_tmp = _nonnegative_int(measurements["maximum_tmp_bytes"], "maximum tmp bytes")
    minimum_free = _nonnegative_int(
        measurements["minimum_disk_free_bytes"], "minimum disk free bytes"
    )
    output_bytes = _nonnegative_int(measurements["output_bytes"], "output bytes")
    peak_rss = _nonnegative_int(measurements["peak_rss_bytes"], "peak RSS bytes")
    wall_clock = measurements["wall_clock_seconds"]
    if type(wall_clock) not in {int, float} or float(wall_clock) < 0:
        raise IdentityMarketSequenceError("candidate wall-clock measurement is invalid")
    receipt_bytes = sum(
        _nonnegative_int(
            _mapping(value, f"{role} receipt").get("bytes"),
            f"{role} receipt bytes",
        )
        for role, value in outputs.items()
    )
    if (
        output_bytes != receipt_bytes
        or output_bytes > caps.max_output_bytes
        or maximum_tmp > caps.max_tmp_bytes
        or peak_rss > caps.rss_bytes_cap
        or minimum_free < caps.disk_free_floor_bytes
        or float(wall_clock) > caps.wall_clock_seconds_cap
    ):
        raise IdentityMarketSequenceError("candidate resource measurements violate the plan")


def _candidate_from_existing(
    root: Path,
    manifest_relative: str,
    *,
    candidate_id: str,
    basis: Mapping[str, object],
    created_at: str,
    actor: str,
    classification_source_available_session: str,
    authorization_recorded_at: str,
    caps: S7MarketSequenceResourceCaps,
    idempotent: bool,
) -> S7MarketSequenceCandidate:
    manifest, _ = _load_canonical_json_file(_safe(root, manifest_relative), "Gate-C candidate")
    payload = dict(manifest)
    manifest_id = payload.pop("manifest_id", None)
    if (
        stable_digest(payload) != manifest_id
        or manifest.get("artifact_type") != "s7_full_market_sequence_candidate"
        or manifest.get("candidate_id") != candidate_id
        or manifest.get("candidate_basis") != dict(basis)
        or manifest.get("capabilities") != _candidate_capabilities()
        or manifest.get("created_at_utc") != created_at
        or manifest.get("created_by") != actor
        or manifest.get("registry_loader_source_refs") != basis.get("registry_loader_source_refs")
        or manifest.get("schema_version") != 1
        or manifest.get("state") != CANDIDATE_STATE
    ):
        raise IdentityMarketSequenceError("existing Gate-C candidate identity differs")
    _verify_candidate_availability(
        root,
        manifest,
        plan_intent_at=created_at,
        authorization_recorded_at=authorization_recorded_at,
        classification_source_available_session=(classification_source_available_session),
    )
    outputs = _mapping(manifest.get("outputs"), "Gate-C outputs")
    if set(outputs) != {
        "bounded_examples",
        "daily_reason_counts",
        "interval_data",
        "qa",
        "reviewed_foreign_source_evidence",
    }:
        raise IdentityMarketSequenceError("existing Gate-C output roles differ")
    for role, value in outputs.items():
        _verify_receipt(
            root,
            _mapping(value, f"Gate-C {role} receipt"),
            parquet=role
            in {
                "interval_data",
                "daily_reason_counts",
                "reviewed_foreign_source_evidence",
            },
        )
    counts = _mapping(manifest.get("counts"), "Gate-C counts")
    _verify_candidate_resources(manifest, outputs=outputs, caps=caps)
    return S7MarketSequenceCandidate(
        candidate_id=candidate_id,
        completion_id="",
        completion_path="",
        completion_sha256="",
        manifest_path=manifest_relative,
        interval_data_path=str(_mapping(outputs["interval_data"], "interval ref")["path"]),
        daily_reason_counts_path=str(_mapping(outputs["daily_reason_counts"], "daily ref")["path"]),
        qa_path=str(_mapping(outputs["qa"], "QA ref")["path"]),
        examples_path=str(_mapping(outputs["bounded_examples"], "example ref")["path"]),
        reviewed_evidence_path=str(
            _mapping(outputs["reviewed_foreign_source_evidence"], "reviewed evidence ref")["path"]
        ),
        source_row_count=_nonnegative_int(counts.get("source_row_count"), "source rows"),
        interval_row_count=_nonnegative_int(counts.get("interval_row_count"), "interval rows"),
        us_locale_non_us_composite_figi_rows=_nonnegative_int(
            counts.get("us_locale_non_us_composite_figi_rows"), "non-US rows"
        ),
        unresolved_rows=_nonnegative_int(counts.get("unresolved_row_count"), "unresolved rows"),
        long_standing_foreign_rows=_nonnegative_int(
            counts.get("long_standing_foreign_row_count"), "long-standing rows"
        ),
        reviewed_foreign_row_count=_nonnegative_int(
            counts.get("reviewed_foreign_source_row_count"), "reviewed foreign rows"
        ),
        reviewed_case_count=_nonnegative_int(
            counts.get("reviewed_case_count"), "reviewed case count"
        ),
        idempotent=idempotent,
    )


def _write_or_load_completion(
    root: Path,
    *,
    candidate: S7MarketSequenceCandidate,
    plan: Mapping[str, object],
    plan_path: str,
    plan_id: str,
    plan_sha256: str,
    authorization_path: str,
    authorization_id: str,
    authorization_sha256: str,
    idempotent: bool,
) -> S7MarketSequenceCandidate:
    candidate_path = _safe(root, candidate.manifest_path)
    candidate_document, candidate_sha = _load_canonical_json_file(
        candidate_path, "Gate-C candidate for completion"
    )
    authorization_document, actual_authorization_sha = _load_canonical_json_file(
        _safe(root, authorization_path), "Gate-C authorization for completion"
    )
    if (
        actual_authorization_sha != authorization_sha256
        or authorization_document.get("authorization_id") != authorization_id
    ):
        raise IdentityMarketSequenceError("Gate-C completion authorization binding differs")
    authorization_request = _mapping(
        authorization_document.get("request"), "Gate-C completion authorization request"
    )
    outputs = _mapping(candidate_document.get("outputs"), "completion candidate outputs")
    counts = _mapping(candidate_document.get("counts"), "completion candidate counts")
    resources = _mapping(
        candidate_document.get("resource_measurements"), "completion candidate resources"
    )
    payload = {
        "artifact_type": "s7_full_market_sequence_execution_completion",
        "authorization": {
            "authorization_id": authorization_id,
            "path": authorization_path,
            "request": authorization_request,
            "sha256": authorization_sha256,
        },
        "availability": _mapping(
            candidate_document.get("availability"), "completion candidate availability"
        ),
        "candidate": {
            "bytes": candidate_path.stat().st_size,
            "candidate_id": candidate.candidate_id,
            "manifest_id": candidate_document["manifest_id"],
            "path": candidate.manifest_path,
            "sha256": candidate_sha,
            "state": CANDIDATE_STATE,
        },
        "candidate_basis_digest": stable_digest(candidate_document["candidate_basis"]),
        "capabilities": _candidate_capabilities(),
        "completion_rule_version": "s7_full_market_sequence_execution_completion_v1",
        "completion_state": CANDIDATE_STATE,
        "counts": counts,
        "outputs": outputs,
        "plan": {"path": plan_path, "plan_id": plan_id, "sha256": plan_sha256},
        "qa": outputs["qa"],
        "resource_measurements": resources,
        "schema_version": 1,
    }
    completion_id = stable_digest(payload)
    document = {**payload, "completion_id": completion_id}
    content = _canonical_bytes(document)
    relative = _completion_path(plan_id, authorization_id)
    path = _safe(root, relative)
    if path.exists() or path.is_symlink():
        if not path.is_file() or path.is_symlink() or path.read_bytes() != content:
            raise IdentityMarketSequenceError("existing Gate-C completion differs")
    else:
        _write_exact(path, content)
    loaded = _candidate_from_completion(
        root,
        completion_relative=relative,
        plan=plan,
        plan_path=plan_path,
        plan_id=plan_id,
        plan_sha256=plan_sha256,
        authorization_path=authorization_path,
        authorization_id=authorization_id,
        authorization_sha256=authorization_sha256,
        caps=S7MarketSequenceResourceCaps(**_mapping(plan["resource_caps"], "resource caps")),
        idempotent=idempotent,
    )
    if loaded.candidate_id != candidate.candidate_id:
        raise IdentityMarketSequenceError("Gate-C completion candidate differs after write")
    return loaded


def _candidate_from_completion(
    root: Path,
    *,
    completion_relative: str,
    plan: Mapping[str, object],
    plan_path: str,
    plan_id: str,
    plan_sha256: str,
    authorization_path: str,
    authorization_id: str,
    authorization_sha256: str,
    caps: S7MarketSequenceResourceCaps,
    idempotent: bool,
) -> S7MarketSequenceCandidate:
    expected_path = _completion_path(plan_id, authorization_id)
    if completion_relative != expected_path:
        raise IdentityMarketSequenceError("Gate-C completion path differs")
    document, completion_sha = _load_canonical_json_file(
        _safe(root, completion_relative), "Gate-C execution completion"
    )
    payload = dict(document)
    completion_id = _digest(payload.pop("completion_id", None), "Gate-C completion ID")
    candidate_ref = _mapping(document.get("candidate"), "completion candidate ref")
    outputs = _mapping(document.get("outputs"), "completion outputs")
    authorization_document, actual_authorization_sha = _load_canonical_json_file(
        _safe(root, authorization_path), "completed Gate-C authorization"
    )
    authorization_request = _mapping(
        authorization_document.get("request"), "completed Gate-C authorization request"
    )
    expected_top_level_keys = {
        "artifact_type",
        "authorization",
        "availability",
        "candidate",
        "candidate_basis_digest",
        "capabilities",
        "completion_id",
        "completion_rule_version",
        "completion_state",
        "counts",
        "outputs",
        "plan",
        "qa",
        "resource_measurements",
        "schema_version",
    }
    if (
        stable_digest(payload) != completion_id
        or set(document) != expected_top_level_keys
        or document.get("artifact_type") != "s7_full_market_sequence_execution_completion"
        or document.get("completion_rule_version")
        != "s7_full_market_sequence_execution_completion_v1"
        or document.get("completion_state") != CANDIDATE_STATE
        or document.get("schema_version") != 1
        or document.get("capabilities") != _candidate_capabilities()
        or document.get("plan") != {"path": plan_path, "plan_id": plan_id, "sha256": plan_sha256}
        or document.get("authorization")
        != {
            "authorization_id": authorization_id,
            "path": authorization_path,
            "request": authorization_request,
            "sha256": authorization_sha256,
        }
        or actual_authorization_sha != authorization_sha256
        or authorization_document.get("authorization_id") != authorization_id
        or set(candidate_ref) != {"bytes", "candidate_id", "manifest_id", "path", "sha256", "state"}
        or candidate_ref.get("state") != CANDIDATE_STATE
        or document.get("qa") != outputs.get("qa")
    ):
        raise IdentityMarketSequenceError("Gate-C execution completion differs")
    candidate_id = _digest(candidate_ref["candidate_id"], "completion candidate ID")
    _digest(candidate_ref["manifest_id"], "completion candidate manifest ID")
    expected_candidate_path = (
        "manifests/silver/identity/full-market-sequence-candidates/"
        f"candidate_id={candidate_id}/manifest.json"
    )
    candidate_receipt = {
        "bytes": _nonnegative_int(candidate_ref["bytes"], "completion candidate bytes"),
        "path": candidate_ref["path"],
        "sha256": candidate_ref["sha256"],
    }
    if candidate_ref["path"] != expected_candidate_path:
        raise IdentityMarketSequenceError("Gate-C completion candidate path differs")
    candidate_path = _verify_receipt(root, candidate_receipt, parquet=False)
    candidate_document, candidate_sha = _load_canonical_json_file(
        candidate_path, "completed Gate-C candidate"
    )
    basis = _mapping(candidate_document.get("candidate_basis"), "completed candidate basis")
    if (
        candidate_sha != candidate_ref["sha256"]
        or candidate_document.get("manifest_id") != candidate_ref["manifest_id"]
        or stable_digest(basis) != candidate_id
        or document.get("candidate_basis_digest") != candidate_id
        or basis.get("plan") != {"path": plan_path, "plan_id": plan_id, "sha256": plan_sha256}
        or basis.get("authorization")
        != {
            "authorization_id": authorization_id,
            "path": authorization_path,
            "request": authorization_request,
            "sha256": authorization_sha256,
        }
        or basis.get("resource_caps") != caps.to_dict()
        or candidate_document.get("availability") != document.get("availability")
    ):
        raise IdentityMarketSequenceError("Gate-C completion candidate binding differs")
    plan_availability = _mapping(plan["availability"], "plan availability")
    candidate = _candidate_from_existing(
        root,
        expected_candidate_path,
        candidate_id=candidate_id,
        basis=basis,
        created_at=str(plan["intent_captured_at_utc"]),
        actor=str(plan["prepared_by"]),
        classification_source_available_session=str(
            plan_availability["classification_source_available_session"]
        ),
        authorization_recorded_at=_parse_utc(
            authorization_document.get("recorded_at_utc"),
            "authorization recorded timestamp",
        ).isoformat(),
        caps=caps,
        idempotent=idempotent,
    )
    if (
        candidate_document.get("outputs") != outputs
        or candidate_document.get("counts") != document.get("counts")
        or candidate_document.get("resource_measurements") != document.get("resource_measurements")
    ):
        raise IdentityMarketSequenceError("Gate-C completion receipts differ from candidate")
    return replace(
        candidate,
        completion_id=str(completion_id),
        completion_path=completion_relative,
        completion_sha256=completion_sha,
    )


def _completion_path(plan_id: str, authorization_id: str) -> str:
    _digest(plan_id, "Gate-C completion plan ID")
    _digest(authorization_id, "Gate-C completion authorization ID")
    return (
        "manifests/silver/identity/full-market-sequence-completions/"
        f"plan_id={plan_id}/authorization_id={authorization_id}/manifest.json"
    )


def _validate_staging(staging: Path, manifest: Mapping[str, object]) -> None:
    outputs = _mapping(manifest.get("outputs"), "staged outputs")
    for value in outputs.values():
        receipt = _mapping(value, "staged receipt")
        relative = str(receipt["path"])
        leaf = relative.split("/", 5)[-1]
        path = staging / leaf
        if (
            not path.is_file()
            or path.is_symlink()
            or path.stat().st_size != receipt["bytes"]
            or sha256_file(path) != receipt["sha256"]
        ):
            raise IdentityMarketSequenceError("staged output receipt differs")


def _verify_receipt(root: Path, receipt: Mapping[str, object], *, parquet: bool) -> Path:
    relative = _relative_path(receipt.get("path"), "artifact receipt path")
    expected_bytes = _nonnegative_int(receipt.get("bytes"), "artifact receipt bytes")
    expected_sha = _digest(receipt.get("sha256"), "artifact receipt SHA-256")
    path = _safe(root, relative)
    if (
        not path.is_file()
        or path.is_symlink()
        or path.stat().st_size != expected_bytes
        or sha256_file(path) != expected_sha
    ):
        raise IdentityMarketSequenceError(f"artifact receipt differs: {relative}")
    if parquet:
        rows = _nonnegative_int(receipt.get("row_count"), "Parquet receipt rows")
        if pq.ParquetFile(path).metadata.num_rows != rows:
            raise IdentityMarketSequenceError(f"Parquet row count differs: {relative}")
    return path


def _staged_receipt(path: Path, final_relative: str) -> dict[str, object]:
    os.chmod(path, 0o444)
    _fsync_file(path)
    return {
        "bytes": path.stat().st_size,
        "path": final_relative,
        "sha256": sha256_file(path),
    }


def _staged_parquet_receipt(
    path: Path, final_relative: str, expected_rows: int
) -> dict[str, object]:
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows != expected_rows:
        raise IdentityMarketSequenceError("staged Parquet row count differs")
    return {
        **_staged_receipt(path, final_relative),
        "row_count": expected_rows,
        "schema_digest": stable_digest(str(parquet.schema_arrow)),
    }


def _load_canonical_json_file(path: Path, label: str) -> tuple[dict[str, object], str]:
    if not path.is_file() or path.is_symlink():
        raise IdentityMarketSequenceError(f"{label} is missing or unsafe")
    content = path.read_bytes()
    document = _decode_canonical_json(content, label)
    return document, hashlib.sha256(content).hexdigest()


def _read_exact_bytes(path: Path, expected_sha256: str, label: str) -> bytes:
    if not path.is_file() or path.is_symlink():
        raise IdentityMarketSequenceError(f"{label} is missing or unsafe")
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != expected_sha256:
        raise IdentityMarketSequenceError(f"{label} SHA-256 differs")
    return content


def _decode_json_object(content: bytes, label: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise IdentityMarketSequenceError(f"{label} has duplicate JSON keys")
            result[key] = value
        return result

    try:
        document = json.loads(content, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityMarketSequenceError(f"{label} is not valid JSON") from exc
    if not isinstance(document, dict):
        raise IdentityMarketSequenceError(f"{label} must be a JSON object")
    return document


def _decode_canonical_json(content: bytes, label: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise IdentityMarketSequenceError(f"{label} has duplicate JSON keys")
            result[key] = value
        return result

    try:
        document = json.loads(content, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityMarketSequenceError(f"{label} is not valid JSON") from exc
    if not isinstance(document, dict) or _canonical_bytes(document) != content:
        raise IdentityMarketSequenceError(f"{label} is not canonical JSON")
    return document


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )


def _compact_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise IdentityMarketSequenceError("reviewed source evidence is not JSON-safe") from exc


def _write_exact(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o444)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(content)
            handle.flush()
            os.fchmod(descriptor, 0o444)
            os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


class _exclusive_nonblocking_lock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def __enter__(self) -> _exclusive_nonblocking_lock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor: int | None = None
        try:
            descriptor = os.open(self.path, flags, 0o600)
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                raise IdentityMarketSequenceError("Gate-C lock is not a regular file")
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            visible = os.stat(self.path, follow_symlinks=False)
            if (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino):
                raise IdentityMarketSequenceError("Gate-C lock path changed during acquisition")
        except BlockingIOError as exc:
            if descriptor is not None:
                os.close(descriptor)
            raise IdentityMarketSequenceError("another Gate-C runner holds the lock") from exc
        except IdentityMarketSequenceError:
            if descriptor is not None:
                os.close(descriptor)
            raise
        except OSError as exc:
            if descriptor is not None:
                os.close(descriptor)
            raise IdentityMarketSequenceError("Gate-C lock cannot be acquired") from exc
        assert descriptor is not None
        self.fd = descriptor
        return self

    def __exit__(self, *_args: object) -> None:
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None


def _rename_directory_noreplace(source: Path, target: Path) -> None:
    if not source.is_dir() or source.is_symlink():
        raise IdentityMarketSequenceError("staging directory is missing or unsafe")
    source_stat = source.stat(follow_symlinks=False)
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform.startswith("linux"):
        rename = getattr(libc, "renameat2", None)
        if rename is None:
            raise IdentityMarketSequenceError("renameat2 is unavailable")
        rename.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        )
        rename.restype = ctypes.c_int
        result = rename(-100, os.fsencode(source), -100, os.fsencode(target), 1)
    elif sys.platform == "darwin":
        rename = getattr(libc, "renamex_np", None)
        if rename is None:
            raise IdentityMarketSequenceError("renamex_np is unavailable")
        rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        result = rename(os.fsencode(source), os.fsencode(target), 0x00000004)
    else:  # pragma: no cover - only supported production platforms
        raise IdentityMarketSequenceError("exclusive directory rename is unavailable")
    if result != 0:
        error_number = ctypes.get_errno() or errno.EIO
        raise IdentityMarketSequenceError(
            f"exclusive directory rename failed: {os.strerror(error_number)}"
        )
    target_stat = target.stat(follow_symlinks=False)
    if (source_stat.st_dev, source_stat.st_ino) != (target_stat.st_dev, target_stat.st_ino):
        raise IdentityMarketSequenceError("published candidate inode differs from staging")


def _validated_root(value: object) -> Path:
    if not isinstance(value, Path):
        raise IdentityMarketSequenceError("data_root must be a Path")
    expanded = value.expanduser()
    if expanded.is_symlink():
        raise IdentityMarketSequenceError("data_root cannot be a symlink")
    root = expanded.resolve()
    if not root.is_dir():
        raise IdentityMarketSequenceError("data_root must be an existing directory")
    return root


def _safe(root: Path, relative: str) -> Path:
    try:
        return safe_relative_path(root, relative)
    except ArtifactError as exc:
        raise IdentityMarketSequenceError(str(exc)) from exc


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise IdentityMarketSequenceError(f"{label} must be an object")
    return dict(value)


def _relative_path(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityMarketSequenceError(f"{label} must be text")
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise IdentityMarketSequenceError(f"{label} is not a safe relative path")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise IdentityMarketSequenceError(f"{label} must be lowercase SHA-256")
    return value


def _figi(value: object, label: str) -> str:
    if not isinstance(value, str) or _FIGI.fullmatch(value) is None:
        raise IdentityMarketSequenceError(f"{label} must be a valid FIGI")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IdentityMarketSequenceError(f"{label} must be a nonnegative integer")
    return value


def _positive_int(value: object, label: str) -> int:
    result = _nonnegative_int(value, label)
    if result == 0:
        raise IdentityMarketSequenceError(f"{label} must be positive")
    return result


def _safe_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value or len(value) > 200:
        raise IdentityMarketSequenceError(f"{label} is unsafe")
    lowered = value.casefold()
    if any(token in lowered for token in ("api_key", "password", "secret", "token=")):
        raise IdentityMarketSequenceError(f"{label} may contain secret material")
    return value


def _parse_utc(value: object, label: str) -> datetime:
    text = _utc_text(value, label)
    return datetime.fromisoformat(text)


def _utc_text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityMarketSequenceError(f"{label} must be text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IdentityMarketSequenceError(f"{label} is not ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise IdentityMarketSequenceError(f"{label} must be UTC")
    if parsed.isoformat() != value:
        raise IdentityMarketSequenceError(f"{label} must be canonical ISO-8601")
    return value


def _date_text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityMarketSequenceError(f"{label} must be text")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise IdentityMarketSequenceError(f"{label} is not an ISO date") from exc
    if parsed.isoformat() != value:
        raise IdentityMarketSequenceError(f"{label} is not canonical")
    return value


def _xnys_session_text(
    value: object,
    label: str,
    *,
    calendar: XNYSCalendarArtifact,
) -> str:
    text = _date_text(value, label)
    try:
        calendar.market_open(date.fromisoformat(text))
    except XNYSCalendarArtifactError as exc:
        raise IdentityMarketSequenceError(f"{label} must be in the bound XNYS calendar") from exc
    return text


def _choose_column(names: set[str], choices: Sequence[str], label: str) -> str:
    selected = [name for name in choices if name in names]
    if len(selected) != 1:
        raise IdentityMarketSequenceError(f"Gate-B {label} column is ambiguous or missing")
    return selected[0]


def _us_context(locale: object, mic: object) -> bool:
    return str(locale or "").casefold() == "us" or str(mic or "").upper() in US_PRIMARY_MICS


def _fail_closed_capabilities() -> dict[str, bool]:
    return {
        "adjudication_authorized": False,
        "canonical_identity_override_authorized": False,
        "full_run_authorized": False,
        "identity_registry_authorized": False,
        "membership_mutation_authorized": False,
        "publication_authorized": False,
        "provider_observation_mutation_authorized": False,
    }


def _candidate_capabilities() -> dict[str, bool]:
    return {
        "adjudication_present": False,
        "canonical_identity_override_present": False,
        "full_run_authorized": False,
        "identity_registry_authorized": False,
        "membership_mutation_authorized": False,
        "publication_authorized": False,
        "provider_observation_mutation_authorized": False,
    }


def _load_bound_calendar(root: Path) -> XNYSCalendarArtifact:
    if xcals.__version__ != CALENDAR_ENGINE_VERSION:
        raise IdentityMarketSequenceError("exchange-calendars runtime version differs")
    try:
        return load_xnys_calendar_artifact(
            root,
            calendar_artifact_id=CALENDAR_ARTIFACT_ID,
            expected_sha256=CALENDAR_ARTIFACT_SHA256,
        )
    except XNYSCalendarArtifactError as exc:
        raise IdentityMarketSequenceError("bound XNYS calendar artifact is invalid") from exc


def _calendar_binding(calendar: XNYSCalendarArtifact) -> dict[str, object]:
    return {
        "artifact_id": calendar.calendar_artifact_id,
        "calendar_name": CALENDAR_NAME,
        "library": "exchange-calendars",
        "library_version": CALENDAR_ENGINE_VERSION,
        "path": calendar.relative_path,
        "rule_version": CALENDAR_ARTIFACT_RULE_VERSION,
        "sha256": calendar.sha256,
    }


def _runtime_paths() -> tuple[Path, ...]:
    from ame_stocks_api.cli import silver_identity_market_sequence as cli_module

    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(cli_module.__file__).resolve(),
                Path(artifacts_module.__file__).resolve(),
                Path(market_consistency_module.__file__).resolve(),
                Path(inventory_plan_module.__file__).resolve(),
                Path(load_xnys_calendar_artifact.__code__.co_filename).resolve(),
                Path(preview_runner_module.__file__).resolve(),
                Path(provider_evidence_module.__file__).resolve(),
                Path(identity_source_module.__file__).resolve(),
            }
        )
    )


def _runtime_provenance() -> dict[str, object]:
    repository = _repository_root()
    if _git(repository, "rev-parse", "--show-toplevel") != str(repository):
        raise IdentityMarketSequenceError("Gate-C runtime repository root differs")
    status = _git_bytes(
        repository,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        "-z",
    )
    if status:
        raise IdentityMarketSequenceError("Gate-C runtime repository is not clean")
    commit = _git(repository, "rev-parse", "HEAD")
    tree = _git(repository, "rev-parse", "HEAD^{tree}")
    if not re.fullmatch(r"[0-9a-f]{40}", commit) or not re.fullmatch(r"[0-9a-f]{40}", tree):
        raise IdentityMarketSequenceError("Gate-C Git commit/tree is invalid")
    if _git_returncode(repository, "diff-index", "--cached", "--quiet", "HEAD", "--") != 0:
        raise IdentityMarketSequenceError("Gate-C Git index tree differs from HEAD")
    index_tree = tree
    index_listing = _git_bytes(repository, "ls-files", "--stage", "-z")
    output: list[dict[str, object]] = []
    for path in _runtime_paths():
        try:
            relative = path.relative_to(repository).as_posix()
        except ValueError as exc:  # pragma: no cover - broken installation
            raise IdentityMarketSequenceError("runtime file is outside the repository") from exc
        index_entry = _git(repository, "ls-files", "--stage", "--", relative)
        match = re.fullmatch(r"([0-7]{6}) ([0-9a-f]{40}) 0\t(.+)", index_entry)
        if match is None or match.group(3) != relative:
            raise IdentityMarketSequenceError(f"runtime file is not uniquely tracked: {relative}")
        mode, blob = match.group(1), match.group(2)
        head_entry = _git(repository, "ls-tree", "HEAD", "--", relative)
        if head_entry != f"{mode} blob {blob}\t{relative}":
            raise IdentityMarketSequenceError(
                f"runtime file HEAD/index binding differs: {relative}"
            )
        actual_blob = _git(repository, "hash-object", "--", relative)
        if actual_blob != blob:
            raise IdentityMarketSequenceError(f"runtime file worktree blob differs: {relative}")
        output.append(
            {
                "bytes": path.stat().st_size,
                "git_blob": blob,
                "git_mode": mode,
                "path": relative,
                "sha256": sha256_file(path),
            }
        )
    return {
        "git": {
            "commit": commit,
            "head_tree": tree,
            "index_listing_sha256": hashlib.sha256(index_listing).hexdigest(),
            "index_matches_head": True,
            "index_tree": index_tree,
            "repository_clean": True,
        },
        "runtime_files": output,
        "versions": {
            "exchange_calendars": xcals.__version__,
            "polars": _package_version("polars"),
            "pyarrow": pa.__version__,
            "python_cache_tag": sys.implementation.cache_tag,
            "python_implementation": platform.python_implementation(),
            "python_version": platform.python_version(),
        },
    }


def _git(repository: Path, *arguments: str) -> str:
    return _git_bytes(repository, *arguments).decode("utf-8").rstrip("\n")


def _git_bytes(repository: Path, *arguments: str) -> bytes:
    try:
        result = subprocess.run(
            ("git", "-C", str(repository), *arguments),
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise IdentityMarketSequenceError(
            f"Gate-C Git provenance command failed: {' '.join(arguments)}"
        ) from exc
    return result.stdout


def _git_returncode(repository: Path, *arguments: str) -> int:
    try:
        return subprocess.run(
            ("git", "-C", str(repository), *arguments),
            check=False,
            capture_output=True,
        ).returncode
    except OSError as exc:
        raise IdentityMarketSequenceError(
            f"Gate-C Git provenance command failed: {' '.join(arguments)}"
        ) from exc


def _package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError as exc:
        raise IdentityMarketSequenceError(f"Gate-C runtime package is missing: {name}") from exc


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _tree_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "GATE_C_RULE_VERSION",
    "IdentityMarketSequenceError",
    "S7MarketSequenceAuthorization",
    "S7MarketSequenceCandidate",
    "S7MarketSequencePlan",
    "S7MarketSequenceResourceCaps",
    "authorize_market_sequence_plan_under_standing_grant",
    "authorize_market_sequence_plan_with_exact_literal",
    "exact_market_sequence_approval_literal",
    "prepare_market_sequence_plan",
    "run_source_bound_market_sequence",
]
