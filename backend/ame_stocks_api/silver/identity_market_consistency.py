"""Fail-closed OpenFIGI market evidence for the S7 Composite inventory.

The production entry point is intentionally narrow.  It binds the one approved Gate-A
inventory, the reviewed S7 external-evidence manifests, an exact user authorization, and
the frozen XNYS calendar before it can make a network request.  Every HTTP attempt is an
immutable Bronze artifact; a successful attempt becomes usable only after a separate
atomic batch commit.  No function in this module mutates provider observations, creates
an identity override, adjudicates an asset, or publishes an S7 research table.
"""

from __future__ import annotations

import base64
import binascii
import fcntl
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import re
import shutil
import stat
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Final

import polars as pl
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
    write_parquet_immutable,
)
from ame_stocks_api.silver.calendar_artifact import (
    CALENDAR_ARTIFACT_RULE_VERSION,
    CALENDAR_NAME,
    XNYSCalendarArtifactError,
    load_xnys_calendar_artifact,
)

OPENFIGI_MAPPING_ENDPOINT: Final = "https://api.openfigi.com/v3/mapping"
MARKET_CONSISTENCY_RUN_VERSION: Final = "s7_openfigi_market_consistency_capture_v3"
MARKET_CLASSIFICATION_VERSION: Final = "s7_openfigi_composite_market_classification_v2"
DIRECT_APPROVAL_SLOT_VERSION: Final = (
    "s7_gate_b_standing_approval_slot_v3_schema_inference_recovery"
)
ATTEMPT_INTENT_VERSION: Final = "s7_openfigi_http_attempt_intent_v1"
INVENTORY_COLUMN: Final = "observed_composite_figi"
ANONYMOUS_JOBS_PER_REQUEST: Final = 10
AUTHENTICATED_JOBS_PER_REQUEST: Final = 100
ANONYMOUS_MIN_INTERVAL_SECONDS: Final = 60.0 / 25.0
AUTHENTICATED_MIN_INTERVAL_SECONDS: Final = 60.0 / 250.0
MAX_RESPONSE_BYTES: Final = 64 * 1024 * 1024
MAX_ATTEMPTS_PER_BATCH: Final = 8
MAX_CUMULATIVE_RESPONSE_BYTES: Final = 8 * 1024 * 1024 * 1024
MAX_CAPTURE_WALL_CLOCK_SECONDS: Final = 8 * 60 * 60
PRODUCTION_DISK_FREE_HARD_FLOOR_BYTES: Final = 40 * 1024 * 1024 * 1024
PRODUCTION_DATA_ROOT: Final = Path("/mnt/HC_Volume_106309665/american_stocks")

# Approved Gate-A output.  Production CLI commands expose no way to replace these pins.
PRODUCTION_INVENTORY_ROW_COUNT: Final = 18_421
PRODUCTION_INVENTORY_CANDIDATE_ID: Final = (
    "b35dc51b5798db2f8cf7783a1f2953990898bc5dde539107beabe53d85a57044"
)
PRODUCTION_INVENTORY_CANDIDATE_SHA256: Final = (
    "11fa38df8aaa07a781e80e80d0844213bf7d859cba3826ef26c693d735697970"
)
PRODUCTION_INVENTORY_DATA_SHA256: Final = (
    "2225aacfca90676b4cb3555b37bc956955ea28b336c5ceefec74fc8ec0b02ceb"
)
PRODUCTION_INVENTORY_COMPLETION_ID: Final = (
    "4472b730bbf5e77b19253c0f6bfc4b78df3135bc2f46424262fff7f735cdce15"
)
PRODUCTION_INVENTORY_COMPLETION_SHA256: Final = (
    "255197634284c23c0b42f17b59398c07d5ab1d9d8c9f82493a363924a240a282"
)
PRODUCTION_INVENTORY_PLAN_ID: Final = (
    "57dcfe2cd7431105e0b664163a75e76a42a023e777055bad935b548f41935eb5"
)
PRODUCTION_INVENTORY_APPROVAL_ID: Final = (
    "9a0b6f07cd6c1294dc1c086cc26b3d94343624c482fae6d2eca2498075f90d5c"
)
PRODUCTION_INVENTORY_CANDIDATE_PATH: Final = (
    "manifests/silver/identity/composite-inventory-candidates/"
    f"candidate_id={PRODUCTION_INVENTORY_CANDIDATE_ID}/manifest.json"
)
PRODUCTION_INVENTORY_DATA_PATH: Final = (
    "manifests/silver/identity/composite-inventory-candidates/"
    f"candidate_id={PRODUCTION_INVENTORY_CANDIDATE_ID}/data/part-00000.parquet"
)
PRODUCTION_INVENTORY_COMPLETION_PATH: Final = (
    "manifests/silver/identity/composite-inventory-execution-completions/"
    f"plan_id={PRODUCTION_INVENTORY_PLAN_ID}/"
    f"approval_id={PRODUCTION_INVENTORY_APPROVAL_ID}/manifest.json"
)

XNYS_CALENDAR_ARTIFACT_ID: Final = (
    "31cc575ae55542a580ee17e09aa242159bbcaedd0a001fd2184021a541b734bd"
)
XNYS_CALENDAR_ARTIFACT_SHA256: Final = (
    "3f026761a9f752d1e00c89c9f72383e7d8c0a7f7dcb2cdf8ef82e5831dfc0da7"
)

CROSS_MARKET_EVIDENCE_PATH: Final = (
    "docs/silver/evidence/s7-cross-market/"
    "identity-cross-market-external-evidence-manifest.candidate.json"
)
CROSS_MARKET_EVIDENCE_ID: Final = "2ae779168e3e56887a5b0ae557bb928b6006c1b96392fe1606c201e1649ff848"
CROSS_MARKET_EVIDENCE_SHA256: Final = (
    "9544537ac7e6817c1b8f946c9ae2d5afb65399b1b553c3fe233a298614b375ab"
)
EXACT_GROUP_EVIDENCE_PATH: Final = (
    "docs/silver/evidence/s7-exact-groups/"
    "identity-exact-group-external-evidence-manifest.candidate.json"
)
EXACT_GROUP_EVIDENCE_ID: Final = "30e3cd9f009c995ce594fd19344ce551ea39133a9c60caea661a7c7211743fdd"
EXACT_GROUP_EVIDENCE_SHA256: Final = (
    "e1a6d365fb2d12f913461576f51003ecabf1bb91a74e546cc717660578f0b17b"
)

S7_CONTINUING_AUTHORIZATION_TEXT: Final = (
    "为什么你就不能自己直接把S7运行完呢，我允许你这么做，只要中间不报错或者明显越界就可以自行继续"  # noqa: RUF001
)
S7_CONTINUING_AUTHORIZATION_SHA256: Final = hashlib.sha256(
    S7_CONTINUING_AUTHORIZATION_TEXT.encode("utf-8")
).hexdigest()
S7_REAFFIRMATION_TEXT: Final = "批准"
S7_REAFFIRMATION_SHA256: Final = hashlib.sha256(S7_REAFFIRMATION_TEXT.encode("utf-8")).hexdigest()

_AUTHORIZED_ACTIONS: Final = (
    "capture_exact_approved_inventory_openfigi_attempts",
    "resume_exact_capture_under_rate_and_resource_caps",
    "materialize_offline_market_classification_candidate_to_awaiting_review",
)
_GATE_B_RECOVERY_PREDECESSOR: Final = {
    "approval_slot_id": "f39167969acee0a41e0069fcd6531c00b27469bd2265deef614a4e076aa03455",
    "capture_run_id": "c9d4ef9973878126036e0f4d5e398dd160424e09ad8a6a7e99a263c31f0d6584",
    "disposition": (
        "capture_complete_not_consumed_due_to_candidate_frame_schema_inference_failure"
    ),
    "runtime_commit": "609ac20fe13f63e7ceb76cf738f4d6b55b78b466",
}
_DIRECT_APPROVAL_SLOT_BASIS: Final = {
    "approval_slot_version": DIRECT_APPROVAL_SLOT_VERSION,
    "authorized_actions": list(_AUTHORIZED_ACTIONS),
    "continuing_authorization_sha256": S7_CONTINUING_AUTHORIZATION_SHA256,
    "production_data_root": PRODUCTION_DATA_ROOT.as_posix(),
    "reaffirmation_sha256": S7_REAFFIRMATION_SHA256,
    "recovery_predecessor": dict(_GATE_B_RECOVERY_PREDECESSOR),
}
DIRECT_APPROVAL_SLOT_ID: Final = stable_digest(_DIRECT_APPROVAL_SLOT_BASIS)
_FALSE_CAPABILITIES: Final = {
    "adjudication_plan": False,
    "asset_master_materialization": False,
    "canonical_identity_override": False,
    "full_run": False,
    "identity_eligibility_decision": False,
    "provider_observation_mutation": False,
    "publication": False,
    "registry_release": False,
    "ticker_alias_materialization": False,
    "universe_daily_materialization": False,
}
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_ATTEMPT_DIRECTORY = re.compile(r"^attempt_index=(\d{6})$")
_ALLOWED_RESPONSE_HEADERS = frozenset(
    {
        "content-length",
        "content-type",
        "date",
        "ratelimit-limit",
        "ratelimit-remaining",
        "ratelimit-reset",
        "retry-after",
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-reset",
    }
)
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_RUNTIME_SOURCE_PATHS: Final = (
    "backend/ame_stocks_api/artifacts.py",
    "backend/ame_stocks_api/cli/silver_identity_market_consistency.py",
    "backend/ame_stocks_api/silver/calendar_artifact.py",
    "backend/ame_stocks_api/silver/identity_market_consistency.py",
)
_UNRESOLVED_CLASSIFICATIONS = frozenset(
    {
        "unresolved_invalid_projection",
        "unresolved_job_error",
        "unresolved_mixed_market",
        "unresolved_no_mapping",
        "unresolved_seed_drift",
        "unresolved_share_class_conflict",
        "unresolved_source_unavailable",
    }
)

# This is deliberately the sole no-self exception.  It is replayed from the approved
# cross-market evidence manifest before use; it cannot expand through current API output.
_FROZEN_NO_SELF_RELATION_EXCEPTIONS: Final = {
    "BBG00R4FG9L2": {
        "figi": "BBG00R4FG9M1",
        "compositeFIGI": "BBG00R4FG9L2",
        "exchCode": "EP",
        "shareClassFIGI": "BBG001T49NZ9",
    }
}


class IdentityMarketConsistencyError(ArtifactError):
    """Raised when an OpenFIGI evidence run cannot be reproduced safely."""


@dataclass(frozen=True, slots=True)
class HttpResult:
    status: int
    headers: Mapping[str, str]
    body: bytes


@dataclass(frozen=True, slots=True)
class MarketConsistencyRun:
    run_id: str
    request_manifest_path: str
    composite_count: int
    batch_count: int
    completed_batch_count: int
    final_manifest_path: str | None
    idempotent: bool


@dataclass(frozen=True, slots=True)
class MarketClassification:
    composite_figi: str
    classification: str
    projection_classification: str
    market_codes: tuple[str, ...]
    exact_row_count: int
    response_batch_index: int
    self_row_count: int = 0
    selected_figi: str | None = None
    selected_share_class_figi: str | None = None
    job_error: str | None = None
    projection_reason_codes: tuple[str, ...] = ()
    relationship_seed_status: str = "not_seed"
    returned_figis: tuple[str, ...] = ()
    returned_composite_figis: tuple[str, ...] = ()
    returned_share_class_figis: tuple[str, ...] = ()
    returned_exchange_codes: tuple[str, ...] = ()
    returned_security_types: tuple[str, ...] = ()
    returned_security_types2: tuple[str, ...] = ()
    returned_security_descriptions: tuple[str, ...] = ()
    returned_market_sectors: tuple[str, ...] = ()
    selected_market_code: str | None = None
    selected_market_sector: str | None = None
    selected_security_type: str | None = None
    selected_security_type2: str | None = None
    selected_security_description: str | None = None
    relation_projection_json: str = "[]"
    raw_response_attempt_id: str | None = None
    raw_response_attempt_path: str | None = None
    raw_response_attempt_sha256: str | None = None
    raw_response_attempt_bytes: int | None = None
    request_started_at_utc: str | None = None
    response_received_at_utc: str | None = None


@dataclass(frozen=True, slots=True)
class MarketClassificationCandidate:
    candidate_id: str
    manifest_path: str
    data_path: str
    qa_path: str
    example_path: str
    composite_count: int
    us_composite_count: int
    non_us_composite_count: int
    unresolved_composite_count: int
    non_us_provider_row_count: int
    unresolved_provider_row_count: int
    idempotent: bool


@dataclass(frozen=True, slots=True)
class _BatchSpec:
    index: int
    figis: tuple[str, ...]
    jobs: tuple[dict[str, object], ...]
    batch_id: str
    prefix: str
    request_bytes: bytes

    @property
    def request_path(self) -> str:
        return f"{self.prefix}/request.json"

    @property
    def accepted_path(self) -> str:
        return f"{self.prefix}/accepted.json"

    def attempt_path(self, attempt_index: int) -> str:
        return f"{self.prefix}/attempts/attempt_index={attempt_index:06d}/attempt.json"

    def attempt_intent_path(self, attempt_index: int) -> str:
        return f"{self.prefix}/attempts/attempt_index={attempt_index:06d}/intent.json"


def prepare_market_consistency_run(
    data_root: Path,
    *,
    inventory_data_path: str,
    inventory_data_sha256: str,
    inventory_candidate_id: str,
    inventory_candidate_sha256: str,
    prepared_at_utc: str,
    prepared_by: str,
    authenticated: bool,
) -> MarketConsistencyRun:
    """Prepare a deterministic fixture/development run without network access.

    Production CLI never calls this permissive interface.  It exists for bounded fixtures
    and callers that need to test capture semantics without impersonating the approved
    18,421-row Gate-A artifact.
    """

    root = _root(data_root)
    _digest(inventory_data_sha256, "inventory data SHA-256")
    _digest(inventory_candidate_id, "inventory candidate ID")
    _digest(inventory_candidate_sha256, "inventory candidate SHA-256")
    source = safe_relative_path(root, inventory_data_path)
    _verify_regular_file(source, inventory_data_sha256, "inventory data")
    figis = _read_inventory_figis(source)
    binding = {
        "candidate": {
            "candidate_id": inventory_candidate_id,
            "path": None,
            "sha256": inventory_candidate_sha256,
        },
        "completion": None,
        "data": {
            "bytes": source.stat().st_size,
            "path": inventory_data_path,
            "row_count": len(figis),
            "sha256": inventory_data_sha256,
        },
        "mode": "fixture",
    }
    return _prepare_run(
        root,
        inventory_binding=binding,
        prepared_at_utc=prepared_at_utc,
        prepared_by=prepared_by,
        authenticated=authenticated,
        direct_approval=None,
        runtime_binding=None,
    )


def prepare_approved_market_consistency_run(
    data_root: Path,
    *,
    authorization_text: str,
    reaffirmation_text: str,
    approved_by: str,
    prepared_by: str,
    authenticated: bool,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> MarketConsistencyRun:
    """Prepare the one production Gate-B run and its replayable direct approval."""

    root = _root(data_root)
    _require_canonical_production_root(root)
    if authorization_text != S7_CONTINUING_AUTHORIZATION_TEXT:
        raise IdentityMarketConsistencyError(
            "production authorization text differs from the pinned S7 continuation literal"
        )
    if reaffirmation_text != S7_REAFFIRMATION_TEXT:
        raise IdentityMarketConsistencyError(
            "production reaffirmation text differs from the pinned latest approval literal"
        )
    evidence = _load_external_evidence_binding()
    # This binding is reconstructed only from the two frozen Gate-A JSON manifests.
    # Persist the single standing authorization before any inventory Parquet byte is
    # hashed or opened.  The full DATA replay below must then equal this manifest-only
    # projection exactly.
    inventory = _production_inventory_manifest_binding(root)
    calendar = _calendar_binding(root)
    runtime_binding = _repository_runtime_binding()
    approver = _text(approved_by, "approved_by")
    preparer = _text(prepared_by, "prepared_by")
    caps = _resource_caps(
        authenticated,
        inventory_row_count=_positive_int(
            _mapping(inventory.get("data"), "production inventory DATA").get("row_count"),
            "production inventory row count",
        ),
        production=True,
    )
    approval_scope = {
        "artifact_type": "s7_openfigi_market_consistency_direct_approval",
        "approval_slot_id": DIRECT_APPROVAL_SLOT_ID,
        "approval_slot_version": DIRECT_APPROVAL_SLOT_VERSION,
        "approval_reaffirmation": {
            "literal_text": reaffirmation_text,
            "literal_text_sha256": S7_REAFFIRMATION_SHA256,
        },
        "authenticated": authenticated,
        "authorized_actions": list(_AUTHORIZED_ACTIONS),
        "calendar_binding": calendar,
        "continuing_authorization": {
            "literal_text": authorization_text,
            "literal_text_sha256": S7_CONTINUING_AUTHORIZATION_SHA256,
        },
        "external_evidence_binding": evidence,
        "false_capabilities": dict(_FALSE_CAPABILITIES),
        "inventory_binding": inventory,
        "recovery_predecessor": dict(_GATE_B_RECOVERY_PREDECESSOR),
        "resource_caps": caps,
        "runtime_binding": runtime_binding,
    }
    # The physical production slot is static.  Authentication mode, runtime, inventory,
    # calendar, caps, actors, or any other scope drift therefore collides with the first
    # receipt and fails closed instead of silently creating another capture lane.
    approval_id = stable_digest(approval_scope)
    relative = _direct_approval_path()
    approval_path = safe_relative_path(root, relative)
    lock_path = safe_relative_path(
        root,
        f"tmp/s7-openfigi-market-consistency/direct-approval-slot-{DIRECT_APPROVAL_SLOT_ID}.lock",
    )
    with _exclusive_nonblocking_lock(lock_path):
        if approval_path.exists():
            document = _load_exact_json(approval_path, "direct approval")
            _verify_direct_approval_slot_document(
                root,
                document,
                expected_scope=approval_scope,
                expected_approval_id=approval_id,
            )
            if document.get("approved_by") != approver or document.get("prepared_by") != preparer:
                raise IdentityMarketConsistencyError(
                    "Gate-B standing approval slot actors differ from the first receipt"
                )
        else:
            approval_time_value = _now_utc(now, "approved_at_utc")
            approval_availability = _derive_control_availability(
                root,
                approval_time_value,
                controlling_field="approval_recorded_at_utc",
                rule="first_bound_xnys_open_strictly_after_approval_recorded_at_v1",
            )
            document = {
                **approval_scope,
                "approval_id": approval_id,
                "approved_at_utc": approval_time_value.isoformat(),
                "approved_by": approver,
                "approval_availability": approval_availability,
                "prepared_by": preparer,
            }
            write_bytes_immutable(root, approval_path, _canonical_json(document))
            _verify_direct_approval_slot_document(
                root,
                document,
                expected_scope=approval_scope,
                expected_approval_id=approval_id,
            )
    receipt = _file_receipt(root, relative)
    direct_approval = {
        **receipt,
        "approval_id": approval_id,
    }
    verified_inventory = _verify_production_inventory_binding(root)
    if verified_inventory != inventory:
        raise IdentityMarketConsistencyError(
            "production inventory Parquet replay differs from its authorized manifest binding"
        )
    # Reusing the recorded approval timestamp makes request construction deterministic
    # even if the process died after persisting the approval but before persisting the
    # request manifest.
    preparation_time = _parse_timestamp(document.get("approved_at_utc"), "approved_at_utc")
    return _prepare_run(
        root,
        inventory_binding=verified_inventory,
        prepared_at_utc=preparation_time.isoformat(),
        prepared_by=preparer,
        authenticated=authenticated,
        direct_approval=direct_approval,
        runtime_binding=runtime_binding,
    )


def _prepare_run(
    root: Path,
    *,
    inventory_binding: Mapping[str, object],
    prepared_at_utc: str,
    prepared_by: str,
    authenticated: bool,
    direct_approval: Mapping[str, object] | None,
    runtime_binding: Mapping[str, object] | None,
) -> MarketConsistencyRun:
    prepared_at = _timestamp(prepared_at_utc, "prepared_at_utc")
    actor = _text(prepared_by, "prepared_by")
    if type(authenticated) is not bool:
        raise IdentityMarketConsistencyError("authenticated must be a native bool")
    data = _mapping(inventory_binding.get("data"), "inventory DATA binding")
    figis = _read_inventory_figis(
        safe_relative_path(root, _text(data.get("path"), "inventory DATA path"))
    )
    if len(figis) != _positive_int(data.get("row_count"), "inventory row count"):
        raise IdentityMarketConsistencyError("inventory binding row count differs")
    evidence = _load_external_evidence_binding()
    calendar = _calendar_binding(root)
    if inventory_binding.get("mode") == "production":
        _require_seed_inventory_coverage(figis, evidence)
        if runtime_binding is None:
            raise IdentityMarketConsistencyError("production request lacks a runtime binding")
    elif runtime_binding is not None:
        raise IdentityMarketConsistencyError("fixture request cannot bind production runtime code")
    jobs_per_request = (
        AUTHENTICATED_JOBS_PER_REQUEST if authenticated else ANONYMOUS_JOBS_PER_REQUEST
    )
    payload = {
        "artifact_type": "s7_openfigi_market_consistency_run_request",
        "authenticated": authenticated,
        "calendar_binding": calendar,
        "direct_approval": dict(direct_approval) if direct_approval is not None else None,
        "endpoint": OPENFIGI_MAPPING_ENDPOINT,
        "external_evidence_binding": evidence,
        "inventory_binding": dict(inventory_binding),
        "job_order_digest": stable_digest(figis),
        "jobs_per_request": jobs_per_request,
        "market_sector_description": "Equity",
        "prepared_at_utc": prepared_at,
        "prepared_by": actor,
        "request_count": (len(figis) + jobs_per_request - 1) // jobs_per_request,
        "request_version": MARKET_CONSISTENCY_RUN_VERSION,
        "resource_caps": _resource_caps(
            authenticated,
            inventory_row_count=len(figis),
            production=inventory_binding.get("mode") == "production",
        ),
        "runtime_binding": dict(runtime_binding) if runtime_binding is not None else None,
        "source_capabilities": dict(_FALSE_CAPABILITIES),
    }
    run_id = stable_digest(payload)
    document = {**payload, "run_id": run_id}
    relative = _request_manifest_path(run_id)
    write_bytes_immutable(root, root / relative, _canonical_json(document))
    return MarketConsistencyRun(
        run_id=run_id,
        request_manifest_path=relative,
        composite_count=len(figis),
        batch_count=int(payload["request_count"]),
        completed_batch_count=0,
        final_manifest_path=None,
        idempotent=False,
    )


def execute_market_consistency_run(
    data_root: Path,
    *,
    run_id: str,
    api_key: str | None = None,
    http_post: Callable[[str, bytes, Mapping[str, str]], HttpResult] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    max_batches: int | None = None,
    require_production_approval: bool = False,
) -> MarketConsistencyRun:
    """Resume a capture under a per-run lock and commit accepted batches atomically."""

    root = _root(data_root)
    _digest(run_id, "run ID")
    if max_batches is not None and (type(max_batches) is not int or max_batches <= 0):
        raise IdentityMarketConsistencyError("max_batches must be a positive native int")
    lock_path = safe_relative_path(root, f"tmp/s7-openfigi-market-consistency/run_id={run_id}.lock")
    with _exclusive_nonblocking_lock(lock_path):
        request = _load_and_verify_request(
            root,
            run_id=run_id,
            require_production_approval=require_production_approval,
        )
        authenticated = _native_bool(request.get("authenticated"), "authenticated")
        if authenticated != bool(api_key):
            raise IdentityMarketConsistencyError(
                "prepared authentication mode differs from API-key availability"
            )
        figis, specs = _rebuild_batch_specs(root, request)
        final_relative = _final_manifest_path(run_id)
        final_path = safe_relative_path(root, final_relative)
        if final_path.exists():
            _verify_final_manifest(root, final_path, request_document=request)
            return MarketConsistencyRun(
                run_id=run_id,
                request_manifest_path=_request_manifest_path(run_id),
                composite_count=len(figis),
                batch_count=len(specs),
                completed_batch_count=len(specs),
                final_manifest_path=final_relative,
                idempotent=True,
            )

        post = http_post or _urllib_post
        runner_started = time.monotonic()
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if api_key:
            headers["X-OPENFIGI-APIKEY"] = api_key
        min_interval = (
            AUTHENTICATED_MIN_INTERVAL_SECONDS if authenticated else ANONYMOUS_MIN_INTERVAL_SECONDS
        )
        caps = _mapping(request.get("resource_caps"), "resource caps")
        # Recover every pre-send durable intent before counting usage.  An intent without
        # a result represents a request whose network outcome cannot be known after a
        # crash.  It is finalized as consumed/unknown and never reuses its attempt index.
        for spec in specs:
            request_path = safe_relative_path(root, spec.request_path)
            if not request_path.exists():
                continue
            request_receipt = _file_receipt(root, spec.request_path)
            _resolve_orphan_attempts(
                root,
                spec,
                request_receipt=request_receipt,
                run_id=run_id,
                min_interval_seconds=min_interval,
                now=now,
            )
        attempt_count, cumulative_response_bytes, latest_retry_not_before = _capture_usage(
            root, specs
        )
        _enforce_capture_caps(
            root,
            caps,
            runner_started=runner_started,
            attempt_count=attempt_count,
            cumulative_response_bytes=cumulative_response_bytes,
        )
        completed_commits: list[dict[str, object]] = []
        newly_completed = 0
        for spec in specs:
            request_receipt = write_bytes_immutable(
                root,
                root / spec.request_path,
                spec.request_bytes,
            )
            accepted_path = safe_relative_path(root, spec.accepted_path)
            if accepted_path.exists():
                commit = _verify_batch_commit(
                    root,
                    _load_exact_json(accepted_path, "batch commit"),
                    request_document=request,
                    spec=spec,
                    request_receipt=request_receipt,
                )
                completed_commits.append(commit)
                continue
            if max_batches is not None and newly_completed >= max_batches:
                break

            attempts = _load_attempts(
                root,
                spec,
                request_receipt=request_receipt,
                run_id=run_id,
            )
            if attempts:
                terminal = attempts[-1]
                if terminal.get("http_status") == 200 and _response_is_acceptable(
                    _attempt_body(terminal), expected_jobs=len(spec.figis)
                ):
                    commit = _write_batch_commit(
                        root,
                        spec=spec,
                        request_receipt=request_receipt,
                        run_id=run_id,
                        terminal_status="response_accepted",
                        now=now,
                    )
                    completed_commits.append(commit)
                    newly_completed += 1
                    continue
                if not all(
                    _attempt_is_source_unavailable(item, expected_jobs=len(spec.figis))
                    for item in attempts
                ):
                    raise IdentityMarketConsistencyError(
                        f"OpenFIGI batch {spec.index} has an immutable non-retryable attempt"
                    )
                if len(attempts) == MAX_ATTEMPTS_PER_BATCH:
                    commit = _write_batch_commit(
                        root,
                        spec=spec,
                        request_receipt=request_receipt,
                        run_id=run_id,
                        terminal_status="source_unavailable",
                        now=now,
                    )
                    completed_commits.append(commit)
                    newly_completed += 1
                    continue
            next_attempt = len(attempts)
            while next_attempt < MAX_ATTEMPTS_PER_BATCH:
                _enforce_capture_caps(
                    root,
                    caps,
                    runner_started=runner_started,
                    attempt_count=attempt_count,
                    cumulative_response_bytes=cumulative_response_bytes,
                    prospective_attempt_count=1,
                )
                request_started = _rate_limited_start(
                    now=now,
                    sleep=sleep,
                    retry_not_before=latest_retry_not_before,
                )
                attempt_intent = _write_attempt_intent(
                    root,
                    spec=spec,
                    attempt_index=next_attempt,
                    request_receipt=request_receipt,
                    request_started=request_started,
                    min_interval_seconds=min_interval,
                )
                attempt_count += 1
                status: int | None = None
                response_headers: dict[str, str] = {}
                response_body = b""
                transport_error_type: str | None = None
                try:
                    result = post(OPENFIGI_MAPPING_ENDPOINT, spec.request_bytes, headers)
                    if not isinstance(result, HttpResult):
                        raise TypeError("HTTP adapter returned a non-HttpResult")
                    if type(result.status) is not int or result.status < 100 or result.status > 599:
                        raise TypeError("HTTP adapter returned an invalid status")
                    if not isinstance(result.body, bytes):
                        raise TypeError("HTTP adapter returned a non-bytes body")
                    status = result.status
                    response_headers = _allowlisted_headers(result.headers)
                    response_body = result.body
                except (OSError, TimeoutError, urllib.error.URLError) as exc:
                    transport_error_type = type(exc).__name__
                response_received = _now_utc(now, "response_received_at_utc")
                if response_received < request_started:
                    raise IdentityMarketConsistencyError(
                        "response_received_at_utc precedes request_started_at_utc"
                    )
                _enforce_capture_caps(
                    root,
                    caps,
                    runner_started=runner_started,
                    attempt_count=attempt_count,
                    cumulative_response_bytes=cumulative_response_bytes,
                    prospective_response_bytes=len(response_body),
                )
                _reject_secret_echo(
                    api_key,
                    response_body=response_body,
                    response_headers=response_headers,
                )
                retryable = (
                    transport_error_type is not None
                    or status in _RETRYABLE_STATUS
                    or (
                        status == 200
                        and not _response_is_acceptable(
                            response_body, expected_jobs=len(spec.figis)
                        )
                    )
                )
                wait_seconds = min_interval
                if retryable:
                    wait_seconds = max(
                        min_interval,
                        _retry_delay_for_index(next_attempt),
                        _retry_after_seconds(response_headers, response_received),
                    )
                retry_not_before = response_received + timedelta(seconds=wait_seconds)
                _write_attempt(
                    root,
                    spec=spec,
                    attempt_index=next_attempt,
                    request_receipt=request_receipt,
                    attempt_intent=attempt_intent,
                    request_started=request_started,
                    response_received=response_received,
                    retry_not_before=retry_not_before,
                    status=status,
                    response_headers=response_headers,
                    response_body=response_body,
                    transport_error_type=transport_error_type,
                )
                latest_retry_not_before = (
                    retry_not_before
                    if latest_retry_not_before is None or retry_not_before > latest_retry_not_before
                    else latest_retry_not_before
                )
                cumulative_response_bytes += len(response_body)
                _enforce_capture_caps(
                    root,
                    caps,
                    runner_started=runner_started,
                    attempt_count=attempt_count,
                    cumulative_response_bytes=cumulative_response_bytes,
                )
                next_attempt += 1
                if transport_error_type is not None:
                    if next_attempt >= MAX_ATTEMPTS_PER_BATCH:
                        break
                    continue
                assert status is not None
                if status in _RETRYABLE_STATUS:
                    if next_attempt >= MAX_ATTEMPTS_PER_BATCH:
                        break
                    continue
                if status != 200:
                    raise IdentityMarketConsistencyError(
                        f"OpenFIGI batch {spec.index} returned immutable HTTP {status} attempt"
                    )
                if not _response_is_acceptable(response_body, expected_jobs=len(spec.figis)):
                    if next_attempt >= MAX_ATTEMPTS_PER_BATCH:
                        break
                    continue
                commit = _write_batch_commit(
                    root,
                    spec=spec,
                    request_receipt=request_receipt,
                    run_id=run_id,
                    terminal_status="response_accepted",
                    now=now,
                )
                completed_commits.append(commit)
                newly_completed += 1
                break
            if not accepted_path.exists():
                attempts = _load_attempts(
                    root,
                    spec,
                    request_receipt=request_receipt,
                    run_id=run_id,
                )
                if len(attempts) != MAX_ATTEMPTS_PER_BATCH or not all(
                    _attempt_is_source_unavailable(item, expected_jobs=len(spec.figis))
                    for item in attempts
                ):
                    raise IdentityMarketConsistencyError(
                        f"OpenFIGI batch {spec.index} stopped without a terminal disposition"
                    )
                commit = _write_batch_commit(
                    root,
                    spec=spec,
                    request_receipt=request_receipt,
                    run_id=run_id,
                    terminal_status="source_unavailable",
                    now=now,
                )
                completed_commits.append(commit)
                newly_completed += 1

        if len(completed_commits) == len(specs):
            completed_commits.sort(key=lambda item: int(item["batch_index"]))
            _validate_batch_order(completed_commits, len(specs))
            latest_response = max(
                _parse_timestamp(item["response_received_at_utc"], "response receipt")
                for item in completed_commits
            )
            completed_at = _now_utc(now, "completed_at_utc")
            if completed_at < latest_response:
                raise IdentityMarketConsistencyError("capture completion predates a response")
            request_relative = _request_manifest_path(run_id)
            request_receipt = _file_receipt(root, request_relative)
            payload = {
                "artifact_type": "s7_openfigi_market_consistency_capture_manifest",
                "batch_count": len(completed_commits),
                "batches": completed_commits,
                "calendar_binding": request["calendar_binding"],
                "completed_at_utc": completed_at.isoformat(),
                "composite_count": len(figis),
                "direct_approval": request["direct_approval"],
                "external_evidence_binding": request["external_evidence_binding"],
                "inventory_binding": request["inventory_binding"],
                "latest_response_received_at_utc": latest_response.isoformat(),
                "request_manifest": request_receipt,
                "run_id": run_id,
                "status": "complete",
                "version": MARKET_CONSISTENCY_RUN_VERSION,
            }
            document = {**payload, "manifest_id": stable_digest(payload)}
            write_bytes_immutable(root, final_path, _canonical_json(document))
            final_manifest: str | None = final_relative
        else:
            final_manifest = None

        return MarketConsistencyRun(
            run_id=run_id,
            request_manifest_path=_request_manifest_path(run_id),
            composite_count=len(figis),
            batch_count=len(specs),
            completed_batch_count=len(completed_commits),
            final_manifest_path=final_manifest,
            idempotent=False,
        )


def classify_market_consistency_run(
    data_root: Path,
    *,
    run_id: str,
    require_production_approval: bool = False,
) -> tuple[MarketClassification, ...]:
    """Replay accepted raw responses using unique Composite self-row semantics."""

    root = _root(data_root)
    request = _load_and_verify_request(
        root,
        run_id=run_id,
        require_production_approval=require_production_approval,
    )
    final = _verify_final_manifest(
        root,
        safe_relative_path(root, _final_manifest_path(run_id)),
        request_document=request,
    )
    seeds = _relationship_seeds()
    output: list[MarketClassification] = []
    for raw_commit in _array(final.get("batches"), "capture batches"):
        commit = _mapping(raw_commit, "capture batch")
        index = _nonnegative_int(commit.get("batch_index"), "batch index")
        terminal_status = _text(commit.get("terminal_status"), "batch terminal status")
        attempt_ref = _mapping(commit.get("attempt"), "terminal attempt receipt")
        attempt_path = safe_relative_path(root, _text(attempt_ref.get("path"), "attempt path"))
        attempt = _load_exact_json(attempt_path, "terminal attempt")
        figis = tuple(_array(commit.get("composite_figis"), "receipt Composite FIGIs"))
        if terminal_status == "source_unavailable":
            for figi in figis:
                query = _figi(figi, "receipt Composite FIGI")
                seed = seeds.get(query)
                classified = _classification(
                    query,
                    "unresolved_source_unavailable",
                    index,
                    reasons=("source_unavailable_attempts_exhausted",),
                    seed=seed,
                )
                output.append(
                    replace(
                        classified,
                        relationship_seed_status="drift" if seed is not None else "not_seed",
                        raw_response_attempt_id=_digest(
                            attempt.get("attempt_id"), "raw terminal attempt ID"
                        ),
                        raw_response_attempt_path=_text(
                            attempt_ref.get("path"), "raw terminal attempt path"
                        ),
                        raw_response_attempt_sha256=_digest(
                            attempt_ref.get("sha256"), "raw terminal attempt SHA"
                        ),
                        raw_response_attempt_bytes=_nonnegative_int(
                            attempt_ref.get("bytes"), "raw terminal attempt bytes"
                        ),
                        request_started_at_utc=_timestamp(
                            attempt.get("request_started_at_utc"), "request start"
                        ),
                        response_received_at_utc=_timestamp(
                            attempt.get("response_received_at_utc"), "attempt receipt"
                        ),
                    )
                )
            continue
        if terminal_status != "response_accepted":
            raise IdentityMarketConsistencyError("batch terminal status is invalid")
        body = _attempt_body(attempt)
        response = _decode_json_bytes(body, "OpenFIGI response")
        if not isinstance(response, list) or len(response) != len(figis):
            raise IdentityMarketConsistencyError("stored OpenFIGI response count differs")
        for figi, item in zip(figis, response, strict=True):
            query = _figi(figi, "receipt Composite FIGI")
            classified = _classify_job(
                query,
                item,
                batch_index=index,
                seed=seeds.get(query),
            )
            output.append(
                replace(
                    classified,
                    raw_response_attempt_id=_digest(
                        attempt.get("attempt_id"), "raw response attempt ID"
                    ),
                    raw_response_attempt_path=_text(
                        attempt_ref.get("path"), "raw response attempt path"
                    ),
                    raw_response_attempt_sha256=_digest(
                        attempt_ref.get("sha256"), "raw response attempt SHA"
                    ),
                    raw_response_attempt_bytes=_nonnegative_int(
                        attempt_ref.get("bytes"), "raw response attempt bytes"
                    ),
                    request_started_at_utc=_timestamp(
                        attempt.get("request_started_at_utc"), "request start"
                    ),
                    response_received_at_utc=_timestamp(
                        attempt.get("response_received_at_utc"), "response receipt"
                    ),
                )
            )
    ordered = tuple(sorted(output, key=lambda item: item.composite_figi))
    if len({item.composite_figi for item in ordered}) != len(ordered):
        raise IdentityMarketConsistencyError("classification repeats a Composite FIGI")
    expected = _positive_int(final.get("composite_count"), "composite count")
    if len(ordered) != expected:
        raise IdentityMarketConsistencyError("classification coverage differs")
    return ordered


def _classify_job(
    query: str,
    item: object,
    *,
    batch_index: int,
    seed: Mapping[str, object] | None,
) -> MarketClassification:
    if not isinstance(item, Mapping):
        return _classification(
            query,
            "unresolved_invalid_projection",
            batch_index,
            reasons=("job_result_not_object",),
            seed=seed,
        )
    result = dict(item)
    error = result.get("error")
    warning = result.get("warning")
    raw_data = result.get("data")
    if error is not None and (not isinstance(error, str) or not error.strip()):
        return _classification(
            query,
            "unresolved_invalid_projection",
            batch_index,
            reasons=("job_error_not_text",),
            seed=seed,
        )
    if warning is not None and (not isinstance(warning, str) or not warning.strip()):
        return _classification(
            query,
            "unresolved_invalid_projection",
            batch_index,
            reasons=("job_warning_not_text",),
            seed=seed,
        )
    if raw_data is None:
        message = error if isinstance(error, str) else warning if isinstance(warning, str) else None
        label = (
            "unresolved_no_mapping"
            if message is None or "no identifier found" in message.lower()
            else "unresolved_job_error"
        )
        return _classification(
            query,
            label,
            batch_index,
            job_error=message,
            reasons=("no_mapping" if label.endswith("no_mapping") else "provider_job_error",),
            seed=seed,
        )
    if raw_data == []:
        return _classification(
            query,
            "unresolved_no_mapping",
            batch_index,
            reasons=("empty_mapping_data",),
            seed=seed,
        )
    if error is not None or warning is not None or not isinstance(raw_data, list):
        return _classification(
            query,
            "unresolved_invalid_projection",
            batch_index,
            job_error=error
            if isinstance(error, str)
            else warning
            if isinstance(warning, str)
            else None,
            reasons=("data_with_error_or_warning",)
            if isinstance(raw_data, list)
            else ("mapping_data_not_array",),
            seed=seed,
        )
    rows: list[dict[str, object]] = []
    malformed = False
    for raw in raw_data:
        if not isinstance(raw, Mapping):
            malformed = True
            continue
        row = dict(raw)
        for field in (
            "figi",
            "compositeFIGI",
            "shareClassFIGI",
            "exchCode",
            "securityType",
            "securityType2",
            "securityDescription",
            "marketSector",
        ):
            value = row.get(field)
            if value is not None and (
                not isinstance(value, str) or value != value.strip() or not value
            ):
                malformed = True
        rows.append(row)
    returned = _returned_fields(rows)
    relation_rows = [row for row in rows if row.get("compositeFIGI") == query]
    self_rows = [
        row
        for row in relation_rows
        if row.get("figi") == query and row.get("compositeFIGI") == query
    ]
    share_classes = {
        row["shareClassFIGI"]
        for row in relation_rows
        if isinstance(row.get("shareClassFIGI"), str) and row["shareClassFIGI"]
    }
    if malformed:
        classification = _classification(
            query,
            "unresolved_invalid_projection",
            batch_index,
            rows=rows,
            self_rows=self_rows,
            reasons=("malformed_mapping_row",),
            seed=seed,
            returned=returned,
        )
    elif len(share_classes) > 1:
        classification = _classification(
            query,
            "unresolved_share_class_conflict",
            batch_index,
            rows=rows,
            self_rows=self_rows,
            reasons=("multiple_relation_share_classes",),
            seed=seed,
            returned=returned,
        )
    elif len(self_rows) == 1:
        selected = self_rows[0]
        market = selected.get("exchCode")
        share = selected.get("shareClassFIGI")
        if not isinstance(market, str) or not market or not isinstance(share, str) or not share:
            classification = _classification(
                query,
                "unresolved_invalid_projection",
                batch_index,
                rows=rows,
                self_rows=self_rows,
                selected=selected,
                reasons=("self_row_missing_market_or_share_class",),
                seed=seed,
                returned=returned,
            )
        else:
            classification = _classification(
                query,
                "us_composite" if market == "US" else "non_us_composite",
                batch_index,
                rows=rows,
                self_rows=self_rows,
                selected=selected,
                market_codes=(market,),
                reasons=("unique_exact_self_row",),
                seed=seed,
                returned=returned,
            )
    elif len(self_rows) > 1:
        codes = tuple(
            sorted(
                {
                    str(row["exchCode"])
                    for row in self_rows
                    if isinstance(row.get("exchCode"), str) and row["exchCode"]
                }
            )
        )
        label = "unresolved_mixed_market" if len(codes) > 1 else "unresolved_invalid_projection"
        classification = _classification(
            query,
            label,
            batch_index,
            rows=rows,
            self_rows=self_rows,
            market_codes=codes,
            reasons=("multiple_self_rows_with_mixed_markets",)
            if len(codes) > 1
            else ("multiple_self_rows",),
            seed=seed,
            returned=returned,
        )
    else:
        exception = _FROZEN_NO_SELF_RELATION_EXCEPTIONS.get(query)
        matches = (
            []
            if exception is None
            else [
                row
                for row in relation_rows
                if all(row.get(key) == value for key, value in exception.items())
            ]
        )
        if exception is not None and len(matches) == 1 and len(relation_rows) == 1:
            selected = matches[0]
            classification = _classification(
                query,
                "non_us_composite",
                batch_index,
                rows=rows,
                self_rows=(),
                selected=selected,
                market_codes=(str(exception["exchCode"]),),
                reasons=("frozen_tnxp_unique_relation_exception",),
                seed=seed,
                returned=returned,
            )
        else:
            classification = _classification(
                query,
                "unresolved_invalid_projection",
                batch_index,
                rows=rows,
                self_rows=(),
                reasons=("no_unique_exact_self_row",),
                seed=seed,
                returned=returned,
            )
    return _apply_seed_check(classification, seed, relation_rows)


def _classification(
    query: str,
    label: str,
    batch_index: int,
    *,
    rows: Sequence[Mapping[str, object]] = (),
    self_rows: Sequence[Mapping[str, object]] = (),
    selected: Mapping[str, object] | None = None,
    market_codes: tuple[str, ...] = (),
    job_error: str | None = None,
    reasons: tuple[str, ...] = (),
    seed: Mapping[str, object] | None,
    returned: Mapping[str, tuple[str, ...]] | None = None,
) -> MarketClassification:
    values = returned or {
        "figis": (),
        "composites": (),
        "shares": (),
        "exchanges": (),
        "security_types": (),
        "security_types2": (),
        "security_descriptions": (),
        "market_sectors": (),
    }
    return MarketClassification(
        composite_figi=query,
        classification=label,
        projection_classification=label,
        market_codes=market_codes,
        exact_row_count=sum(row.get("compositeFIGI") == query for row in rows),
        response_batch_index=batch_index,
        self_row_count=len(self_rows),
        selected_figi=(
            str(selected["figi"])
            if selected is not None and isinstance(selected.get("figi"), str)
            else None
        ),
        selected_share_class_figi=(
            str(selected["shareClassFIGI"])
            if selected is not None and isinstance(selected.get("shareClassFIGI"), str)
            else None
        ),
        job_error=job_error,
        projection_reason_codes=tuple(sorted(set(reasons))),
        relationship_seed_status="pending_check" if seed is not None else "not_seed",
        returned_figis=values["figis"],
        returned_composite_figis=values["composites"],
        returned_share_class_figis=values["shares"],
        returned_exchange_codes=values["exchanges"],
        returned_security_types=values["security_types"],
        returned_security_types2=values["security_types2"],
        returned_security_descriptions=values["security_descriptions"],
        returned_market_sectors=values["market_sectors"],
        selected_market_code=(
            str(selected["exchCode"])
            if selected is not None and isinstance(selected.get("exchCode"), str)
            else None
        ),
        selected_market_sector=(
            str(selected["marketSector"])
            if selected is not None and isinstance(selected.get("marketSector"), str)
            else None
        ),
        selected_security_type=(
            str(selected["securityType"])
            if selected is not None and isinstance(selected.get("securityType"), str)
            else None
        ),
        selected_security_type2=(
            str(selected["securityType2"])
            if selected is not None and isinstance(selected.get("securityType2"), str)
            else None
        ),
        selected_security_description=(
            str(selected["securityDescription"])
            if selected is not None and isinstance(selected.get("securityDescription"), str)
            else None
        ),
        relation_projection_json=_canonical_json_text(
            [
                {
                    key: row.get(key)
                    for key in (
                        "figi",
                        "compositeFIGI",
                        "shareClassFIGI",
                        "exchCode",
                        "marketSector",
                        "securityType",
                        "securityType2",
                        "securityDescription",
                    )
                }
                for row in rows
                if row.get("compositeFIGI") == query
            ]
        ),
    )


def _apply_seed_check(
    item: MarketClassification,
    seed: Mapping[str, object] | None,
    relation_rows: Sequence[Mapping[str, object]],
) -> MarketClassification:
    if seed is None:
        return item
    expected_market = _text(seed.get("expected_market_code"), "seed market code")
    expected_share = _figi(seed.get("expected_share_class_figi"), "seed Share Class FIGI")
    relation_match = any(
        row.get("compositeFIGI") == item.composite_figi
        and row.get("exchCode") == expected_market
        and row.get("shareClassFIGI") == expected_share
        for row in relation_rows
    )
    expected_label = "us_composite" if expected_market == "US" else "non_us_composite"
    matched = (
        relation_match
        and item.classification == expected_label
        and item.market_codes == (expected_market,)
        and item.selected_share_class_figi == expected_share
    )
    values = {field: getattr(item, field) for field in MarketClassification.__dataclass_fields__}
    values["relationship_seed_status"] = "matched" if matched else "drift"
    if not matched:
        values["classification"] = "unresolved_seed_drift"
        values["projection_reason_codes"] = tuple(
            sorted(set((*item.projection_reason_codes, "approved_relationship_seed_drift")))
        )
    return MarketClassification(**values)


def _returned_fields(rows: Sequence[Mapping[str, object]]) -> dict[str, tuple[str, ...]]:
    fields = {
        "figis": "figi",
        "composites": "compositeFIGI",
        "shares": "shareClassFIGI",
        "exchanges": "exchCode",
        "security_types": "securityType",
        "security_types2": "securityType2",
        "security_descriptions": "securityDescription",
        "market_sectors": "marketSector",
    }
    return {
        output: tuple(
            sorted(
                {
                    str(row[source])
                    for row in rows
                    if isinstance(row.get(source), str) and row[source]
                }
            )
        )
        for output, source in fields.items()
    }


def materialize_market_classification_candidate(
    data_root: Path,
    *,
    run_id: str,
    materialized_at_utc: str,
    materialized_by: str,
    source_available_session: str | None = None,
    require_production_approval: bool = False,
) -> MarketClassificationCandidate:
    """Materialize and fully replay one offline awaiting-review candidate.

    Availability is always derived from the latest accepted response and the bound XNYS
    calendar.  ``source_available_session`` remains only as a compatibility assertion;
    callers cannot use it to choose a date.
    """

    root = _root(data_root)
    created_at = _parse_timestamp(materialized_at_utc, "materialized_at_utc")
    actor = _text(materialized_by, "materialized_by")
    request = _load_and_verify_request(
        root,
        run_id=run_id,
        require_production_approval=require_production_approval,
    )
    final_relative = _final_manifest_path(run_id)
    final_path = safe_relative_path(root, final_relative)
    final = _verify_final_manifest(root, final_path, request_document=request)
    latest_response = _parse_timestamp(
        final.get("latest_response_received_at_utc"), "latest response receipt"
    )
    if created_at < latest_response:
        raise IdentityMarketConsistencyError(
            "materialized_at_utc cannot precede the latest accepted response"
        )
    availability = _derive_availability(root, latest_response)
    derived_session = _text(availability.get("source_available_session"), "availability session")
    if source_available_session is not None and source_available_session != derived_session:
        raise IdentityMarketConsistencyError(
            "source_available_session differs from the calendar-derived value"
        )

    classifications = classify_market_consistency_run(
        root,
        run_id=run_id,
        require_production_approval=require_production_approval,
    )
    inventory = _mapping(request.get("inventory_binding"), "inventory binding")
    data_ref = _mapping(inventory.get("data"), "inventory DATA binding")
    inventory_rows = _read_inventory_details(
        safe_relative_path(root, _text(data_ref.get("path"), "inventory DATA path"))
    )
    if set(inventory_rows) != {row.composite_figi for row in classifications}:
        raise IdentityMarketConsistencyError("classification and inventory coverage differ")
    rows = _candidate_rows(
        classifications,
        inventory_rows,
        derived_session,
        request=request,
        final=final,
        final_sha=sha256_file(final_path),
        availability=availability,
    )
    row_digest = stable_digest(rows)
    final_sha = sha256_file(final_path)
    candidate_basis = {
        "availability_proof_digest": availability["proof_digest"],
        "classification_row_digest": row_digest,
        "classification_version": MARKET_CLASSIFICATION_VERSION,
        "composite_count": len(rows),
        "external_evidence_binding": request["external_evidence_binding"],
        "inventory_binding": request["inventory_binding"],
        "source_capture_manifest_id": _digest(final.get("manifest_id"), "capture manifest ID"),
        "source_capture_manifest_sha256": final_sha,
        "source_run_id": run_id,
    }
    candidate_id = stable_digest(candidate_basis)
    prefix = _candidate_prefix(candidate_id)
    manifest_path = safe_relative_path(root, f"{prefix}/manifest.json")
    expected = _candidate_documents(
        candidate_id=candidate_id,
        candidate_basis=candidate_basis,
        rows=rows,
        availability=availability,
        source_capture_manifest={
            "bytes": final_path.stat().st_size,
            "manifest_id": final["manifest_id"],
            "path": final_relative,
            "sha256": final_sha,
        },
        created_at=created_at,
        actor=actor,
        request=request,
    )
    if manifest_path.exists():
        manifest = _load_exact_json(manifest_path, "classification candidate manifest")
        return _verify_candidate_replay(
            root,
            manifest,
            expected_candidate_id=candidate_id,
            rows=rows,
            expected_static=expected,
            idempotent=True,
        )

    frame = _candidate_frame(rows)
    data_receipt = write_parquet_immutable(
        root,
        root / f"{prefix}/data/classification.parquet",
        frame,
    )
    examples = expected["examples"]
    example_receipt = write_bytes_immutable(
        root,
        root / f"{prefix}/examples/market-consistency.json",
        _canonical_json(examples),
    )
    qa = _qa_document(
        candidate_id=candidate_id,
        rows=rows,
        example_path=_text(example_receipt["path"], "example path"),
        production=inventory.get("mode") == "production",
    )
    qa_receipt = write_bytes_immutable(
        root,
        root / f"{prefix}/qa/qa.json",
        _canonical_json(qa),
    )
    payload = {
        "artifact_type": "s7_openfigi_market_consistency_candidate",
        "availability": availability,
        "candidate_basis": candidate_basis,
        "candidate_id": candidate_id,
        "classification_counts": expected["classification_counts"],
        "classification_row_counts": expected["classification_row_counts"],
        "created_at_utc": created_at.isoformat(),
        "created_by": actor,
        "data": data_receipt,
        "direct_approval": request["direct_approval"],
        "examples": example_receipt,
        "external_evidence_binding": request["external_evidence_binding"],
        "inventory_binding": request["inventory_binding"],
        "qa": qa_receipt,
        "source_available_session": derived_session,
        "source_capture_manifest": {
            "bytes": final_path.stat().st_size,
            "manifest_id": final["manifest_id"],
            "path": final_relative,
            "sha256": final_sha,
        },
        "state": "awaiting_review",
    }
    document = {**payload, "manifest_id": stable_digest(payload)}
    write_bytes_immutable(root, manifest_path, _canonical_json(document))
    return _verify_candidate_replay(
        root,
        document,
        expected_candidate_id=candidate_id,
        rows=rows,
        expected_static=expected,
        idempotent=False,
    )


def verify_market_classification_candidate(
    data_root: Path,
    *,
    candidate_path: str,
    candidate_id: str,
    candidate_sha256: str,
    require_production_approval: bool = True,
) -> MarketClassificationCandidate:
    """Strictly replay one existing Gate-B candidate without writing artifacts.

    This is the sole downstream trust boundary.  It follows the candidate back through
    its exact request, standing approval, final capture, batch commits, pre-send intents,
    response attempts, inventory DATA, external evidence, and calendar availability;
    classifications and every candidate DATA/QA/example byte are then rebuilt offline.
    """

    root = _root(data_root)
    expected_id = _digest(candidate_id, "classification candidate ID")
    expected_sha = _digest(candidate_sha256, "classification candidate SHA-256")
    relative = _relative_path(candidate_path, "classification candidate path")
    canonical = f"{_candidate_prefix(expected_id)}/manifest.json"
    if relative != canonical:
        raise IdentityMarketConsistencyError("classification candidate path is not canonical")
    manifest_path = safe_relative_path(root, relative)
    _verify_regular_file(manifest_path, expected_sha, "classification candidate")
    document = _load_exact_json(manifest_path, "classification candidate manifest")
    _expect_keys(
        document,
        {
            "artifact_type",
            "availability",
            "candidate_basis",
            "candidate_id",
            "classification_counts",
            "classification_row_counts",
            "created_at_utc",
            "created_by",
            "data",
            "direct_approval",
            "examples",
            "external_evidence_binding",
            "inventory_binding",
            "manifest_id",
            "qa",
            "source_available_session",
            "source_capture_manifest",
            "state",
        },
        "classification candidate manifest",
    )
    if document.get("artifact_type") != "s7_openfigi_market_consistency_candidate":
        raise IdentityMarketConsistencyError("classification candidate artifact type differs")
    basis = _mapping(document.get("candidate_basis"), "classification candidate basis")
    run_id = _digest(basis.get("source_run_id"), "classification source run ID")
    request = _load_and_verify_request(
        root,
        run_id=run_id,
        require_production_approval=require_production_approval,
    )
    final_relative = _final_manifest_path(run_id)
    final_path = safe_relative_path(root, final_relative)
    final = _verify_final_manifest(root, final_path, request_document=request)
    final_sha = sha256_file(final_path)
    latest_response = _parse_timestamp(
        final.get("latest_response_received_at_utc"), "latest response receipt"
    )
    availability = _derive_availability(root, latest_response)
    classifications = classify_market_consistency_run(
        root,
        run_id=run_id,
        require_production_approval=require_production_approval,
    )
    inventory = _mapping(request.get("inventory_binding"), "inventory binding")
    data_ref = _mapping(inventory.get("data"), "inventory DATA binding")
    inventory_rows = _read_inventory_details(
        safe_relative_path(root, _text(data_ref.get("path"), "inventory DATA path"))
    )
    if set(inventory_rows) != {row.composite_figi for row in classifications}:
        raise IdentityMarketConsistencyError("classification and inventory coverage differ")
    derived_session = _text(availability.get("source_available_session"), "availability session")
    rows = _candidate_rows(
        classifications,
        inventory_rows,
        derived_session,
        request=request,
        final=final,
        final_sha=final_sha,
        availability=availability,
    )
    expected_basis = {
        "availability_proof_digest": availability["proof_digest"],
        "classification_row_digest": stable_digest(rows),
        "classification_version": MARKET_CLASSIFICATION_VERSION,
        "composite_count": len(rows),
        "external_evidence_binding": request["external_evidence_binding"],
        "inventory_binding": request["inventory_binding"],
        "source_capture_manifest_id": _digest(final.get("manifest_id"), "capture manifest ID"),
        "source_capture_manifest_sha256": final_sha,
        "source_run_id": run_id,
    }
    if basis != expected_basis or stable_digest(expected_basis) != expected_id:
        raise IdentityMarketConsistencyError("classification candidate basis replay differs")
    source_capture = {
        "bytes": final_path.stat().st_size,
        "manifest_id": final["manifest_id"],
        "path": final_relative,
        "sha256": final_sha,
    }
    created_at = _parse_timestamp(document.get("created_at_utc"), "candidate created_at_utc")
    actor = _text(document.get("created_by"), "candidate created_by")
    expected_static = _candidate_documents(
        candidate_id=expected_id,
        candidate_basis=expected_basis,
        rows=rows,
        availability=availability,
        source_capture_manifest=source_capture,
        created_at=created_at,
        actor=actor,
        request=request,
    )
    if document.get("source_available_session") != derived_session:
        raise IdentityMarketConsistencyError("classification candidate availability differs")
    return _verify_candidate_replay(
        root,
        document,
        expected_candidate_id=expected_id,
        rows=rows,
        expected_static=expected_static,
        idempotent=True,
    )


def _candidate_rows(
    classifications: Sequence[MarketClassification],
    inventory_rows: Mapping[str, Mapping[str, object]],
    source_available_session: str,
    *,
    request: Mapping[str, object],
    final: Mapping[str, object],
    final_sha: str,
    availability: Mapping[str, object],
) -> list[dict[str, object]]:
    inventory_binding = _mapping(request.get("inventory_binding"), "inventory binding")
    inventory_candidate = _mapping(inventory_binding.get("candidate"), "inventory candidate")
    inventory_data = _mapping(inventory_binding.get("data"), "inventory DATA")
    completion_raw = inventory_binding.get("completion")
    inventory_completion = (
        _mapping(completion_raw, "inventory completion") if completion_raw is not None else None
    )
    rows: list[dict[str, object]] = []
    for item in classifications:
        inventory = inventory_rows[item.composite_figi]
        active = _nonnegative_int(inventory["active_row_count"], "active row count")
        inactive = _nonnegative_int(inventory["inactive_row_count"], "inactive row count")
        rows.append(
            {
                "active_row_count": active,
                "classification": item.classification,
                "composite_figi": item.composite_figi,
                "exact_openfigi_row_count": item.exact_row_count,
                "first_session": _date_text(inventory["first_session"], "first session"),
                "inactive_row_count": inactive,
                "inventory_source_record_lineage_digest": _digest(
                    inventory["source_record_lineage_digest"], "inventory lineage digest"
                ),
                "job_error": item.job_error,
                "last_session": _date_text(inventory["last_session"], "last session"),
                "market_codes": list(item.market_codes),
                "projection_reason_codes": list(item.projection_reason_codes),
                "projection_classification": item.projection_classification,
                "provider_observation_row_count": active + inactive,
                "relationship_seed_status": item.relationship_seed_status,
                "reference_build_run_id": request["run_id"],
                "reference_version": MARKET_CLASSIFICATION_VERSION,
                "response_batch_index": item.response_batch_index,
                "returned_composite_figis": list(item.returned_composite_figis),
                "returned_exchange_codes": list(item.returned_exchange_codes),
                "returned_figis": list(item.returned_figis),
                "returned_market_sectors": list(item.returned_market_sectors),
                "returned_security_descriptions": list(item.returned_security_descriptions),
                "returned_security_types": list(item.returned_security_types),
                "returned_security_types2": list(item.returned_security_types2),
                "returned_share_class_figis": list(item.returned_share_class_figis),
                "selected_figi": item.selected_figi,
                "selected_market_code": item.selected_market_code,
                "selected_market_sector": item.selected_market_sector,
                "selected_security_description": item.selected_security_description,
                "selected_security_type": item.selected_security_type,
                "selected_security_type2": item.selected_security_type2,
                "selected_share_class_figi": item.selected_share_class_figi,
                "self_openfigi_row_count": item.self_row_count,
                "relation_projection_json": item.relation_projection_json,
                "raw_response_attempt_bytes": item.raw_response_attempt_bytes,
                "raw_response_attempt_id": item.raw_response_attempt_id,
                "raw_response_attempt_path": item.raw_response_attempt_path,
                "raw_response_attempt_sha256": item.raw_response_attempt_sha256,
                "request_started_at_utc": item.request_started_at_utc,
                "response_received_at_utc": item.response_received_at_utc,
                "source_capture_manifest_id": final["manifest_id"],
                "source_capture_manifest_sha256": final_sha,
                "source_publication_status": "unavailable_current_snapshot_not_point_in_time",
                "source_published_at_utc": None,
                "source_available_session": source_available_session,
                "source_availability_rule": availability["first_open_rule"],
                "inventory_candidate_id": inventory_candidate["candidate_id"],
                "inventory_candidate_sha256": inventory_candidate["sha256"],
                "inventory_completion_id": (
                    inventory_completion.get("completion_id")
                    if inventory_completion is not None
                    else None
                ),
                "inventory_completion_sha256": (
                    inventory_completion.get("sha256") if inventory_completion is not None else None
                ),
                "inventory_data_sha256": inventory_data["sha256"],
                "inventory_release_id": None,
                "inventory_release_manifest_sha256": None,
                "inventory_release_status": "not_published_gate_a_candidate_only",
            }
        )
    rows.sort(key=lambda row: str(row["composite_figi"]))
    return rows


def _candidate_documents(
    *,
    candidate_id: str,
    candidate_basis: Mapping[str, object],
    rows: Sequence[Mapping[str, object]],
    availability: Mapping[str, object],
    source_capture_manifest: Mapping[str, object],
    created_at: datetime,
    actor: str,
    request: Mapping[str, object],
) -> dict[str, object]:
    counts, row_counts = _classification_counts(rows)
    unresolved = [row for row in rows if str(row["classification"]).startswith("unresolved_")]
    exact_group_drift_keys = set(_exact_group_seed_drift(rows))
    examples = {
        "artifact_type": "s7_openfigi_market_consistency_bounded_examples",
        "non_us_composites": [
            dict(row) for row in rows if row["classification"] == "non_us_composite"
        ][:100],
        "relationship_seed_drift": [
            dict(row) for row in rows if row["relationship_seed_status"] == "drift"
        ][:100],
        "exact_group_seed_drift": [
            dict(row) for row in rows if str(row["composite_figi"]) in exact_group_drift_keys
        ][:100],
        "unresolved_composites": [dict(row) for row in unresolved][:100],
    }
    return {
        "availability": dict(availability),
        "candidate_basis": dict(candidate_basis),
        "classification_counts": counts,
        "classification_row_counts": row_counts,
        "created_at_utc": created_at.isoformat(),
        "created_by": actor,
        "direct_approval": request["direct_approval"],
        "examples": examples,
        "external_evidence_binding": request["external_evidence_binding"],
        "inventory_binding": request["inventory_binding"],
        "source_capture_manifest": dict(source_capture_manifest),
    }


def _classification_counts(
    rows: Sequence[Mapping[str, object]],
) -> tuple[dict[str, int], dict[str, int]]:
    counts: dict[str, int] = {}
    row_counts: dict[str, int] = {}
    for row in rows:
        label = str(row["classification"])
        counts[label] = counts.get(label, 0) + 1
        row_counts[label] = row_counts.get(label, 0) + int(row["provider_observation_row_count"])
    return dict(sorted(counts.items())), dict(sorted(row_counts.items()))


def _qa_document(
    *,
    candidate_id: str,
    rows: Sequence[Mapping[str, object]],
    example_path: str,
    production: bool,
) -> dict[str, object]:
    _, row_counts = _classification_counts(rows)
    provider_rows = sum(row_counts.values())
    non_us = row_counts.get("non_us_composite", 0)
    unresolved_counts = {
        label: count for label, count in row_counts.items() if label.startswith("unresolved_")
    }
    unresolved = sum(unresolved_counts.values())
    seeds = _relationship_seeds()
    keys = {str(row["composite_figi"]) for row in rows}
    missing_seeds = sorted(set(seeds) - keys)
    seed_drift = [row for row in rows if row["relationship_seed_status"] == "drift"]
    exact_group_drift = _exact_group_seed_drift(rows)
    critical = len(missing_seeds) if production else 0
    return {
        "artifact_type": "s7_openfigi_market_consistency_qa",
        "candidate_id": candidate_id,
        "critical_failure_count": critical,
        "results": [
            {
                "check_id": "classification_coverage_invalid",
                "denominator": len(rows),
                "numerator": 0,
                "severity": "critical",
                "status": "passed",
            },
            {
                "check_id": "classification_primary_key_duplicate",
                "denominator": len(rows),
                "numerator": 0,
                "severity": "critical",
                "status": "passed",
            },
            {
                "check_id": "reference_inventory_unattempted_rows",
                "denominator": len(rows),
                "numerator": 0,
                "severity": "critical",
                "status": "passed",
            },
            {
                "bounded_examples_path": example_path,
                "check_id": "approved_relationship_seed_missing_inventory_keys",
                "denominator": 18,
                "missing_keys": missing_seeds,
                "numerator": len(missing_seeds) if production else 0,
                "observed_fixture_missing_keys": missing_seeds if not production else [],
                "severity": "critical",
                "status": "failed" if production and missing_seeds else "passed",
            },
            {
                "bounded_examples_path": example_path,
                "check_id": "approved_relationship_seed_drift",
                "denominator": 18,
                "numerator": len(seed_drift),
                "reason_counts": {"current_openfigi_differs_from_frozen_seed": len(seed_drift)},
                "severity": "high",
                "status": "warning" if seed_drift else "passed",
            },
            {
                "bounded_examples_path": example_path,
                "check_id": "exact_group_openfigi_seed_drift",
                "denominator": len(_exact_group_composite_seeds()),
                "numerator": len(exact_group_drift),
                "reason_counts": {
                    "current_openfigi_differs_from_exact_group_evidence": len(exact_group_drift)
                },
                "severity": "high",
                "status": "warning" if exact_group_drift else "passed",
            },
            {
                "bounded_examples_path": example_path,
                "check_id": "us_locale_non_us_composite_figi_rows",
                "denominator": provider_rows,
                "numerator": non_us,
                "reason_counts": {"unique_openfigi_self_market_not_us": non_us},
                "severity": "high",
                "status": "warning" if non_us else "passed",
            },
            {
                "bounded_examples_path": example_path,
                "check_id": "openfigi_market_classification_unresolved_rows",
                "denominator": provider_rows,
                "numerator": unresolved,
                "reason_counts": dict(sorted(unresolved_counts.items())),
                "severity": "high",
                "status": "warning" if unresolved else "passed",
            },
        ],
    }


def _verify_candidate_replay(
    root: Path,
    document: Mapping[str, object],
    *,
    expected_candidate_id: str,
    rows: Sequence[Mapping[str, object]],
    expected_static: Mapping[str, object],
    idempotent: bool,
) -> MarketClassificationCandidate:
    payload = dict(document)
    manifest_id = payload.pop("manifest_id", None)
    if stable_digest(payload) != manifest_id:
        raise IdentityMarketConsistencyError("classification manifest ID differs")
    if (
        document.get("candidate_id") != expected_candidate_id
        or document.get("state") != "awaiting_review"
    ):
        raise IdentityMarketConsistencyError("classification candidate identity differs")
    if (
        document.get("candidate_basis") != expected_static["candidate_basis"]
        or document.get("availability") != expected_static["availability"]
        or document.get("classification_counts") != expected_static["classification_counts"]
        or document.get("classification_row_counts") != expected_static["classification_row_counts"]
        or document.get("external_evidence_binding") != expected_static["external_evidence_binding"]
        or document.get("inventory_binding") != expected_static["inventory_binding"]
        or document.get("direct_approval") != expected_static["direct_approval"]
        or document.get("source_capture_manifest") != expected_static["source_capture_manifest"]
    ):
        raise IdentityMarketConsistencyError("classification candidate controls differ")
    created = _parse_timestamp(document.get("created_at_utc"), "candidate created_at_utc")
    latest = _parse_timestamp(
        _mapping(document.get("availability"), "candidate availability").get(
            "controlling_response_received_at_utc"
        ),
        "candidate controlling response",
    )
    if created < latest:
        raise IdentityMarketConsistencyError("classification candidate predates source capture")
    _text(document.get("created_by"), "candidate created_by")
    for field in ("data", "qa", "examples", "source_capture_manifest"):
        _verify_file_receipt(root, _mapping(document.get(field), f"{field} receipt"))
    data_ref = _mapping(document["data"], "data receipt")
    data_path = safe_relative_path(root, _text(data_ref.get("path"), "candidate DATA path"))
    actual_rows = pl.read_parquet(data_path).to_dicts()
    if actual_rows != list(rows):
        raise IdentityMarketConsistencyError("classification candidate Parquet replay differs")
    examples_ref = _mapping(document["examples"], "example receipt")
    examples_path = safe_relative_path(root, _text(examples_ref.get("path"), "example path"))
    expected_examples = expected_static["examples"]
    if _load_exact_json(examples_path, "candidate examples") != expected_examples:
        raise IdentityMarketConsistencyError("classification candidate examples replay differs")
    qa_ref = _mapping(document["qa"], "QA receipt")
    qa_path = safe_relative_path(root, _text(qa_ref.get("path"), "QA path"))
    inventory_mode = _mapping(
        _mapping(document.get("candidate_basis"), "candidate basis").get(
            "external_evidence_binding"
        ),
        "candidate evidence binding",
    )
    del inventory_mode  # evidence is verified above; production status comes from direct approval.
    expected_qa = _qa_document(
        candidate_id=expected_candidate_id,
        rows=rows,
        example_path=_text(examples_ref.get("path"), "example path"),
        production=document.get("direct_approval") is not None,
    )
    if _load_exact_json(qa_path, "candidate QA") != expected_qa:
        raise IdentityMarketConsistencyError("classification candidate QA replay differs")
    counts = _mapping(document.get("classification_counts"), "classification counts")
    row_counts = _mapping(document.get("classification_row_counts"), "classification row counts")
    return MarketClassificationCandidate(
        candidate_id=expected_candidate_id,
        manifest_path=f"{_candidate_prefix(expected_candidate_id)}/manifest.json",
        data_path=_text(data_ref.get("path"), "data path"),
        qa_path=_text(qa_ref.get("path"), "QA path"),
        example_path=_text(examples_ref.get("path"), "example path"),
        composite_count=sum(_nonnegative_int(v, "classification count") for v in counts.values()),
        us_composite_count=_nonnegative_int(counts.get("us_composite", 0), "US count"),
        non_us_composite_count=_nonnegative_int(counts.get("non_us_composite", 0), "non-US count"),
        unresolved_composite_count=sum(
            _nonnegative_int(counts.get(label, 0), "unresolved count")
            for label in _UNRESOLVED_CLASSIFICATIONS
        ),
        non_us_provider_row_count=_nonnegative_int(
            row_counts.get("non_us_composite", 0), "non-US row count"
        ),
        unresolved_provider_row_count=sum(
            _nonnegative_int(row_counts.get(label, 0), "unresolved row count")
            for label in _UNRESOLVED_CLASSIFICATIONS
        ),
        idempotent=idempotent,
    )


def _candidate_columns() -> tuple[str, ...]:
    return (
        "composite_figi",
        "classification",
        "market_codes",
        "exact_openfigi_row_count",
        "self_openfigi_row_count",
        "selected_figi",
        "selected_market_code",
        "selected_market_sector",
        "selected_security_type",
        "selected_security_type2",
        "selected_security_description",
        "selected_share_class_figi",
        "relation_projection_json",
        "raw_response_attempt_id",
        "raw_response_attempt_path",
        "raw_response_attempt_sha256",
        "raw_response_attempt_bytes",
        "request_started_at_utc",
        "response_received_at_utc",
        "job_error",
        "projection_reason_codes",
        "projection_classification",
        "relationship_seed_status",
        "returned_figis",
        "returned_composite_figis",
        "returned_share_class_figis",
        "returned_exchange_codes",
        "returned_security_types",
        "returned_security_types2",
        "returned_security_descriptions",
        "returned_market_sectors",
        "first_session",
        "last_session",
        "active_row_count",
        "inactive_row_count",
        "provider_observation_row_count",
        "inventory_source_record_lineage_digest",
        "inventory_candidate_id",
        "inventory_candidate_sha256",
        "inventory_completion_id",
        "inventory_completion_sha256",
        "inventory_data_sha256",
        "inventory_release_id",
        "inventory_release_manifest_sha256",
        "inventory_release_status",
        "response_batch_index",
        "reference_version",
        "reference_build_run_id",
        "source_capture_manifest_id",
        "source_capture_manifest_sha256",
        "source_publication_status",
        "source_published_at_utc",
        "source_available_session",
        "source_availability_rule",
    )


def _candidate_frame(rows: Sequence[Mapping[str, object]]) -> pl.DataFrame:
    optional_strings = (
        "inventory_completion_id",
        "inventory_completion_sha256",
        "inventory_release_id",
        "inventory_release_manifest_sha256",
        "job_error",
        "selected_figi",
        "selected_market_code",
        "selected_market_sector",
        "selected_security_description",
        "selected_security_type",
        "selected_security_type2",
        "selected_share_class_figi",
        "source_published_at_utc",
    )
    list_strings = (
        "market_codes",
        "projection_reason_codes",
        "returned_composite_figis",
        "returned_exchange_codes",
        "returned_figis",
        "returned_market_sectors",
        "returned_security_descriptions",
        "returned_security_types",
        "returned_security_types2",
        "returned_share_class_figis",
    )
    schema_overrides = {
        **{name: pl.String for name in optional_strings},
        **{name: pl.List(pl.String) for name in list_strings},
    }
    frame = pl.DataFrame(rows, schema_overrides=schema_overrides).select(*_candidate_columns())
    return frame.with_columns(
        *(pl.col(name).cast(pl.String) for name in optional_strings),
        *(pl.col(name).cast(pl.List(pl.String)) for name in list_strings),
    )


def _load_and_verify_request(
    root: Path,
    *,
    run_id: str,
    require_production_approval: bool,
) -> dict[str, object]:
    _digest(run_id, "run ID")
    path = safe_relative_path(root, _request_manifest_path(run_id))
    document = _load_exact_json(path, "request manifest")
    _expect_keys(
        document,
        {
            "artifact_type",
            "authenticated",
            "calendar_binding",
            "direct_approval",
            "endpoint",
            "external_evidence_binding",
            "inventory_binding",
            "job_order_digest",
            "jobs_per_request",
            "market_sector_description",
            "prepared_at_utc",
            "prepared_by",
            "request_count",
            "request_version",
            "resource_caps",
            "runtime_binding",
            "run_id",
            "source_capabilities",
        },
        "request manifest",
    )
    payload = dict(document)
    claimed = payload.pop("run_id", None)
    if claimed != run_id or stable_digest(payload) != run_id:
        raise IdentityMarketConsistencyError("request manifest ID recomputation failed")
    if (
        document.get("artifact_type") != "s7_openfigi_market_consistency_run_request"
        or document.get("request_version") != MARKET_CONSISTENCY_RUN_VERSION
        or document.get("endpoint") != OPENFIGI_MAPPING_ENDPOINT
        or document.get("market_sector_description") != "Equity"
        or document.get("source_capabilities") != _FALSE_CAPABILITIES
    ):
        raise IdentityMarketConsistencyError("request manifest semantics differ")
    authenticated = _native_bool(document.get("authenticated"), "authenticated")
    evidence = _load_external_evidence_binding()
    if document.get("external_evidence_binding") != evidence:
        raise IdentityMarketConsistencyError("request external-evidence binding differs")
    if document.get("calendar_binding") != _calendar_binding(root):
        raise IdentityMarketConsistencyError("request calendar binding differs")
    inventory = _mapping(document.get("inventory_binding"), "inventory binding")
    inventory_data = _mapping(inventory.get("data"), "inventory DATA binding")
    production = inventory.get("mode") == "production"
    if document.get("resource_caps") != _resource_caps(
        authenticated,
        inventory_row_count=_positive_int(inventory_data.get("row_count"), "inventory row count"),
        production=production,
    ):
        raise IdentityMarketConsistencyError("request resource caps differ")
    if inventory.get("mode") == "production":
        _require_canonical_production_root(root)
        runtime_binding = _mapping(document.get("runtime_binding"), "runtime binding")
        if runtime_binding != _repository_runtime_binding():
            raise IdentityMarketConsistencyError("production runtime source binding differs")
        if inventory != _verify_production_inventory_binding(root):
            raise IdentityMarketConsistencyError("production inventory binding differs")
        _verify_direct_approval(root, document.get("direct_approval"), request=document)
    elif inventory.get("mode") == "fixture":
        if document.get("runtime_binding") is not None:
            raise IdentityMarketConsistencyError(
                "fixture request cannot bind production runtime code"
            )
        if document.get("direct_approval") is not None:
            raise IdentityMarketConsistencyError(
                "fixture request cannot bind a production approval"
            )
        _verify_fixture_inventory_binding(root, inventory)
    else:
        raise IdentityMarketConsistencyError("request inventory mode is invalid")
    if require_production_approval and inventory.get("mode") != "production":
        raise IdentityMarketConsistencyError("production CLI refuses a fixture request")
    return document


def _verify_direct_approval(
    root: Path,
    raw: object,
    *,
    request: Mapping[str, object],
) -> dict[str, object]:
    receipt = _mapping(raw, "direct approval receipt")
    _expect_keys(receipt, {"approval_id", "bytes", "path", "sha256"}, "direct approval receipt")
    _verify_file_receipt(root, receipt)
    path = safe_relative_path(root, _text(receipt.get("path"), "direct approval path"))
    document = _load_exact_json(path, "direct approval")
    _expect_keys(
        document,
        {
            "approval_id",
            "approval_slot_id",
            "approval_slot_version",
            "approved_at_utc",
            "approved_by",
            "approval_availability",
            "approval_reaffirmation",
            "artifact_type",
            "authenticated",
            "authorized_actions",
            "calendar_binding",
            "continuing_authorization",
            "external_evidence_binding",
            "false_capabilities",
            "inventory_binding",
            "prepared_by",
            "recovery_predecessor",
            "resource_caps",
            "runtime_binding",
        },
        "direct approval",
    )
    approval_id = _digest(document.get("approval_id"), "direct approval ID")
    approval_scope = _direct_approval_scope(document)
    _verify_direct_approval_slot_document(
        root,
        document,
        expected_scope=approval_scope,
        expected_approval_id=approval_id,
    )
    if approval_id != receipt.get("approval_id"):
        raise IdentityMarketConsistencyError("direct approval receipt ID differs")
    if receipt.get("path") != _direct_approval_path():
        raise IdentityMarketConsistencyError("direct approval path is not canonical")
    continuation = _mapping(document.get("continuing_authorization"), "continuing authorization")
    if continuation != {
        "literal_text": S7_CONTINUING_AUTHORIZATION_TEXT,
        "literal_text_sha256": S7_CONTINUING_AUTHORIZATION_SHA256,
    }:
        raise IdentityMarketConsistencyError("direct approval literal differs")
    reaffirmation = _mapping(document.get("approval_reaffirmation"), "approval reaffirmation")
    if reaffirmation != {
        "literal_text": S7_REAFFIRMATION_TEXT,
        "literal_text_sha256": S7_REAFFIRMATION_SHA256,
    }:
        raise IdentityMarketConsistencyError("direct approval reaffirmation differs")
    approved_at = _parse_timestamp(document.get("approved_at_utc"), "approved_at_utc")
    expected_availability = _derive_control_availability(
        root,
        approved_at,
        controlling_field="approval_recorded_at_utc",
        rule="first_bound_xnys_open_strictly_after_approval_recorded_at_v1",
    )
    if document.get("approval_availability") != expected_availability:
        raise IdentityMarketConsistencyError("direct approval availability differs")
    if (
        document.get("artifact_type") != "s7_openfigi_market_consistency_direct_approval"
        or document.get("authenticated") != request.get("authenticated")
        or document.get("authorized_actions") != list(_AUTHORIZED_ACTIONS)
        or document.get("false_capabilities") != _FALSE_CAPABILITIES
        or document.get("resource_caps") != request.get("resource_caps")
        or document.get("inventory_binding") != request.get("inventory_binding")
        or document.get("external_evidence_binding") != request.get("external_evidence_binding")
        or document.get("calendar_binding") != request.get("calendar_binding")
        or document.get("recovery_predecessor") != _GATE_B_RECOVERY_PREDECESSOR
        or document.get("runtime_binding") != request.get("runtime_binding")
        or document.get("prepared_by") != request.get("prepared_by")
    ):
        raise IdentityMarketConsistencyError("direct approval scope differs")
    _text(document.get("approved_by"), "approved_by")
    _text(document.get("prepared_by"), "prepared_by")
    return document


def _direct_approval_scope(document: Mapping[str, object]) -> dict[str, object]:
    """Project a direct approval onto its time- and actor-independent fixed slot."""

    return {
        "artifact_type": document.get("artifact_type"),
        "approval_slot_id": document.get("approval_slot_id"),
        "approval_slot_version": document.get("approval_slot_version"),
        "approval_reaffirmation": document.get("approval_reaffirmation"),
        "authenticated": document.get("authenticated"),
        "authorized_actions": document.get("authorized_actions"),
        "calendar_binding": document.get("calendar_binding"),
        "continuing_authorization": document.get("continuing_authorization"),
        "external_evidence_binding": document.get("external_evidence_binding"),
        "false_capabilities": document.get("false_capabilities"),
        "inventory_binding": document.get("inventory_binding"),
        "recovery_predecessor": document.get("recovery_predecessor"),
        "resource_caps": document.get("resource_caps"),
        "runtime_binding": document.get("runtime_binding"),
    }


def _verify_direct_approval_slot_document(
    root: Path,
    document: Mapping[str, object],
    *,
    expected_scope: Mapping[str, object],
    expected_approval_id: str,
) -> None:
    """Fail closed if a fixed Gate-B approval slot is malformed or rebound."""

    _expect_keys(
        document,
        {
            "approval_id",
            "approval_slot_id",
            "approval_slot_version",
            "approved_at_utc",
            "approved_by",
            "approval_availability",
            "approval_reaffirmation",
            "artifact_type",
            "authenticated",
            "authorized_actions",
            "calendar_binding",
            "continuing_authorization",
            "external_evidence_binding",
            "false_capabilities",
            "inventory_binding",
            "prepared_by",
            "recovery_predecessor",
            "resource_caps",
            "runtime_binding",
        },
        "direct approval",
    )
    approval_id = _digest(document.get("approval_id"), "direct approval ID")
    if (
        approval_id != expected_approval_id
        or stable_digest(expected_scope) != expected_approval_id
        or _direct_approval_scope(document) != dict(expected_scope)
        or document.get("approval_slot_id") != DIRECT_APPROVAL_SLOT_ID
        or document.get("approval_slot_version") != DIRECT_APPROVAL_SLOT_VERSION
    ):
        raise IdentityMarketConsistencyError("direct approval fixed-slot binding differs")
    approved_at = _parse_timestamp(document.get("approved_at_utc"), "approved_at_utc")
    expected_availability = _derive_control_availability(
        root,
        approved_at,
        controlling_field="approval_recorded_at_utc",
        rule="first_bound_xnys_open_strictly_after_approval_recorded_at_v1",
    )
    if document.get("approval_availability") != expected_availability:
        raise IdentityMarketConsistencyError("direct approval availability differs")
    _text(document.get("approved_by"), "approved_by")
    _text(document.get("prepared_by"), "prepared_by")


def _verify_fixture_inventory_binding(root: Path, binding: Mapping[str, object]) -> None:
    _expect_keys(binding, {"candidate", "completion", "data", "mode"}, "fixture inventory binding")
    if binding.get("completion") is not None:
        raise IdentityMarketConsistencyError("fixture inventory cannot claim a completion")
    data = _mapping(binding.get("data"), "fixture inventory DATA")
    _expect_keys(data, {"bytes", "path", "row_count", "sha256"}, "fixture inventory DATA")
    path = safe_relative_path(root, _text(data.get("path"), "fixture inventory path"))
    _verify_regular_file(
        path, _digest(data.get("sha256"), "fixture inventory SHA"), "fixture inventory"
    )
    if path.stat().st_size != _nonnegative_int(data.get("bytes"), "fixture inventory bytes"):
        raise IdentityMarketConsistencyError("fixture inventory byte count differs")
    figis = _read_inventory_figis(path)
    if len(figis) != _positive_int(data.get("row_count"), "fixture inventory rows"):
        raise IdentityMarketConsistencyError("fixture inventory rows differ")


def _production_inventory_manifest_binding(root: Path) -> dict[str, object]:
    """Rebuild the frozen Gate-A binding without touching inventory Parquet bytes."""

    candidate_path = safe_relative_path(root, PRODUCTION_INVENTORY_CANDIDATE_PATH)
    completion_path = safe_relative_path(root, PRODUCTION_INVENTORY_COMPLETION_PATH)
    _verify_regular_file(
        candidate_path, PRODUCTION_INVENTORY_CANDIDATE_SHA256, "inventory candidate"
    )
    _verify_regular_file(
        completion_path, PRODUCTION_INVENTORY_COMPLETION_SHA256, "inventory completion"
    )
    candidate = _load_json(candidate_path, "inventory candidate")
    completion = _load_json(completion_path, "inventory completion")
    completion_payload = dict(completion)
    completion_id = completion_payload.pop("completion_id", None)
    if (
        stable_digest(completion_payload) != completion_id
        or completion_id != PRODUCTION_INVENTORY_COMPLETION_ID
    ):
        raise IdentityMarketConsistencyError("inventory completion ID differs")
    candidate_payload = dict(candidate)
    candidate_id = candidate_payload.pop("candidate_id", None)
    candidate_payload.pop("canonical_paths", None)
    if (
        stable_digest(candidate_payload) != candidate_id
        or candidate_id != PRODUCTION_INVENTORY_CANDIDATE_ID
    ):
        raise IdentityMarketConsistencyError("inventory candidate ID differs")
    completion_candidate = _mapping(completion.get("candidate"), "completion candidate")
    completion_data = _mapping(completion_candidate.get("data"), "completion DATA")
    counts = _mapping(completion.get("counts"), "completion counts")
    if (
        completion.get("plan_id") != PRODUCTION_INVENTORY_PLAN_ID
        or completion_candidate.get("candidate_id") != PRODUCTION_INVENTORY_CANDIDATE_ID
        or completion_candidate.get("path") != PRODUCTION_INVENTORY_CANDIDATE_PATH
        or completion_candidate.get("sha256") != PRODUCTION_INVENTORY_CANDIDATE_SHA256
        or completion_data.get("path") != PRODUCTION_INVENTORY_DATA_PATH
        or completion_data.get("sha256") != PRODUCTION_INVENTORY_DATA_SHA256
        or completion_candidate.get("bytes") != candidate_path.stat().st_size
        or counts.get("inventory_row_count") != PRODUCTION_INVENTORY_ROW_COUNT
        or candidate.get("candidate_state") != "awaiting_review"
        or completion.get("completion_state") != "awaiting_review"
    ):
        raise IdentityMarketConsistencyError("inventory completion/candidate binding differs")
    artifacts = _array(candidate.get("artifacts"), "inventory candidate artifacts")
    data_artifacts = [
        _mapping(item, "inventory candidate artifact")
        for item in artifacts
        if isinstance(item, Mapping) and item.get("role") == "data"
    ]
    if len(data_artifacts) != 1:
        raise IdentityMarketConsistencyError("inventory candidate DATA receipt is not unique")
    artifact = data_artifacts[0]
    data_bytes = _nonnegative_int(completion_data.get("bytes"), "completion DATA bytes")
    if (
        artifact.get("path") != "data/part-00000.parquet"
        or artifact.get("sha256") != PRODUCTION_INVENTORY_DATA_SHA256
        or artifact.get("bytes") != data_bytes
        or artifact.get("row_count") != PRODUCTION_INVENTORY_ROW_COUNT
        or _mapping(candidate.get("canonical_paths"), "inventory canonical paths").get("data")
        != PRODUCTION_INVENTORY_DATA_PATH
    ):
        raise IdentityMarketConsistencyError("inventory candidate DATA receipt differs")
    return {
        "candidate": {
            "bytes": candidate_path.stat().st_size,
            "candidate_id": PRODUCTION_INVENTORY_CANDIDATE_ID,
            "path": PRODUCTION_INVENTORY_CANDIDATE_PATH,
            "sha256": PRODUCTION_INVENTORY_CANDIDATE_SHA256,
        },
        "completion": {
            "bytes": completion_path.stat().st_size,
            "completion_id": PRODUCTION_INVENTORY_COMPLETION_ID,
            "path": PRODUCTION_INVENTORY_COMPLETION_PATH,
            "sha256": PRODUCTION_INVENTORY_COMPLETION_SHA256,
        },
        "data": {
            "bytes": data_bytes,
            "path": PRODUCTION_INVENTORY_DATA_PATH,
            "row_count": PRODUCTION_INVENTORY_ROW_COUNT,
            "sha256": PRODUCTION_INVENTORY_DATA_SHA256,
        },
        "mode": "production",
    }


def _verify_production_inventory_binding(root: Path) -> dict[str, object]:
    binding = _production_inventory_manifest_binding(root)
    data_path = safe_relative_path(root, PRODUCTION_INVENTORY_DATA_PATH)
    _verify_regular_file(data_path, PRODUCTION_INVENTORY_DATA_SHA256, "inventory DATA")
    data = _mapping(binding.get("data"), "inventory DATA binding")
    if data_path.stat().st_size != _nonnegative_int(data.get("bytes"), "inventory DATA bytes"):
        raise IdentityMarketConsistencyError("inventory DATA byte count differs")
    parquet = pq.ParquetFile(data_path)
    if parquet.metadata.num_rows != PRODUCTION_INVENTORY_ROW_COUNT:
        raise IdentityMarketConsistencyError("inventory DATA Parquet row count differs")
    figis = _read_inventory_figis(data_path)
    if len(figis) != PRODUCTION_INVENTORY_ROW_COUNT:
        raise IdentityMarketConsistencyError("inventory DATA key count differs")
    evidence = _load_external_evidence_binding()
    _require_seed_inventory_coverage(figis, evidence)
    return binding


def _load_external_evidence_binding() -> dict[str, object]:
    repo = _repository_root()
    cross = _verify_repository_manifest(
        repo,
        relative=CROSS_MARKET_EVIDENCE_PATH,
        manifest_id=CROSS_MARKET_EVIDENCE_ID,
        sha256=CROSS_MARKET_EVIDENCE_SHA256,
        expected_type="identity_cross_market_external_evidence",
    )
    exact = _verify_repository_manifest(
        repo,
        relative=EXACT_GROUP_EVIDENCE_PATH,
        manifest_id=EXACT_GROUP_EVIDENCE_ID,
        sha256=EXACT_GROUP_EVIDENCE_SHA256,
        expected_type="identity_exact_group_external_evidence",
    )
    seeds = _relationship_seeds(cross)
    if len(seeds) != 18:
        raise IdentityMarketConsistencyError("approved relationship seed count differs")
    exact_seeds = _exact_group_composite_seeds(exact)
    _replay_exact_group_assertions(exact)
    _verify_tnxp_exception(seeds)
    return {
        "cross_market": {
            "manifest_id": CROSS_MARKET_EVIDENCE_ID,
            "path": CROSS_MARKET_EVIDENCE_PATH,
            "relationship_seed_count": len(seeds),
            "relationship_seed_set_digest": stable_digest(list(seeds.values())),
            "sha256": CROSS_MARKET_EVIDENCE_SHA256,
        },
        "exact_groups": {
            "composite_seed_count": len(exact_seeds),
            "composite_seed_set_digest": stable_digest(list(exact_seeds.values())),
            "manifest_id": EXACT_GROUP_EVIDENCE_ID,
            "path": EXACT_GROUP_EVIDENCE_PATH,
            "sha256": EXACT_GROUP_EVIDENCE_SHA256,
        },
    }


def _verify_repository_manifest(
    repo: Path,
    *,
    relative: str,
    manifest_id: str,
    sha256: str,
    expected_type: str,
) -> dict[str, object]:
    path = repo / relative
    _verify_regular_file(path, sha256, "external evidence manifest")
    document = _load_json(path, "external evidence manifest")
    payload = dict(document)
    claimed = payload.pop("manifest_id", None)
    if claimed != manifest_id or stable_digest(payload) != manifest_id:
        raise IdentityMarketConsistencyError("external evidence manifest ID differs")
    if document.get("manifest_type") != expected_type:
        raise IdentityMarketConsistencyError("external evidence manifest type differs")
    for raw in _array(document.get("artifacts"), "external evidence artifacts"):
        receipt = _mapping(raw, "external evidence artifact")
        artifact = repo / _relative_path(receipt.get("path"), "external evidence artifact path")
        _verify_regular_file(
            artifact,
            _digest(receipt.get("sha256"), "external evidence artifact SHA"),
            "external evidence artifact",
        )
        if artifact.stat().st_size != _nonnegative_int(
            receipt.get("bytes"), "external evidence bytes"
        ):
            raise IdentityMarketConsistencyError("external evidence artifact size differs")
    return document


def _relationship_seeds(
    manifest: Mapping[str, object] | None = None,
) -> dict[str, dict[str, object]]:
    if manifest is None:
        path = _repository_root() / CROSS_MARKET_EVIDENCE_PATH
        _verify_regular_file(path, CROSS_MARKET_EVIDENCE_SHA256, "cross-market evidence")
        manifest = _load_json(path, "cross-market evidence")
    repo = _repository_root()
    output: dict[str, dict[str, object]] = {}
    for raw_claim in _array(manifest.get("mapping_assertions"), "mapping assertions"):
        claim = _mapping(raw_claim, "mapping assertion")
        ticker = _text(claim.get("ticker"), "mapping assertion ticker")
        for role in ("canonical_composite", "foreign_composite"):
            relation = _mapping(claim.get(role), "mapping relation")
            composite = _figi(relation.get("expected_composite_figi"), "seed Composite FIGI")
            seed = {
                "expected_composite_figi": composite,
                "expected_market_code": _text(relation.get("expected_market_code"), "seed market"),
                "expected_share_class_figi": _figi(
                    relation.get("expected_share_class_figi"), "seed Share Class FIGI"
                ),
                "role": role,
                "ticker": ticker,
            }
            if composite in output:
                raise IdentityMarketConsistencyError("relationship seed repeats a Composite")
            _replay_relation_assertion(repo, relation, seed)
            output[composite] = seed
    return dict(sorted(output.items()))


def _replay_relation_assertion(
    repo: Path,
    relation: Mapping[str, object],
    seed: Mapping[str, object],
) -> None:
    request = _load_json_value(
        repo / _relative_path(relation.get("request_path"), "seed request path"),
        "seed request",
    )
    response = _load_json_value(
        repo / _relative_path(relation.get("response_path"), "seed response path"),
        "seed response",
    )
    if not isinstance(request, list) or not isinstance(response, list):
        raise IdentityMarketConsistencyError("seed request/response roots must be arrays")
    index = _nonnegative_int(relation.get("request_job_index"), "seed job index")
    if index >= len(request) or index >= len(response):
        raise IdentityMarketConsistencyError("seed request job index is out of range")
    expected_composite = seed["expected_composite_figi"]
    if request[index] != _mapping_job(str(expected_composite)):
        raise IdentityMarketConsistencyError("seed request job differs")
    item = _mapping(response[index], "seed response item")
    rows = _array(item.get("data"), "seed response data")
    if not any(
        isinstance(row, Mapping)
        and row.get("compositeFIGI") == expected_composite
        and row.get("shareClassFIGI") == seed["expected_share_class_figi"]
        and row.get("exchCode") == seed["expected_market_code"]
        for row in rows
    ):
        raise IdentityMarketConsistencyError("seed relationship does not replay")


def _verify_tnxp_exception(seeds: Mapping[str, Mapping[str, object]]) -> None:
    exception = _FROZEN_NO_SELF_RELATION_EXCEPTIONS["BBG00R4FG9L2"]
    seed = seeds.get("BBG00R4FG9L2")
    if seed is None or (
        seed.get("expected_market_code") != exception["exchCode"]
        or seed.get("expected_share_class_figi") != exception["shareClassFIGI"]
    ):
        raise IdentityMarketConsistencyError("TNXP no-self exception is not evidence-bound")


def _exact_group_composite_seeds(
    manifest: Mapping[str, object] | None = None,
) -> dict[str, dict[str, object]]:
    if manifest is None:
        path = _repository_root() / EXACT_GROUP_EVIDENCE_PATH
        _verify_regular_file(path, EXACT_GROUP_EVIDENCE_SHA256, "exact-group evidence")
        manifest = _load_json(path, "exact-group evidence")
    output: dict[str, dict[str, object]] = {}
    for raw_case in _array(manifest.get("cases"), "exact-group evidence cases"):
        case = _mapping(raw_case, "exact-group evidence case")
        name = _text(case.get("case"), "exact-group case")
        for raw_assertion in _array(case.get("openfigi_assertions"), "exact-group assertions"):
            assertion = _mapping(raw_assertion, "exact-group assertion")
            expected = assertion.get("expected")
            if not isinstance(expected, Mapping) or "compositeFIGI" not in expected:
                continue
            record = dict(expected)
            composite = _figi(record.get("compositeFIGI"), "exact-group Composite")
            value = {
                "case": name,
                "expected_composite_figi": composite,
                "expected_market_code": _text(record.get("exchCode"), "exact-group market"),
                "expected_share_class_figi": _figi(
                    record.get("shareClassFIGI"), "exact-group Share Class"
                ),
            }
            if composite in output and output[composite] != value:
                raise IdentityMarketConsistencyError("exact-group Composite seed conflicts")
            output[composite] = value
    return dict(sorted(output.items()))


def _replay_exact_group_assertions(manifest: Mapping[str, object]) -> None:
    repo = _repository_root()
    for raw_case in _array(manifest.get("cases"), "exact-group evidence cases"):
        case = _mapping(raw_case, "exact-group evidence case")
        for raw_assertion in _array(case.get("openfigi_assertions"), "exact-group assertions"):
            assertion = _mapping(raw_assertion, "exact-group assertion")
            request = _load_json_value(
                repo / _relative_path(assertion.get("request_path"), "exact-group request path"),
                "exact-group request",
            )
            response = _load_json_value(
                repo / _relative_path(assertion.get("response_path"), "exact-group response path"),
                "exact-group response",
            )
            if not isinstance(request, list) or not isinstance(response, list):
                raise IdentityMarketConsistencyError(
                    "exact-group request/response roots must be arrays"
                )
            index = _nonnegative_int(assertion.get("request_job_index"), "exact-group job index")
            if index >= len(request) or index >= len(response):
                raise IdentityMarketConsistencyError("exact-group job index is out of range")
            expected_warning = assertion.get("expected_warning")
            if expected_warning is not None:
                if response[index] != {"warning": expected_warning}:
                    raise IdentityMarketConsistencyError("exact-group warning assertion drifted")
                continue
            expected = _mapping(assertion.get("expected"), "exact-group expected row")
            item = _mapping(response[index], "exact-group response item")
            rows = _array(item.get("data"), "exact-group response data")
            if not any(
                isinstance(row, Mapping)
                and all(row.get(key) == value for key, value in expected.items())
                for row in rows
            ):
                raise IdentityMarketConsistencyError("exact-group row assertion drifted")


def _exact_group_seed_drift(rows: Sequence[Mapping[str, object]]) -> list[str]:
    seeds = _exact_group_composite_seeds()
    by_key = {str(row["composite_figi"]): row for row in rows}
    drift: list[str] = []
    for key, seed in seeds.items():
        row = by_key.get(key)
        if row is None:
            continue
        expected_label = (
            "us_composite" if seed["expected_market_code"] == "US" else "non_us_composite"
        )
        if (
            row["classification"] != expected_label
            or row["selected_share_class_figi"] != seed["expected_share_class_figi"]
            or row["market_codes"] != [seed["expected_market_code"]]
        ):
            drift.append(key)
    return drift


def _require_seed_inventory_coverage(
    figis: Sequence[str], evidence_binding: Mapping[str, object]
) -> None:
    del evidence_binding
    missing = sorted(set(_relationship_seeds()) - set(figis))
    if missing:
        raise IdentityMarketConsistencyError(
            f"approved relationship seeds are absent from production inventory: {missing}"
        )


def _calendar_binding(root: Path) -> dict[str, object]:
    try:
        artifact = load_xnys_calendar_artifact(
            root,
            calendar_artifact_id=XNYS_CALENDAR_ARTIFACT_ID,
            expected_sha256=XNYS_CALENDAR_ARTIFACT_SHA256,
        )
    except XNYSCalendarArtifactError as exc:
        raise IdentityMarketConsistencyError("bound XNYS calendar artifact is invalid") from exc
    version = importlib.metadata.version("exchange-calendars")
    if version != "4.13.2":
        raise IdentityMarketConsistencyError("exchange-calendars runtime version differs")
    return {
        "artifact_id": artifact.calendar_artifact_id,
        "calendar_name": CALENDAR_NAME,
        "library": "exchange-calendars",
        "library_version": version,
        "path": artifact.relative_path,
        "rule_version": CALENDAR_ARTIFACT_RULE_VERSION,
        "sha256": artifact.sha256,
    }


def _derive_availability(root: Path, latest_response: datetime) -> dict[str, object]:
    return _derive_control_availability(
        root,
        latest_response,
        controlling_field="controlling_response_received_at_utc",
        rule="first_bound_xnys_open_strictly_after_latest_accepted_response_v1",
    )


def _derive_control_availability(
    root: Path,
    controlling_time: datetime,
    *,
    controlling_field: str,
    rule: str,
) -> dict[str, object]:
    binding = _calendar_binding(root)
    artifact = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=XNYS_CALENDAR_ARTIFACT_ID,
        expected_sha256=XNYS_CALENDAR_ARTIFACT_SHA256,
    )
    try:
        session, opening = artifact.first_open_after(controlling_time)
    except XNYSCalendarArtifactError as exc:
        raise IdentityMarketConsistencyError("cannot derive evidence availability") from exc
    proof = {
        "calendar_binding": binding,
        _text(controlling_field, "availability controlling field"): controlling_time.isoformat(),
        "first_open_rule": _text(rule, "availability rule"),
        "first_xnys_open_utc": opening.isoformat(),
        "source_available_session": session.isoformat(),
    }
    return {**proof, "proof_digest": stable_digest(proof)}


def _rebuild_batch_specs(
    root: Path, request: Mapping[str, object]
) -> tuple[tuple[str, ...], tuple[_BatchSpec, ...]]:
    inventory = _mapping(request.get("inventory_binding"), "inventory binding")
    data = _mapping(inventory.get("data"), "inventory DATA")
    path = safe_relative_path(root, _text(data.get("path"), "inventory DATA path"))
    _verify_regular_file(path, _digest(data.get("sha256"), "inventory DATA SHA"), "inventory DATA")
    figis = _read_inventory_figis(path)
    if len(figis) != _positive_int(data.get("row_count"), "inventory row count"):
        raise IdentityMarketConsistencyError("inventory row count differs")
    if stable_digest(figis) != request.get("job_order_digest"):
        raise IdentityMarketConsistencyError("OpenFIGI job order differs")
    jobs_per_request = _positive_int(request.get("jobs_per_request"), "jobs per request")
    authenticated = _native_bool(request.get("authenticated"), "authenticated")
    expected_size = AUTHENTICATED_JOBS_PER_REQUEST if authenticated else ANONYMOUS_JOBS_PER_REQUEST
    if jobs_per_request != expected_size:
        raise IdentityMarketConsistencyError("OpenFIGI request size policy differs")
    specs: list[_BatchSpec] = []
    run_id = _digest(request.get("run_id"), "run ID")
    for index, offset in enumerate(range(0, len(figis), jobs_per_request)):
        batch = tuple(figis[offset : offset + jobs_per_request])
        jobs = tuple(_mapping_job(figi) for figi in batch)
        batch_id = stable_digest(jobs)
        prefix = (
            "bronze/external/openfigi/s7-market-consistency/"
            f"run_id={run_id}/batches/batch_index={index:05d}-batch_id={batch_id}"
        )
        specs.append(
            _BatchSpec(
                index=index,
                figis=batch,
                jobs=jobs,
                batch_id=batch_id,
                prefix=prefix,
                request_bytes=_canonical_json(list(jobs)),
            )
        )
    if len(specs) != _positive_int(request.get("request_count"), "request count"):
        raise IdentityMarketConsistencyError("OpenFIGI request partition differs")
    return figis, tuple(specs)


def _write_attempt_intent(
    root: Path,
    *,
    spec: _BatchSpec,
    attempt_index: int,
    request_receipt: Mapping[str, object],
    request_started: datetime,
    min_interval_seconds: float,
) -> dict[str, object]:
    payload = {
        "artifact_type": "s7_openfigi_market_consistency_http_attempt_intent",
        "attempt_index": attempt_index,
        "batch_id": spec.batch_id,
        "batch_index": spec.index,
        "endpoint": OPENFIGI_MAPPING_ENDPOINT,
        "intent_version": ATTEMPT_INTENT_VERSION,
        "min_seconds_between_attempts": min_interval_seconds,
        "request": dict(request_receipt),
        "request_started_at_utc": request_started.isoformat(),
        "run_id": spec.prefix.split("run_id=", 1)[1].split("/", 1)[0],
    }
    document = {**payload, "attempt_intent_id": stable_digest(payload)}
    write_bytes_immutable(
        root,
        root / spec.attempt_intent_path(attempt_index),
        _canonical_json(document),
    )
    return document


def _write_attempt(
    root: Path,
    *,
    spec: _BatchSpec,
    attempt_index: int,
    request_receipt: Mapping[str, object],
    attempt_intent: Mapping[str, object],
    request_started: datetime,
    response_received: datetime,
    retry_not_before: datetime,
    status: int | None,
    response_headers: Mapping[str, str],
    response_body: bytes,
    transport_error_type: str | None,
    outcome: str | None = None,
) -> dict[str, object]:
    resolved_outcome = outcome or ("transport_error" if transport_error_type else "http_response")
    payload = {
        "artifact_type": "s7_openfigi_market_consistency_http_attempt",
        "attempt_intent": {
            **_file_receipt(root, spec.attempt_intent_path(attempt_index)),
            "attempt_intent_id": _digest(
                attempt_intent.get("attempt_intent_id"), "attempt intent ID"
            ),
        },
        "attempt_index": attempt_index,
        "batch_id": spec.batch_id,
        "batch_index": spec.index,
        "endpoint": OPENFIGI_MAPPING_ENDPOINT,
        "http_status": status,
        "outcome": resolved_outcome,
        "request": dict(request_receipt),
        "request_started_at_utc": request_started.isoformat(),
        "response_body_base64": base64.b64encode(response_body).decode("ascii"),
        "response_body_bytes": len(response_body),
        "response_body_sha256": hashlib.sha256(response_body).hexdigest(),
        "response_headers": dict(sorted(response_headers.items())),
        "response_received_at_utc": response_received.isoformat(),
        "retry_not_before_utc": retry_not_before.isoformat(),
        "run_id": spec.prefix.split("run_id=", 1)[1].split("/", 1)[0],
        "transport_error_type": transport_error_type,
    }
    document = {**payload, "attempt_id": stable_digest(payload)}
    path = spec.attempt_path(attempt_index)
    write_bytes_immutable(root, root / path, _canonical_json(document))
    return document


def _verify_attempt_intent(
    document: Mapping[str, object],
    *,
    spec: _BatchSpec,
    expected_index: int,
    request_receipt: Mapping[str, object],
    run_id: str,
) -> dict[str, object]:
    _expect_keys(
        document,
        {
            "artifact_type",
            "attempt_index",
            "attempt_intent_id",
            "batch_id",
            "batch_index",
            "endpoint",
            "intent_version",
            "min_seconds_between_attempts",
            "request",
            "request_started_at_utc",
            "run_id",
        },
        "HTTP attempt intent",
    )
    payload = dict(document)
    intent_id = payload.pop("attempt_intent_id", None)
    if stable_digest(payload) != intent_id:
        raise IdentityMarketConsistencyError("HTTP attempt intent ID differs")
    if (
        document.get("artifact_type") != "s7_openfigi_market_consistency_http_attempt_intent"
        or document.get("intent_version") != ATTEMPT_INTENT_VERSION
        or document.get("attempt_index") != expected_index
        or document.get("batch_index") != spec.index
        or document.get("batch_id") != spec.batch_id
        or document.get("endpoint") != OPENFIGI_MAPPING_ENDPOINT
        or document.get("run_id") != run_id
        or document.get("request") != request_receipt
    ):
        raise IdentityMarketConsistencyError("HTTP attempt intent binding differs")
    _parse_timestamp(document.get("request_started_at_utc"), "attempt intent request start")
    interval = document.get("min_seconds_between_attempts")
    if type(interval) not in {int, float} or float(interval) not in {
        ANONYMOUS_MIN_INTERVAL_SECONDS,
        AUTHENTICATED_MIN_INTERVAL_SECONDS,
    }:
        raise IdentityMarketConsistencyError("HTTP attempt intent rate interval differs")
    return dict(document)


def _load_attempt_states(
    root: Path,
    spec: _BatchSpec,
    *,
    request_receipt: Mapping[str, object],
    run_id: str,
) -> tuple[tuple[dict[str, object], dict[str, object] | None], ...]:
    parent = safe_relative_path(root, f"{spec.prefix}/attempts")
    if not parent.exists():
        return ()
    if not parent.is_dir() or parent.is_symlink():
        raise IdentityMarketConsistencyError("attempt root is unsafe")
    found: dict[int, tuple[dict[str, object], dict[str, object] | None]] = {}
    for child in parent.iterdir():
        match = _ATTEMPT_DIRECTORY.fullmatch(child.name)
        if match is None:
            if child.name.startswith("."):
                continue
            raise IdentityMarketConsistencyError("attempt root contains an unexpected entry")
        index = int(match.group(1))
        if not child.is_dir() or child.is_symlink():
            raise IdentityMarketConsistencyError("attempt slot is unsafe")
        names = {item.name for item in child.iterdir() if not item.name.startswith(".")}
        if not names.issubset({"intent.json", "attempt.json"}):
            raise IdentityMarketConsistencyError("attempt slot contains an unexpected entry")
        intent_path = child / "intent.json"
        attempt_path = child / "attempt.json"
        if not intent_path.exists():
            if attempt_path.exists():
                raise IdentityMarketConsistencyError("HTTP attempt exists without durable intent")
            # A directory or hidden atomic temporary file can survive before the intent
            # rename.  Since no network call was authorized by a durable intent, it is
            # safe to reuse this empty slot.
            continue
        if not intent_path.is_file() or intent_path.is_symlink():
            raise IdentityMarketConsistencyError("attempt intent artifact is unsafe")
        intent = _verify_attempt_intent(
            _load_exact_json(intent_path, "HTTP attempt intent"),
            spec=spec,
            expected_index=index,
            request_receipt=request_receipt,
            run_id=run_id,
        )
        result: dict[str, object] | None = None
        if attempt_path.exists():
            if not attempt_path.is_file() or attempt_path.is_symlink():
                raise IdentityMarketConsistencyError("attempt artifact is unsafe")
            result = _verify_attempt(
                _load_exact_json(attempt_path, "HTTP attempt"),
                spec=spec,
                expected_index=index,
                request_receipt=request_receipt,
                run_id=run_id,
                expected_path=spec.attempt_path(index),
                root=root,
            )
        found[index] = (intent, result)
    indexes = sorted(found)
    if indexes != list(range(len(indexes))):
        raise IdentityMarketConsistencyError("immutable attempt indexes are not contiguous")
    if len(indexes) > MAX_ATTEMPTS_PER_BATCH:
        raise IdentityMarketConsistencyError("immutable attempt count exceeds the approved cap")
    return tuple(found[index] for index in indexes)


def _load_attempts(
    root: Path,
    spec: _BatchSpec,
    *,
    request_receipt: Mapping[str, object],
    run_id: str,
) -> tuple[dict[str, object], ...]:
    states = _load_attempt_states(
        root,
        spec,
        request_receipt=request_receipt,
        run_id=run_id,
    )
    if any(result is None for _, result in states):
        raise IdentityMarketConsistencyError(
            "durable HTTP attempt intent has no outcome receipt and requires recovery"
        )
    return tuple(result for _, result in states if result is not None)


def _resolve_orphan_attempts(
    root: Path,
    spec: _BatchSpec,
    *,
    request_receipt: Mapping[str, object],
    run_id: str,
    min_interval_seconds: float,
    now: Callable[[], datetime],
) -> None:
    states = _load_attempt_states(
        root,
        spec,
        request_receipt=request_receipt,
        run_id=run_id,
    )
    for intent, result in states:
        if result is not None:
            continue
        index = _nonnegative_int(intent.get("attempt_index"), "orphan attempt index")
        if float(intent.get("min_seconds_between_attempts", 0.0)) != min_interval_seconds:
            raise IdentityMarketConsistencyError("orphan attempt rate interval differs")
        started = _parse_timestamp(intent.get("request_started_at_utc"), "orphan request start")
        recovered = _now_utc(now, "unknown outcome recovery time")
        if recovered < started:
            raise IdentityMarketConsistencyError("unknown-outcome recovery predates request intent")
        retry_not_before = recovered + timedelta(
            seconds=max(min_interval_seconds, _retry_delay_for_index(index))
        )
        _write_attempt(
            root,
            spec=spec,
            attempt_index=index,
            request_receipt=request_receipt,
            attempt_intent=intent,
            request_started=started,
            response_received=recovered,
            retry_not_before=retry_not_before,
            status=None,
            response_headers={},
            response_body=b"",
            transport_error_type="UnknownNetworkOutcome",
            outcome="unknown_network_outcome",
        )


def _response_is_acceptable(content: bytes, *, expected_jobs: int) -> bool:
    """Return whether a 200 body is a complete batch-level OpenFIGI response.

    Per-job errors remain valid evidence and are classified offline.  Only malformed JSON,
    a non-array top level, or a job-count mismatch is retryable at capture time.
    """

    try:
        response = _decode_json_bytes(content, "OpenFIGI response")
    except IdentityMarketConsistencyError:
        return False
    return isinstance(response, list) and len(response) == expected_jobs


def _attempt_is_source_unavailable(attempt: Mapping[str, object], *, expected_jobs: int) -> bool:
    if attempt.get("outcome") in {"transport_error", "unknown_network_outcome"}:
        return True
    status = attempt.get("http_status")
    if status in _RETRYABLE_STATUS:
        return True
    return status == 200 and not _response_is_acceptable(
        _attempt_body(attempt), expected_jobs=expected_jobs
    )


def _attempt_reference(
    root: Path, spec: _BatchSpec, attempt: Mapping[str, object]
) -> dict[str, object]:
    index = _nonnegative_int(attempt.get("attempt_index"), "attempt index")
    relative = spec.attempt_path(index)
    return {
        **_file_receipt(root, relative),
        "attempt_id": _digest(attempt.get("attempt_id"), "attempt ID"),
    }


def _write_batch_commit(
    root: Path,
    *,
    spec: _BatchSpec,
    request_receipt: Mapping[str, object],
    run_id: str,
    terminal_status: str,
    now: Callable[[], datetime],
) -> dict[str, object]:
    attempts = _load_attempts(
        root,
        spec,
        request_receipt=request_receipt,
        run_id=run_id,
    )
    if not attempts:
        raise IdentityMarketConsistencyError("cannot commit a batch without an attempt")
    terminal = attempts[-1]
    if terminal_status == "response_accepted":
        if terminal.get("http_status") != 200 or not _response_is_acceptable(
            _attempt_body(terminal), expected_jobs=len(spec.figis)
        ):
            raise IdentityMarketConsistencyError("terminal response is not acceptable")
        accepted_attempt_id: str | None = _digest(terminal.get("attempt_id"), "accepted attempt ID")
    elif terminal_status == "source_unavailable":
        if len(attempts) != MAX_ATTEMPTS_PER_BATCH or not all(
            _attempt_is_source_unavailable(item, expected_jobs=len(spec.figis)) for item in attempts
        ):
            raise IdentityMarketConsistencyError(
                "source-unavailable terminal disposition lacks exact retry exhaustion"
            )
        accepted_attempt_id = None
    else:
        raise IdentityMarketConsistencyError("batch terminal status is invalid")
    received = _parse_timestamp(
        terminal.get("response_received_at_utc"), "terminal attempt receipt"
    )
    committed = _now_utc(now, "committed_at_utc")
    if committed < received:
        raise IdentityMarketConsistencyError("batch commit predates terminal attempt")
    references = [_attempt_reference(root, spec, item) for item in attempts]
    payload = {
        "accepted_attempt_id": accepted_attempt_id,
        "artifact_type": "s7_openfigi_market_consistency_batch_commit",
        "attempt": references[-1],
        "attempts": references,
        "batch_id": spec.batch_id,
        "batch_index": spec.index,
        "committed_at_utc": committed.isoformat(),
        "composite_figis": list(spec.figis),
        "endpoint": OPENFIGI_MAPPING_ENDPOINT,
        "request": dict(request_receipt),
        "response_received_at_utc": received.isoformat(),
        "run_id": run_id,
        "terminal_status": terminal_status,
    }
    document = {**payload, "batch_commit_id": stable_digest(payload)}
    write_bytes_immutable(root, root / spec.accepted_path, _canonical_json(document))
    return document


def _verify_attempt(
    document: Mapping[str, object],
    *,
    spec: _BatchSpec,
    expected_index: int,
    request_receipt: Mapping[str, object],
    run_id: str,
    expected_path: str,
    root: Path,
) -> dict[str, object]:
    _expect_keys(
        document,
        {
            "artifact_type",
            "attempt_intent",
            "attempt_id",
            "attempt_index",
            "batch_id",
            "batch_index",
            "endpoint",
            "http_status",
            "outcome",
            "request",
            "request_started_at_utc",
            "response_body_base64",
            "response_body_bytes",
            "response_body_sha256",
            "response_headers",
            "response_received_at_utc",
            "retry_not_before_utc",
            "run_id",
            "transport_error_type",
        },
        "HTTP attempt",
    )
    payload = dict(document)
    attempt_id = payload.pop("attempt_id", None)
    if stable_digest(payload) != attempt_id:
        raise IdentityMarketConsistencyError("HTTP attempt ID differs")
    if (
        document.get("artifact_type") != "s7_openfigi_market_consistency_http_attempt"
        or document.get("attempt_index") != expected_index
        or document.get("batch_index") != spec.index
        or document.get("batch_id") != spec.batch_id
        or document.get("endpoint") != OPENFIGI_MAPPING_ENDPOINT
        or document.get("run_id") != run_id
        or document.get("request") != request_receipt
    ):
        raise IdentityMarketConsistencyError("HTTP attempt binding differs")
    intent_ref = _mapping(document.get("attempt_intent"), "HTTP attempt intent receipt")
    _expect_keys(
        intent_ref,
        {"attempt_intent_id", "bytes", "path", "sha256"},
        "HTTP attempt intent receipt",
    )
    expected_intent_path = spec.attempt_intent_path(expected_index)
    if intent_ref.get("path") != expected_intent_path:
        raise IdentityMarketConsistencyError("HTTP attempt intent path differs")
    _verify_file_receipt(root, intent_ref)
    intent = _verify_attempt_intent(
        _load_exact_json(safe_relative_path(root, expected_intent_path), "HTTP attempt intent"),
        spec=spec,
        expected_index=expected_index,
        request_receipt=request_receipt,
        run_id=run_id,
    )
    if intent_ref.get("attempt_intent_id") != intent.get("attempt_intent_id"):
        raise IdentityMarketConsistencyError("HTTP attempt intent receipt ID differs")
    actual_receipt = _file_receipt(root, expected_path)
    if actual_receipt["path"] != expected_path:
        raise IdentityMarketConsistencyError("HTTP attempt path differs")
    started = _parse_timestamp(document.get("request_started_at_utc"), "attempt request start")
    if document.get("request_started_at_utc") != intent.get("request_started_at_utc"):
        raise IdentityMarketConsistencyError("HTTP attempt start differs from durable intent")
    received = _parse_timestamp(
        document.get("response_received_at_utc"), "attempt response receipt"
    )
    if received < started:
        raise IdentityMarketConsistencyError("HTTP attempt response predates request")
    retry_not_before = _parse_timestamp(
        document.get("retry_not_before_utc"), "attempt retry-not-before"
    )
    if retry_not_before < received:
        raise IdentityMarketConsistencyError("HTTP attempt retry boundary predates response")
    body = _attempt_body(document)
    if len(body) != _nonnegative_int(document.get("response_body_bytes"), "attempt body bytes"):
        raise IdentityMarketConsistencyError("HTTP attempt body size differs")
    if hashlib.sha256(body).hexdigest() != _digest(
        document.get("response_body_sha256"), "attempt body SHA"
    ):
        raise IdentityMarketConsistencyError("HTTP attempt body SHA differs")
    status = document.get("http_status")
    transport = document.get("transport_error_type")
    if document.get("outcome") == "transport_error":
        if status is not None or not isinstance(transport, str) or not transport or body:
            raise IdentityMarketConsistencyError("transport-error attempt semantics differ")
    elif document.get("outcome") == "unknown_network_outcome":
        if (
            status is not None
            or transport != "UnknownNetworkOutcome"
            or body
            or document.get("response_headers") != {}
        ):
            raise IdentityMarketConsistencyError("unknown-outcome attempt semantics differ")
    elif document.get("outcome") == "http_response":
        if type(status) is not int or status < 100 or status > 599 or transport is not None:
            raise IdentityMarketConsistencyError("HTTP-response attempt semantics differ")
    else:
        raise IdentityMarketConsistencyError("HTTP attempt outcome is invalid")
    headers = _mapping(document.get("response_headers"), "attempt response headers")
    if headers != _allowlisted_headers(headers):
        raise IdentityMarketConsistencyError("attempt response headers are not allowlisted")
    retryable = (
        document.get("outcome") in {"transport_error", "unknown_network_outcome"}
        or status in _RETRYABLE_STATUS
        or (status == 200 and not _response_is_acceptable(body, expected_jobs=len(spec.figis)))
    )
    minimum_interval = float(intent["min_seconds_between_attempts"])
    required_wait = minimum_interval
    if retryable:
        required_wait = max(
            minimum_interval,
            _retry_delay_for_index(expected_index),
            _retry_after_seconds(headers, received),
        )
    actual_wait = (retry_not_before - received).total_seconds()
    if actual_wait + 1e-6 < required_wait:
        raise IdentityMarketConsistencyError("HTTP attempt retry boundary was shortened")
    return dict(document)


def _verify_batch_commit(
    root: Path,
    document: Mapping[str, object],
    *,
    request_document: Mapping[str, object],
    spec: _BatchSpec,
    request_receipt: Mapping[str, object],
) -> dict[str, object]:
    _expect_keys(
        document,
        {
            "accepted_attempt_id",
            "artifact_type",
            "attempt",
            "attempts",
            "batch_commit_id",
            "batch_id",
            "batch_index",
            "committed_at_utc",
            "composite_figis",
            "endpoint",
            "request",
            "response_received_at_utc",
            "run_id",
            "terminal_status",
        },
        "batch commit",
    )
    payload = dict(document)
    commit_id = payload.pop("batch_commit_id", None)
    if stable_digest(payload) != commit_id:
        raise IdentityMarketConsistencyError("batch commit ID differs")
    run_id = _digest(request_document.get("run_id"), "run ID")
    if (
        document.get("artifact_type") != "s7_openfigi_market_consistency_batch_commit"
        or document.get("batch_id") != spec.batch_id
        or document.get("batch_index") != spec.index
        or document.get("composite_figis") != list(spec.figis)
        or document.get("endpoint") != OPENFIGI_MAPPING_ENDPOINT
        or document.get("request") != request_receipt
        or document.get("run_id") != run_id
    ):
        raise IdentityMarketConsistencyError("batch commit binding differs")
    all_attempts = _load_attempts(
        root,
        spec,
        request_receipt=request_receipt,
        run_id=run_id,
    )
    if not all_attempts:
        raise IdentityMarketConsistencyError("batch commit has no immutable attempts")
    expected_references = [_attempt_reference(root, spec, item) for item in all_attempts]
    raw_references = _array(document.get("attempts"), "batch attempt receipts")
    if raw_references != expected_references:
        raise IdentityMarketConsistencyError("batch attempt receipt set differs")
    attempt_ref = _mapping(document.get("attempt"), "terminal attempt receipt")
    if attempt_ref != expected_references[-1]:
        raise IdentityMarketConsistencyError("terminal attempt receipt differs")
    attempt = all_attempts[-1]
    terminal_status = _text(document.get("terminal_status"), "batch terminal status")
    accepted_attempt_id = document.get("accepted_attempt_id")
    if terminal_status == "response_accepted":
        expected_id = _digest(attempt.get("attempt_id"), "accepted attempt ID")
        if (
            accepted_attempt_id != expected_id
            or attempt.get("http_status") != 200
            or not _response_is_acceptable(_attempt_body(attempt), expected_jobs=len(spec.figis))
        ):
            raise IdentityMarketConsistencyError(
                "accepted attempt is not the exact successful terminal attempt"
            )
    elif terminal_status == "source_unavailable":
        if (
            accepted_attempt_id is not None
            or len(all_attempts) != MAX_ATTEMPTS_PER_BATCH
            or not all(
                _attempt_is_source_unavailable(item, expected_jobs=len(spec.figis))
                for item in all_attempts
            )
        ):
            raise IdentityMarketConsistencyError(
                "source-unavailable batch did not exhaust exact retryable attempts"
            )
    else:
        raise IdentityMarketConsistencyError("batch terminal status is invalid")
    response_received = _parse_timestamp(
        attempt.get("response_received_at_utc"), "terminal attempt receipt"
    )
    if document.get("response_received_at_utc") != response_received.isoformat():
        raise IdentityMarketConsistencyError("batch commit response time differs")
    if _parse_timestamp(document.get("committed_at_utc"), "batch commit time") < response_received:
        raise IdentityMarketConsistencyError("batch commit predates response")
    return dict(document)


def _verify_final_manifest(
    root: Path,
    path: Path,
    *,
    request_document: Mapping[str, object],
) -> dict[str, object]:
    document = _load_exact_json(path, "final capture manifest")
    _expect_keys(
        document,
        {
            "artifact_type",
            "batch_count",
            "batches",
            "calendar_binding",
            "completed_at_utc",
            "composite_count",
            "direct_approval",
            "external_evidence_binding",
            "inventory_binding",
            "latest_response_received_at_utc",
            "manifest_id",
            "request_manifest",
            "run_id",
            "status",
            "version",
        },
        "final capture manifest",
    )
    payload = dict(document)
    manifest_id = payload.pop("manifest_id", None)
    if stable_digest(payload) != manifest_id:
        raise IdentityMarketConsistencyError("final capture manifest ID differs")
    run_id = _digest(request_document.get("run_id"), "run ID")
    if (
        document.get("artifact_type") != "s7_openfigi_market_consistency_capture_manifest"
        or document.get("status") != "complete"
        or document.get("version") != MARKET_CONSISTENCY_RUN_VERSION
        or document.get("run_id") != run_id
        or document.get("calendar_binding") != request_document.get("calendar_binding")
        or document.get("direct_approval") != request_document.get("direct_approval")
        or document.get("external_evidence_binding")
        != request_document.get("external_evidence_binding")
        or document.get("inventory_binding") != request_document.get("inventory_binding")
    ):
        raise IdentityMarketConsistencyError("final capture controls differ")
    request_relative = _request_manifest_path(run_id)
    if document.get("request_manifest") != _file_receipt(root, request_relative):
        raise IdentityMarketConsistencyError("final request-manifest receipt differs")
    figis, specs = _rebuild_batch_specs(root, request_document)
    batches = _array(document.get("batches"), "capture batches")
    if (
        len(batches) != len(specs)
        or document.get("batch_count") != len(specs)
        or document.get("composite_count") != len(figis)
    ):
        raise IdentityMarketConsistencyError("final capture coverage differs")
    verified: list[dict[str, object]] = []
    for spec, raw in zip(specs, batches, strict=True):
        request_receipt = {
            "bytes": len(spec.request_bytes),
            "path": spec.request_path,
            "sha256": hashlib.sha256(spec.request_bytes).hexdigest(),
        }
        _verify_file_receipt(root, request_receipt)
        commit_path = safe_relative_path(root, spec.accepted_path)
        stored = _load_exact_json(commit_path, "stored batch commit")
        if raw != stored:
            raise IdentityMarketConsistencyError("final batch differs from exact batch commit")
        verified.append(
            _verify_batch_commit(
                root,
                stored,
                request_document=request_document,
                spec=spec,
                request_receipt=request_receipt,
            )
        )
    _validate_batch_order(verified, len(specs))
    latest = max(
        _parse_timestamp(item["response_received_at_utc"], "response receipt") for item in verified
    )
    if document.get("latest_response_received_at_utc") != latest.isoformat():
        raise IdentityMarketConsistencyError("final latest response time differs")
    if _parse_timestamp(document.get("completed_at_utc"), "capture completion") < latest:
        raise IdentityMarketConsistencyError("capture completion predates response")
    return document


def _capture_usage(root: Path, specs: Sequence[_BatchSpec]) -> tuple[int, int, datetime | None]:
    attempt_count = 0
    response_bytes = 0
    latest: datetime | None = None
    for spec in specs:
        request_path = safe_relative_path(root, spec.request_path)
        if not request_path.exists():
            continue
        receipt = _file_receipt(root, spec.request_path)
        attempts = _load_attempts(
            root,
            spec,
            request_receipt=receipt,
            run_id=spec.prefix.split("run_id=", 1)[1].split("/", 1)[0],
        )
        for attempt in attempts:
            attempt_count += 1
            response_bytes += _nonnegative_int(
                attempt.get("response_body_bytes"), "attempt response bytes"
            )
            value = _parse_timestamp(attempt["retry_not_before_utc"], "attempt retry boundary")
            latest = value if latest is None or value > latest else latest
    return attempt_count, response_bytes, latest


def _enforce_capture_caps(
    root: Path,
    caps: Mapping[str, object],
    *,
    runner_started: float,
    attempt_count: int,
    cumulative_response_bytes: int,
    prospective_attempt_count: int = 0,
    prospective_response_bytes: int = 0,
) -> None:
    if prospective_response_bytes > _positive_int(
        caps.get("max_response_bytes"), "max response bytes"
    ):
        raise IdentityMarketConsistencyError("single response exceeds the approved byte cap")
    if attempt_count + prospective_attempt_count > _positive_int(
        caps.get("max_total_attempts"), "max total attempts"
    ):
        raise IdentityMarketConsistencyError("capture exceeds the approved total-attempt cap")
    if cumulative_response_bytes + prospective_response_bytes > _positive_int(
        caps.get("max_cumulative_response_bytes"), "max cumulative response bytes"
    ):
        raise IdentityMarketConsistencyError(
            "capture exceeds the approved cumulative-response-byte cap"
        )
    wall_clock = time.monotonic() - runner_started
    if wall_clock > _positive_int(caps.get("max_wall_clock_seconds"), "max wall-clock seconds"):
        raise IdentityMarketConsistencyError("capture exceeds the approved wall-clock cap")
    disk_floor = _nonnegative_int(caps.get("disk_free_hard_floor_bytes"), "disk free hard floor")
    free = shutil.disk_usage(root).free
    if free - prospective_response_bytes < disk_floor:
        raise IdentityMarketConsistencyError("capture would cross the approved disk hard floor")


def _rate_limited_start(
    *,
    now: Callable[[], datetime],
    sleep: Callable[[float], None],
    retry_not_before: datetime | None,
) -> datetime:
    current = _now_utc(now, "request_started_at_utc")
    if retry_not_before is not None and current < retry_not_before:
        sleep((retry_not_before - current).total_seconds())
        current = _now_utc(now, "request_started_at_utc")
        if current < retry_not_before:
            raise IdentityMarketConsistencyError(
                "rate-limit clock did not advance after the required wait"
            )
    return current


def _retry_delay_for_index(attempt_index: int) -> float:
    if type(attempt_index) is not int or attempt_index < 0:
        raise IdentityMarketConsistencyError("attempt index is invalid for retry policy")
    return min(2.0 * (2**attempt_index), 60.0)


def _retry_after_seconds(headers: Mapping[str, str], received_at: datetime) -> float:
    raw = headers.get("retry-after")
    if raw is None:
        return 0.0
    try:
        value = float(raw)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(raw)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return 0.0
        value = (parsed.astimezone(UTC) - received_at).total_seconds()
    if not math.isfinite(value) or value < 0.0:
        return 0.0
    return value


def _allowlisted_headers(headers: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(headers, Mapping):
        return {}
    output: dict[str, str] = {}
    for key, value in headers.items():
        if (
            isinstance(key, str)
            and isinstance(value, str)
            and key.lower() in _ALLOWED_RESPONSE_HEADERS
        ):
            output[key.lower()] = value
    return dict(sorted(output.items()))


def _reject_secret_echo(
    api_key: str | None,
    *,
    response_body: bytes,
    response_headers: Mapping[str, str],
) -> None:
    if not api_key:
        return
    secret = api_key.encode("utf-8")
    if secret in response_body or any(
        secret in value.encode("utf-8") for value in response_headers.values()
    ):
        raise IdentityMarketConsistencyError(
            "OpenFIGI response echoed the exact API key; refusing any persistent write"
        )


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


def _urllib_post(url: str, body: bytes, headers: Mapping[str, str]) -> HttpResult:
    if url != OPENFIGI_MAPPING_ENDPOINT:
        raise IdentityMarketConsistencyError("OpenFIGI adapter refuses a different endpoint")
    request = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
    opener = urllib.request.build_opener(_NoRedirectHandler())
    try:
        with opener.open(request, timeout=60) as response:
            return HttpResult(
                status=int(response.status),
                headers=dict(response.headers.items()),
                body=response.read(MAX_RESPONSE_BYTES + 1),
            )
    except urllib.error.HTTPError as exc:
        return HttpResult(
            status=int(exc.code),
            headers=dict(exc.headers.items()) if exc.headers else {},
            body=exc.read(MAX_RESPONSE_BYTES + 1),
        )


class _exclusive_nonblocking_lock(AbstractContextManager["_exclusive_nonblocking_lock"]):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def __enter__(self) -> _exclusive_nonblocking_lock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd: int | None = None
        try:
            fd = os.open(self.path, flags, 0o600)
            opened = os.fstat(fd)
            if not stat.S_ISREG(opened.st_mode):
                raise IdentityMarketConsistencyError("run lock is not a regular file")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            visible = os.stat(self.path, follow_symlinks=False)
            locked = os.fstat(fd)
            if not stat.S_ISREG(visible.st_mode) or (visible.st_dev, visible.st_ino) != (
                locked.st_dev,
                locked.st_ino,
            ):
                raise IdentityMarketConsistencyError("run lock path changed while acquiring")
        except BlockingIOError as exc:
            if fd is not None:
                os.close(fd)
            raise IdentityMarketConsistencyError(
                "another process holds the nonblocking run lock"
            ) from exc
        except Exception:
            if fd is not None:
                os.close(fd)
            raise
        self.fd = fd
        return self

    def __exit__(self, *args: object) -> None:
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None


def _read_inventory_details(path: Path) -> dict[str, dict[str, object]]:
    columns = (
        "observed_composite_figi",
        "first_session",
        "last_session",
        "active_row_count",
        "inactive_row_count",
        "source_record_lineage_digest",
    )
    table = pq.read_table(path, columns=list(columns))
    values = {name: table.column(name).to_pylist() for name in columns}
    output: dict[str, dict[str, object]] = {}
    for index in range(table.num_rows):
        figi = _figi(values["observed_composite_figi"][index], "inventory Composite FIGI")
        if figi in output:
            raise IdentityMarketConsistencyError("inventory repeats a Composite FIGI")
        output[figi] = {name: values[name][index] for name in columns if name != columns[0]}
    return output


def _read_inventory_figis(path: Path) -> tuple[str, ...]:
    table = pq.read_table(path, columns=[INVENTORY_COLUMN])
    values = table.column(INVENTORY_COLUMN).to_pylist()
    figis = tuple(sorted(_figi(value, "inventory Composite FIGI") for value in values))
    if not figis or len(set(figis)) != len(figis):
        raise IdentityMarketConsistencyError("Composite inventory must be nonempty and unique")
    return figis


def _mapping_job(figi: str) -> dict[str, object]:
    return {
        "idType": "COMPOSITE_ID_BB_GLOBAL",
        "idValue": _figi(figi, "Composite FIGI"),
        "includeUnlistedEquities": True,
        "marketSecDes": "Equity",
    }


def _resource_caps(
    authenticated: bool,
    *,
    inventory_row_count: int,
    production: bool,
) -> dict[str, object]:
    if type(authenticated) is not bool:
        raise IdentityMarketConsistencyError("authenticated must be a native bool")
    if type(production) is not bool:
        raise IdentityMarketConsistencyError("production must be a native bool")
    rows = _positive_int(inventory_row_count, "inventory row count")
    jobs_per_request = (
        AUTHENTICATED_JOBS_PER_REQUEST if authenticated else ANONYMOUS_JOBS_PER_REQUEST
    )
    request_count = (rows + jobs_per_request - 1) // jobs_per_request
    return {
        "disk_free_hard_floor_bytes": (PRODUCTION_DISK_FREE_HARD_FLOOR_BYTES if production else 0),
        "endpoint": OPENFIGI_MAPPING_ENDPOINT,
        "jobs_per_request": jobs_per_request,
        "max_attempts_per_batch": MAX_ATTEMPTS_PER_BATCH,
        "max_cumulative_response_bytes": MAX_CUMULATIVE_RESPONSE_BYTES,
        "max_response_bytes": MAX_RESPONSE_BYTES,
        "max_total_attempts": request_count * MAX_ATTEMPTS_PER_BATCH,
        "max_wall_clock_seconds": MAX_CAPTURE_WALL_CLOCK_SECONDS,
        "min_seconds_between_attempts": (
            AUTHENTICATED_MIN_INTERVAL_SECONDS if authenticated else ANONYMOUS_MIN_INTERVAL_SECONDS
        ),
        "network_concurrency": 1,
    }


def _request_manifest_path(run_id: str) -> str:
    return (
        "bronze/external/openfigi/s7-market-consistency/"
        f"run_id={_digest(run_id, 'run ID')}/request-manifest.json"
    )


def _direct_approval_path() -> str:
    return (
        "manifests/silver/identity/openfigi-market-consistency-direct-approvals/"
        f"slot_id={DIRECT_APPROVAL_SLOT_ID}/manifest.json"
    )


def _final_manifest_path(run_id: str) -> str:
    return (
        "manifests/silver/identity/openfigi-market-consistency-runs/"
        f"run_id={_digest(run_id, 'run ID')}/manifest.json"
    )


def _candidate_prefix(candidate_id: str) -> str:
    return (
        "manifests/silver/identity/openfigi-market-consistency-candidates/"
        f"candidate_id={_digest(candidate_id, 'candidate ID')}"
    )


def _attempt_body(document: Mapping[str, object]) -> bytes:
    encoded = document.get("response_body_base64")
    if not isinstance(encoded, str):
        raise IdentityMarketConsistencyError("attempt body is not base64 text")
    try:
        return base64.b64decode(encoded.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error) as exc:
        raise IdentityMarketConsistencyError("attempt body base64 is invalid") from exc


def _file_receipt(root: Path, relative: str) -> dict[str, object]:
    path = safe_relative_path(root, relative)
    if not path.is_file() or path.is_symlink():
        raise IdentityMarketConsistencyError(f"artifact is missing or unsafe: {relative}")
    return {"bytes": path.stat().st_size, "path": relative, "sha256": sha256_file(path)}


def _verify_file_receipt(root: Path, receipt: Mapping[str, object]) -> None:
    path = safe_relative_path(root, _text(receipt.get("path"), "artifact path"))
    if (
        not path.is_file()
        or path.is_symlink()
        or path.stat().st_size != _nonnegative_int(receipt.get("bytes"), "artifact bytes")
        or sha256_file(path) != _digest(receipt.get("sha256"), "artifact SHA-256")
    ):
        raise IdentityMarketConsistencyError("captured artifact receipt differs")


def _verify_regular_file(path: Path, expected_sha: str, label: str) -> None:
    if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha:
        raise IdentityMarketConsistencyError(
            f"{label} is missing, unsafe, or has a different SHA-256"
        )


def _validate_batch_order(receipts: Sequence[Mapping[str, object]], count: int) -> None:
    if [item.get("batch_index") for item in receipts] != list(range(count)):
        raise IdentityMarketConsistencyError("OpenFIGI batch commit order differs")


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _canonical_json_text(value: object) -> str:
    return _canonical_json(value).decode("utf-8")


def _load_exact_json(path: Path, label: str) -> dict[str, object]:
    if not path.is_file() or path.is_symlink():
        raise IdentityMarketConsistencyError(f"{label} is missing or unsafe")
    content = path.read_bytes()
    value = _mapping(_decode_json_bytes(content, label), label)
    if _canonical_json(value) != content:
        raise IdentityMarketConsistencyError(f"{label} is not canonical JSON")
    return value


def _load_json(path: Path, label: str) -> dict[str, object]:
    return _mapping(_load_json_value(path, label), label)


def _load_json_value(path: Path, label: str) -> object:
    if not path.is_file() or path.is_symlink():
        raise IdentityMarketConsistencyError(f"{label} is missing or unsafe")
    return _decode_json_bytes(path.read_bytes(), label)


def _decode_json_bytes(content: bytes, label: str) -> object:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        output: dict[str, object] = {}
        for key, value in pairs:
            if key in output:
                raise IdentityMarketConsistencyError(f"{label} contains duplicate JSON keys")
            output[key] = value
        return output

    try:
        return json.loads(
            content.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda value: _reject_constant(value, label),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityMarketConsistencyError(f"{label} is not valid strict JSON") from exc


def _reject_constant(value: str, label: str) -> object:
    raise IdentityMarketConsistencyError(f"{label} contains non-finite JSON: {value}")


def _repository_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").is_file() and (parent / "docs").is_dir():
            return parent
    raise IdentityMarketConsistencyError("repository root cannot be located")


def _git_output(repo: Path, *arguments: str, label: str) -> bytes:
    try:
        completed = subprocess.run(
            ("git", "-C", str(repo), *arguments),
            capture_output=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise IdentityMarketConsistencyError(f"cannot inspect Git {label}") from exc
    if completed.returncode != 0:
        raise IdentityMarketConsistencyError(f"cannot inspect Git {label}")
    return completed.stdout


def _git_identifier(content: bytes, label: str) -> str:
    try:
        value = content.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise IdentityMarketConsistencyError(f"Git {label} is not ASCII") from exc
    if re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", value) is None:
        raise IdentityMarketConsistencyError(f"Git {label} is invalid")
    return value


def _repository_runtime_binding() -> dict[str, object]:
    """Bind production execution to one clean, tracked Git source tree."""

    repo = _repository_root()
    status = _git_output(
        repo,
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        label="worktree status",
    )
    if status:
        raise IdentityMarketConsistencyError("production runtime checkout is not exact and clean")
    commit = _git_identifier(
        _git_output(repo, "rev-parse", "--verify", "HEAD", label="commit"),
        "commit",
    )
    tree = _git_identifier(
        _git_output(repo, "rev-parse", "--verify", "HEAD^{tree}", label="tree"),
        "tree",
    )
    files: list[dict[str, object]] = []
    for relative in _RUNTIME_SOURCE_PATHS:
        path = repo / relative
        if not path.is_file() or path.is_symlink():
            raise IdentityMarketConsistencyError(f"runtime source is missing or unsafe: {relative}")
        tracked = _git_output(
            repo,
            "ls-files",
            "--error-unmatch",
            "--",
            relative,
            label=f"tracked runtime source {relative}",
        )
        if tracked.decode("utf-8", errors="strict").strip() != relative:
            raise IdentityMarketConsistencyError(f"runtime source tracking differs: {relative}")
        working_blob = _git_identifier(
            _git_output(repo, "hash-object", "--", relative, label=f"blob {relative}"),
            f"blob {relative}",
        )
        committed_blob = _git_identifier(
            _git_output(
                repo,
                "rev-parse",
                f"HEAD:{relative}",
                label=f"committed blob {relative}",
            ),
            f"committed blob {relative}",
        )
        if working_blob != committed_blob:
            raise IdentityMarketConsistencyError(f"runtime source blob differs: {relative}")
        stage = _git_output(
            repo,
            "ls-files",
            "--stage",
            "--",
            relative,
            label=f"runtime source mode {relative}",
        )
        try:
            metadata, staged_path = stage.decode("utf-8", errors="strict").strip().split("\t", 1)
            git_mode, stage_blob, stage_number = metadata.split(" ", 2)
        except ValueError as exc:
            raise IdentityMarketConsistencyError(
                f"runtime source index entry is invalid: {relative}"
            ) from exc
        if staged_path != relative or stage_blob != committed_blob or stage_number != "0":
            raise IdentityMarketConsistencyError(f"runtime source index entry differs: {relative}")
        files.append(
            {
                "bytes": path.stat().st_size,
                "git_blob": committed_blob,
                "git_mode": git_mode,
                "path": relative,
                "sha256": sha256_file(path),
            }
        )
    file_set_digest = stable_digest(files)
    return {
        "binding_version": "s7_gate_b_runtime_git_binding_v1",
        "exact_checkout_clean": True,
        "repository_commit": commit,
        "repository_tree": tree,
        "runtime_file_set_digest": file_set_digest,
        "runtime_files": files,
        "runtime_versions": {
            "exchange_calendars": importlib.metadata.version("exchange-calendars"),
            "polars": importlib.metadata.version("polars"),
            "pyarrow": importlib.metadata.version("pyarrow"),
            "python": platform.python_version(),
        },
    }


def _root(path: Path) -> Path:
    if not isinstance(path, Path):
        raise IdentityMarketConsistencyError("data root must be a Path")
    expanded = path.expanduser()
    if expanded.is_symlink():
        raise IdentityMarketConsistencyError("data root cannot be a symlink")
    root = expanded.resolve()
    if not root.is_dir() or root == Path("/"):
        raise IdentityMarketConsistencyError("data root is unavailable or unsafe")
    return root


def _require_canonical_production_root(root: Path) -> None:
    expected = PRODUCTION_DATA_ROOT.expanduser().resolve()
    if root != expected:
        raise IdentityMarketConsistencyError(
            "production Gate-B execution requires the canonical data root"
        )


def _timestamp(value: object, label: str) -> str:
    return _parse_timestamp(value, label).isoformat()


def _parse_timestamp(value: object, label: str) -> datetime:
    text = _text(value, label)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IdentityMarketConsistencyError(f"{label} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IdentityMarketConsistencyError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _now_utc(now: Callable[[], datetime], label: str) -> datetime:
    value = now()
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityMarketConsistencyError(f"{label} clock must return an aware datetime")
    return value.astimezone(UTC)


def _date_text(value: object, label: str) -> str:
    text = value.isoformat() if hasattr(value, "isoformat") else value  # type: ignore[union-attr]
    if not isinstance(text, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) is None:
        raise IdentityMarketConsistencyError(f"{label} must be a date")
    return text


def _figi(value: object, label: str) -> str:
    if not isinstance(value, str) or _FIGI.fullmatch(value) is None:
        raise IdentityMarketConsistencyError(f"{label} is invalid")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise IdentityMarketConsistencyError(f"{label} must be a lowercase SHA-256")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise IdentityMarketConsistencyError(f"{label} must be trimmed nonempty text")
    return value


def _relative_path(value: object, label: str) -> str:
    text = _text(value, label)
    path = Path(text)
    if path.is_absolute() or path.as_posix() != text or ".." in path.parts:
        raise IdentityMarketConsistencyError(f"{label} is not a safe relative path")
    return text


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise IdentityMarketConsistencyError(f"{label} must be an object")
    return dict(value)


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise IdentityMarketConsistencyError(f"{label} must be an array")
    return value


def _native_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise IdentityMarketConsistencyError(f"{label} must be a native bool")
    return value


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IdentityMarketConsistencyError(f"{label} must be a positive native int")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IdentityMarketConsistencyError(f"{label} must be a non-negative native int")
    return value


def _expect_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise IdentityMarketConsistencyError(f"{label} schema is not exact")


__all__ = [
    "CROSS_MARKET_EVIDENCE_ID",
    "CROSS_MARKET_EVIDENCE_SHA256",
    "EXACT_GROUP_EVIDENCE_ID",
    "EXACT_GROUP_EVIDENCE_SHA256",
    "PRODUCTION_INVENTORY_CANDIDATE_ID",
    "PRODUCTION_INVENTORY_CANDIDATE_SHA256",
    "PRODUCTION_INVENTORY_COMPLETION_ID",
    "PRODUCTION_INVENTORY_COMPLETION_SHA256",
    "PRODUCTION_INVENTORY_DATA_SHA256",
    "PRODUCTION_INVENTORY_ROW_COUNT",
    "S7_CONTINUING_AUTHORIZATION_SHA256",
    "S7_CONTINUING_AUTHORIZATION_TEXT",
    "S7_REAFFIRMATION_SHA256",
    "S7_REAFFIRMATION_TEXT",
    "HttpResult",
    "IdentityMarketConsistencyError",
    "MarketClassification",
    "MarketClassificationCandidate",
    "MarketConsistencyRun",
    "classify_market_consistency_run",
    "execute_market_consistency_run",
    "materialize_market_classification_candidate",
    "prepare_approved_market_consistency_run",
    "prepare_market_consistency_run",
    "verify_market_classification_candidate",
]
