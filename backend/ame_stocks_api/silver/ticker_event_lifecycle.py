"""Fail-closed S5 lifecycle for the paired formal ticker-event Silver tables.

The complete formal identifier receipt is small enough that preview and full use the same
scope.  Two one-table workflows are advanced in lockstep; request status is the parent and is
published and re-read before the event child can be published.
"""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
from collections import Counter
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, stable_digest, write_bytes_immutable
from ame_stocks_api.silver.contracts import (
    QA_RESULT_ARROW_SCHEMA,
    QUARANTINE_ARROW_SCHEMA,
    ArtifactRef,
    ArtifactRole,
    BuildIntent,
    BuildKind,
    BuildManifest,
    PreviewMetadata,
    QACheckResult,
    QASeverity,
    QAStatus,
    ReleaseManifest,
    SourceInventory,
    SourceLayer,
    TableContract,
    arrow_schema_digest,
    thaw_json,
)
from ame_stocks_api.silver.reader import PublishedRelease, PublishedSilverReader
from ame_stocks_api.silver.store import (
    WORKFLOW_EVENT_VERSION,
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowSnapshot,
    WorkflowState,
)
from ame_stocks_api.silver.ticker_event_contract import (
    TICKER_CHANGE_EVENT_CONTRACT,
    TICKER_EVENT_REQUEST_STATUS_CONTRACT,
)
from ame_stocks_api.silver.ticker_event_source import (
    TickerEventSourceBatch,
    build_ticker_event_source_inventory,
    read_ticker_event_source_inventory,
    ticker_event_coverage_receipt_path,
    ticker_event_transform_inputs,
)
from ame_stocks_api.silver.ticker_event_source_profile import (
    PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_PATH,
    PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_SHA256,
    PRODUCTION_PILOT_IDENTIFIER_RECEIPT_PATH,
    PRODUCTION_PILOT_IDENTIFIER_RECEIPT_SHA256,
    accepted_coverage_receipt,
    coverage_receipt_bytes,
    profile_ticker_event_source,
)
from ame_stocks_api.silver.ticker_events import (
    TICKER_EVENT_TRANSFORM_VERSION,
    transform_ticker_events,
)

S5_COMPLETION_AUTHORIZATION = "我建议如果中间没发生预期外的事情，直接把S5推进到结束吧"  # noqa: RUF001
S5_DATE_QUALITY_AUTHORIZATION = "批准 S5 日期质量方案，本来我们也不关心这么远的日期"  # noqa: RUF001
S5_DATE_QUALITY_DECISION_ID = "approved_s5_date_quality_v1"
S5_DATE_QUALITY_DECISION_SHA256 = stable_digest({"decision": S5_DATE_QUALITY_AUTHORIZATION})
S5_SCHEMA_CREATED_AT = "2026-07-14T08:41:00+00:00"
S5_SCHEMA_DECIDED_AT = "2026-07-14T08:41:01+00:00"
S5_WORKFLOW_ACTOR = "s5-ticker-events-lifecycle"
S5_APPROVER = "user-delegated-s5-completion-authority"
S5_SAMPLE_LIMIT = 100

EXPECTED_FORMAL_REQUESTS = 15_173
EXPECTED_COMPLETE_REQUESTS = 11_471
EXPECTED_NOT_FOUND_REQUESTS = 3_702
EXPECTED_RAW_EVENTS = 13_088
EXPECTED_EVENT_ROWS = 12_895
EXPECTED_BLANK_TARGETS = 193
EXPECTED_PILOT_REQUESTS = 100

_CONTRACTS = (TICKER_EVENT_REQUEST_STATUS_CONTRACT, TICKER_CHANGE_EVENT_CONTRACT)
_ACTIVE_POST_SCHEMA_STATES = frozenset(
    {
        WorkflowState.CODE_READY,
        WorkflowState.PREVIEW_READY,
        WorkflowState.AWAITING_REVIEW,
        WorkflowState.APPROVED_FULL_RUN,
        WorkflowState.FULL_READY,
        WorkflowState.AWAITING_PUBLISH,
        WorkflowState.PUBLISHED,
    }
)
_EXPECTED_ROWS = {
    TICKER_EVENT_REQUEST_STATUS_CONTRACT.table: EXPECTED_FORMAL_REQUESTS,
    TICKER_CHANGE_EVENT_CONTRACT.table: EXPECTED_EVENT_ROWS,
}
_EXPECTED_WARNING_NUMERATORS = {
    TICKER_EVENT_REQUEST_STATUS_CONTRACT.table: {
        "identifier_not_found_404_requests": EXPECTED_NOT_FOUND_REQUESTS,
        "response_cik_missing_complete_requests": 2_527,
        "excluded_pilot_manifests": EXPECTED_PILOT_REQUESTS,
    },
    TICKER_CHANGE_EVENT_CONTRACT.table: {
        "blank_target_placeholder_rows": EXPECTED_BLANK_TARGETS,
        "response_cik_missing_requests": 2_527,
        "sentinel_1969_12_31_rows": 766,
        "request_boundary_2003_09_10_rows": 1_334,
        "provider_cluster_2023_11_18_rows": 480,
        "weekend_event_rows": 481,
        "same_figi_date_multiple_ticker_groups": 2,
        "ticker_reuse_multiple_figi_groups": 430,
        "figi_multiple_ticker_groups": 1_244,
        "event_before_s4_window_rows": 4_298,
        "event_after_request_end_rows": 1,
    },
}
_EXPECTED_QUARANTINE_ROWS = {
    TICKER_EVENT_REQUEST_STATUS_CONTRACT.table: 0,
    TICKER_CHANGE_EVENT_CONTRACT.table: EXPECTED_BLANK_TARGETS,
}
_LOGIC_CLOSURE = (
    "backend/ame_stocks_api/cli/silver_ticker_events_lifecycle.py",
    "backend/ame_stocks_api/silver/contracts.py",
    "backend/ame_stocks_api/silver/reader.py",
    "backend/ame_stocks_api/silver/schema_resources/ticker_change_event.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/ticker_event_request_status.schema-v1.json",
    "backend/ame_stocks_api/silver/store.py",
    "backend/ame_stocks_api/silver/ticker_event_contract.py",
    "backend/ame_stocks_api/silver/ticker_event_lifecycle.py",
    "backend/ame_stocks_api/silver/ticker_event_source.py",
    "backend/ame_stocks_api/silver/ticker_event_source_profile.py",
    "backend/ame_stocks_api/silver/ticker_events.py",
    "docs/silver/contracts/identity/ticker_change_event.schema-v1.candidate.json",
    "docs/silver/contracts/identity/ticker_event_request_status.schema-v1.candidate.json",
    "pyproject.toml",
)


@dataclass(frozen=True, slots=True)
class TickerEventAuthorization:
    formal_receipt_path: str = PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_PATH
    formal_receipt_sha256: str = PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_SHA256
    pilot_receipt_path: str = PRODUCTION_PILOT_IDENTIFIER_RECEIPT_PATH
    pilot_receipt_sha256: str = PRODUCTION_PILOT_IDENTIFIER_RECEIPT_SHA256
    expected_formal_requests: int = EXPECTED_FORMAL_REQUESTS
    expected_complete_requests: int = EXPECTED_COMPLETE_REQUESTS
    expected_not_found_requests: int = EXPECTED_NOT_FOUND_REQUESTS
    expected_raw_events: int = EXPECTED_RAW_EVENTS
    expected_event_rows: int = EXPECTED_EVENT_ROWS
    expected_blank_targets: int = EXPECTED_BLANK_TARGETS
    expected_pilot_requests: int = EXPECTED_PILOT_REQUESTS
    sample_limit: int = S5_SAMPLE_LIMIT

    def __post_init__(self) -> None:
        if (
            self.formal_receipt_path != PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_PATH
            or self.formal_receipt_sha256 != PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_SHA256
            or self.pilot_receipt_path != PRODUCTION_PILOT_IDENTIFIER_RECEIPT_PATH
            or self.pilot_receipt_sha256 != PRODUCTION_PILOT_IDENTIFIER_RECEIPT_SHA256
        ):
            raise ValueError("S5 identifier receipt identity is not production-authorized")
        if (
            self.expected_formal_requests != EXPECTED_FORMAL_REQUESTS
            or self.expected_complete_requests != EXPECTED_COMPLETE_REQUESTS
            or self.expected_not_found_requests != EXPECTED_NOT_FOUND_REQUESTS
            or self.expected_raw_events != EXPECTED_RAW_EVENTS
            or self.expected_event_rows != EXPECTED_EVENT_ROWS
            or self.expected_blank_targets != EXPECTED_BLANK_TARGETS
            or self.expected_pilot_requests != EXPECTED_PILOT_REQUESTS
            or not 1 <= self.sample_limit <= 100
        ):
            raise ValueError("S5 authorized source cardinality or sample bound changed")


CURRENT_TICKER_EVENT_AUTHORIZATION = TickerEventAuthorization()


@dataclass(frozen=True, slots=True)
class TickerEventTableRun:
    contract: TableContract
    workflow: WorkflowSnapshot
    preview: BuildManifest
    preview_document: StoredDocument
    full: BuildManifest
    full_document: StoredDocument
    release: ReleaseManifest
    release_document: StoredDocument
    published: PublishedRelease


@dataclass(frozen=True, slots=True)
class TickerEventLifecycleRun:
    request_status: TickerEventTableRun
    ticker_change: TickerEventTableRun
    inventory: SourceInventory
    inventory_document: StoredDocument
    coverage_receipt_path: str
    coverage_receipt_sha256: str
    profile_sha256: str

    def by_table(self, table: str) -> TickerEventTableRun:
        for item in (self.request_status, self.ticker_change):
            if item.contract.table == table:
                return item
        raise KeyError(table)


def complete_ticker_event_lifecycle(
    data_root: Path,
    *,
    repo_root: Path,
    git_commit: str,
) -> TickerEventLifecycleRun:
    """Profile, preview, recompute, approve and publish the exact formal S5 scope."""

    _verify_git_checkout(repo_root, git_commit)
    root = data_root.expanduser().resolve()
    store = SilverStore(root)
    authorization = CURRENT_TICKER_EVENT_AUTHORIZATION
    (
        batch,
        inventory,
        inventory_document,
        inputs,
        coverage_path,
        coverage_sha,
        profile_sha,
    ) = _prepare_authorized_source(
        root,
        store=store,
        git_commit=git_commit,
        authorization=authorization,
    )
    request_inputs, occurrence_inputs = ticker_event_transform_inputs(batch)

    workflows = {
        contract.table: _ensure_schema_approved(store, contract) for contract in _CONTRACTS
    }
    previews: dict[str, tuple[BuildManifest, StoredDocument]] = {}
    for contract in _CONTRACTS:
        workflows[contract.table], previews[contract.table] = _ensure_preview(
            root,
            store=store,
            snapshot=workflows[contract.table],
            contract=contract,
            batch=batch,
            request_inputs=request_inputs,
            occurrence_inputs=occurrence_inputs,
            inputs=inputs,
            inventory=inventory,
            git_commit=git_commit,
            profile_sha256=profile_sha,
            coverage_receipt_path=coverage_path,
            coverage_receipt_sha256=coverage_sha,
            authorization=authorization,
        )

    _require_pair_integrity(
        _read_build_data_table(root, previews[TICKER_EVENT_REQUEST_STATUS_CONTRACT.table][0]),
        _read_build_data_table(root, previews[TICKER_CHANGE_EVENT_CONTRACT.table][0]),
    )
    for contract in _CONTRACTS:
        workflows[contract.table] = _ensure_full_approved(
            store,
            workflows[contract.table],
            previews[contract.table][0],
            contract,
        )

    fulls: dict[str, tuple[BuildManifest, StoredDocument]] = {}
    for contract in _CONTRACTS:
        workflows[contract.table], fulls[contract.table] = _ensure_full(
            root,
            store=store,
            snapshot=workflows[contract.table],
            contract=contract,
            preview=previews[contract.table][0],
            batch=batch,
            request_inputs=request_inputs,
            occurrence_inputs=occurrence_inputs,
            inventory=inventory,
            authorization=authorization,
        )
    for contract in _CONTRACTS:
        workflows[contract.table] = _ensure_awaiting_publish(
            store, workflows[contract.table], fulls[contract.table][0]
        )

    _verify_git_checkout(repo_root, git_commit)
    status_full = _read_build_data_table(root, fulls[TICKER_EVENT_REQUEST_STATUS_CONTRACT.table][0])
    event_full = _read_build_data_table(root, fulls[TICKER_CHANGE_EVENT_CONTRACT.table][0])
    _require_pair_integrity(status_full, event_full)

    runs: dict[str, TickerEventTableRun] = {}
    status_contract = TICKER_EVENT_REQUEST_STATUS_CONTRACT
    snapshot, release = _ensure_published(
        store, workflows[status_contract.table], fulls[status_contract.table][0], status_contract
    )
    runs[status_contract.table] = _verified_run(
        root,
        store=store,
        contract=status_contract,
        snapshot=snapshot,
        preview=previews[status_contract.table],
        full=fulls[status_contract.table],
        release=release,
    )

    _require_pair_integrity(
        _read_published_table(runs[status_contract.table].published), event_full
    )
    _verify_git_checkout(repo_root, git_commit)
    event_contract = TICKER_CHANGE_EVENT_CONTRACT
    snapshot, release = _ensure_published(
        store, workflows[event_contract.table], fulls[event_contract.table][0], event_contract
    )
    runs[event_contract.table] = _verified_run(
        root,
        store=store,
        contract=event_contract,
        snapshot=snapshot,
        preview=previews[event_contract.table],
        full=fulls[event_contract.table],
        release=release,
    )
    _require_pair_integrity(
        _read_published_table(runs[status_contract.table].published),
        _read_published_table(runs[event_contract.table].published),
    )
    return TickerEventLifecycleRun(
        request_status=runs[status_contract.table],
        ticker_change=runs[event_contract.table],
        inventory=inventory,
        inventory_document=inventory_document,
        coverage_receipt_path=coverage_path,
        coverage_receipt_sha256=coverage_sha,
        profile_sha256=profile_sha,
    )


def _prepare_authorized_source(
    root: Path,
    *,
    store: SilverStore,
    git_commit: str,
    authorization: TickerEventAuthorization,
) -> tuple[
    TickerEventSourceBatch,
    SourceInventory,
    StoredDocument,
    tuple[ArtifactRef, ...],
    str,
    str,
    str,
]:
    profile = profile_ticker_event_source(root)
    _require_authorized_profile(profile, authorization)
    receipt = accepted_coverage_receipt(profile)
    content = coverage_receipt_bytes(receipt)
    relative = ticker_event_coverage_receipt_path(receipt)
    stored = write_bytes_immutable(root, root / relative, content)
    coverage_sha = str(stored["sha256"])
    inventory = build_ticker_event_source_inventory(
        root,
        coverage_receipt_path=relative,
        coverage_receipt_sha256=coverage_sha,
        git_commit=git_commit,
    )
    batch = read_ticker_event_source_inventory(root, inventory)
    if (
        batch.request_count != authorization.expected_formal_requests
        or batch.page_count != authorization.expected_complete_requests
        or batch.not_found_count != authorization.expected_not_found_requests
        or batch.row_count != authorization.expected_raw_events
    ):
        raise SilverStoreError("S5 verified batch cardinality differs from authorization")
    inventory_document = store.register_source_inventory(inventory)
    inputs = tuple(
        ArtifactRef(
            path=item.path,
            sha256=item.sha256,
            bytes=item.bytes,
            row_count=item.row_count,
            media_type=item.media_type,
            role=ArtifactRole.SOURCE,
            source_dataset=inventory.source_dataset,
            source_layer=SourceLayer.BRONZE,
            lineage_manifest_path=inventory_document.path,
            lineage_manifest_sha256=inventory_document.sha256,
        )
        for item in inventory.artifacts
    )
    for contract in _CONTRACTS:
        store.verify_source_artifacts(inputs, contract)
    profile_sha = profile.get("profile_sha256")
    if not isinstance(profile_sha, str) or len(profile_sha) != 64:
        raise SilverStoreError("S5 source profile has no valid profile SHA-256")
    return (
        batch,
        inventory,
        inventory_document,
        inputs,
        relative,
        coverage_sha,
        profile_sha,
    )


def _require_authorized_profile(
    profile: dict[str, object], authorization: TickerEventAuthorization
) -> None:
    if profile.get("status") != "passed_with_warnings":
        raise SilverStoreError("S5 source profile is not in the accepted reviewed state")
    gates = profile.get("hard_gate_counts")
    if not isinstance(gates, dict) or any(value != 0 for value in gates.values()):
        raise SilverStoreError("S5 source profile has a nonzero hard gate")
    receipt = profile.get("accepted_coverage_receipt")
    if not isinstance(receipt, dict):
        raise SilverStoreError("S5 source profile has no accepted coverage receipt")
    formal = receipt.get("formal_counts")
    formal_receipt = receipt.get("formal_identifier_receipt")
    pilot = receipt.get("pilot_exclusion")
    diagnostics = receipt.get("diagnostics")
    if not all(isinstance(item, dict) for item in (formal, formal_receipt, pilot, diagnostics)):
        raise SilverStoreError("S5 coverage receipt summary is malformed")
    assert isinstance(formal, dict)
    assert isinstance(formal_receipt, dict)
    assert isinstance(pilot, dict)
    assert isinstance(diagnostics, dict)
    if formal != {
        "artifacts": authorization.expected_complete_requests,
        "complete": authorization.expected_complete_requests,
        "events": authorization.expected_raw_events,
        "identifiers": authorization.expected_formal_requests,
        "not_found_404": authorization.expected_not_found_requests,
    }:
        raise SilverStoreError("S5 formal coverage counts differ from authorization")
    if formal_receipt != {
        "identifier_count": authorization.expected_formal_requests,
        "path": authorization.formal_receipt_path,
        "sha256": authorization.formal_receipt_sha256,
    }:
        raise SilverStoreError("S5 formal identifier receipt differs from authorization")
    if (
        pilot.get("identifier_count") != authorization.expected_pilot_requests
        or pilot.get("complete") != 16
        or pilot.get("not_found_404") != 84
        or pilot.get("included_in_inventory") is not False
        or pilot.get("path") != authorization.pilot_receipt_path
        or pilot.get("sha256") != authorization.pilot_receipt_sha256
    ):
        raise SilverStoreError("S5 pilot exclusion differs from authorization")
    date_quality = diagnostics.get("date_quality")
    cik = diagnostics.get("cik_coverage")
    cluster = diagnostics.get("date_2023_11_18")
    expected_quality = {
        "after_declared_snapshot_boundary": 1,
        "blank_target_placeholder": authorization.expected_blank_targets,
        "coverage_floor_baseline": 1_334,
        "provider_sentinel_unknown_date": 766,
        "valid_effective_date": 10_794,
    }
    if date_quality != expected_quality or cik != {"missing": 2_527, "present": 8_944}:
        raise SilverStoreError("S5 date or CIK profile differs from the approved review")
    if cluster != {
        "blank_target_events": authorization.expected_blank_targets,
        "nonblank_target_events": 287,
        "total_events": 480,
    }:
        raise SilverStoreError("S5 2023-11-18 provider cluster differs from review")
    expected_scalars = {
        "figi_with_multiple_tickers": 1_244,
        "same_figi_same_date_multi_ticker_groups": 2,
        "semantic_duplicate_groups": 0,
        "responses_with_blank_target": authorization.expected_blank_targets,
        "ticker_reused_multiple_figis": 430,
        "valid_siblings_in_blank_target_responses": 262,
        "weekend_blank_target_events": authorization.expected_blank_targets,
        "weekend_events": 481,
    }
    if any(diagnostics.get(key) != value for key, value in expected_scalars.items()):
        raise SilverStoreError("S5 identity/date diagnostics differ from approved review")
    profile_sha = profile.get("profile_sha256")
    preimage = dict(profile)
    preimage.pop("profile_sha256", None)
    preimage.pop("profile_sha256_preimage", None)
    recomputed = hashlib.sha256(
        json.dumps(
            preimage,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()
    if profile_sha != recomputed:
        raise SilverStoreError("S5 source profile digest does not match its canonical preimage")


def _ensure_schema_approved(store: SilverStore, contract: TableContract) -> WorkflowSnapshot:
    actor = f"{S5_WORKFLOW_ACTOR}-{contract.table}"
    workflow_id = stable_digest(
        {
            "actor": actor,
            "contract_id": contract.contract_id,
            "created_at": S5_SCHEMA_CREATED_AT,
            "workflow_event_version": WORKFLOW_EVENT_VERSION,
        }
    )
    event_dir = store.root / "manifests/silver/workflows" / workflow_id / "events"
    if event_dir.exists():
        snapshot = store.verify_workflow_trust_chain(workflow_id, verify_artifacts=True)
        existing, _ = store.load_workflow_contract(workflow_id)
        if existing != contract:
            raise SilverStoreError("deterministic S5 workflow contract changed")
    else:
        snapshot = store.create_workflow(
            contract,
            actor=actor,
            created_at=S5_SCHEMA_CREATED_AT,
            note=f"Registered reviewed S5 contract for {contract.table}.",
        )
    if snapshot.state is WorkflowState.PLANNED:
        snapshot = store.submit_schema_review(
            workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            actor=actor,
            created_at=S5_SCHEMA_CREATED_AT,
            note=f"Submitted reviewed S5 {contract.table} contract for schema approval.",
        )
    if snapshot.state is WorkflowState.SCHEMA_REVIEW:
        snapshot = store.approve_schema(
            workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            approver=S5_APPROVER,
            decided_at=S5_SCHEMA_DECIDED_AT,
            note=(
                f"Completion delegation: {S5_COMPLETION_AUTHORIZATION}. "
                f"Date-quality approval: {S5_DATE_QUALITY_AUTHORIZATION}. "
                "Approved evidence-only S5 schema; S7 identity reconciliation remains required."
            ),
        )
    if snapshot.state not in _ACTIVE_POST_SCHEMA_STATES:
        raise SilverStoreError("S5 schema workflow is in an unsupported state")
    return snapshot


def _ensure_preview(
    root: Path,
    *,
    store: SilverStore,
    snapshot: WorkflowSnapshot,
    contract: TableContract,
    batch: TickerEventSourceBatch,
    request_inputs: tuple[dict[str, object], ...],
    occurrence_inputs: tuple[dict[str, object], ...],
    inputs: tuple[ArtifactRef, ...],
    inventory: SourceInventory,
    git_commit: str,
    profile_sha256: str,
    coverage_receipt_path: str,
    coverage_receipt_sha256: str,
    authorization: TickerEventAuthorization,
) -> tuple[WorkflowSnapshot, tuple[BuildManifest, StoredDocument]]:
    parameters = _parameters(
        contract,
        batch=batch,
        inventory=inventory,
        profile_sha256=profile_sha256,
        coverage_receipt_path=coverage_receipt_path,
        coverage_receipt_sha256=coverage_receipt_sha256,
        authorization=authorization,
    )
    intent = BuildIntent(
        workflow_id=snapshot.workflow_id,
        domain=contract.domain,
        table=contract.table,
        schema_version=contract.schema_version,
        contract_id=contract.contract_id,
        kind=BuildKind.PREVIEW,
        attempt=1,
        retry_of_build_id=None,
        transform_version=TICKER_EVENT_TRANSFORM_VERSION,
        git_commit=git_commit,
        exchange_calendar_version=f"exchange-calendars=={version('exchange-calendars')}",
        inputs=inputs,
        parameters=parameters,
    )
    if snapshot.state is WorkflowState.CODE_READY:
        build = _load_or_materialize(
            root,
            store=store,
            intent=intent,
            contract=contract,
            batch=batch,
            request_inputs=request_inputs,
            occurrence_inputs=occurrence_inputs,
            inventory=inventory,
            authorization=authorization,
            preview=True,
        )
        snapshot = store.record_preview_build(
            build,
            expected_event_sha256=snapshot.event_sha256,
            actor=S5_WORKFLOW_ACTOR,
            recorded_at=_now_utc(),
            note=f"Registered complete-formal-scope S5 preview for {contract.table}.",
        )
    if snapshot.state is WorkflowState.PREVIEW_READY:
        snapshot = store.request_preview_review(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            actor=S5_WORKFLOW_ACTOR,
            created_at=_now_utc(),
            note=(
                f"Completion delegation: {S5_COMPLETION_AUTHORIZATION}. "
                "Submitted the full formal scope as the bounded preview."
            ),
        )
    build, document = _event_build(store, snapshot.workflow_id, contract, BuildKind.PREVIEW)
    if build.intent != intent:
        raise SilverStoreError("existing S5 preview intent differs from authorized run")
    _require_expected_build(build, contract)
    return snapshot, (build, document)


def _ensure_full_approved(
    store: SilverStore,
    snapshot: WorkflowSnapshot,
    preview: BuildManifest,
    contract: TableContract,
) -> WorkflowSnapshot:
    _require_expected_build(preview, contract)
    if snapshot.state is WorkflowState.AWAITING_REVIEW:
        waivers, accepted = _approval_exceptions(preview)
        snapshot = store.approve_full_run(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            approver=S5_APPROVER,
            decided_at=_now_utc(),
            note=(
                f"Completion delegation: {S5_COMPLETION_AUTHORIZATION}. "
                f"Date-quality approval: {S5_DATE_QUALITY_AUTHORIZATION}. "
                "Accepted only exact reviewed Medium warnings and High blank-target occurrences."
            ),
            waived_qa_result_ids=waivers,
            accepted_quarantine_issue_ids=accepted,
        )
    return snapshot


def _ensure_full(
    root: Path,
    *,
    store: SilverStore,
    snapshot: WorkflowSnapshot,
    contract: TableContract,
    preview: BuildManifest,
    batch: TickerEventSourceBatch,
    request_inputs: tuple[dict[str, object], ...],
    occurrence_inputs: tuple[dict[str, object], ...],
    inventory: SourceInventory,
    authorization: TickerEventAuthorization,
) -> tuple[WorkflowSnapshot, tuple[BuildManifest, StoredDocument]]:
    if preview.preview is None:
        raise SilverStoreError("S5 reviewed preview metadata is missing")
    parameters = thaw_json(preview.intent.parameters)
    parameters["approved_preview_build_id"] = preview.build_id
    intent = BuildIntent(
        workflow_id=preview.intent.workflow_id,
        domain=preview.intent.domain,
        table=preview.intent.table,
        schema_version=preview.intent.schema_version,
        contract_id=preview.intent.contract_id,
        kind=BuildKind.FULL,
        attempt=1,
        retry_of_build_id=None,
        transform_version=preview.intent.transform_version,
        git_commit=preview.intent.git_commit,
        exchange_calendar_version=preview.intent.exchange_calendar_version,
        inputs=preview.preview.full_run_inputs,
        parameters=parameters,
    )
    if snapshot.state is WorkflowState.APPROVED_FULL_RUN:
        build = _load_or_materialize(
            root,
            store=store,
            intent=intent,
            contract=contract,
            batch=batch,
            request_inputs=request_inputs,
            occurrence_inputs=occurrence_inputs,
            inventory=inventory,
            authorization=authorization,
            preview=False,
        )
        _require_preview_parity(root, preview, build, contract)
        snapshot = store.record_full_build(
            build,
            expected_event_sha256=snapshot.event_sha256,
            actor=S5_WORKFLOW_ACTOR,
            recorded_at=_now_utc(),
            note=f"Registered review-bound S5 full build for {contract.table}.",
        )
    build, document = _event_build(store, snapshot.workflow_id, contract, BuildKind.FULL)
    if build.intent != intent:
        raise SilverStoreError("existing S5 full intent differs from reviewed preview")
    _require_expected_build(build, contract)
    _require_preview_parity(root, preview, build, contract)
    return snapshot, (build, document)


def _ensure_awaiting_publish(
    store: SilverStore, snapshot: WorkflowSnapshot, full: BuildManifest
) -> WorkflowSnapshot:
    if snapshot.state is WorkflowState.FULL_READY:
        snapshot = store.request_publish(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            actor=S5_WORKFLOW_ACTOR,
            created_at=_now_utc(),
            note=f"Submitted verified S5 full build {full.build_id} for publication.",
        )
    return snapshot


def _ensure_published(
    store: SilverStore,
    snapshot: WorkflowSnapshot,
    full: BuildManifest,
    contract: TableContract,
) -> tuple[WorkflowSnapshot, ReleaseManifest]:
    _require_expected_build(full, contract)
    if snapshot.state is WorkflowState.AWAITING_PUBLISH:
        waivers, accepted = _approval_exceptions(full)
        snapshot, release = store.publish(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            approver=S5_APPROVER,
            decided_at=_now_utc(),
            note=(
                f"Completion delegation: {S5_COMPLETION_AUTHORIZATION}. "
                f"Date-quality approval: {S5_DATE_QUALITY_AUTHORIZATION}. "
                "Published evidence-only S5 rows with exact reviewed exceptions; S7 remains gated."
            ),
            waived_qa_result_ids=waivers,
            accepted_quarantine_issue_ids=accepted,
        )
        return snapshot, release
    if snapshot.state is not WorkflowState.PUBLISHED:
        raise SilverStoreError(f"S5 workflow cannot publish from {snapshot.state.value}")
    release_id = snapshot.evidence.get("release_id")
    if not isinstance(release_id, str):
        raise SilverStoreError("published S5 workflow has no release ID")
    return snapshot, store.load_release(release_id)[0]


def _verified_run(
    root: Path,
    *,
    store: SilverStore,
    contract: TableContract,
    snapshot: WorkflowSnapshot,
    preview: tuple[BuildManifest, StoredDocument],
    full: tuple[BuildManifest, StoredDocument],
    release: ReleaseManifest,
) -> TickerEventTableRun:
    verified = store.verify_workflow_trust_chain(snapshot.workflow_id, verify_artifacts=True)
    _require_recorded_exceptions(store, verified.workflow_id, preview[0], full[0])
    release_document = store.load_release(release.release_id)[1]
    published = PublishedSilverReader(root).inspect(release.release_id)
    return TickerEventTableRun(
        contract=contract,
        workflow=verified,
        preview=preview[0],
        preview_document=preview[1],
        full=full[0],
        full_document=full[1],
        release=release,
        release_document=release_document,
        published=published,
    )


def _load_or_materialize(
    root: Path,
    *,
    store: SilverStore,
    intent: BuildIntent,
    contract: TableContract,
    batch: TickerEventSourceBatch,
    request_inputs: tuple[dict[str, object], ...],
    occurrence_inputs: tuple[dict[str, object], ...],
    inventory: SourceInventory,
    authorization: TickerEventAuthorization,
    preview: bool,
) -> BuildManifest:
    path = (
        root
        / "manifests/silver/builds"
        / intent.table
        / f"build_id={intent.build_id}/manifest.json"
    )
    if path.exists():
        build, _ = store.load_build(intent.table, intent.build_id)
        if build.intent != intent:
            raise SilverStoreError("orphan S5 build intent differs from authorized run")
        store.verify_build(build, contract)
        return build
    started_at = _now_utc()
    transformed = transform_ticker_events(
        request_inputs,
        occurrence_inputs,
        build_id=intent.build_id,
        calendar_name="XNYS",
        excluded_pilot_manifests=authorization.expected_pilot_requests,
    )
    result = transformed.by_table(contract.table)
    if result.table.num_rows != _EXPECTED_ROWS[contract.table]:
        raise SilverStoreError(f"S5 {contract.table} row count differs from authorization")
    _require_expected_result(result, contract)
    fixed_path = f"{SilverStore.build_output_prefix(intent)}/samples/reviewed-s5-profile.json"
    qa_checks = tuple(
        replace(check, bounded_examples_path=fixed_path) for check in result.qa_checks
    )
    outputs = _write_outputs(
        root,
        intent=intent,
        contract=contract,
        batch=batch,
        table=result.table,
        qa_checks=qa_checks,
        quarantine_records=result.quarantine_records,
        fixed_path=fixed_path,
        authorization=authorization,
    )
    metadata = None
    if preview:
        input_sample = next(item for item in outputs if item.path.endswith("input-sample.json"))
        output_sample = next(item for item in outputs if item.path.endswith("output-sample.json"))
        metadata = PreviewMetadata(
            fixed_case_ids=("reviewed_formal_s5_profile",),
            fixed_case_qa_result_ids={
                "reviewed_formal_s5_profile": tuple(item.result_id for item in qa_checks)
            },
            input_sample_path=input_sample.path,
            input_sample_rows=int(input_sample.row_count or 0),
            output_sample_path=output_sample.path,
            output_sample_rows=int(output_sample.row_count or 0),
            examples_truncated=True,
            full_run_inputs=intent.inputs,
            resource_usage={
                "basis": "complete formal ticker-event inventory",
                "source_artifacts": len(inventory.artifacts),
                "input_requests": batch.request_count,
                "input_event_occurrences": batch.row_count,
                "output_rows": result.table.num_rows,
                "serialized_bytes": sum(item.bytes for item in outputs),
            },
            full_run_projection={
                "basis": "preview equals complete reviewed formal scope",
                "projection_multiplier": 1.0,
                "source_inventory_id": inventory.inventory_id,
            },
        )
    return BuildManifest(
        intent=intent,
        outputs=outputs,
        row_funnel=result.row_funnel,
        qa_checks=qa_checks,
        quarantine_issue_rows=len(result.quarantine_records),
        quarantine_unique_source_rows=len(
            {item.source_record_id for item in result.quarantine_records}
        ),
        quarantine_issue_ids_by_severity={
            severity.value: tuple(
                item.issue_id for item in result.quarantine_records if item.severity is severity
            )
            for severity in QASeverity
        },
        started_at=started_at,
        completed_at=_now_utc(),
        preview=metadata,
    )


def _write_outputs(
    root: Path,
    *,
    intent: BuildIntent,
    contract: TableContract,
    batch: TickerEventSourceBatch,
    table: pa.Table,
    qa_checks: tuple[QACheckResult, ...],
    quarantine_records: tuple[Any, ...],
    fixed_path: str,
    authorization: TickerEventAuthorization,
) -> tuple[ArtifactRef, ...]:
    prefix = SilverStore.build_output_prefix(intent)
    if len(contract.partition_by) != 1:
        raise SilverStoreError("S5 contracts require one capture-date partition")
    partition_name = contract.partition_by[0]
    partition_values = sorted(set(table.column(partition_name).to_pylist()))
    if len(partition_values) != 1 or not isinstance(partition_values[0], date):
        raise SilverStoreError("S5 authorized scope must have one date partition")
    partition_value = partition_values[0].isoformat()
    data = _write_parquet(
        root,
        f"{prefix}/data/{partition_name}={partition_value}/part-00000.parquet",
        table,
        ArtifactRole.DATA,
        contract.table,
    )
    qa = _write_parquet(
        root,
        f"{prefix}/qa/qa-check-result.parquet",
        pa.Table.from_pylist(
            [item.to_output_dict(intent.build_id) for item in qa_checks],
            QA_RESULT_ARROW_SCHEMA,
        ),
        ArtifactRole.QA,
        "qa_check_result",
    )
    quarantine = _write_parquet(
        root,
        f"{prefix}/quarantine/quarantine-record.parquet",
        pa.Table.from_pylist(
            [item.to_dict() for item in quarantine_records], QUARANTINE_ARROW_SCHEMA
        ),
        ArtifactRole.QUARANTINE,
        "quarantine_record",
    )
    input_sample = _write_sample(
        root,
        f"{prefix}/samples/input-sample.json",
        _input_sample(batch, authorization.sample_limit),
    )
    output_sample = _write_sample(
        root,
        f"{prefix}/samples/output-sample.json",
        [_json_safe(item) for item in table.slice(0, authorization.sample_limit).to_pylist()],
    )
    fixed = _write_sample(
        root,
        fixed_path,
        [
            {
                "completion_delegation": S5_COMPLETION_AUTHORIZATION,
                "date_quality_decision_id": S5_DATE_QUALITY_DECISION_ID,
                "date_quality_decision_sha256": S5_DATE_QUALITY_DECISION_SHA256,
                "expected_blank_targets": authorization.expected_blank_targets,
                "expected_complete_requests": authorization.expected_complete_requests,
                "expected_event_rows": authorization.expected_event_rows,
                "expected_formal_requests": authorization.expected_formal_requests,
                "expected_not_found_requests": authorization.expected_not_found_requests,
                "expected_raw_events": authorization.expected_raw_events,
                "table": contract.table,
            }
        ],
    )
    return data, qa, quarantine, input_sample, output_sample, fixed


def _write_parquet(
    root: Path, relative: str, table: pa.Table, role: ArtifactRole, table_name: str
) -> ArtifactRef:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", version="2.6", write_statistics=True)
    stored = write_bytes_immutable(root, root / relative, sink.getvalue().to_pybytes())
    return ArtifactRef(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
        row_count=table.num_rows,
        media_type="application/vnd.apache.parquet",
        role=role,
        table=table_name,
        schema_digest=arrow_schema_digest(table.schema),
    )


def _write_sample(root: Path, relative: str, rows: list[dict[str, object]]) -> ArtifactRef:
    content = (
        json.dumps(rows, allow_nan=False, ensure_ascii=False, indent=2, sort_keys=True).encode()
        + b"\n"
    )
    stored = write_bytes_immutable(root, root / relative, content)
    return ArtifactRef(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
        row_count=len(rows),
        media_type="application/json",
        role=ArtifactRole.SAMPLE,
    )


def _input_sample(batch: TickerEventSourceBatch, limit: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for status in batch.request_statuses[: min(limit // 2, len(batch.request_statuses))]:
        rows.append(
            {
                "kind": "request_status",
                "outcome": status.outcome,
                "requested_identifier": status.requested_identifier,
                "source_manifest_sha256": status.source_manifest_sha256,
                "source_request_id": status.source_request_id,
            }
        )
    for record in batch.iter_records():
        rows.append(
            {
                "kind": "event_occurrence",
                "raw": _json_safe(dict(record.row)),
                "requested_identifier": record.requested_identifier,
                "source_artifact_sha256": record.source_artifact_sha256,
                "source_request_id": record.source_request_id,
                "source_row_ordinal": record.source_row_ordinal,
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _parameters(
    contract: TableContract,
    *,
    batch: TickerEventSourceBatch,
    inventory: SourceInventory,
    profile_sha256: str,
    coverage_receipt_path: str,
    coverage_receipt_sha256: str,
    authorization: TickerEventAuthorization,
) -> dict[str, object]:
    return {
        "backtest_identity_eligible": False,
        "completion_delegation": S5_COMPLETION_AUTHORIZATION,
        "coverage_receipt_path": coverage_receipt_path,
        "coverage_receipt_sha256": coverage_receipt_sha256,
        "date_quality_decision_id": S5_DATE_QUALITY_DECISION_ID,
        "date_quality_decision_sha256": S5_DATE_QUALITY_DECISION_SHA256,
        "expected_blank_targets": authorization.expected_blank_targets,
        "expected_complete_requests": authorization.expected_complete_requests,
        "expected_event_rows": authorization.expected_event_rows,
        "expected_formal_requests": authorization.expected_formal_requests,
        "expected_not_found_requests": authorization.expected_not_found_requests,
        "expected_raw_events": authorization.expected_raw_events,
        "full_formal_scope_preview": True,
        "profile_sha256": profile_sha256,
        "pyarrow_version": pa.__version__,
        "python_version": platform.python_version(),
        "request_end_label_not_provider_filter": batch.request_end.isoformat(),
        "request_start_label_not_provider_filter": batch.request_start.isoformat(),
        "sample_limit": authorization.sample_limit,
        "source_inventory_id": inventory.inventory_id,
        "target_table": contract.table,
    }


def _require_expected_result(result: Any, contract: TableContract) -> None:
    if result.table.schema != contract.arrow_schema:
        raise SilverStoreError(f"S5 {contract.table} result violates its contract schema")
    if len(result.qa_checks) != len(contract.qa_rules):
        raise SilverStoreError(f"S5 {contract.table} result QA rule set changed")
    _require_expected_qa(result.qa_checks, contract)
    _require_expected_quarantine(result.quarantine_records, contract, build_id=None)


def _require_expected_build(build: BuildManifest, contract: TableContract) -> None:
    if build.intent.contract_id != contract.contract_id:
        raise SilverStoreError(f"S5 {contract.table} build contract changed")
    _require_expected_qa(build.qa_checks, contract)
    if build.row_funnel.output_rows_by_table != {contract.table: _EXPECTED_ROWS[contract.table]}:
        raise SilverStoreError(f"S5 {contract.table} row funnel output changed")
    expected_funnel = (
        (
            EXPECTED_FORMAL_REQUESTS,
            EXPECTED_FORMAL_REQUESTS,
            0,
            0,
            0,
            0,
        )
        if contract == TICKER_EVENT_REQUEST_STATUS_CONTRACT
        else (
            EXPECTED_RAW_EVENTS,
            EXPECTED_EVENT_ROWS,
            0,
            EXPECTED_BLANK_TARGETS,
            0,
            0,
        )
    )
    actual_funnel = (
        build.row_funnel.input_rows,
        build.row_funnel.accepted_source_rows,
        build.row_funnel.exact_duplicate_excess,
        build.row_funnel.quarantined_source_rows,
        build.row_funnel.unmapped_source_rows,
        build.row_funnel.version_preserved_rows,
    )
    if actual_funnel != expected_funnel:
        raise SilverStoreError(f"S5 {contract.table} row funnel accounting changed")
    expected_quarantine = _EXPECTED_QUARANTINE_ROWS[contract.table]
    if (
        build.quarantine_issue_rows != expected_quarantine
        or build.quarantine_unique_source_rows != expected_quarantine
        or len(build.quarantine_issue_ids_by_severity[QASeverity.HIGH.value]) != expected_quarantine
        or build.quarantine_issue_ids_by_severity[QASeverity.CRITICAL.value]
    ):
        raise SilverStoreError(f"S5 {contract.table} quarantine cardinality changed")
    if any(
        build.quarantine_issue_ids_by_severity[item.value]
        for item in (QASeverity.MEDIUM, QASeverity.LOW)
    ):
        raise SilverStoreError(f"S5 {contract.table} has unexpected non-High quarantine")
    data = [item for item in build.outputs if item.role is ArtifactRole.DATA]
    if (
        len(data) != 1
        or data[0].table != contract.table
        or data[0].row_count != _EXPECTED_ROWS[contract.table]
    ):
        raise SilverStoreError(f"S5 {contract.table} DATA output changed")


def _require_expected_qa(checks: tuple[QACheckResult, ...], contract: TableContract) -> None:
    if (
        len(checks) != len(contract.qa_rules)
        or len({item.check_id for item in checks}) != len(checks)
        or {item.check_id for item in checks} != set(contract.required_qa_checks)
    ):
        raise SilverStoreError(f"S5 {contract.table} QA check IDs changed")
    expected_partition = f"{contract.partition_by[0]}=2026-07-11"
    if any(
        item.table != contract.table or item.partition_key != expected_partition for item in checks
    ):
        raise SilverStoreError(f"S5 {contract.table} QA partition identity changed")
    expected_nonzero = _EXPECTED_WARNING_NUMERATORS[contract.table]
    observed_nonzero: dict[str, int] = {}
    for check in checks:
        if check.blocks_publish:
            raise SilverStoreError(f"S5 {contract.table} has blocking QA: {check.check_id}")
        if check.numerator:
            observed_nonzero[check.check_id] = check.numerator
            if check.status is not QAStatus.WARNING or check.severity not in {
                QASeverity.MEDIUM,
                QASeverity.LOW,
            }:
                raise SilverStoreError(
                    f"S5 {contract.table} has unexpected nonzero QA: {check.check_id}"
                )
        elif check.status is not QAStatus.PASSED:
            raise SilverStoreError(
                f"S5 {contract.table} zero-numerator QA is not passed: {check.check_id}"
            )
    if observed_nonzero != expected_nonzero:
        raise SilverStoreError(
            f"S5 {contract.table} reviewed warning profile changed: {observed_nonzero}"
        )


def _require_expected_quarantine(
    records: tuple[Any, ...], contract: TableContract, *, build_id: str | None
) -> None:
    expected = _EXPECTED_QUARANTINE_ROWS[contract.table]
    if len(records) != expected:
        raise SilverStoreError(f"S5 {contract.table} result quarantine count changed")
    for item in records:
        if (
            item.table_name != contract.table
            or item.issue_code != "blank_target_ticker"
            or item.severity is not QASeverity.HIGH
            or (build_id is not None and item.detected_build_id != build_id)
        ):
            raise SilverStoreError(f"S5 {contract.table} quarantine semantics changed")


def _approval_exceptions(build: BuildManifest) -> tuple[tuple[str, ...], tuple[str, ...]]:
    waivers = tuple(
        sorted(
            item.result_id
            for item in build.qa_checks
            if item.status is QAStatus.WARNING
            or (
                item.status is QAStatus.FAILED
                and item.severity in {QASeverity.MEDIUM, QASeverity.LOW}
            )
        )
    )
    accepted = tuple(sorted(build.quarantine_issue_ids_by_severity[QASeverity.HIGH.value]))
    return waivers, accepted


def _require_preview_parity(
    root: Path, preview: BuildManifest, full: BuildManifest, contract: TableContract
) -> None:
    preview_data = next(item for item in preview.outputs if item.role is ArtifactRole.DATA)
    full_data = next(item for item in full.outputs if item.role is ArtifactRole.DATA)
    if preview_data.sha256 != full_data.sha256:
        raise SilverStoreError(f"S5 {contract.table} full DATA differs from preview")
    if preview.row_funnel != full.row_funnel:
        raise SilverStoreError(f"S5 {contract.table} full row funnel differs from preview")
    if {item.check_id: _qa_core(item) for item in preview.qa_checks} != {
        item.check_id: _qa_core(item) for item in full.qa_checks
    }:
        raise SilverStoreError(f"S5 {contract.table} full QA metrics differ from preview")
    if (
        preview.quarantine_issue_rows != full.quarantine_issue_rows
        or preview.quarantine_unique_source_rows != full.quarantine_unique_source_rows
        or _quarantine_semantic_rows(root, preview) != _quarantine_semantic_rows(root, full)
    ):
        raise SilverStoreError(f"S5 {contract.table} full quarantine evidence differs from preview")
    if (
        not pq.ParquetFile(root / preview_data.path)
        .read()
        .equals(pq.ParquetFile(root / full_data.path).read())
    ):
        raise SilverStoreError(f"S5 {contract.table} recomputed rows differ from preview")


def _qa_core(check: QACheckResult) -> tuple[object, ...]:
    return (
        check.table,
        check.partition_key,
        check.check_id,
        check.severity,
        check.status,
        check.numerator,
        check.denominator,
        check.rate,
        check.threshold,
    )


def _quarantine_semantic_rows(root: Path, build: BuildManifest) -> list[tuple[object, ...]]:
    artifact = next(item for item in build.outputs if item.role is ArtifactRole.QUARANTINE)
    table = pq.ParquetFile(root / artifact.path).read()
    rows = []
    for item in table.to_pylist():
        rows.append(
            tuple(
                item[name]
                for name in (
                    "source_record_id",
                    "table_name",
                    "issue_code",
                    "severity",
                    "source_pointer",
                    "field_name",
                    "observed_value",
                    "expected_rule",
                    "review_status",
                )
            )
        )
    return sorted(rows)


def _require_pair_integrity(status: pa.Table, events: pa.Table) -> None:
    if status.num_rows != EXPECTED_FORMAL_REQUESTS or events.num_rows != EXPECTED_EVENT_ROWS:
        raise SilverStoreError("S5 parent/child cardinalities differ from authorization")
    status_rows = status.to_pylist()
    event_rows = events.to_pylist()
    status_ids = [str(item["source_request_id"]) for item in status_rows]
    event_ids = [str(item["source_record_id"]) for item in event_rows]
    if len(status_ids) != len(set(status_ids)) or len(event_ids) != len(set(event_ids)):
        raise SilverStoreError("S5 parent or child primary key is duplicated")
    parent = {str(item["source_request_id"]): item for item in status_rows}
    complete = {
        key for key, item in parent.items() if item["request_outcome"] == "complete_timeline"
    }
    not_found = {key for key, item in parent.items() if item["request_outcome"] == "not_found_404"}
    if len(complete) != EXPECTED_COMPLETE_REQUESTS or len(not_found) != EXPECTED_NOT_FOUND_REQUESTS:
        raise SilverStoreError("S5 request outcome counts changed")
    accepted_by_request = Counter(str(item["source_request_id"]) for item in event_rows)
    if set(accepted_by_request).difference(complete):
        raise SilverStoreError("S5 event child references a non-complete parent")
    for request_id, item in parent.items():
        raw = int(item["raw_event_count"])
        accepted = int(item["accepted_event_count"])
        quarantined = int(item["quarantined_event_count"])
        if raw != accepted + quarantined or accepted_by_request[request_id] != accepted:
            raise SilverStoreError("S5 per-request parent/child event counts do not reconcile")
        if request_id in not_found and (raw or accepted or quarantined):
            raise SilverStoreError("S5 404 parent unexpectedly contains events")
        if item["backtest_identity_eligible"] is not False:
            raise SilverStoreError("S5 parent became backtest identity eligible")
    if sum(int(item["raw_event_count"]) for item in status_rows) != EXPECTED_RAW_EVENTS:
        raise SilverStoreError("S5 parent raw event count changed")
    if sum(int(item["quarantined_event_count"]) for item in status_rows) != EXPECTED_BLANK_TARGETS:
        raise SilverStoreError("S5 parent blank-target count changed")
    if any(item["backtest_identity_eligible"] is not False for item in event_rows):
        raise SilverStoreError("S5 event became backtest identity eligible")


def _event_build(
    store: SilverStore,
    workflow_id: str,
    contract: TableContract,
    kind: BuildKind,
) -> tuple[BuildManifest, StoredDocument]:
    wanted = WorkflowState.PREVIEW_READY if kind is BuildKind.PREVIEW else WorkflowState.FULL_READY
    for record in reversed(store.workflow_events(workflow_id)):
        if record.event.to_state is wanted:
            build, document = store.load_build(
                contract.table, str(record.event.evidence["build_id"])
            )
            if build.intent.kind is not kind or build.intent.workflow_id != workflow_id:
                raise SilverStoreError("S5 workflow event references wrong build")
            store.verify_build(build, contract)
            return build, document
    raise SilverStoreError(f"S5 workflow has no registered {kind.value} build")


def _require_recorded_exceptions(
    store: SilverStore,
    workflow_id: str,
    preview: BuildManifest,
    full: BuildManifest,
) -> None:
    expected = {
        WorkflowState.APPROVED_FULL_RUN: _approval_exceptions(preview),
        WorkflowState.PUBLISHED: _approval_exceptions(full),
    }
    seen: set[WorkflowState] = set()
    for record in store.workflow_events(workflow_id):
        if record.event.to_state not in expected:
            continue
        approval_id = record.event.evidence.get("approval_id")
        if not isinstance(approval_id, str):
            raise SilverStoreError("S5 approval event has no approval ID")
        approval, _ = store.load_approval(approval_id)
        waivers, accepted = expected[record.event.to_state]
        if approval.waived_qa_result_ids != tuple(
            sorted(waivers)
        ) or approval.accepted_quarantine_issue_ids != tuple(sorted(accepted)):
            raise SilverStoreError("S5 approval exceptions differ from reviewed build")
        seen.add(record.event.to_state)
    if seen != set(expected):
        raise SilverStoreError("S5 workflow is missing a review or publish approval")


def _read_published_table(published: PublishedRelease) -> pa.Table:
    tables = [pq.ParquetFile(path).read() for path in published.data_paths]
    if not tables:
        raise SilverStoreError("published S5 release has no DATA table")
    return tables[0] if len(tables) == 1 else pa.concat_tables(tables)


def _read_build_data_table(root: Path, build: BuildManifest) -> pa.Table:
    data = [item for item in build.outputs if item.role is ArtifactRole.DATA]
    if len(data) != 1:
        raise SilverStoreError("S5 one-table build must expose exactly one DATA artifact")
    return pq.ParquetFile(safe_relative_path(root, data[0].path)).read()


def _verify_git_checkout(repo_root: Path, git_commit: str) -> None:
    root = repo_root.expanduser().resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise SilverStoreError("S5 lifecycle code is not executing from --repo-root") from exc
    if module_relative != "backend/ame_stocks_api/silver/ticker_event_lifecycle.py":
        raise SilverStoreError("S5 lifecycle module path is not canonical")
    if Path(_git(root, "rev-parse", "--show-toplevel")).resolve() != root:
        raise SilverStoreError("S5 --repo-root is not the exact Git top level")
    if _git(root, "rev-parse", "HEAD") != git_commit:
        raise SilverStoreError("S5 Git HEAD differs from --git-commit")
    if _git(root, "status", "--porcelain=v1", "--untracked-files=all"):
        raise SilverStoreError("S5 Git checkout is not clean")
    for relative in _LOGIC_CLOSURE:
        _git(root, "ls-files", "--error-unmatch", "--", relative)


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown Git error"
        raise SilverStoreError(f"cannot verify S5 Git checkout: {detail}")
    return completed.stdout.strip()


def _json_safe(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


__all__ = [
    "CURRENT_TICKER_EVENT_AUTHORIZATION",
    "S5_COMPLETION_AUTHORIZATION",
    "S5_DATE_QUALITY_AUTHORIZATION",
    "S5_DATE_QUALITY_DECISION_ID",
    "S5_DATE_QUALITY_DECISION_SHA256",
    "TickerEventAuthorization",
    "TickerEventLifecycleRun",
    "TickerEventTableRun",
    "complete_ticker_event_lifecycle",
]
