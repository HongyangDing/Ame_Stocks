"""Fail-closed S6 lifecycle for the formal Ticker Overview identity evidence table.

The reviewed S6 scope is small enough that preview and full use the same 30,739 lifecycle
requests.  The lifecycle is deliberately self-contained: it re-profiles the immutable source,
re-registers both lifecycle-control and Bronze inventories, recomputes the transform, checks
preview/full parity, accepts only the reviewed 169 unresolved identities, and publishes only
the 30,570 resolved evidence rows.  It never advances S7 identity reconciliation.
"""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import stable_digest, write_bytes_immutable
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
from ame_stocks_api.silver.ticker_overview_contract import TICKER_OVERVIEW_SAFE_CONTRACT
from ame_stocks_api.silver.ticker_overview_source import (
    TickerOverviewSourceBatch,
    build_ticker_overview_lifecycle_source_inventory,
    build_ticker_overview_source_inventory,
    read_ticker_overview_source_inventory,
    ticker_overview_coverage_receipt_path,
    ticker_overview_transform_inputs,
)
from ame_stocks_api.silver.ticker_overview_source_profile import (
    accepted_coverage_receipt,
    coverage_receipt_bytes,
    profile_ticker_overview_source,
)
from ame_stocks_api.silver.ticker_overviews import (
    TICKER_OVERVIEW_SAFE_TRANSFORM_VERSION,
    transform_ticker_overview_safe,
)

S6_COMPLETION_AUTHORIZATION = "那下一步是不是可以直接走完S6，等S7的时候再回到逐步审批的模式"  # noqa: RUF001
S6_EXECUTION_AUTHORIZATION = "开始S6"
S6_SCHEMA_CREATED_AT = "2026-07-14T12:00:00+00:00"
S6_SCHEMA_DECIDED_AT = "2026-07-14T12:00:01+00:00"
S6_WORKFLOW_ACTOR = "s6-ticker-overview-lifecycle"
S6_APPROVER = "user-delegated-s6-completion-authority"
S6_SAMPLE_LIMIT = 100

EXPECTED_SOURCE_ROWS = 30_739
EXPECTED_OUTPUT_ROWS = 30_570
EXPECTED_UNRESOLVED_ROWS = 169
EXPECTED_CAPTURE_DATE = date(2026, 7, 11)

_EXPECTED_WARNING_NUMERATORS = {
    "list_date_missing_rows": 7_322,
    "retrospective_query_without_archived_vintage_rows": EXPECTED_OUTPUT_ROWS,
    "sic_code_missing_rows": 14_057,
    "unresolved_identity_rows": EXPECTED_UNRESOLVED_ROWS,
}
_EXPECTED_WARNING_DENOMINATORS = {
    "retrospective_query_without_archived_vintage_rows": EXPECTED_OUTPUT_ROWS,
    "list_date_missing_rows": EXPECTED_SOURCE_ROWS,
    "sic_code_missing_rows": EXPECTED_SOURCE_ROWS,
    "unresolved_identity_rows": EXPECTED_SOURCE_ROWS,
}
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
_LOGIC_CLOSURE = (
    "backend/ame_stocks_api/cli/silver_ticker_overview_lifecycle.py",
    "backend/ame_stocks_api/silver/contracts.py",
    "backend/ame_stocks_api/silver/reader.py",
    "backend/ame_stocks_api/silver/schema_resources/ticker_overview_safe.schema-v1.json",
    "backend/ame_stocks_api/silver/store.py",
    "backend/ame_stocks_api/silver/ticker_overview_contract.py",
    "backend/ame_stocks_api/silver/ticker_overview_lifecycle.py",
    "backend/ame_stocks_api/silver/ticker_overview_source.py",
    "backend/ame_stocks_api/silver/ticker_overview_source_profile.py",
    "backend/ame_stocks_api/silver/ticker_overviews.py",
    "docs/silver/contracts/identity/ticker_overview_safe.schema-v1.candidate.json",
    "pyproject.toml",
)


@dataclass(frozen=True, slots=True)
class TickerOverviewAuthorization:
    expected_source_rows: int = EXPECTED_SOURCE_ROWS
    expected_output_rows: int = EXPECTED_OUTPUT_ROWS
    expected_unresolved_rows: int = EXPECTED_UNRESOLVED_ROWS
    expected_capture_date: date = EXPECTED_CAPTURE_DATE
    sample_limit: int = S6_SAMPLE_LIMIT

    def __post_init__(self) -> None:
        if (
            self.expected_source_rows != EXPECTED_SOURCE_ROWS
            or self.expected_output_rows != EXPECTED_OUTPUT_ROWS
            or self.expected_unresolved_rows != EXPECTED_UNRESOLVED_ROWS
            or self.expected_capture_date != EXPECTED_CAPTURE_DATE
            or not 1 <= self.sample_limit <= 100
        ):
            raise ValueError(
                "S6 authorized source cardinality, capture date, or sample bound changed"
            )
        if self.expected_output_rows + self.expected_unresolved_rows != self.expected_source_rows:
            raise ValueError("S6 authorized row funnel does not reconcile")


CURRENT_TICKER_OVERVIEW_AUTHORIZATION = TickerOverviewAuthorization()


@dataclass(frozen=True, slots=True)
class TickerOverviewLifecycleRun:
    contract: TableContract
    workflow: WorkflowSnapshot
    preview: BuildManifest
    preview_document: StoredDocument
    full: BuildManifest
    full_document: StoredDocument
    release: ReleaseManifest
    release_document: StoredDocument
    published: PublishedRelease
    lifecycle_inventory: SourceInventory
    lifecycle_inventory_document: StoredDocument
    overview_inventory: SourceInventory
    overview_inventory_document: StoredDocument
    coverage_receipt_path: str
    coverage_receipt_sha256: str
    profile_sha256: str


@dataclass(frozen=True, slots=True)
class _PreparedSource:
    batch: TickerOverviewSourceBatch
    transform_inputs: tuple[dict[str, object], ...]
    lifecycle_inventory: SourceInventory
    lifecycle_inventory_document: StoredDocument
    overview_inventory: SourceInventory
    overview_inventory_document: StoredDocument
    input_artifacts: tuple[ArtifactRef, ...]
    coverage_receipt_path: str
    coverage_receipt_sha256: str
    profile_sha256: str


def complete_ticker_overview_lifecycle(
    data_root: Path,
    *,
    repo_root: Path,
    git_commit: str,
) -> TickerOverviewLifecycleRun:
    """Advance the exact reviewed S6 scope through publication, then stop before S7."""

    _verify_git_checkout(repo_root, git_commit)
    root = data_root.expanduser().resolve()
    store = SilverStore(root)
    authorization = CURRENT_TICKER_OVERVIEW_AUTHORIZATION
    prepared = _prepare_authorized_source(
        root,
        store=store,
        git_commit=git_commit,
        authorization=authorization,
    )
    contract = TICKER_OVERVIEW_SAFE_CONTRACT
    snapshot = _ensure_schema_approved(store, contract)
    snapshot, preview = _ensure_preview(
        root,
        store=store,
        snapshot=snapshot,
        contract=contract,
        prepared=prepared,
        git_commit=git_commit,
        authorization=authorization,
    )
    snapshot = _ensure_full_approved(store, snapshot, preview[0])
    snapshot, full = _ensure_full(
        root,
        store=store,
        snapshot=snapshot,
        contract=contract,
        preview=preview[0],
        prepared=prepared,
        authorization=authorization,
    )
    snapshot = _ensure_awaiting_publish(store, snapshot, full[0])
    _verify_git_checkout(repo_root, git_commit)
    snapshot, release = _ensure_published(store, snapshot, full[0], contract)
    verified = store.verify_workflow_trust_chain(snapshot.workflow_id, verify_artifacts=True)
    _require_recorded_exceptions(store, verified.workflow_id, preview[0], full[0])
    release_document = store.load_release(release.release_id)[1]
    published = PublishedSilverReader(root).inspect(release.release_id)
    published_table = _read_published_table(published)
    if published_table.num_rows != authorization.expected_output_rows:
        raise SilverStoreError("published S6 release row count differs from authorization")
    if published_table.schema != contract.arrow_schema:
        raise SilverStoreError("published S6 release schema differs from its contract")
    return TickerOverviewLifecycleRun(
        contract=contract,
        workflow=verified,
        preview=preview[0],
        preview_document=preview[1],
        full=full[0],
        full_document=full[1],
        release=release,
        release_document=release_document,
        published=published,
        lifecycle_inventory=prepared.lifecycle_inventory,
        lifecycle_inventory_document=prepared.lifecycle_inventory_document,
        overview_inventory=prepared.overview_inventory,
        overview_inventory_document=prepared.overview_inventory_document,
        coverage_receipt_path=prepared.coverage_receipt_path,
        coverage_receipt_sha256=prepared.coverage_receipt_sha256,
        profile_sha256=prepared.profile_sha256,
    )


def _prepare_authorized_source(
    root: Path,
    *,
    store: SilverStore,
    git_commit: str,
    authorization: TickerOverviewAuthorization,
) -> _PreparedSource:
    profile = profile_ticker_overview_source(root)
    _require_authorized_profile(profile)
    receipt = accepted_coverage_receipt(profile)
    receipt_content = coverage_receipt_bytes(receipt)
    receipt_path = ticker_overview_coverage_receipt_path(receipt)
    stored = write_bytes_immutable(root, root / receipt_path, receipt_content)
    receipt_sha = str(stored["sha256"])
    lifecycle_inventory = build_ticker_overview_lifecycle_source_inventory(
        root,
        coverage_receipt_path=receipt_path,
        coverage_receipt_sha256=receipt_sha,
        git_commit=git_commit,
    )
    overview_inventory = build_ticker_overview_source_inventory(
        root,
        coverage_receipt_path=receipt_path,
        coverage_receipt_sha256=receipt_sha,
        git_commit=git_commit,
    )
    _require_source_inventory_pair(
        lifecycle_inventory,
        overview_inventory,
        git_commit=git_commit,
        authorization=authorization,
    )
    batch = read_ticker_overview_source_inventory(
        root,
        overview_inventory,
        lifecycle_inventory=lifecycle_inventory,
    )
    transform_inputs = ticker_overview_transform_inputs(batch)
    if len(transform_inputs) != authorization.expected_source_rows:
        raise SilverStoreError("S6 verified transform-input count differs from authorization")
    lifecycle_document = store.register_source_inventory(lifecycle_inventory)
    overview_document = store.register_source_inventory(overview_inventory)
    lifecycle_artifacts = _source_artifact_refs(lifecycle_inventory, lifecycle_document)
    overview_artifacts = _source_artifact_refs(overview_inventory, overview_document)
    store.verify_source_artifacts(lifecycle_artifacts, TICKER_OVERVIEW_SAFE_CONTRACT)
    store.verify_source_artifacts(overview_artifacts, TICKER_OVERVIEW_SAFE_CONTRACT)
    profile_sha = profile.get("profile_sha256")
    if not isinstance(profile_sha, str) or len(profile_sha) != 64:
        raise SilverStoreError("S6 source profile has no valid profile SHA-256")
    return _PreparedSource(
        batch=batch,
        transform_inputs=transform_inputs,
        lifecycle_inventory=lifecycle_inventory,
        lifecycle_inventory_document=lifecycle_document,
        overview_inventory=overview_inventory,
        overview_inventory_document=overview_document,
        # Every Overview response has exactly one result row, so the 30,739 Bronze pages are
        # the direct transform grain and keep BuildManifest source_rows at 30,739.  The
        # lifecycle control inventory remains receipt-bound auxiliary evidence; including both
        # inventories as direct inputs would incorrectly double the row funnel.
        input_artifacts=overview_artifacts,
        coverage_receipt_path=receipt_path,
        coverage_receipt_sha256=receipt_sha,
        profile_sha256=profile_sha,
    )


def _source_artifact_refs(
    inventory: SourceInventory, document: StoredDocument
) -> tuple[ArtifactRef, ...]:
    return tuple(
        ArtifactRef(
            path=item.path,
            sha256=item.sha256,
            bytes=item.bytes,
            row_count=item.row_count,
            media_type=item.media_type,
            role=ArtifactRole.SOURCE,
            source_dataset=inventory.source_dataset,
            source_layer=inventory.source_layer,
            lineage_manifest_path=document.path,
            lineage_manifest_sha256=document.sha256,
        )
        for item in inventory.artifacts
    )


def _require_source_inventory_pair(
    lifecycle: SourceInventory,
    overview: SourceInventory,
    *,
    git_commit: str,
    authorization: TickerOverviewAuthorization,
) -> None:
    lifecycle_receipts = set(lifecycle.upstream_manifests)
    overview_upstreams = set(overview.upstream_manifests)
    if (
        lifecycle.source_dataset != "ticker_overview"
        or overview.source_dataset != "ticker_overview"
        or lifecycle.source_layer is not SourceLayer.CONTROL_MANIFEST
        or overview.source_layer is not SourceLayer.BRONZE
        or lifecycle.git_commit != git_commit
        or overview.git_commit != git_commit
        or len(lifecycle.upstream_manifests) != 1
        or not lifecycle_receipts.issubset(overview_upstreams)
        or len(overview_upstreams) != authorization.expected_source_rows + 1
    ):
        raise SilverStoreError("S6 lifecycle/Bronze inventories do not share exact source lineage")
    if (
        len(lifecycle.artifacts) != 1
        or lifecycle.artifacts[0].row_count != authorization.expected_source_rows
        or lifecycle.artifacts[0].media_type != "text/plain"
    ):
        raise SilverStoreError("S6 lifecycle-control inventory grain differs from authorization")
    if (
        len(overview.artifacts) != authorization.expected_source_rows
        or sum(item.row_count for item in overview.artifacts)
        != authorization.expected_source_rows
        or any(item.row_count != 1 for item in overview.artifacts)
        or any(item.media_type != "application/gzip+json" for item in overview.artifacts)
    ):
        raise SilverStoreError("S6 Bronze inventory grain differs from authorization")


def _require_authorized_profile(profile: dict[str, object]) -> None:
    if profile.get("status") != "passed_with_warnings":
        raise SilverStoreError("S6 source profile is not in the accepted reviewed state")
    gates = profile.get("hard_gate_counts")
    if not isinstance(gates, dict) or any(value != 0 for value in gates.values()):
        raise SilverStoreError("S6 source profile has a nonzero hard gate")
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
        raise SilverStoreError("S6 source profile digest does not match its canonical preimage")


def _ensure_schema_approved(store: SilverStore, contract: TableContract) -> WorkflowSnapshot:
    workflow_id = stable_digest(
        {
            "actor": S6_WORKFLOW_ACTOR,
            "contract_id": contract.contract_id,
            "created_at": S6_SCHEMA_CREATED_AT,
            "workflow_event_version": WORKFLOW_EVENT_VERSION,
        }
    )
    event_dir = store.root / "manifests/silver/workflows" / workflow_id / "events"
    if event_dir.exists():
        snapshot = store.verify_workflow_trust_chain(workflow_id, verify_artifacts=True)
        existing, _ = store.load_workflow_contract(workflow_id)
        if existing != contract:
            raise SilverStoreError("deterministic S6 workflow contract changed")
    else:
        snapshot = store.create_workflow(
            contract,
            actor=S6_WORKFLOW_ACTOR,
            created_at=S6_SCHEMA_CREATED_AT,
            note="Registered reviewed S6 ticker_overview_safe contract.",
        )
    if snapshot.state is WorkflowState.PLANNED:
        snapshot = store.submit_schema_review(
            workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            actor=S6_WORKFLOW_ACTOR,
            created_at=S6_SCHEMA_CREATED_AT,
            note="Submitted reviewed S6 contract for delegated completion.",
        )
    if snapshot.state is WorkflowState.SCHEMA_REVIEW:
        snapshot = store.approve_schema(
            workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            approver=S6_APPROVER,
            decided_at=S6_SCHEMA_DECIDED_AT,
            note=(
                f"Completion delegation: {S6_COMPLETION_AUTHORIZATION}. "
                f"Execution authorization: {S6_EXECUTION_AUTHORIZATION}. "
                "Approved evidence-only S6 schema; S7 remains separately gated."
            ),
        )
    if snapshot.state not in _ACTIVE_POST_SCHEMA_STATES:
        raise SilverStoreError("S6 schema workflow is in an unsupported state")
    return snapshot


def _ensure_preview(
    root: Path,
    *,
    store: SilverStore,
    snapshot: WorkflowSnapshot,
    contract: TableContract,
    prepared: _PreparedSource,
    git_commit: str,
    authorization: TickerOverviewAuthorization,
) -> tuple[WorkflowSnapshot, tuple[BuildManifest, StoredDocument]]:
    intent = BuildIntent(
        workflow_id=snapshot.workflow_id,
        domain=contract.domain,
        table=contract.table,
        schema_version=contract.schema_version,
        contract_id=contract.contract_id,
        kind=BuildKind.PREVIEW,
        attempt=1,
        retry_of_build_id=None,
        transform_version=TICKER_OVERVIEW_SAFE_TRANSFORM_VERSION,
        git_commit=git_commit,
        exchange_calendar_version=f"exchange-calendars=={version('exchange-calendars')}",
        inputs=prepared.input_artifacts,
        parameters=_parameters(prepared, authorization),
    )
    if snapshot.state is WorkflowState.CODE_READY:
        build = _load_or_materialize(
            root,
            store=store,
            intent=intent,
            contract=contract,
            prepared=prepared,
            authorization=authorization,
            preview=True,
        )
        snapshot = store.record_preview_build(
            build,
            expected_event_sha256=snapshot.event_sha256,
            actor=S6_WORKFLOW_ACTOR,
            recorded_at=_now_utc(),
            note="Registered complete-scope S6 preview.",
        )
    if snapshot.state is WorkflowState.PREVIEW_READY:
        snapshot = store.request_preview_review(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            actor=S6_WORKFLOW_ACTOR,
            created_at=_now_utc(),
            note=f"Completion delegation: {S6_COMPLETION_AUTHORIZATION}.",
        )
    build, document = _event_build(store, snapshot.workflow_id, contract, BuildKind.PREVIEW)
    if build.intent != intent:
        raise SilverStoreError("existing S6 preview intent differs from authorized run")
    _require_expected_build(build, contract, authorization)
    return snapshot, (build, document)


def _ensure_full_approved(
    store: SilverStore, snapshot: WorkflowSnapshot, preview: BuildManifest
) -> WorkflowSnapshot:
    _require_expected_build(
        preview,
        TICKER_OVERVIEW_SAFE_CONTRACT,
        CURRENT_TICKER_OVERVIEW_AUTHORIZATION,
    )
    if snapshot.state is WorkflowState.AWAITING_REVIEW:
        waivers, accepted = _approval_exceptions(preview)
        snapshot = store.approve_full_run(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            approver=S6_APPROVER,
            decided_at=_now_utc(),
            note=(
                f"Completion delegation: {S6_COMPLETION_AUTHORIZATION}. "
                "Accepted only the exact reviewed warning profile and 169 unresolved identities."
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
    prepared: _PreparedSource,
    authorization: TickerOverviewAuthorization,
) -> tuple[WorkflowSnapshot, tuple[BuildManifest, StoredDocument]]:
    if preview.preview is None:
        raise SilverStoreError("S6 reviewed preview metadata is missing")
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
            prepared=prepared,
            authorization=authorization,
            preview=False,
        )
        _require_preview_parity(root, preview, build)
        snapshot = store.record_full_build(
            build,
            expected_event_sha256=snapshot.event_sha256,
            actor=S6_WORKFLOW_ACTOR,
            recorded_at=_now_utc(),
            note="Registered review-bound S6 full build.",
        )
    build, document = _event_build(store, snapshot.workflow_id, contract, BuildKind.FULL)
    if build.intent != intent:
        raise SilverStoreError("existing S6 full intent differs from reviewed preview")
    _require_expected_build(build, contract, authorization)
    _require_preview_parity(root, preview, build)
    return snapshot, (build, document)


def _ensure_awaiting_publish(
    store: SilverStore, snapshot: WorkflowSnapshot, full: BuildManifest
) -> WorkflowSnapshot:
    if snapshot.state is WorkflowState.FULL_READY:
        snapshot = store.request_publish(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            actor=S6_WORKFLOW_ACTOR,
            created_at=_now_utc(),
            note=f"Submitted verified S6 full build {full.build_id} for publication.",
        )
    return snapshot


def _ensure_published(
    store: SilverStore,
    snapshot: WorkflowSnapshot,
    full: BuildManifest,
    contract: TableContract,
) -> tuple[WorkflowSnapshot, ReleaseManifest]:
    _require_expected_build(full, contract, CURRENT_TICKER_OVERVIEW_AUTHORIZATION)
    if snapshot.state is WorkflowState.AWAITING_PUBLISH:
        waivers, accepted = _approval_exceptions(full)
        return store.publish(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            approver=S6_APPROVER,
            decided_at=_now_utc(),
            note=(
                f"Completion delegation: {S6_COMPLETION_AUTHORIZATION}. "
                "Published evidence-only S6 rows; S7 identity reconciliation remains gated."
            ),
            waived_qa_result_ids=waivers,
            accepted_quarantine_issue_ids=accepted,
        )
    if snapshot.state is not WorkflowState.PUBLISHED:
        raise SilverStoreError(f"S6 workflow cannot publish from {snapshot.state.value}")
    release_id = snapshot.evidence.get("release_id")
    if not isinstance(release_id, str):
        raise SilverStoreError("published S6 workflow has no release ID")
    return snapshot, store.load_release(release_id)[0]


def _load_or_materialize(
    root: Path,
    *,
    store: SilverStore,
    intent: BuildIntent,
    contract: TableContract,
    prepared: _PreparedSource,
    authorization: TickerOverviewAuthorization,
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
            raise SilverStoreError("orphan S6 build intent differs from authorized run")
        store.verify_build(build, contract)
        return build
    started_at = _now_utc()
    result = transform_ticker_overview_safe(
        prepared.transform_inputs,
        build_id=intent.build_id,
        calendar_name="XNYS",
    )
    if result.table.num_rows != authorization.expected_output_rows:
        raise SilverStoreError("S6 output row count differs from authorization")
    _require_expected_result(result, contract, authorization)
    fixed_path = f"{SilverStore.build_output_prefix(intent)}/samples/reviewed-s6-profile.json"
    qa_checks = tuple(
        replace(check, bounded_examples_path=fixed_path) for check in result.qa_checks
    )
    outputs = _write_outputs(
        root,
        intent=intent,
        contract=contract,
        inputs=prepared.transform_inputs,
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
        by_id = {item.check_id: item for item in qa_checks}
        metadata = PreviewMetadata(
            fixed_case_ids=("current_reference_snapshot", "ticker_reuse"),
            fixed_case_qa_result_ids={
                "current_reference_snapshot": tuple(item.result_id for item in qa_checks),
                "ticker_reuse": (by_id["unresolved_identity_rows"].result_id,),
            },
            input_sample_path=input_sample.path,
            input_sample_rows=int(input_sample.row_count or 0),
            output_sample_path=output_sample.path,
            output_sample_rows=int(output_sample.row_count or 0),
            examples_truncated=True,
            full_run_inputs=intent.inputs,
            resource_usage={
                "basis": "complete formal Ticker Overview lifecycle inventory",
                "input_rows": len(prepared.transform_inputs),
                "output_rows": result.table.num_rows,
                "direct_source_artifacts": len(prepared.input_artifacts),
                "bound_bronze_artifacts": len(prepared.overview_inventory.artifacts),
                "serialized_bytes": sum(item.bytes for item in outputs),
            },
            full_run_projection={
                "basis": "preview equals complete reviewed S6 scope",
                "projection_multiplier": 1.0,
                "lifecycle_inventory_id": prepared.lifecycle_inventory.inventory_id,
                "overview_inventory_id": prepared.overview_inventory.inventory_id,
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
    inputs: tuple[dict[str, object], ...],
    table: pa.Table,
    qa_checks: tuple[QACheckResult, ...],
    quarantine_records: tuple[Any, ...],
    fixed_path: str,
    authorization: TickerOverviewAuthorization,
) -> tuple[ArtifactRef, ...]:
    prefix = SilverStore.build_output_prefix(intent)
    if contract.partition_by != ("source_capture_date",):
        raise SilverStoreError("S6 contract requires one source_capture_date partition")
    partition_values = sorted(set(table.column("source_capture_date").to_pylist()))
    if partition_values != [authorization.expected_capture_date]:
        raise SilverStoreError("S6 output capture-date partition changed")
    data = _write_parquet(
        root,
        f"{prefix}/data/source_capture_date={EXPECTED_CAPTURE_DATE.isoformat()}"
        "/part-00000.parquet",
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
        [_json_safe(dict(item)) for item in inputs[: authorization.sample_limit]],
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
                "completion_delegation": S6_COMPLETION_AUTHORIZATION,
                "execution_instruction": S6_EXECUTION_AUTHORIZATION,
                "expected_capture_date": authorization.expected_capture_date.isoformat(),
                "expected_output_rows": authorization.expected_output_rows,
                "expected_source_rows": authorization.expected_source_rows,
                "expected_unresolved_rows": authorization.expected_unresolved_rows,
                "s7_started": False,
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


def _parameters(
    prepared: _PreparedSource, authorization: TickerOverviewAuthorization
) -> dict[str, object]:
    return {
        "backtest_identity_eligible": False,
        "completion_delegation": S6_COMPLETION_AUTHORIZATION,
        "coverage_receipt_path": prepared.coverage_receipt_path,
        "coverage_receipt_sha256": prepared.coverage_receipt_sha256,
        "execution_instruction": S6_EXECUTION_AUTHORIZATION,
        "expected_capture_date": authorization.expected_capture_date.isoformat(),
        "expected_output_rows": authorization.expected_output_rows,
        "expected_source_rows": authorization.expected_source_rows,
        "expected_unresolved_rows": authorization.expected_unresolved_rows,
        "full_formal_scope_preview": True,
        "lifecycle_inventory_id": prepared.lifecycle_inventory.inventory_id,
        "overview_inventory_id": prepared.overview_inventory.inventory_id,
        "profile_sha256": prepared.profile_sha256,
        "pyarrow_version": pa.__version__,
        "python_version": platform.python_version(),
        "s7_started": False,
        "sample_limit": authorization.sample_limit,
    }


def _require_expected_result(
    result: Any,
    contract: TableContract,
    authorization: TickerOverviewAuthorization,
) -> None:
    if result.table.schema != contract.arrow_schema:
        raise SilverStoreError("S6 transform result violates its contract schema")
    _require_expected_qa(result.qa_checks, contract, authorization)
    _require_expected_quarantine(result.quarantine_records, contract, build_id=None)
    expected_funnel = (
        authorization.expected_source_rows,
        authorization.expected_output_rows,
        0,
        authorization.expected_unresolved_rows,
        0,
        0,
        {contract.table: authorization.expected_output_rows},
    )
    funnel = result.row_funnel
    actual_funnel = (
        funnel.input_rows,
        funnel.accepted_source_rows,
        funnel.exact_duplicate_excess,
        funnel.quarantined_source_rows,
        funnel.unmapped_source_rows,
        funnel.version_preserved_rows,
        dict(funnel.output_rows_by_table),
    )
    if actual_funnel != expected_funnel:
        raise SilverStoreError("S6 transform row funnel differs from authorization")


def _require_expected_build(
    build: BuildManifest,
    contract: TableContract,
    authorization: TickerOverviewAuthorization,
) -> None:
    if build.intent.contract_id != contract.contract_id:
        raise SilverStoreError("S6 build contract changed")
    _require_expected_qa(build.qa_checks, contract, authorization)
    expected_funnel = (
        authorization.expected_source_rows,
        authorization.expected_output_rows,
        0,
        authorization.expected_unresolved_rows,
        0,
        0,
        {contract.table: authorization.expected_output_rows},
    )
    funnel = build.row_funnel
    actual_funnel = (
        funnel.input_rows,
        funnel.accepted_source_rows,
        funnel.exact_duplicate_excess,
        funnel.quarantined_source_rows,
        funnel.unmapped_source_rows,
        funnel.version_preserved_rows,
        dict(funnel.output_rows_by_table),
    )
    if actual_funnel != expected_funnel:
        raise SilverStoreError("S6 build row funnel differs from authorization")
    if (
        build.quarantine_issue_rows != authorization.expected_unresolved_rows
        or build.quarantine_unique_source_rows != authorization.expected_unresolved_rows
        or len(build.quarantine_issue_ids_by_severity[QASeverity.HIGH.value])
        != authorization.expected_unresolved_rows
        or build.quarantine_issue_ids_by_severity[QASeverity.CRITICAL.value]
        or build.quarantine_issue_ids_by_severity[QASeverity.MEDIUM.value]
        or build.quarantine_issue_ids_by_severity[QASeverity.LOW.value]
    ):
        raise SilverStoreError("S6 quarantine cardinality or severity changed")
    data = [item for item in build.outputs if item.role is ArtifactRole.DATA]
    if (
        len(data) != 1
        or data[0].table != contract.table
        or data[0].row_count != authorization.expected_output_rows
    ):
        raise SilverStoreError("S6 DATA output changed")


def _require_expected_qa(
    checks: tuple[QACheckResult, ...],
    contract: TableContract,
    authorization: TickerOverviewAuthorization,
) -> None:
    if (
        len(checks) != len(contract.qa_rules)
        or len({item.check_id for item in checks}) != len(checks)
        or {item.check_id for item in checks} != set(contract.required_qa_checks)
    ):
        raise SilverStoreError("S6 QA check IDs changed")
    partition = f"source_capture_date={authorization.expected_capture_date.isoformat()}"
    if any(item.table != contract.table or item.partition_key != partition for item in checks):
        raise SilverStoreError("S6 QA partition identity changed")
    observed_nonzero: dict[str, int] = {}
    for check in checks:
        if check.blocks_publish:
            raise SilverStoreError(f"S6 has blocking QA: {check.check_id}")
        if check.numerator:
            observed_nonzero[check.check_id] = check.numerator
            if check.status is not QAStatus.WARNING:
                raise SilverStoreError(f"S6 has unexpected nonzero QA: {check.check_id}")
            expected_denominator = _EXPECTED_WARNING_DENOMINATORS.get(check.check_id)
            if check.denominator != expected_denominator:
                raise SilverStoreError(
                    f"S6 reviewed warning denominator changed: {check.check_id}"
                )
            if check.check_id == "unresolved_identity_rows":
                if check.severity is not QASeverity.HIGH:
                    raise SilverStoreError("S6 unresolved identity QA severity changed")
            elif check.severity is not QASeverity.MEDIUM:
                raise SilverStoreError(f"S6 reviewed warning severity changed: {check.check_id}")
        elif check.status is not QAStatus.PASSED:
            raise SilverStoreError(f"S6 zero-numerator QA is not passed: {check.check_id}")
    if observed_nonzero != _EXPECTED_WARNING_NUMERATORS:
        raise SilverStoreError(f"S6 reviewed warning profile changed: {observed_nonzero}")


def _require_expected_quarantine(
    records: tuple[Any, ...], contract: TableContract, *, build_id: str | None
) -> None:
    if len(records) != EXPECTED_UNRESOLVED_ROWS:
        raise SilverStoreError("S6 result quarantine count changed")
    for item in records:
        if (
            item.table_name != contract.table
            or item.issue_code != "identity_evidence_unresolved"
            or item.severity is not QASeverity.HIGH
            or (build_id is not None and item.detected_build_id != build_id)
        ):
            raise SilverStoreError("S6 quarantine semantics changed")


def _approval_exceptions(build: BuildManifest) -> tuple[tuple[str, ...], tuple[str, ...]]:
    waivers = tuple(
        sorted(item.result_id for item in build.qa_checks if item.status is QAStatus.WARNING)
    )
    accepted = tuple(sorted(build.quarantine_issue_ids_by_severity[QASeverity.HIGH.value]))
    return waivers, accepted


def _require_preview_parity(root: Path, preview: BuildManifest, full: BuildManifest) -> None:
    preview_data = next(item for item in preview.outputs if item.role is ArtifactRole.DATA)
    full_data = next(item for item in full.outputs if item.role is ArtifactRole.DATA)
    if preview_data.sha256 != full_data.sha256:
        raise SilverStoreError("S6 full DATA differs from preview")
    if preview.row_funnel != full.row_funnel:
        raise SilverStoreError("S6 full row funnel differs from preview")
    if {item.check_id: _qa_core(item) for item in preview.qa_checks} != {
        item.check_id: _qa_core(item) for item in full.qa_checks
    }:
        raise SilverStoreError("S6 full QA metrics differ from preview")
    if (
        preview.quarantine_issue_rows != full.quarantine_issue_rows
        or preview.quarantine_unique_source_rows != full.quarantine_unique_source_rows
        or _quarantine_semantic_rows(root, preview) != _quarantine_semantic_rows(root, full)
    ):
        raise SilverStoreError("S6 full quarantine evidence differs from preview")
    if not pq.ParquetFile(root / preview_data.path).read().equals(
        pq.ParquetFile(root / full_data.path).read()
    ):
        raise SilverStoreError("S6 recomputed rows differ from preview")


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
    return sorted(
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
        for item in table.to_pylist()
    )


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
                raise SilverStoreError("S6 workflow event references wrong build")
            store.verify_build(build, contract)
            return build, document
    raise SilverStoreError(f"S6 workflow has no registered {kind.value} build")


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
            raise SilverStoreError("S6 approval event has no approval ID")
        approval, _ = store.load_approval(approval_id)
        waivers, accepted = expected[record.event.to_state]
        if (
            approval.waived_qa_result_ids != tuple(sorted(waivers))
            or approval.accepted_quarantine_issue_ids != tuple(sorted(accepted))
        ):
            raise SilverStoreError("S6 approval exceptions differ from reviewed build")
        seen.add(record.event.to_state)
    if seen != set(expected):
        raise SilverStoreError("S6 workflow is missing a review or publish approval")


def _read_published_table(published: PublishedRelease) -> pa.Table:
    tables = [pq.ParquetFile(path).read() for path in published.data_paths]
    if not tables:
        raise SilverStoreError("published S6 release has no DATA table")
    return tables[0] if len(tables) == 1 else pa.concat_tables(tables)


def _verify_git_checkout(repo_root: Path, git_commit: str) -> None:
    root = repo_root.expanduser().resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise SilverStoreError("S6 lifecycle code is not executing from --repo-root") from exc
    if module_relative != "backend/ame_stocks_api/silver/ticker_overview_lifecycle.py":
        raise SilverStoreError("S6 lifecycle module path is not canonical")
    if Path(_git(root, "rev-parse", "--show-toplevel")).resolve() != root:
        raise SilverStoreError("S6 --repo-root is not the exact Git top level")
    if _git(root, "rev-parse", "HEAD") != git_commit:
        raise SilverStoreError("S6 Git HEAD differs from --git-commit")
    if _git(root, "status", "--porcelain=v1", "--untracked-files=all"):
        raise SilverStoreError("S6 Git checkout is not clean")
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
        raise SilverStoreError(f"cannot verify S6 Git checkout: {detail}")
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
    "CURRENT_TICKER_OVERVIEW_AUTHORIZATION",
    "S6_COMPLETION_AUTHORIZATION",
    "S6_EXECUTION_AUTHORIZATION",
    "TickerOverviewAuthorization",
    "TickerOverviewLifecycleRun",
    "complete_ticker_overview_lifecycle",
]
