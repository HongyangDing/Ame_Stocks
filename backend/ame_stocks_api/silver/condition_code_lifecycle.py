"""Fail-closed S3 lifecycle for the paired condition-code Silver tables.

The provider snapshot has one logical transformation but Silver's registry has a
one-contract/one-table workflow invariant.  This module therefore advances two
independent workflows in lockstep and publishes neither until both review-bound
full builds are ready.
"""

from __future__ import annotations

import json
import platform
import subprocess
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, stable_digest, write_bytes_immutable
from ame_stocks_api.silver.condition_code_contract import (
    CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT,
    CONDITION_CODE_DIM_CONTRACT,
)
from ame_stocks_api.silver.condition_code_source import (
    ConditionCodeSourceBatch,
    build_condition_code_source_inventory,
    read_condition_code_source_inventory,
)
from ame_stocks_api.silver.condition_codes import (
    CONDITION_CODE_TRANSFORM_VERSION,
    transform_condition_code_batch,
)
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
    UpstreamManifestRef,
    arrow_schema_digest,
    thaw_json,
)
from ame_stocks_api.silver.exchange_contract import EXCHANGE_DIM_CONTRACT
from ame_stocks_api.silver.reader import PublishedRelease, PublishedSilverReader
from ame_stocks_api.silver.store import (
    WORKFLOW_EVENT_VERSION,
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowSnapshot,
    WorkflowState,
)

S3_COMPLETION_AUTHORIZATION = "你直接把S3推进到完成吧"
S3_SCHEMA_CREATED_AT = "2026-07-13T05:00:00+00:00"
S3_SCHEMA_DECIDED_AT = "2026-07-13T05:00:01+00:00"
S3_WORKFLOW_ACTOR = "s3-condition-codes-lifecycle"
S3_APPROVER = "user-delegated-s3-design-authority"
S3_SAMPLE_LIMIT = 94

CURRENT_CONDITION_CODES_MANIFEST_PATH = (
    "manifests/massive/condition_codes/"
    "3054f84fb36c30dceadd16d0533efd7be8ddc13b4cbb64ccf93ac9c2ee5d4bf3.json"
)
CURRENT_CONDITION_CODES_MANIFEST_SHA256 = (
    "f4bfc27b609605551a25ccadb77e68ec6f224903259db59a0f72311b46582a40"
)
CURRENT_CONDITION_CODES_REQUEST_ID = (
    "3054f84fb36c30dceadd16d0533efd7be8ddc13b4cbb64ccf93ac9c2ee5d4bf3"
)
CURRENT_CONDITION_CODES_ARTIFACT_PATH = (
    "bronze/massive/condition_codes/request_id="
    f"{CURRENT_CONDITION_CODES_REQUEST_ID}/page-00000.json.gz"
)
CURRENT_CONDITION_CODES_ARTIFACT_SHA256 = (
    "85861aecc2d6fc369578323b11362b4c179d7ff012b9d93d09a244e2463b778a"
)
CURRENT_CONDITION_CODES_EXPECTED_ROWS = 94
CURRENT_CONDITION_CODE_BRIDGE_EXPECTED_ROWS = 123

S1_EXCHANGE_RELEASE_ID = "feab0e1f32a5685d1115a6e4e87aab8ff50c18b99c6336a8790ecba44464d838"
S1_EXCHANGE_RELEASE_MANIFEST_SHA256 = (
    "d8789e6cf760ffb6274077736c18e37bd69330139ea1c6ecf2f420bb56f93f07"
)

_CONTRACTS = (CONDITION_CODE_DIM_CONTRACT, CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT)
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
_DIM_PARENT_KEY = (
    "capture_date",
    "asset_class",
    "condition_type",
    "condition_id",
    "is_legacy",
)
_BRIDGE_KEY = (*_DIM_PARENT_KEY, "data_type")
_LOGIC_CLOSURE = (
    "backend/ame_stocks_api/silver/condition_code_contract.py",
    "backend/ame_stocks_api/silver/condition_code_lifecycle.py",
    "backend/ame_stocks_api/silver/condition_code_source.py",
    "backend/ame_stocks_api/silver/condition_codes.py",
    "backend/ame_stocks_api/silver/contracts.py",
    "backend/ame_stocks_api/silver/reader.py",
    "backend/ame_stocks_api/silver/schema_resources/condition_code_data_type_bridge.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/condition_code_dim.schema-v1.json",
    "backend/ame_stocks_api/silver/store.py",
)


@dataclass(frozen=True, slots=True)
class ConditionCodeAuthorization:
    manifest_path: str = CURRENT_CONDITION_CODES_MANIFEST_PATH
    manifest_sha256: str = CURRENT_CONDITION_CODES_MANIFEST_SHA256
    request_id: str = CURRENT_CONDITION_CODES_REQUEST_ID
    artifact_path: str = CURRENT_CONDITION_CODES_ARTIFACT_PATH
    artifact_sha256: str = CURRENT_CONDITION_CODES_ARTIFACT_SHA256
    expected_source_rows: int = CURRENT_CONDITION_CODES_EXPECTED_ROWS
    expected_dim_rows: int = CURRENT_CONDITION_CODES_EXPECTED_ROWS
    expected_bridge_rows: int = CURRENT_CONDITION_CODE_BRIDGE_EXPECTED_ROWS
    exchange_release_id: str = S1_EXCHANGE_RELEASE_ID
    exchange_release_manifest_sha256: str = S1_EXCHANGE_RELEASE_MANIFEST_SHA256
    sample_limit: int = S3_SAMPLE_LIMIT

    def __post_init__(self) -> None:
        if self.manifest_path != CURRENT_CONDITION_CODES_MANIFEST_PATH:
            raise ValueError("S3 manifest path is not production-authorized")
        if self.request_id != CURRENT_CONDITION_CODES_REQUEST_ID:
            raise ValueError("S3 request ID is not production-authorized")
        if self.artifact_path != CURRENT_CONDITION_CODES_ARTIFACT_PATH:
            raise ValueError("S3 artifact path is not production-authorized")
        for label, value in (
            ("manifest_sha256", self.manifest_sha256),
            ("request_id", self.request_id),
            ("artifact_sha256", self.artifact_sha256),
            ("exchange_release_id", self.exchange_release_id),
            ("exchange_release_manifest_sha256", self.exchange_release_manifest_sha256),
        ):
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise ValueError(f"S3 {label} is not a lowercase SHA-256")
        if (
            self.expected_source_rows != 94
            or self.expected_dim_rows != 94
            or self.expected_bridge_rows != 123
            or not 1 <= self.sample_limit <= 100
        ):
            raise ValueError("S3 authorized cardinalities or sample bound changed")


CURRENT_CONDITION_CODE_AUTHORIZATION = ConditionCodeAuthorization()


@dataclass(frozen=True, slots=True)
class ConditionCodeTableRun:
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
class ConditionCodeLifecycleRun:
    dim: ConditionCodeTableRun
    bridge: ConditionCodeTableRun
    inventory: SourceInventory
    inventory_document: StoredDocument
    exchange_ids: tuple[int, ...]

    def by_table(self, table: str) -> ConditionCodeTableRun:
        if table == self.dim.contract.table:
            return self.dim
        if table == self.bridge.contract.table:
            return self.bridge
        raise KeyError(table)


def complete_condition_code_lifecycle(
    data_root: Path,
    *,
    repo_root: Path,
    git_commit: str,
) -> ConditionCodeLifecycleRun:
    """Advance both exact S3 workflows through verified publication, idempotently."""

    _verify_git_checkout(repo_root, git_commit)
    root = data_root.expanduser().resolve()
    store = SilverStore(root)
    authorization = CURRENT_CONDITION_CODE_AUTHORIZATION
    batch, bronze_inventory = _load_authorized_bronze(
        root,
        git_commit=git_commit,
        authorization=authorization,
    )
    exchange_ids, exchange_release_document = _load_pit_exchange_ids(
        root,
        store=store,
        batch=batch,
        authorization=authorization,
    )
    inventory, inventory_document, inputs = _register_bound_source_inventory(
        store,
        bronze_inventory,
        exchange_release_document=exchange_release_document,
    )

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
            inputs=inputs,
            inventory=inventory,
            git_commit=git_commit,
            exchange_ids=exchange_ids,
            authorization=authorization,
        )

    # Lockstep gates: both reviewed previews exist before either full-run approval;
    # both full builds exist before either publication request.
    for contract in _CONTRACTS:
        workflows[contract.table] = _ensure_full_approved(
            store, workflows[contract.table], previews[contract.table][0]
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
            inventory=inventory,
            exchange_ids=exchange_ids,
            authorization=authorization,
        )
    for contract in _CONTRACTS:
        workflows[contract.table] = _ensure_awaiting_publish(
            store, workflows[contract.table], fulls[contract.table][0]
        )

    _verify_git_checkout(repo_root, git_commit)
    dim_table_name = CONDITION_CODE_DIM_CONTRACT.table
    bridge_table_name = CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT.table
    dim_full_table = _read_build_data_table(root, fulls[dim_table_name][0])
    bridge_full_table = _read_build_data_table(root, fulls[bridge_table_name][0])
    _require_bridge_parent_coverage(
        dim_full_table,
        bridge_full_table,
        expected_dim_rows=authorization.expected_dim_rows,
        expected_bridge_rows=authorization.expected_bridge_rows,
    )

    runs: dict[str, ConditionCodeTableRun] = {}
    # Publish and re-verify the parent first.  The bridge cannot be published
    # until its full output has also matched the release-only parent bytes.
    for contract in (CONDITION_CODE_DIM_CONTRACT,):
        snapshot, release = _ensure_published(store, workflows[contract.table])
        verified = store.verify_workflow_trust_chain(snapshot.workflow_id, verify_artifacts=True)
        _require_zero_exceptions(store, verified.workflow_id)
        release_document = store.load_release(release.release_id)[1]
        published = PublishedSilverReader(root).inspect(release.release_id)
        runs[contract.table] = ConditionCodeTableRun(
            contract=contract,
            workflow=verified,
            preview=previews[contract.table][0],
            preview_document=previews[contract.table][1],
            full=fulls[contract.table][0],
            full_document=fulls[contract.table][1],
            release=release,
            release_document=release_document,
            published=published,
        )

    _require_bridge_parent_coverage(
        _read_published_table(runs[dim_table_name].published),
        bridge_full_table,
        expected_dim_rows=authorization.expected_dim_rows,
        expected_bridge_rows=authorization.expected_bridge_rows,
    )
    _verify_git_checkout(repo_root, git_commit)
    contract = CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT
    snapshot, release = _ensure_published(store, workflows[contract.table])
    verified = store.verify_workflow_trust_chain(snapshot.workflow_id, verify_artifacts=True)
    _require_zero_exceptions(store, verified.workflow_id)
    release_document = store.load_release(release.release_id)[1]
    published = PublishedSilverReader(root).inspect(release.release_id)
    runs[contract.table] = ConditionCodeTableRun(
        contract=contract,
        workflow=verified,
        preview=previews[contract.table][0],
        preview_document=previews[contract.table][1],
        full=fulls[contract.table][0],
        full_document=fulls[contract.table][1],
        release=release,
        release_document=release_document,
        published=published,
    )

    dim = runs[CONDITION_CODE_DIM_CONTRACT.table]
    bridge = runs[CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT.table]
    _require_bridge_parent_coverage(
        _read_published_table(dim.published),
        _read_published_table(bridge.published),
        expected_dim_rows=authorization.expected_dim_rows,
        expected_bridge_rows=authorization.expected_bridge_rows,
    )
    return ConditionCodeLifecycleRun(
        dim=dim,
        bridge=bridge,
        inventory=inventory,
        inventory_document=inventory_document,
        exchange_ids=tuple(sorted(exchange_ids)),
    )


def _load_authorized_bronze(
    root: Path,
    *,
    git_commit: str,
    authorization: ConditionCodeAuthorization,
) -> tuple[ConditionCodeSourceBatch, SourceInventory]:
    inventory = build_condition_code_source_inventory(
        root, manifest_paths=(authorization.manifest_path,), git_commit=git_commit
    )
    if len(inventory.upstream_manifests) != 1 or (
        inventory.upstream_manifests[0].path != authorization.manifest_path
        or inventory.upstream_manifests[0].sha256 != authorization.manifest_sha256
    ):
        raise SilverStoreError("S3 Bronze manifest identity differs from authorization")
    if len(inventory.artifacts) != 1:
        raise SilverStoreError("S3 requires one exact Bronze page")
    item = inventory.artifacts[0]
    if (
        item.path != authorization.artifact_path
        or item.sha256 != authorization.artifact_sha256
        or item.row_count != authorization.expected_source_rows
    ):
        raise SilverStoreError("S3 Bronze page identity or row count differs from authorization")
    batch = read_condition_code_source_inventory(root, inventory)
    if batch.row_count != authorization.expected_source_rows or batch.page_count != 1:
        raise SilverStoreError("S3 verified source cardinality differs from authorization")
    if (
        len(batch.snapshots) != 1
        or batch.snapshots[0].source_request_id != authorization.request_id
    ):
        raise SilverStoreError("S3 source request identity differs from authorization")
    return batch, inventory


def _register_bound_source_inventory(
    store: SilverStore,
    bronze_inventory: SourceInventory,
    *,
    exchange_release_document: StoredDocument,
) -> tuple[SourceInventory, StoredDocument, tuple[ArtifactRef, ...]]:
    """Bind the lookup release into auditable lineage without inflating source row funnel."""

    exchange_release = UpstreamManifestRef(
        path=exchange_release_document.path,
        sha256=exchange_release_document.sha256,
    )
    inventory = replace(
        bronze_inventory,
        upstream_manifests=(*bronze_inventory.upstream_manifests, exchange_release),
    )
    document = store.register_source_inventory(inventory)
    inputs = tuple(
        ArtifactRef(
            path=source.path,
            sha256=source.sha256,
            bytes=source.bytes,
            row_count=source.row_count,
            media_type=source.media_type,
            role=ArtifactRole.SOURCE,
            source_dataset=inventory.source_dataset,
            source_layer=SourceLayer.BRONZE,
            lineage_manifest_path=document.path,
            lineage_manifest_sha256=document.sha256,
        )
        for source in inventory.artifacts
    )
    for contract in _CONTRACTS:
        store.verify_source_artifacts(inputs, contract)
    return inventory, document, inputs


def _load_pit_exchange_ids(
    root: Path,
    *,
    store: SilverStore,
    batch: ConditionCodeSourceBatch,
    authorization: ConditionCodeAuthorization,
) -> tuple[frozenset[int], StoredDocument]:
    release, release_document = store.load_release(authorization.exchange_release_id)
    if release_document.sha256 != authorization.exchange_release_manifest_sha256:
        raise SilverStoreError("S3 S1 exchange release manifest SHA differs from authorization")
    published = PublishedSilverReader(root).inspect(release.release_id)
    if published.contract != EXCHANGE_DIM_CONTRACT:
        raise SilverStoreError("S3 exchange dependency is not the approved exchange_dim contract")
    cutoff = _first_xnys_open_after(max(item.source_capture_at_utc for item in batch.snapshots))
    table = _read_published_table(published)
    eligible = table.filter(
        pc.less_equal(
            table.column("available_at_utc"),
            pa.scalar(cutoff, pa.timestamp("ns", tz="UTC")),
        )
    )
    if eligible.num_rows == 0:
        raise SilverStoreError("S3 has no PIT-eligible published exchange snapshot")
    latest_capture = max(eligible.column("capture_date").to_pylist())
    latest = eligible.filter(
        pc.equal(eligible.column("capture_date"), pa.scalar(latest_capture, pa.date32()))
    )
    ids = frozenset(int(item) for item in latest.column("exchange_id").to_pylist())
    if not ids or len(ids) != latest.num_rows:
        raise SilverStoreError("S3 PIT exchange snapshot has missing or duplicate exchange IDs")
    return ids, release_document


def _ensure_schema_approved(store: SilverStore, contract: TableContract) -> WorkflowSnapshot:
    actor = f"{S3_WORKFLOW_ACTOR}-{contract.table}"
    workflow_id = stable_digest(
        {
            "actor": actor,
            "contract_id": contract.contract_id,
            "created_at": S3_SCHEMA_CREATED_AT,
            "workflow_event_version": WORKFLOW_EVENT_VERSION,
        }
    )
    event_dir = store.root / "manifests/silver/workflows" / workflow_id / "events"
    if event_dir.exists():
        snapshot = store.verify_workflow_trust_chain(workflow_id, verify_artifacts=True)
        existing, _ = store.load_workflow_contract(workflow_id)
        if existing != contract:
            raise SilverStoreError("deterministic S3 workflow contract changed")
    else:
        snapshot = store.create_workflow(
            contract,
            actor=actor,
            created_at=S3_SCHEMA_CREATED_AT,
            note=f"Registered delegated S3 contract for {contract.table}.",
        )
    if snapshot.state is WorkflowState.PLANNED:
        snapshot = store.submit_schema_review(
            workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            actor=actor,
            created_at=S3_SCHEMA_CREATED_AT,
            note=f"Submitted delegated S3 {contract.table} contract for schema review.",
        )
    if snapshot.state is WorkflowState.SCHEMA_REVIEW:
        snapshot = store.approve_schema(
            workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            approver=S3_APPROVER,
            decided_at=S3_SCHEMA_DECIDED_AT,
            note=(
                f"User delegation: {S3_COMPLETION_AUTHORIZATION}. "
                f"Approved the reviewed {contract.table} design authority; no data exception."
            ),
        )
    if snapshot.state not in _ACTIVE_POST_SCHEMA_STATES:
        raise SilverStoreError("S3 schema workflow is in an unsupported state")
    return snapshot


def _ensure_preview(
    root: Path,
    *,
    store: SilverStore,
    snapshot: WorkflowSnapshot,
    contract: TableContract,
    batch: ConditionCodeSourceBatch,
    inputs: tuple[ArtifactRef, ...],
    inventory: SourceInventory,
    git_commit: str,
    exchange_ids: frozenset[int],
    authorization: ConditionCodeAuthorization,
) -> tuple[WorkflowSnapshot, tuple[BuildManifest, StoredDocument]]:
    parameters = _parameters(contract, batch, inventory, exchange_ids, authorization)
    intent = BuildIntent(
        workflow_id=snapshot.workflow_id,
        domain=contract.domain,
        table=contract.table,
        schema_version=contract.schema_version,
        contract_id=contract.contract_id,
        kind=BuildKind.PREVIEW,
        attempt=1,
        retry_of_build_id=None,
        transform_version=CONDITION_CODE_TRANSFORM_VERSION,
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
            inventory=inventory,
            exchange_ids=exchange_ids,
            authorization=authorization,
            preview=True,
        )
        snapshot = store.record_preview_build(
            build,
            expected_event_sha256=snapshot.event_sha256,
            actor=S3_WORKFLOW_ACTOR,
            recorded_at=_now_utc(),
            note=f"Registered exact bounded S3 preview for {contract.table}.",
        )
    if snapshot.state is WorkflowState.PREVIEW_READY:
        snapshot = store.request_preview_review(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            actor=S3_WORKFLOW_ACTOR,
            created_at=_now_utc(),
            note=(
                f"User delegation: {S3_COMPLETION_AUTHORIZATION}. "
                f"Submitted {contract.table} preview before review-bound full build."
            ),
        )
    build, document = _event_build(store, snapshot.workflow_id, contract, BuildKind.PREVIEW)
    if build.intent != intent:
        raise SilverStoreError("existing S3 preview intent differs from authorized run")
    _require_clean_build(build, contract)
    return snapshot, (build, document)


def _ensure_full_approved(
    store: SilverStore, snapshot: WorkflowSnapshot, preview: BuildManifest
) -> WorkflowSnapshot:
    if snapshot.state is WorkflowState.AWAITING_REVIEW:
        snapshot = store.approve_full_run(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            approver=S3_APPROVER,
            decided_at=_now_utc(),
            note=(
                f"User delegation: {S3_COMPLETION_AUTHORIZATION}. Approved the exact S3 preview "
                "for a review-bound full run with zero QA waivers and quarantine acceptances."
            ),
            waived_qa_result_ids=(),
            accepted_quarantine_issue_ids=(),
        )
    return snapshot


def _ensure_full(
    root: Path,
    *,
    store: SilverStore,
    snapshot: WorkflowSnapshot,
    contract: TableContract,
    preview: BuildManifest,
    batch: ConditionCodeSourceBatch,
    inventory: SourceInventory,
    exchange_ids: frozenset[int],
    authorization: ConditionCodeAuthorization,
) -> tuple[WorkflowSnapshot, tuple[BuildManifest, StoredDocument]]:
    if preview.preview is None:
        raise SilverStoreError("S3 reviewed preview metadata is missing")
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
            inventory=inventory,
            exchange_ids=exchange_ids,
            authorization=authorization,
            preview=False,
        )
        _require_preview_parity(root, preview, build, contract)
        snapshot = store.record_full_build(
            build,
            expected_event_sha256=snapshot.event_sha256,
            actor=S3_WORKFLOW_ACTOR,
            recorded_at=_now_utc(),
            note=f"Registered the exact review-bound S3 full build for {contract.table}.",
        )
    build, document = _event_build(store, snapshot.workflow_id, contract, BuildKind.FULL)
    if build.intent != intent:
        raise SilverStoreError("existing S3 full intent differs from reviewed preview")
    _require_clean_build(build, contract)
    _require_preview_parity(root, preview, build, contract)
    return snapshot, (build, document)


def _ensure_awaiting_publish(
    store: SilverStore, snapshot: WorkflowSnapshot, full: BuildManifest
) -> WorkflowSnapshot:
    if snapshot.state is WorkflowState.FULL_READY:
        snapshot = store.request_publish(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            actor=S3_WORKFLOW_ACTOR,
            created_at=_now_utc(),
            note=f"Submitted verified S3 full build {full.build_id} for publication.",
        )
    return snapshot


def _ensure_published(
    store: SilverStore, snapshot: WorkflowSnapshot
) -> tuple[WorkflowSnapshot, ReleaseManifest]:
    if snapshot.state is WorkflowState.AWAITING_PUBLISH:
        snapshot, release = store.publish(
            snapshot.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            approver=S3_APPROVER,
            decided_at=_now_utc(),
            note=(
                f"User delegation: {S3_COMPLETION_AUTHORIZATION}. Authorized S3 publication "
                "with zero QA waivers and quarantine acceptances."
            ),
            waived_qa_result_ids=(),
            accepted_quarantine_issue_ids=(),
        )
        return snapshot, release
    if snapshot.state is not WorkflowState.PUBLISHED:
        raise SilverStoreError(f"S3 workflow cannot publish from {snapshot.state.value}")
    release_id = snapshot.evidence.get("release_id")
    if not isinstance(release_id, str):
        raise SilverStoreError("published S3 workflow has no release ID")
    return snapshot, store.load_release(release_id)[0]


def _load_or_materialize(
    root: Path,
    *,
    store: SilverStore,
    intent: BuildIntent,
    contract: TableContract,
    batch: ConditionCodeSourceBatch,
    inventory: SourceInventory,
    exchange_ids: frozenset[int],
    authorization: ConditionCodeAuthorization,
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
            raise SilverStoreError("orphan S3 build intent differs from authorized run")
        store.verify_build(build, contract)
        return build
    started_at = _now_utc()
    transformed = transform_condition_code_batch(
        batch,
        build_id=intent.build_id,
        known_exchange_ids=exchange_ids,
        calendar_name="XNYS",
    )
    result = transformed.by_table(contract.table)
    expected_rows = (
        authorization.expected_dim_rows
        if contract == CONDITION_CODE_DIM_CONTRACT
        else authorization.expected_bridge_rows
    )
    if result.table.num_rows != expected_rows:
        raise SilverStoreError(f"S3 {contract.table} row count differs from authorization")
    _require_result_clean(result, contract)
    fixed_path = (
        f"{SilverStore.build_output_prefix(intent)}/samples/current-reference-snapshot.json"
    )
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
            fixed_case_ids=("current_reference_snapshot",),
            fixed_case_qa_result_ids={
                "current_reference_snapshot": tuple(item.result_id for item in qa_checks)
            },
            input_sample_path=input_sample.path,
            input_sample_rows=int(input_sample.row_count or 0),
            output_sample_path=output_sample.path,
            output_sample_rows=int(output_sample.row_count or 0),
            examples_truncated=(
                batch.row_count > authorization.sample_limit
                or result.table.num_rows > authorization.sample_limit
            ),
            full_run_inputs=intent.inputs,
            resource_usage={
                "basis": "complete authorized 94-row current snapshot",
                "input_rows": batch.row_count,
                "output_rows": result.table.num_rows,
                "serialized_bytes": sum(item.bytes for item in outputs),
            },
            full_run_projection={
                "basis": "preview inventory is the complete authorized snapshot",
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
    batch: ConditionCodeSourceBatch,
    table: pa.Table,
    qa_checks: tuple[QACheckResult, ...],
    quarantine_records: tuple[Any, ...],
    fixed_path: str,
    authorization: ConditionCodeAuthorization,
) -> tuple[ArtifactRef, ...]:
    prefix = SilverStore.build_output_prefix(intent)
    capture_dates = set(table.column("capture_date").to_pylist())
    if len(capture_dates) != 1:
        raise SilverStoreError("S3 authorized snapshot must have exactly one capture partition")
    capture_date = next(iter(capture_dates))
    data = _write_parquet(
        root,
        f"{prefix}/data/capture_date={capture_date.isoformat()}/part-00000.parquet",
        table,
        ArtifactRole.DATA,
        contract.table,
    )
    qa_table = pa.Table.from_pylist(
        [item.to_output_dict(intent.build_id) for item in qa_checks], QA_RESULT_ARROW_SCHEMA
    )
    qa = _write_parquet(
        root, f"{prefix}/qa/qa-check-result.parquet", qa_table, ArtifactRole.QA, "qa_check_result"
    )
    quarantine_table = pa.Table.from_pylist(
        [item.to_dict() for item in quarantine_records], QUARANTINE_ARROW_SCHEMA
    )
    quarantine = _write_parquet(
        root,
        f"{prefix}/quarantine/quarantine-record.parquet",
        quarantine_table,
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
                "user_delegation": S3_COMPLETION_AUTHORIZATION,
                "condition_manifest_sha256": authorization.manifest_sha256,
                "exchange_release_id": authorization.exchange_release_id,
                "input_rows": batch.row_count,
                "output_rows": table.num_rows,
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


def _input_sample(batch: ConditionCodeSourceBatch, limit: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for snapshot in batch.snapshots:
        for page in snapshot.pages:
            for ordinal, raw in enumerate(page.rows):
                rows.append(
                    {
                        "raw": _json_safe(dict(raw)),
                        "source_artifact_sha256": page.source_artifact_sha256,
                        "source_capture_at_utc": snapshot.source_capture_at_utc.isoformat(),
                        "source_page_sequence": page.sequence,
                        "source_provider_request_id": page.source_provider_request_id,
                        "source_request_id": snapshot.source_request_id,
                        "source_row_ordinal": ordinal,
                    }
                )
                if len(rows) == limit:
                    return rows
    return rows


def _parameters(
    contract: TableContract,
    batch: ConditionCodeSourceBatch,
    inventory: SourceInventory,
    exchange_ids: frozenset[int],
    authorization: ConditionCodeAuthorization,
) -> dict[str, object]:
    return {
        "user_delegation": S3_COMPLETION_AUTHORIZATION,
        "bronze_artifact_path": authorization.artifact_path,
        "bronze_artifact_sha256": authorization.artifact_sha256,
        "bronze_manifest_path": authorization.manifest_path,
        "bronze_manifest_sha256": authorization.manifest_sha256,
        "expected_input_rows": authorization.expected_source_rows,
        "exchange_id_count": len(exchange_ids),
        "exchange_ids_digest": stable_digest(sorted(exchange_ids)),
        "exchange_release_id": authorization.exchange_release_id,
        "exchange_release_manifest_sha256": authorization.exchange_release_manifest_sha256,
        "pandas_version": pd.__version__,
        "pyarrow_version": pa.__version__,
        "python_version": platform.python_version(),
        "sample_limit": authorization.sample_limit,
        "source_inventory_id": inventory.inventory_id,
        "target_table": contract.table,
        "timezone_key": "America/New_York",
        "timezone_probe_digest": stable_digest(
            [
                {
                    "capture_at_utc": snapshot.source_capture_at_utc.isoformat(),
                    "capture_at_new_york": snapshot.source_capture_at_utc.astimezone(
                        ZoneInfo("America/New_York")
                    ).isoformat(),
                    "timezone_name": snapshot.source_capture_at_utc.astimezone(
                        ZoneInfo("America/New_York")
                    ).tzname(),
                }
                for snapshot in batch.snapshots
            ]
        ),
    }


def _require_result_clean(result: Any, contract: TableContract) -> None:
    if result.table.schema != contract.arrow_schema:
        raise SilverStoreError(f"S3 {contract.table} result violates its contract schema")
    if len(result.qa_checks) != len(contract.qa_rules) or any(
        item.status is not QAStatus.PASSED for item in result.qa_checks
    ):
        raise SilverStoreError(f"S3 {contract.table} result QA is not entirely passed")
    if result.quarantine_records:
        raise SilverStoreError(f"S3 {contract.table} result has quarantine records")


def _require_clean_build(build: BuildManifest, contract: TableContract) -> None:
    if len(build.qa_checks) != len(contract.qa_rules) or any(
        item.status is not QAStatus.PASSED for item in build.qa_checks
    ):
        raise SilverStoreError(f"S3 {contract.table} build QA is not entirely passed")
    if build.quarantine_issue_rows or build.quarantine_unique_source_rows:
        raise SilverStoreError(f"S3 {contract.table} build has quarantine rows")
    data = [item for item in build.outputs if item.role is ArtifactRole.DATA]
    if len(data) != 1 or data[0].table != contract.table:
        raise SilverStoreError(f"S3 {contract.table} build must have one table DATA output")


def _require_preview_parity(
    root: Path, preview: BuildManifest, full: BuildManifest, contract: TableContract
) -> None:
    preview_data = next(item for item in preview.outputs if item.role is ArtifactRole.DATA)
    full_data = next(item for item in full.outputs if item.role is ArtifactRole.DATA)
    if preview_data.sha256 != full_data.sha256:
        raise SilverStoreError(f"S3 {contract.table} full data differs from reviewed preview")
    if preview.row_funnel != full.row_funnel:
        raise SilverStoreError(f"S3 {contract.table} full row funnel differs from preview")
    if {item.check_id: _qa_core(item) for item in preview.qa_checks} != {
        item.check_id: _qa_core(item) for item in full.qa_checks
    }:
        raise SilverStoreError(f"S3 {contract.table} full QA metrics differ from preview")
    if (
        preview.quarantine_issue_rows != full.quarantine_issue_rows
        or preview.quarantine_unique_source_rows != full.quarantine_unique_source_rows
        or dict(preview.quarantine_issue_ids_by_severity)
        != dict(full.quarantine_issue_ids_by_severity)
    ):
        raise SilverStoreError(f"S3 {contract.table} full quarantine evidence differs from preview")
    if (
        not pq.ParquetFile(root / preview_data.path)
        .read()
        .equals(pq.ParquetFile(root / full_data.path).read())
    ):
        raise SilverStoreError(f"S3 {contract.table} recomputed rows differ from preview")


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
                raise SilverStoreError("S3 workflow event references wrong build")
            store.verify_build(build, contract)
            return build, document
    raise SilverStoreError(f"S3 workflow has no registered {kind.value} build")


def _require_zero_exceptions(store: SilverStore, workflow_id: str) -> None:
    for record in store.workflow_events(workflow_id):
        if record.event.to_state not in {
            WorkflowState.APPROVED_FULL_RUN,
            WorkflowState.PUBLISHED,
        }:
            continue
        approval_id = record.event.evidence.get("approval_id")
        if not isinstance(approval_id, str):
            raise SilverStoreError("S3 approval event has no approval ID")
        approval, _ = store.load_approval(approval_id)
        if approval.waived_qa_result_ids or approval.accepted_quarantine_issue_ids:
            raise SilverStoreError("S3 publication contains a QA or quarantine exception")


def _read_published_table(published: PublishedRelease) -> pa.Table:
    tables = [pq.ParquetFile(path).read() for path in published.data_paths]
    if not tables:
        raise SilverStoreError("published S3 release has no data table")
    return tables[0] if len(tables) == 1 else pa.concat_tables(tables)


def _read_build_data_table(root: Path, build: BuildManifest) -> pa.Table:
    data = [item for item in build.outputs if item.role is ArtifactRole.DATA]
    if len(data) != 1:
        raise SilverStoreError("S3 one-table build must expose exactly one DATA artifact")
    return pq.ParquetFile(safe_relative_path(root, data[0].path)).read()


def _require_bridge_parent_coverage(
    dim: pa.Table,
    bridge: pa.Table,
    *,
    expected_dim_rows: int = CURRENT_CONDITION_CODES_EXPECTED_ROWS,
    expected_bridge_rows: int = CURRENT_CONDITION_CODE_BRIDGE_EXPECTED_ROWS,
) -> None:
    if dim.num_rows != expected_dim_rows or bridge.num_rows != expected_bridge_rows:
        raise SilverStoreError("S3 published dim/bridge cardinalities differ from authorization")
    if (
        tuple(CONDITION_CODE_DIM_CONTRACT.primary_key) != _DIM_PARENT_KEY
        or tuple(CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT.primary_key) != _BRIDGE_KEY
    ):
        raise SilverStoreError("S3 published parent/bridge key contracts changed")
    dim_keys = [tuple(row[name] for name in _DIM_PARENT_KEY) for row in dim.to_pylist()]
    bridge_rows = bridge.to_pylist()
    bridge_keys = [tuple(row[name] for name in _BRIDGE_KEY) for row in bridge_rows]
    bridge_parents = {tuple(row[name] for name in _DIM_PARENT_KEY) for row in bridge_rows}
    if len(dim_keys) != len(set(dim_keys)) or len(bridge_keys) != len(set(bridge_keys)):
        raise SilverStoreError("S3 published dim or bridge primary key is duplicated")
    if set(dim_keys) != bridge_parents:
        raise SilverStoreError("S3 bridge parent coverage is not exact")


def _first_xnys_open_after(capture_at: datetime) -> datetime:
    calendar = xcals.get_calendar("XNYS")
    start = capture_at.date() - timedelta(days=1)
    end = capture_at.date() + timedelta(days=14)
    captured = pd.Timestamp(capture_at.astimezone(UTC))
    for session in calendar.sessions_in_range(start.isoformat(), end.isoformat()):
        opening = calendar.session_open(session)
        if opening > captured:
            return opening.to_pydatetime().astimezone(UTC)
    raise SilverStoreError("cannot find XNYS open after S3 source capture")


def _verify_git_checkout(repo_root: Path, git_commit: str) -> None:
    root = repo_root.expanduser().resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise SilverStoreError("S3 lifecycle code is not executing from --repo-root") from exc
    if module_relative != "backend/ame_stocks_api/silver/condition_code_lifecycle.py":
        raise SilverStoreError("S3 lifecycle module path is not canonical")
    if Path(_git(root, "rev-parse", "--show-toplevel")).resolve() != root:
        raise SilverStoreError("S3 --repo-root is not the exact Git top level")
    if _git(root, "rev-parse", "HEAD") != git_commit:
        raise SilverStoreError("S3 Git HEAD differs from --git-commit")
    if _git(root, "status", "--porcelain=v1", "--untracked-files=all"):
        raise SilverStoreError("S3 Git checkout is not clean")
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
        raise SilverStoreError(f"cannot verify S3 Git checkout: {detail}")
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
    "CURRENT_CONDITION_CODE_AUTHORIZATION",
    "S3_COMPLETION_AUTHORIZATION",
    "ConditionCodeLifecycleRun",
    "ConditionCodeTableRun",
    "complete_condition_code_lifecycle",
]
