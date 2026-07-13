"""Immutable Silver registry and explicit human-review workflow."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import (
    safe_relative_path,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.contracts import (
    SEPARATE_FULL_RUN_PLAN_POLICY,
    ApprovalDecision,
    ApprovalReceipt,
    ApprovalStage,
    ArtifactRef,
    ArtifactRole,
    BuildIntent,
    BuildKind,
    BuildManifest,
    FullRunPlan,
    QASeverity,
    QAStatus,
    QuarantineRecord,
    QuarantineReviewStatus,
    ReleaseManifest,
    SilverContractError,
    SourceInventory,
    SourceInventoryItem,
    SourceLayer,
    TableContract,
    arrow_schema_digest,
    ensure_json_safe,
    thaw_json,
)

WORKFLOW_EVENT_VERSION = 1

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_EVENT_FILE = re.compile(r"^(?P<sequence>[0-9]{6})-(?P<sha>[0-9a-f]{64})\.json$")


class SilverStoreError(SilverContractError):
    """Raised when immutable Silver state is incomplete or inconsistent."""


class WorkflowState(StrEnum):
    PLANNED = "planned"
    SCHEMA_REVIEW = "schema_review"
    CODE_READY = "code_ready"
    PREVIEW_READY = "preview_ready"
    AWAITING_REVIEW = "awaiting_review"
    FULL_RUN_PLAN_REVIEW = "full_run_plan_review"
    APPROVED_FULL_RUN = "approved_full_run"
    FULL_READY = "full_ready"
    AWAITING_PUBLISH = "awaiting_publish"
    PUBLISHED = "published"
    FAILED = "failed"
    REJECTED = "rejected"


_FORWARD_TRANSITIONS: dict[WorkflowState, frozenset[WorkflowState]] = {
    WorkflowState.PLANNED: frozenset({WorkflowState.SCHEMA_REVIEW}),
    WorkflowState.SCHEMA_REVIEW: frozenset({WorkflowState.CODE_READY}),
    WorkflowState.CODE_READY: frozenset({WorkflowState.PREVIEW_READY}),
    WorkflowState.PREVIEW_READY: frozenset({WorkflowState.AWAITING_REVIEW}),
    WorkflowState.AWAITING_REVIEW: frozenset(
        {WorkflowState.APPROVED_FULL_RUN, WorkflowState.FULL_RUN_PLAN_REVIEW}
    ),
    WorkflowState.FULL_RUN_PLAN_REVIEW: frozenset({WorkflowState.APPROVED_FULL_RUN}),
    WorkflowState.APPROVED_FULL_RUN: frozenset({WorkflowState.FULL_READY}),
    WorkflowState.FULL_READY: frozenset({WorkflowState.AWAITING_PUBLISH}),
    WorkflowState.AWAITING_PUBLISH: frozenset({WorkflowState.PUBLISHED}),
    WorkflowState.PUBLISHED: frozenset(),
    WorkflowState.FAILED: frozenset(),
    WorkflowState.REJECTED: frozenset(),
}


@dataclass(frozen=True, slots=True)
class StoredDocument:
    path: str
    sha256: str
    bytes: int


@dataclass(frozen=True, slots=True)
class WorkflowEvent:
    workflow_id: str
    sequence: int
    previous_event_sha256: str | None
    from_state: WorkflowState | None
    to_state: WorkflowState
    actor: str
    created_at: str
    evidence: Mapping[str, object]
    note: str

    def __post_init__(self) -> None:
        _digest(self.workflow_id, "workflow_id")
        if type(self.sequence) is not int or self.sequence <= 0:
            raise SilverStoreError("workflow event sequence must be a positive native int")
        if self.previous_event_sha256 is not None:
            _digest(self.previous_event_sha256, "previous_event_sha256")
        if self.from_state is not None and not isinstance(self.from_state, WorkflowState):
            raise SilverStoreError("workflow from_state is invalid")
        if not isinstance(self.to_state, WorkflowState):
            raise SilverStoreError("workflow to_state is invalid")
        _clean_text(self.actor, "actor", maximum=200)
        _utc_text(self.created_at, "created_at")
        _clean_text(self.note, "note", maximum=4_000, allow_empty=True)
        safe_evidence = ensure_json_safe(self.evidence, label="workflow evidence")
        ensure_json_safe({"actor": self.actor, "note": self.note}, label="workflow text")
        object.__setattr__(self, "evidence", safe_evidence)

    def to_dict(self) -> dict[str, object]:
        return {
            "actor": self.actor,
            "created_at": self.created_at,
            "evidence": thaw_json(self.evidence),
            "from_state": None if self.from_state is None else self.from_state.value,
            "note": self.note,
            "previous_event_sha256": self.previous_event_sha256,
            "sequence": self.sequence,
            "to_state": self.to_state.value,
            "workflow_event_version": WORKFLOW_EVENT_VERSION,
            "workflow_id": self.workflow_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> WorkflowEvent:
        document = _object(value, "workflow event")
        expected = {
            "actor",
            "created_at",
            "evidence",
            "from_state",
            "note",
            "previous_event_sha256",
            "sequence",
            "to_state",
            "workflow_event_version",
            "workflow_id",
        }
        _exact_keys(document, expected, "workflow event")
        if document["workflow_event_version"] != WORKFLOW_EVENT_VERSION:
            raise SilverStoreError("unsupported workflow event version")
        try:
            from_state = (
                None if document["from_state"] is None else WorkflowState(document["from_state"])
            )
            to_state = WorkflowState(document["to_state"])
        except (TypeError, ValueError) as exc:
            raise SilverStoreError("workflow state is invalid") from exc
        return cls(
            workflow_id=_text(document["workflow_id"], "workflow_id"),
            sequence=_positive_int(document["sequence"], "sequence"),
            previous_event_sha256=(
                None
                if document["previous_event_sha256"] is None
                else _text(document["previous_event_sha256"], "previous_event_sha256")
            ),
            from_state=from_state,
            to_state=to_state,
            actor=_text(document["actor"], "actor"),
            created_at=_text(document["created_at"], "created_at"),
            evidence=_object(document["evidence"], "workflow evidence"),
            note=_text(document["note"], "note", allow_empty=True),
        )


@dataclass(frozen=True, slots=True)
class WorkflowEventRecord:
    event: WorkflowEvent
    event_sha256: str
    path: str


@dataclass(frozen=True, slots=True)
class WorkflowSnapshot:
    workflow_id: str
    state: WorkflowState
    event_sha256: str
    sequence: int
    event_path: str
    created_at: str
    evidence: Mapping[str, object]


class SilverStore:
    """Persist reviewed Silver evidence without running any data transformation."""

    def __init__(self, data_root: Path) -> None:
        self.root = data_root.expanduser().resolve()

    def register_contract(self, contract: TableContract) -> StoredDocument:
        path = self.contract_path(contract)
        version_directory = path.parent
        with self._directory_lock(version_directory, ".contract.lock"):
            existing = sorted(version_directory.glob("contract-*.json"))
            unexpected = [item for item in existing if item.name != path.name]
            if unexpected:
                raise SilverStoreError(
                    f"schema version already has a different immutable contract: {unexpected[0]}"
                )
            return self._write_document(path, contract.to_dict())

    def register_source_inventory(self, inventory: SourceInventory) -> StoredDocument:
        self._verify_source_inventory(inventory)
        path = (
            self.root
            / "manifests"
            / "silver"
            / "source-inventories"
            / inventory.source_dataset
            / f"inventory-{inventory.inventory_id}.json"
        )
        return self._write_document(path, inventory.to_dict())

    def contract_path(self, contract: TableContract) -> Path:
        return (
            self.root
            / "manifests"
            / "silver"
            / "contracts"
            / contract.domain
            / contract.table
            / f"schema-v{contract.schema_version}"
            / f"contract-{contract.contract_id}.json"
        )

    def full_run_plan_path(self, plan: FullRunPlan) -> Path:
        return (
            self.root
            / "manifests"
            / "silver"
            / "full-run-plans"
            / plan.table
            / f"plan_id={plan.plan_id}"
            / "manifest.json"
        )

    def create_workflow(
        self,
        contract: TableContract,
        *,
        actor: str,
        created_at: str,
        note: str = "",
    ) -> WorkflowSnapshot:
        contract_document = self.register_contract(contract)
        workflow_id = stable_digest(
            {
                "actor": actor,
                "contract_id": contract.contract_id,
                "created_at": created_at,
                "workflow_event_version": WORKFLOW_EVENT_VERSION,
            }
        )
        with self._workflow_lock(workflow_id):
            if self._event_files(workflow_id):
                raise SilverStoreError(f"workflow already exists: {workflow_id}")
            event = WorkflowEvent(
                workflow_id=workflow_id,
                sequence=1,
                previous_event_sha256=None,
                from_state=None,
                to_state=WorkflowState.PLANNED,
                actor=actor,
                created_at=created_at,
                evidence={
                    "contract_id": contract.contract_id,
                    "contract_path": contract_document.path,
                    "contract_sha256": contract_document.sha256,
                },
                note=note,
            )
            record = self._write_event(event)
        return self._snapshot(record)

    def status(self, workflow_id: str) -> WorkflowSnapshot:
        return self.verify_workflow_trust_chain(workflow_id)

    def workflow_events(self, workflow_id: str) -> tuple[WorkflowEventRecord, ...]:
        _digest(workflow_id, "workflow_id")
        records: list[WorkflowEventRecord] = []
        previous: WorkflowEventRecord | None = None
        for path in self._event_files(workflow_id):
            match = _EVENT_FILE.fullmatch(path.name)
            if match is None:  # pragma: no cover - filtered by _event_files
                raise SilverStoreError(f"invalid workflow event filename: {path}")
            document, stored = self._read_document(
                path,
                "workflow event",
                expected_sha256=match.group("sha"),
            )
            checksum = stored.sha256
            event = WorkflowEvent.from_dict(document)
            filename_sequence = int(match.group("sequence"))
            if event.workflow_id != workflow_id or event.sequence != filename_sequence:
                raise SilverStoreError(f"workflow event identity mismatch: {path}")
            if previous is None:
                if (
                    event.sequence != 1
                    or event.previous_event_sha256 is not None
                    or event.from_state is not None
                    or event.to_state is not WorkflowState.PLANNED
                ):
                    raise SilverStoreError("workflow does not start with a valid planned event")
                contract_id = self._evidence_text(event.evidence, "contract_id")
                expected_workflow_id = stable_digest(
                    {
                        "actor": event.actor,
                        "contract_id": contract_id,
                        "created_at": event.created_at,
                        "workflow_event_version": WORKFLOW_EVENT_VERSION,
                    }
                )
                if expected_workflow_id != workflow_id:
                    raise SilverStoreError("workflow ID does not match its creation evidence")
            else:
                if event.sequence != previous.event.sequence + 1:
                    raise SilverStoreError("workflow event sequence is not contiguous")
                if event.previous_event_sha256 != previous.event_sha256:
                    raise SilverStoreError("workflow event hash chain is broken")
                if event.from_state is not previous.event.to_state:
                    raise SilverStoreError("workflow from_state does not match prior state")
                if _utc_datetime(event.created_at, "created_at") < _utc_datetime(
                    previous.event.created_at,
                    "previous created_at",
                ):
                    raise SilverStoreError("workflow event timestamps are not monotonic")
                self._validate_transition(event.from_state, event.to_state)
            record = WorkflowEventRecord(
                event=event,
                event_sha256=checksum,
                path=str(path.relative_to(self.root)),
            )
            records.append(record)
            previous = record
        if not records:
            raise SilverStoreError(f"workflow has no events: {workflow_id}")
        return tuple(records)

    def verify_workflow_trust_chain(
        self,
        workflow_id: str,
        *,
        verify_artifacts: bool = False,
    ) -> WorkflowSnapshot:
        """Validate every gate and referenced immutable object in a workflow."""

        records = self.workflow_events(workflow_id)
        first = records[0]
        self._require_evidence_keys(
            first.event,
            {"contract_id", "contract_path", "contract_sha256"},
        )
        contract_document, contract_stored = self._load_stored_json(
            self._evidence_text(first.event.evidence, "contract_path"),
            self._evidence_text(first.event.evidence, "contract_sha256"),
            "table contract",
        )
        contract = TableContract.from_dict(contract_document)
        if contract.contract_id != self._evidence_text(first.event.evidence, "contract_id"):
            raise SilverStoreError("planned event does not bind the table contract ID")
        expected_contract_path = str(self.contract_path(contract).relative_to(self.root))
        if contract_stored.path != expected_contract_path:
            raise SilverStoreError("planned event contract path is not canonical")

        preview: BuildManifest | None = None
        preview_stored: StoredDocument | None = None
        full_run_plan: FullRunPlan | None = None
        full_run_plan_stored: StoredDocument | None = None
        full: BuildManifest | None = None
        full_stored: StoredDocument | None = None
        for index, record in enumerate(records[1:], start=1):
            event = record.event
            previous = records[index - 1]
            if event.to_state is WorkflowState.SCHEMA_REVIEW:
                self._require_evidence_keys(event, set())
            elif event.to_state is WorkflowState.CODE_READY:
                self._require_evidence_keys(
                    event,
                    {"approval_id", "approval_path", "approval_sha256"},
                )
                approval, approval_stored = self.load_approval(
                    self._evidence_text(event.evidence, "approval_id")
                )
                self._validate_approval_event(
                    approval,
                    approval_stored,
                    event,
                    previous,
                    expected_stage=ApprovalStage.SCHEMA,
                    expected_subject_id=contract.contract_id,
                    expected_subject_sha256=contract_stored.sha256,
                )
            elif event.to_state is WorkflowState.PREVIEW_READY:
                self._require_evidence_keys(
                    event,
                    {
                        "build_id",
                        "build_kind",
                        "build_manifest_path",
                        "build_manifest_sha256",
                    },
                )
                preview, preview_stored = self._load_event_build(event, contract)
                if preview.intent.kind is not BuildKind.PREVIEW:
                    raise SilverStoreError("preview_ready event references a non-preview build")
                self._validate_build_event_time(preview, event, previous)
                if verify_artifacts:
                    self.verify_build(preview, contract)
            elif event.to_state is WorkflowState.AWAITING_REVIEW:
                self._require_evidence_keys(event, set())
            elif event.to_state is WorkflowState.FULL_RUN_PLAN_REVIEW:
                if preview is None or preview_stored is None:
                    raise SilverStoreError("full-run plan has no preview build evidence")
                self._require_evidence_keys(
                    event,
                    {
                        "full_run_plan_id",
                        "full_run_plan_path",
                        "full_run_plan_sha256",
                        "reviewed_preview_build_id",
                        "reviewed_preview_event_sha256",
                        "reviewed_preview_manifest_sha256",
                    },
                )
                full_run_plan, full_run_plan_stored = self.load_full_run_plan(
                    contract.table,
                    self._evidence_text(event.evidence, "full_run_plan_id"),
                )
                if (
                    self._evidence_text(event.evidence, "full_run_plan_path")
                    != full_run_plan_stored.path
                    or self._evidence_text(event.evidence, "full_run_plan_sha256")
                    != full_run_plan_stored.sha256
                ):
                    raise SilverStoreError("full-run plan event does not bind the plan document")
                self._validate_full_run_plan_context(
                    full_run_plan,
                    preview,
                    preview_stored,
                    reviewed_preview_event_sha256=previous.event_sha256,
                    contract=contract,
                )
                if (
                    self._evidence_text(event.evidence, "reviewed_preview_build_id")
                    != full_run_plan.reviewed_preview_build_id
                    or self._evidence_text(
                        event.evidence,
                        "reviewed_preview_manifest_sha256",
                    )
                    != full_run_plan.reviewed_preview_manifest_sha256
                    or self._evidence_text(
                        event.evidence,
                        "reviewed_preview_event_sha256",
                    )
                    != full_run_plan.reviewed_preview_event_sha256
                ):
                    raise SilverStoreError("full-run plan event does not bind the reviewed preview")
                if verify_artifacts:
                    self.verify_source_artifacts(full_run_plan.inputs, contract)
            elif event.to_state is WorkflowState.APPROVED_FULL_RUN:
                if preview is None or preview_stored is None:
                    raise SilverStoreError("full-run approval has no preview build evidence")
                plan_branch = previous.event.to_state is WorkflowState.FULL_RUN_PLAN_REVIEW
                expected_evidence = {
                    "approval_id",
                    "approval_path",
                    "approval_sha256",
                    "approved_preview_build_id",
                    "approved_preview_manifest_sha256",
                }
                if plan_branch:
                    expected_evidence.update(
                        {
                            "approved_full_run_plan_id",
                            "approved_full_run_plan_sha256",
                        }
                    )
                self._require_evidence_keys(event, expected_evidence)
                if (
                    self._evidence_text(event.evidence, "approved_preview_build_id")
                    != preview.build_id
                    or self._evidence_text(
                        event.evidence,
                        "approved_preview_manifest_sha256",
                    )
                    != preview_stored.sha256
                ):
                    raise SilverStoreError("full-run approval evidence does not match preview")
                approval, approval_stored = self.load_approval(
                    self._evidence_text(event.evidence, "approval_id")
                )
                if plan_branch:
                    if full_run_plan is None or full_run_plan_stored is None:
                        raise SilverStoreError("full-run approval has no plan evidence")
                    if (
                        self._evidence_text(event.evidence, "approved_full_run_plan_id")
                        != full_run_plan.plan_id
                        or self._evidence_text(
                            event.evidence,
                            "approved_full_run_plan_sha256",
                        )
                        != full_run_plan_stored.sha256
                    ):
                        raise SilverStoreError("full-run approval evidence does not match plan")
                    expected_subject_id = full_run_plan.plan_id
                    expected_subject_sha256 = full_run_plan_stored.sha256
                else:
                    if self._preview_requires_separate_full_run_plan(preview):
                        raise SilverStoreError(
                            "deferred preview cannot use legacy full-run approval"
                        )
                    expected_subject_id = preview.build_id
                    expected_subject_sha256 = preview_stored.sha256
                self._validate_approval_event(
                    approval,
                    approval_stored,
                    event,
                    previous,
                    expected_stage=ApprovalStage.FULL_RUN,
                    expected_subject_id=expected_subject_id,
                    expected_subject_sha256=expected_subject_sha256,
                )
                self._validate_qa_gate(
                    preview,
                    approval.waived_qa_result_ids,
                    approval.accepted_quarantine_issue_ids,
                )
            elif event.to_state is WorkflowState.FULL_READY:
                if preview is None:
                    raise SilverStoreError("full build has no approved preview evidence")
                self._require_evidence_keys(
                    event,
                    {
                        "build_id",
                        "build_kind",
                        "build_manifest_path",
                        "build_manifest_sha256",
                    },
                )
                full, full_stored = self._load_event_build(event, contract)
                if full.intent.kind is not BuildKind.FULL:
                    raise SilverStoreError("full_ready event references a non-full build")
                self._validate_full_build_binding(full, preview, full_run_plan)
                self._validate_build_event_time(full, event, previous)
                if verify_artifacts:
                    self.verify_build(full, contract)
            elif event.to_state is WorkflowState.AWAITING_PUBLISH:
                self._require_evidence_keys(event, set())
            elif event.to_state is WorkflowState.PUBLISHED:
                if full is None or full_stored is None:
                    raise SilverStoreError("published event has no full build evidence")
                self._require_evidence_keys(
                    event,
                    {
                        "approval_id",
                        "approval_path",
                        "approval_sha256",
                        "build_id",
                        "build_manifest_sha256",
                        "release_id",
                        "release_path",
                        "release_sha256",
                    },
                )
                if (
                    self._evidence_text(event.evidence, "build_id") != full.build_id
                    or self._evidence_text(event.evidence, "build_manifest_sha256")
                    != full_stored.sha256
                ):
                    raise SilverStoreError("published build evidence does not match full build")
                approval, approval_stored = self.load_approval(
                    self._evidence_text(event.evidence, "approval_id")
                )
                self._validate_approval_event(
                    approval,
                    approval_stored,
                    event,
                    previous,
                    expected_stage=ApprovalStage.PUBLISH,
                    expected_subject_id=full.build_id,
                    expected_subject_sha256=full_stored.sha256,
                )
                self._validate_qa_gate(
                    full,
                    approval.waived_qa_result_ids,
                    approval.accepted_quarantine_issue_ids,
                )
                release, release_stored = self.load_release(
                    self._evidence_text(event.evidence, "release_id")
                )
                self._validate_release_event(
                    release,
                    release_stored,
                    approval,
                    approval_stored,
                    full,
                    full_stored,
                    contract,
                    event,
                )
            elif event.to_state is WorkflowState.FAILED:
                self._require_evidence_keys(event, {"failure_code"})
            elif event.to_state is WorkflowState.REJECTED:
                self._require_evidence_keys(event, {"reason_code"})
            else:  # pragma: no cover - enum and transition validation guard this
                raise SilverStoreError(f"unhandled workflow state: {event.to_state.value}")
        return self._snapshot(records[-1])

    def submit_schema_review(
        self,
        workflow_id: str,
        *,
        expected_event_sha256: str,
        actor: str,
        created_at: str,
        note: str = "",
    ) -> WorkflowSnapshot:
        return self._transition(
            workflow_id,
            expected_event_sha256=expected_event_sha256,
            required_state=WorkflowState.PLANNED,
            next_state=WorkflowState.SCHEMA_REVIEW,
            actor=actor,
            created_at=created_at,
            evidence={},
            note=note,
        )

    def _load_event_build(
        self,
        event: WorkflowEvent,
        contract: TableContract,
    ) -> tuple[BuildManifest, StoredDocument]:
        build_id = self._evidence_text(event.evidence, "build_id")
        build, stored = self.load_build(contract.table, build_id)
        if self._evidence_text(event.evidence, "build_kind") != build.intent.kind.value:
            raise SilverStoreError("workflow build kind does not match the manifest")
        if self._evidence_text(event.evidence, "build_manifest_path") != stored.path:
            raise SilverStoreError("workflow build path does not match the canonical manifest")
        if self._evidence_text(event.evidence, "build_manifest_sha256") != stored.sha256:
            raise SilverStoreError("workflow build SHA does not match the manifest")
        self.validate_build_manifest(build, contract, workflow_id=event.workflow_id)
        return build, stored

    def _validate_approval_event(
        self,
        approval: ApprovalReceipt,
        stored: StoredDocument,
        event: WorkflowEvent,
        previous: WorkflowEventRecord,
        *,
        expected_stage: ApprovalStage,
        expected_subject_id: str,
        expected_subject_sha256: str,
    ) -> None:
        evidence = event.evidence
        if (
            self._evidence_text(evidence, "approval_id") != approval.approval_id
            or self._evidence_text(evidence, "approval_path") != stored.path
            or self._evidence_text(evidence, "approval_sha256") != stored.sha256
        ):
            raise SilverStoreError("approval evidence does not match its immutable receipt")
        if (
            approval.workflow_id != event.workflow_id
            or approval.stage is not expected_stage
            or approval.decision is not ApprovalDecision.APPROVED
            or approval.subject_id != expected_subject_id
            or approval.subject_manifest_sha256 != expected_subject_sha256
            or approval.expected_event_sha256 != previous.event_sha256
            or approval.approver != event.actor
            or approval.decided_at != event.created_at
            or approval.note != event.note
        ):
            raise SilverStoreError("approval receipt is not bound to its workflow event")
        if expected_stage is ApprovalStage.SCHEMA and (
            approval.waived_qa_result_ids or approval.accepted_quarantine_issue_ids
        ):
            raise SilverStoreError("schema approvals cannot accept QA/quarantine exceptions")

    def _validate_build_event_time(
        self,
        build: BuildManifest,
        event: WorkflowEvent,
        previous: WorkflowEventRecord,
    ) -> None:
        if _utc_datetime(build.started_at, "build started_at") < _utc_datetime(
            previous.event.created_at,
            "authorizing event created_at",
        ):
            raise SilverStoreError("build started before its authorizing workflow event")
        if _utc_datetime(build.completed_at, "build completed_at") > _utc_datetime(
            event.created_at,
            "build event created_at",
        ):
            raise SilverStoreError("build event predates build completion")

    def _validate_full_build_binding(
        self,
        full: BuildManifest,
        preview: BuildManifest,
        plan: FullRunPlan | None = None,
    ) -> None:
        if preview.preview is None:
            raise SilverStoreError("approved preview metadata is missing")
        if full.intent.parameters.get("approved_preview_build_id") != preview.build_id:
            raise SilverStoreError("full build does not bind the approved preview_build_id")
        if plan is not None:
            if (
                full.intent.parameters.get("approved_full_run_plan_id")
                != plan.plan_id
            ):
                raise SilverStoreError("full build does not bind the approved full-run plan ID")
            if (
                full.intent.workflow_id != plan.workflow_id
                or full.intent.domain != plan.domain
                or full.intent.table != plan.table
                or full.intent.schema_version != plan.schema_version
                or full.intent.contract_id != plan.contract_id
            ):
                raise SilverStoreError(
                    "full build identity differs from the approved full-run plan"
                )
            if full.intent.source_digest != plan.source_digest:
                raise SilverStoreError(
                    "full build inputs differ from the approved full-run plan inventory"
                )
            if (
                full.intent.git_commit != plan.git_commit
                or full.intent.transform_version != plan.transform_version
                or full.intent.exchange_calendar_version != plan.exchange_calendar_version
            ):
                raise SilverStoreError(
                    "full build code and calendar versions differ from the approved full-run plan"
                )
            full_parameters = dict(full.intent.parameters)
            full_parameters.pop("approved_preview_build_id", None)
            full_parameters.pop("approved_full_run_plan_id", None)
            if full_parameters != dict(plan.parameters):
                raise SilverStoreError(
                    "full build logic parameters differ from the approved full-run plan"
                )
            return
        if full.intent.source_digest != preview.preview.full_run_source_digest:
            raise SilverStoreError(
                "full build inputs differ from the approved full-run source inventory"
            )
        if (
            full.intent.git_commit != preview.intent.git_commit
            or full.intent.transform_version != preview.intent.transform_version
            or full.intent.exchange_calendar_version != preview.intent.exchange_calendar_version
        ):
            raise SilverStoreError(
                "full build code and calendar versions must match the approved preview"
            )
        full_parameters = dict(full.intent.parameters)
        full_parameters.pop("approved_preview_build_id", None)
        if full_parameters != dict(preview.intent.parameters):
            raise SilverStoreError("full build logic parameters must match the approved preview")

    def _validate_full_run_plan_context(
        self,
        plan: FullRunPlan,
        preview: BuildManifest,
        preview_stored: StoredDocument,
        *,
        reviewed_preview_event_sha256: str,
        contract: TableContract,
    ) -> None:
        if preview.preview is None:
            raise SilverStoreError("reviewed preview metadata is missing")
        if (
            self._preview_scope_policy(preview) != SEPARATE_FULL_RUN_PLAN_POLICY
            or plan.parameters.get("full_run_scope_policy")
            != SEPARATE_FULL_RUN_PLAN_POLICY
        ):
            raise SilverStoreError("preview does not authorize a separate full-run plan")
        if (
            plan.workflow_id != preview.intent.workflow_id
            or plan.domain != contract.domain
            or plan.table != contract.table
            or plan.schema_version != contract.schema_version
            or plan.contract_id != contract.contract_id
        ):
            raise SilverStoreError("full-run plan does not match the workflow contract")
        if (
            plan.reviewed_preview_build_id != preview.build_id
            or plan.reviewed_preview_manifest_sha256 != preview_stored.sha256
            or plan.reviewed_preview_event_sha256 != reviewed_preview_event_sha256
        ):
            raise SilverStoreError("full-run plan does not bind the reviewed preview checkpoint")

    @staticmethod
    def _preview_requires_separate_full_run_plan(preview: BuildManifest) -> bool:
        return SilverStore._preview_scope_policy(preview) == SEPARATE_FULL_RUN_PLAN_POLICY

    @staticmethod
    def _preview_scope_policy(preview: BuildManifest) -> str | None:
        if preview.intent.kind is not BuildKind.PREVIEW or preview.preview is None:
            raise SilverStoreError("preview scope policy requires preview metadata")
        parameter_key = "full_run_scope_policy"
        projection_key = "scope_binding_mode"
        has_parameter = parameter_key in preview.intent.parameters
        has_projection = projection_key in preview.preview.full_run_projection
        if not has_parameter and not has_projection:
            return None
        parameter = preview.intent.parameters.get(parameter_key)
        projection = preview.preview.full_run_projection.get(projection_key)
        if (
            has_parameter
            and has_projection
            and parameter == SEPARATE_FULL_RUN_PLAN_POLICY
            and projection == SEPARATE_FULL_RUN_PLAN_POLICY
        ):
            return SEPARATE_FULL_RUN_PLAN_POLICY
        raise SilverStoreError(
            "preview full-run scope policy is incomplete, mismatched, or unsupported"
        )

    def _validate_release_event(
        self,
        release: ReleaseManifest,
        release_stored: StoredDocument,
        approval: ApprovalReceipt,
        approval_stored: StoredDocument,
        full: BuildManifest,
        full_stored: StoredDocument,
        contract: TableContract,
        event: WorkflowEvent,
    ) -> None:
        evidence = event.evidence
        if (
            self._evidence_text(evidence, "release_path") != release_stored.path
            or self._evidence_text(evidence, "release_sha256") != release_stored.sha256
        ):
            raise SilverStoreError("published event does not bind the release document")
        if (
            release.workflow_id != event.workflow_id
            or release.domain != contract.domain
            or release.table != contract.table
            or release.schema_version != contract.schema_version
            or release.contract_id != contract.contract_id
            or release.build_id != full.build_id
            or release.build_manifest_sha256 != full_stored.sha256
            or release.approval_id != approval.approval_id
            or release.approval_sha256 != approval_stored.sha256
            or release.released_at != event.created_at
        ):
            raise SilverStoreError("release is not bound to the approved workflow objects")
        expected_outputs = tuple(
            output for output in full.outputs if output.role is ArtifactRole.DATA
        )
        if _artifact_identity_set(release.outputs) != _artifact_identity_set(expected_outputs):
            raise SilverStoreError("release outputs differ from the reviewed full build")

    @staticmethod
    def _require_evidence_keys(event: WorkflowEvent, expected: set[str]) -> None:
        actual = set(event.evidence)
        if actual != expected:
            raise SilverStoreError(
                f"{event.to_state.value} evidence keys mismatch: "
                f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
            )

    def approve_schema(
        self,
        workflow_id: str,
        *,
        expected_event_sha256: str,
        approver: str,
        decided_at: str,
        note: str = "",
    ) -> WorkflowSnapshot:
        with self._workflow_lock(workflow_id):
            current = self._require_current(
                workflow_id,
                expected_event_sha256,
                WorkflowState.SCHEMA_REVIEW,
            )
            contract, contract_document = self.load_workflow_contract(workflow_id)
            receipt = ApprovalReceipt(
                workflow_id=workflow_id,
                stage=ApprovalStage.SCHEMA,
                decision=ApprovalDecision.APPROVED,
                subject_id=contract.contract_id,
                subject_manifest_sha256=contract_document.sha256,
                expected_event_sha256=current.event_sha256,
                approver=approver,
                decided_at=decided_at,
                note=note,
            )
            approval_document = self._store_approval(receipt)
            record = self._append_event(
                current,
                next_state=WorkflowState.CODE_READY,
                actor=approver,
                created_at=decided_at,
                evidence={
                    "approval_id": receipt.approval_id,
                    "approval_path": approval_document.path,
                    "approval_sha256": approval_document.sha256,
                },
                note=note,
            )
        return self._snapshot(record)

    def record_preview_build(
        self,
        manifest: BuildManifest,
        *,
        expected_event_sha256: str,
        actor: str,
        recorded_at: str,
        note: str = "",
    ) -> WorkflowSnapshot:
        return self._record_build(
            manifest,
            expected_event_sha256=expected_event_sha256,
            required_state=WorkflowState.CODE_READY,
            next_state=WorkflowState.PREVIEW_READY,
            kind=BuildKind.PREVIEW,
            actor=actor,
            recorded_at=recorded_at,
            note=note,
        )

    def request_preview_review(
        self,
        workflow_id: str,
        *,
        expected_event_sha256: str,
        actor: str,
        created_at: str,
        note: str = "",
    ) -> WorkflowSnapshot:
        return self._transition(
            workflow_id,
            expected_event_sha256=expected_event_sha256,
            required_state=WorkflowState.PREVIEW_READY,
            next_state=WorkflowState.AWAITING_REVIEW,
            actor=actor,
            created_at=created_at,
            evidence={},
            note=note,
        )

    def record_full_run_plan(
        self,
        plan: FullRunPlan,
        *,
        expected_event_sha256: str,
        actor: str,
        recorded_at: str,
        note: str = "",
    ) -> WorkflowSnapshot:
        """Freeze a future full scope for a separate human approval checkpoint."""

        workflow_id = plan.workflow_id
        with self._workflow_lock(workflow_id):
            current = self._require_current(
                workflow_id,
                expected_event_sha256,
                WorkflowState.AWAITING_REVIEW,
            )
            preview, preview_stored = self._latest_build(workflow_id, BuildKind.PREVIEW)
            contract, _ = self.load_workflow_contract(workflow_id)
            self._validate_full_run_plan_context(
                plan,
                preview,
                preview_stored,
                reviewed_preview_event_sha256=current.event_sha256,
                contract=contract,
            )
            self.verify_source_artifacts(plan.inputs, contract)
            plan_document = self._store_full_run_plan(plan)
            record = self._append_event(
                current,
                next_state=WorkflowState.FULL_RUN_PLAN_REVIEW,
                actor=actor,
                created_at=recorded_at,
                evidence={
                    "full_run_plan_id": plan.plan_id,
                    "full_run_plan_path": plan_document.path,
                    "full_run_plan_sha256": plan_document.sha256,
                    "reviewed_preview_build_id": plan.reviewed_preview_build_id,
                    "reviewed_preview_event_sha256": plan.reviewed_preview_event_sha256,
                    "reviewed_preview_manifest_sha256": (
                        plan.reviewed_preview_manifest_sha256
                    ),
                },
                note=note,
            )
        return self._snapshot(record)

    def approve_full_run(
        self,
        workflow_id: str,
        *,
        expected_event_sha256: str,
        approver: str,
        decided_at: str,
        note: str = "",
        waived_qa_result_ids: tuple[str, ...] = (),
        accepted_quarantine_issue_ids: tuple[str, ...] = (),
    ) -> WorkflowSnapshot:
        with self._workflow_lock(workflow_id):
            current = self._require_current(
                workflow_id,
                expected_event_sha256,
                WorkflowState.AWAITING_REVIEW,
            )
            build, build_document = self._latest_build(workflow_id, BuildKind.PREVIEW)
            if self._preview_requires_separate_full_run_plan(build):
                raise SilverStoreError(
                    "preview requires a separately reviewed full-run plan"
                )
            self._validate_qa_gate(
                build,
                waived_qa_result_ids,
                accepted_quarantine_issue_ids,
            )
            receipt = ApprovalReceipt(
                workflow_id=workflow_id,
                stage=ApprovalStage.FULL_RUN,
                decision=ApprovalDecision.APPROVED,
                subject_id=build.build_id,
                subject_manifest_sha256=build_document.sha256,
                expected_event_sha256=current.event_sha256,
                approver=approver,
                decided_at=decided_at,
                note=note,
                waived_qa_result_ids=waived_qa_result_ids,
                accepted_quarantine_issue_ids=accepted_quarantine_issue_ids,
            )
            approval_document = self._store_approval(receipt)
            record = self._append_event(
                current,
                next_state=WorkflowState.APPROVED_FULL_RUN,
                actor=approver,
                created_at=decided_at,
                evidence={
                    "approval_id": receipt.approval_id,
                    "approval_path": approval_document.path,
                    "approval_sha256": approval_document.sha256,
                    "approved_preview_build_id": build.build_id,
                    "approved_preview_manifest_sha256": build_document.sha256,
                },
                note=note,
            )
        return self._snapshot(record)

    def approve_full_run_plan(
        self,
        workflow_id: str,
        *,
        expected_event_sha256: str,
        expected_plan_id: str,
        expected_plan_sha256: str,
        approver: str,
        decided_at: str,
        note: str = "",
        waived_qa_result_ids: tuple[str, ...] = (),
        accepted_quarantine_issue_ids: tuple[str, ...] = (),
    ) -> WorkflowSnapshot:
        """Approve the exact immutable plan shown at full_run_plan_review."""

        _digest(expected_plan_id, "expected full-run plan ID")
        _digest(expected_plan_sha256, "expected full-run plan SHA-256")
        with self._workflow_lock(workflow_id):
            current = self._require_current(
                workflow_id,
                expected_event_sha256,
                WorkflowState.FULL_RUN_PLAN_REVIEW,
            )
            if (
                self._evidence_text(current.evidence, "full_run_plan_id")
                != expected_plan_id
                or self._evidence_text(current.evidence, "full_run_plan_sha256")
                != expected_plan_sha256
            ):
                raise SilverStoreError(
                    "explicit full-run plan ID/SHA does not match the reviewed plan"
                )
            contract, _ = self.load_workflow_contract(workflow_id)
            plan, plan_document = self.load_full_run_plan(
                contract.table,
                self._evidence_text(current.evidence, "full_run_plan_id"),
            )
            if (
                self._evidence_text(current.evidence, "full_run_plan_path")
                != plan_document.path
                or self._evidence_text(current.evidence, "full_run_plan_sha256")
                != plan_document.sha256
            ):
                raise SilverStoreError("full-run plan review evidence does not match plan")
            preview, preview_document = self._latest_build(workflow_id, BuildKind.PREVIEW)
            self._validate_full_run_plan_context(
                plan,
                preview,
                preview_document,
                reviewed_preview_event_sha256=plan.reviewed_preview_event_sha256,
                contract=contract,
            )
            self.verify_source_artifacts(plan.inputs, contract)
            self._validate_qa_gate(
                preview,
                waived_qa_result_ids,
                accepted_quarantine_issue_ids,
            )
            receipt = ApprovalReceipt(
                workflow_id=workflow_id,
                stage=ApprovalStage.FULL_RUN,
                decision=ApprovalDecision.APPROVED,
                subject_id=plan.plan_id,
                subject_manifest_sha256=plan_document.sha256,
                expected_event_sha256=current.event_sha256,
                approver=approver,
                decided_at=decided_at,
                note=note,
                waived_qa_result_ids=waived_qa_result_ids,
                accepted_quarantine_issue_ids=accepted_quarantine_issue_ids,
            )
            approval_document = self._store_approval(receipt)
            record = self._append_event(
                current,
                next_state=WorkflowState.APPROVED_FULL_RUN,
                actor=approver,
                created_at=decided_at,
                evidence={
                    "approval_id": receipt.approval_id,
                    "approval_path": approval_document.path,
                    "approval_sha256": approval_document.sha256,
                    "approved_full_run_plan_id": plan.plan_id,
                    "approved_full_run_plan_sha256": plan_document.sha256,
                    "approved_preview_build_id": preview.build_id,
                    "approved_preview_manifest_sha256": preview_document.sha256,
                },
                note=note,
            )
        return self._snapshot(record)

    def record_full_build(
        self,
        manifest: BuildManifest,
        *,
        expected_event_sha256: str,
        actor: str,
        recorded_at: str,
        note: str = "",
    ) -> WorkflowSnapshot:
        return self._record_build(
            manifest,
            expected_event_sha256=expected_event_sha256,
            required_state=WorkflowState.APPROVED_FULL_RUN,
            next_state=WorkflowState.FULL_READY,
            kind=BuildKind.FULL,
            actor=actor,
            recorded_at=recorded_at,
            note=note,
        )

    def request_publish(
        self,
        workflow_id: str,
        *,
        expected_event_sha256: str,
        actor: str,
        created_at: str,
        note: str = "",
    ) -> WorkflowSnapshot:
        return self._transition(
            workflow_id,
            expected_event_sha256=expected_event_sha256,
            required_state=WorkflowState.FULL_READY,
            next_state=WorkflowState.AWAITING_PUBLISH,
            actor=actor,
            created_at=created_at,
            evidence={},
            note=note,
        )

    def publish(
        self,
        workflow_id: str,
        *,
        expected_event_sha256: str,
        approver: str,
        decided_at: str,
        note: str = "",
        waived_qa_result_ids: tuple[str, ...] = (),
        accepted_quarantine_issue_ids: tuple[str, ...] = (),
    ) -> tuple[WorkflowSnapshot, ReleaseManifest]:
        with self._workflow_lock(workflow_id):
            current = self._require_current(
                workflow_id,
                expected_event_sha256,
                WorkflowState.AWAITING_PUBLISH,
            )
            contract, _ = self.load_workflow_contract(workflow_id)
            build, build_document = self._latest_build(workflow_id, BuildKind.FULL)
            self._validate_qa_gate(
                build,
                waived_qa_result_ids,
                accepted_quarantine_issue_ids,
            )
            receipt = ApprovalReceipt(
                workflow_id=workflow_id,
                stage=ApprovalStage.PUBLISH,
                decision=ApprovalDecision.APPROVED,
                subject_id=build.build_id,
                subject_manifest_sha256=build_document.sha256,
                expected_event_sha256=current.event_sha256,
                approver=approver,
                decided_at=decided_at,
                note=note,
                waived_qa_result_ids=waived_qa_result_ids,
                accepted_quarantine_issue_ids=accepted_quarantine_issue_ids,
            )
            approval_document = self._store_approval(receipt)
            release = ReleaseManifest(
                workflow_id=workflow_id,
                domain=contract.domain,
                table=contract.table,
                schema_version=contract.schema_version,
                contract_id=contract.contract_id,
                build_id=build.build_id,
                build_manifest_sha256=build_document.sha256,
                approval_id=receipt.approval_id,
                approval_sha256=approval_document.sha256,
                released_at=decided_at,
                outputs=tuple(
                    output for output in build.outputs if output.role is ArtifactRole.DATA
                ),
            )
            release_document = self._store_release(release)
            record = self._append_event(
                current,
                next_state=WorkflowState.PUBLISHED,
                actor=approver,
                created_at=decided_at,
                evidence={
                    "approval_id": receipt.approval_id,
                    "approval_path": approval_document.path,
                    "approval_sha256": approval_document.sha256,
                    "build_id": build.build_id,
                    "build_manifest_sha256": build_document.sha256,
                    "release_id": release.release_id,
                    "release_path": release_document.path,
                    "release_sha256": release_document.sha256,
                },
                note=note,
            )
        return self._snapshot(record), release

    def fail(
        self,
        workflow_id: str,
        *,
        expected_event_sha256: str,
        actor: str,
        created_at: str,
        failure_code: str,
        note: str,
    ) -> WorkflowSnapshot:
        return self._terminal_transition(
            workflow_id,
            expected_event_sha256=expected_event_sha256,
            next_state=WorkflowState.FAILED,
            actor=actor,
            created_at=created_at,
            evidence={"failure_code": _clean_code(failure_code)},
            note=note,
        )

    def reject(
        self,
        workflow_id: str,
        *,
        expected_event_sha256: str,
        actor: str,
        created_at: str,
        reason_code: str,
        note: str,
    ) -> WorkflowSnapshot:
        return self._terminal_transition(
            workflow_id,
            expected_event_sha256=expected_event_sha256,
            next_state=WorkflowState.REJECTED,
            actor=actor,
            created_at=created_at,
            evidence={"reason_code": _clean_code(reason_code)},
            note=note,
        )

    def load_workflow_contract(self, workflow_id: str) -> tuple[TableContract, StoredDocument]:
        first = self.workflow_events(workflow_id)[0]
        evidence = first.event.evidence
        contract_path = self._evidence_text(evidence, "contract_path")
        document, stored = self._load_stored_json(
            contract_path,
            self._evidence_text(evidence, "contract_sha256"),
            "table contract",
        )
        contract = TableContract.from_dict(document)
        if contract.contract_id != self._evidence_text(evidence, "contract_id"):
            raise SilverStoreError("workflow contract ID does not match its evidence")
        return contract, stored

    def load_build(self, table: str, build_id: str) -> tuple[BuildManifest, StoredDocument]:
        _clean_code(table)
        _digest(build_id, "build_id")
        path = (
            self.root
            / "manifests"
            / "silver"
            / "builds"
            / table
            / f"build_id={build_id}"
            / "manifest.json"
        )
        document, stored = self._read_document(path, "build manifest")
        build = BuildManifest.from_dict(document)
        if build.build_id != build_id or build.intent.table != table:
            raise SilverStoreError("build path identity does not match the manifest")
        return build, stored

    def load_full_run_plan(
        self,
        table: str,
        plan_id: str,
    ) -> tuple[FullRunPlan, StoredDocument]:
        _clean_code(table)
        _digest(plan_id, "full-run plan ID")
        path = (
            self.root
            / "manifests"
            / "silver"
            / "full-run-plans"
            / table
            / f"plan_id={plan_id}"
            / "manifest.json"
        )
        document, stored = self._read_document(path, "full-run plan")
        plan = FullRunPlan.from_dict(document)
        if (
            plan.plan_id != plan_id
            or plan.table != table
            or path != self.full_run_plan_path(plan)
        ):
            raise SilverStoreError("full-run plan path identity does not match the manifest")
        return plan, stored

    def load_approval(self, approval_id: str) -> tuple[ApprovalReceipt, StoredDocument]:
        _digest(approval_id, "approval_id")
        path = self.root / "manifests" / "silver" / "approvals" / f"{approval_id}.json"
        document, stored = self._read_document(path, "approval receipt")
        receipt = ApprovalReceipt.from_dict(document)
        if receipt.approval_id != approval_id:
            raise SilverStoreError("approval path identity does not match the receipt")
        return receipt, stored

    def load_release(self, release_id: str) -> tuple[ReleaseManifest, StoredDocument]:
        _digest(release_id, "release_id")
        path = self.root / "manifests" / "silver" / "releases" / f"release_id={release_id}.json"
        document, stored = self._read_document(path, "release manifest")
        release = ReleaseManifest.from_dict(document)
        if release.release_id != release_id:
            raise SilverStoreError("release path identity does not match the manifest")
        return release, stored

    def verify_build(self, manifest: BuildManifest, contract: TableContract) -> None:
        self.validate_build_manifest(manifest, contract)
        intent = manifest.intent
        source_map = {stable_digest(item.to_dict()): item for item in intent.inputs}
        if manifest.preview is not None:
            for source in manifest.preview.full_run_inputs:
                source_map.setdefault(stable_digest(source.to_dict()), source)
        self.verify_source_artifacts(tuple(source_map.values()), contract)
        expected_prefix = self.build_output_prefix(intent)
        prefix_path = safe_relative_path(self.root, expected_prefix)
        if not prefix_path.is_dir():
            raise SilverStoreError(f"build output directory is missing: {prefix_path}")
        data_count = 0
        quarantine_rows = 0
        quarantine_source_ids: set[str] = set()
        quarantine_issue_ids: dict[str, set[str]] = {
            severity.value: set() for severity in QASeverity
        }
        qa_output_rows: list[dict[str, object]] = []
        for output in manifest.outputs:
            if not _is_relative_to(output.path, expected_prefix):
                raise SilverStoreError(
                    f"build output is outside its immutable build directory: {output.path}"
                )
            output_path = self.verify_artifact(
                output,
                contract=contract if output.role is ArtifactRole.DATA else None,
            )
            if output.role is ArtifactRole.DATA:
                data_count += 1
                if output.table != contract.table:
                    raise SilverStoreError("data artifact table does not match contract table")
            if output.role is ArtifactRole.QUARANTINE:
                quarantine_rows += int(output.row_count or 0)
                quarantine_table = pq.read_table(output_path)
                for row in quarantine_table.to_pylist():
                    record = QuarantineRecord.from_dict(row)
                    if record.detected_build_id != manifest.build_id:
                        raise SilverStoreError("quarantine row is not bound to the detected build")
                    if record.table_name != contract.table:
                        raise SilverStoreError(
                            "quarantine row table does not match the table contract"
                        )
                    if record.review_status is not QuarantineReviewStatus.PENDING:
                        raise SilverStoreError(
                            "build-produced quarantine rows must start pending review"
                        )
                    quarantine_source_ids.add(record.source_record_id)
                    quarantine_issue_ids[record.severity.value].add(record.issue_id)
            if output.role is ArtifactRole.QA:
                qa_output_rows.extend(pq.read_table(output_path).to_pylist())
        if data_count == 0:
            raise SilverStoreError("build must declare at least one data artifact")
        if quarantine_rows != manifest.quarantine_issue_rows:
            raise SilverStoreError("quarantine artifacts do not reconcile to the build manifest")
        if len(quarantine_source_ids) != manifest.quarantine_unique_source_rows:
            raise SilverStoreError("quarantine source IDs do not reconcile to the build manifest")
        declared_issue_ids = {
            severity: set(issue_ids)
            for severity, issue_ids in manifest.quarantine_issue_ids_by_severity.items()
        }
        if quarantine_issue_ids != declared_issue_ids:
            raise SilverStoreError("quarantine issue IDs/severities differ from the build manifest")
        expected_qa_rows = [check.to_output_dict(manifest.build_id) for check in manifest.qa_checks]
        if sorted(qa_output_rows, key=_qa_row_key) != sorted(
            expected_qa_rows,
            key=_qa_row_key,
        ):
            raise SilverStoreError("QA artifact rows differ from embedded QA results")
        declared = {output.path for output in manifest.outputs}
        actual: set[str] = set()
        for path in prefix_path.rglob("*"):
            self._reject_symlink_path(path)
            if path.is_file():
                actual.add(str(path.relative_to(self.root)))
        if actual != declared:
            raise SilverStoreError(
                "build output file set differs from the manifest: "
                f"missing={sorted(declared - actual)}, extra={sorted(actual - declared)}"
            )

    def validate_build_manifest(
        self,
        manifest: BuildManifest,
        contract: TableContract,
        *,
        workflow_id: str | None = None,
    ) -> None:
        intent = manifest.intent
        if workflow_id is not None and intent.workflow_id != workflow_id:
            raise SilverStoreError("build workflow_id does not match workflow evidence")
        if (
            intent.domain != contract.domain
            or intent.table != contract.table
            or intent.schema_version != contract.schema_version
            or intent.contract_id != contract.contract_id
        ):
            raise SilverStoreError("build intent does not match the registered table contract")
        if intent.kind is BuildKind.PREVIEW:
            self._preview_scope_policy(manifest)
        self._validate_retry_lineage(manifest, contract)
        rules = {rule.check_id: rule for rule in contract.qa_rules}
        observed_checks = {check.check_id for check in manifest.qa_checks}
        missing_checks = sorted(set(contract.required_qa_checks).difference(observed_checks))
        if missing_checks:
            raise SilverStoreError(f"build is missing required QA checks: {missing_checks}")
        unexpected_checks = sorted(observed_checks.difference(contract.required_qa_checks))
        if unexpected_checks:
            raise SilverStoreError(
                f"build contains QA checks absent from policy: {unexpected_checks}"
            )
        for check in manifest.qa_checks:
            rule = rules[check.check_id]
            if check.table != contract.table:
                raise SilverStoreError("QA result table does not match the table contract")
            if check.severity is not rule.severity:
                raise SilverStoreError(f"QA severity differs from policy: {check.check_id}")
            if check.threshold != rule.threshold_expression:
                raise SilverStoreError(f"QA threshold differs from policy: {check.check_id}")
            expected_status = rule.expected_status(
                numerator=check.numerator,
                rate=check.rate,
            )
            if check.status is not expected_status:
                raise SilverStoreError(f"QA status differs from evaluated policy: {check.check_id}")

    def _validate_retry_lineage(
        self,
        manifest: BuildManifest,
        contract: TableContract,
    ) -> None:
        intent = manifest.intent
        if intent.retry_of_build_id is None:
            return
        previous, _ = self.load_build(intent.table, intent.retry_of_build_id)
        prior = previous.intent
        if (
            prior.workflow_id != intent.workflow_id
            or prior.kind is not intent.kind
            or prior.contract_id != contract.contract_id
            or prior.schema_version != contract.schema_version
            or intent.attempt != prior.attempt + 1
            or prior.source_digest != intent.source_digest
            or prior.transform_version != intent.transform_version
            or prior.git_commit != intent.git_commit
            or prior.exchange_calendar_version != intent.exchange_calendar_version
            or dict(prior.parameters) != dict(intent.parameters)
        ):
            raise SilverStoreError("retry build does not continue the exact prior logical attempt")

    def validate_qa_gate(
        self,
        build: BuildManifest,
        waived_qa_result_ids: tuple[str, ...],
        accepted_quarantine_issue_ids: tuple[str, ...] = (),
    ) -> None:
        """Validate the exact QA waiver set used by an approval receipt."""

        self._validate_qa_gate(
            build,
            waived_qa_result_ids,
            accepted_quarantine_issue_ids,
        )

    def verify_artifact(
        self,
        artifact: ArtifactRef,
        *,
        contract: TableContract | None,
    ) -> Path:
        path = safe_relative_path(self.root, artifact.path)
        self._reject_symlink_path(path)
        row_count, schema, content = self._inspect_file(
            path,
            expected_bytes=artifact.bytes,
            expected_sha256=artifact.sha256,
            parquet=artifact.media_type == "application/vnd.apache.parquet",
            require_readonly=artifact.role is not ArtifactRole.SOURCE,
            capture_content=(
                artifact.role is ArtifactRole.SAMPLE and artifact.media_type == "application/json"
            ),
        )
        if schema is not None:
            if row_count != artifact.row_count:
                raise SilverStoreError(f"artifact row count mismatch: {path}")
            if arrow_schema_digest(schema) != artifact.schema_digest:
                raise SilverStoreError(f"artifact schema digest mismatch: {path}")
            if contract is not None and schema != contract.arrow_schema:
                raise SilverStoreError(f"data artifact violates its table contract: {path}")
        if content is not None:
            try:
                sample = json.loads(content.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise SilverStoreError(f"sample artifact is not valid JSON: {path}") from exc
            if not isinstance(sample, list) or len(sample) != artifact.row_count:
                raise SilverStoreError(
                    f"sample JSON rows do not match the artifact row_count: {path}"
                )
        return path

    def verify_source_artifact(
        self,
        source: ArtifactRef,
        contract: TableContract,
    ) -> Path:
        self.verify_source_artifacts((source,), contract)
        return safe_relative_path(self.root, source.path)

    def verify_source_artifacts(
        self,
        sources: tuple[ArtifactRef, ...],
        contract: TableContract,
    ) -> None:
        groups: dict[tuple[str, str], list[ArtifactRef]] = {}
        for source in sources:
            if source.role is not ArtifactRole.SOURCE:
                raise SilverStoreError("build source inventory contains a non-source artifact")
            if source.source_dataset not in contract.source_datasets:
                raise SilverStoreError(
                    f"source dataset is absent from the table contract: {source.source_dataset}"
                )
            if not isinstance(source.source_layer, SourceLayer):
                raise SilverStoreError("source artifact has no valid source layer")
            self._validate_source_layer_path(
                source.source_layer,
                str(source.source_dataset),
                source.path,
            )
            if _is_relative_to(source.path, "staging"):
                raise SilverStoreError("unpublished staging artifacts cannot be Silver inputs")
            if source.lineage_manifest_path is None or source.lineage_manifest_sha256 is None:
                raise SilverStoreError("source lineage manifest is missing")
            expected_prefix = f"manifests/silver/source-inventories/{source.source_dataset}"
            if not _is_relative_to(source.lineage_manifest_path, expected_prefix):
                raise SilverStoreError(
                    "source lineage must use the registered Silver source-inventory namespace"
                )
            key = (source.lineage_manifest_path, source.lineage_manifest_sha256)
            groups.setdefault(key, []).append(source)
        for (lineage_path, lineage_sha256), grouped_sources in groups.items():
            lineage, _ = self._load_stored_json(
                lineage_path,
                lineage_sha256,
                "source inventory",
            )
            inventory = SourceInventory.from_dict(lineage)
            self._verify_source_inventory(inventory)
            inventory_items = {item.path: item for item in inventory.artifacts}
            for source in grouped_sources:
                if inventory.source_dataset != source.source_dataset:
                    raise SilverStoreError("source inventory dataset does not match the artifact")
                if inventory.source_layer is not source.source_layer:
                    raise SilverStoreError("source inventory layer does not match the artifact")
                expected_item = SourceInventoryItem(
                    path=source.path,
                    sha256=source.sha256,
                    bytes=source.bytes,
                    row_count=int(source.row_count or 0),
                    media_type=source.media_type,
                    table=source.table,
                    schema_digest=source.schema_digest,
                )
                if inventory_items.get(source.path) != expected_item:
                    raise SilverStoreError(
                        "source inventory does not contain the exact source artifact"
                    )

    def _verify_source_inventory(self, inventory: SourceInventory) -> None:
        upstream_documents: list[dict[str, object]] = []
        for upstream in inventory.upstream_manifests:
            if not _is_relative_to(upstream.path, "manifests"):
                raise SilverStoreError("upstream lineage must use the manifests namespace")
            document, _ = self._load_stored_json(
                upstream.path,
                upstream.sha256,
                "upstream source manifest",
            )
            if not _is_release_manifest_path(upstream.path):
                status_value = document.get("status")
                if status_value not in {"complete", "passed", "passed_with_warnings"}:
                    raise SilverStoreError(
                        "upstream source manifests must have an accepted terminal status"
                    )
            upstream_documents.append(document)
        file_bindings: dict[str, set[tuple[str, int | None]]] = {}
        row_bindings: dict[str, set[int]] = {}
        for document in upstream_documents:
            _index_upstream_bindings(document, file_bindings, row_bindings)
        published_items = self._published_source_items(inventory)
        for item in inventory.artifacts:
            self._validate_source_layer_path(
                inventory.source_layer,
                inventory.source_dataset,
                item.path,
            )
            self._verify_inventory_item(item)
            file_candidates = file_bindings.get(item.path, set())
            if not any(
                checksum == item.sha256 and (size is None or size == item.bytes)
                for checksum, size in file_candidates
            ):
                raise SilverStoreError(
                    "upstream manifests do not bind the inventory path and checksum"
                )
            if item.row_count not in row_bindings.get(item.path, set()):
                raise SilverStoreError("upstream manifests do not bind the inventory row count")
            if _is_relative_to(item.path, "silver") and item not in published_items:
                raise SilverStoreError(
                    "Silver source artifact is not backed by a published release"
                )

    @staticmethod
    def _validate_source_layer_path(
        layer: SourceLayer,
        source_dataset: str,
        path: str,
    ) -> None:
        if layer is SourceLayer.BRONZE:
            valid = _is_relative_to(path, "bronze")
        elif layer is SourceLayer.PUBLISHED_SILVER:
            valid = _is_relative_to(path, "silver")
        elif layer is SourceLayer.SYNTHETIC_FIXTURE:
            valid = source_dataset.startswith("synthetic_") and _is_relative_to(
                path,
                "fixtures",
            )
        else:  # pragma: no cover - enum validation guards this
            valid = False
        if not valid:
            raise SilverStoreError(
                f"source path is outside the declared {layer.value} layer: {path}"
            )

    def _published_source_items(
        self,
        inventory: SourceInventory,
    ) -> set[SourceInventoryItem]:
        published: set[SourceInventoryItem] = set()
        candidates = [
            upstream
            for upstream in inventory.upstream_manifests
            if _is_release_manifest_path(upstream.path)
        ]
        for upstream in candidates:
            release_id = Path(upstream.path).stem.removeprefix("release_id=")
            release, stored = self.load_release(release_id)
            if stored.sha256 != upstream.sha256:
                continue
            self.verify_workflow_trust_chain(release.workflow_id)
            for output in release.outputs:
                published.add(
                    SourceInventoryItem(
                        path=output.path,
                        sha256=output.sha256,
                        bytes=output.bytes,
                        row_count=int(output.row_count or 0),
                        media_type=output.media_type,
                        table=output.table,
                        schema_digest=output.schema_digest,
                    )
                )
        return published

    def _verify_inventory_item(self, item: SourceInventoryItem) -> None:
        path = safe_relative_path(self.root, item.path)
        self._reject_symlink_path(path)
        row_count, schema, _ = self._inspect_file(
            path,
            expected_bytes=item.bytes,
            expected_sha256=item.sha256,
            parquet=item.media_type == "application/vnd.apache.parquet",
            require_readonly=False,
            capture_content=False,
        )
        if schema is not None:
            if row_count != item.row_count:
                raise SilverStoreError(f"source inventory row count mismatch: {path}")
            if arrow_schema_digest(schema) != item.schema_digest:
                raise SilverStoreError(f"source inventory schema mismatch: {path}")

    def _inspect_file(
        self,
        path: Path,
        *,
        expected_bytes: int,
        expected_sha256: str,
        parquet: bool,
        require_readonly: bool,
        capture_content: bool,
    ) -> tuple[int | None, pa.Schema | None, bytes | None]:
        flags = os.O_RDONLY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise SilverStoreError(f"cannot safely open artifact: {path}") from exc
        row_count: int | None = None
        schema: pa.Schema | None = None
        captured_chunks: list[bytes] | None = [] if capture_content else None
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                raise SilverStoreError(f"artifact must be a single-link regular file: {path}")
            if require_readonly and before.st_mode & 0o222:
                raise SilverStoreError(f"immutable output remains writable: {path}")
            if before.st_size != expected_bytes:
                raise SilverStoreError(f"artifact byte count mismatch: {path}")
            digest = hashlib.sha256()
            with os.fdopen(descriptor, "rb", closefd=False) as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
                    if captured_chunks is not None:
                        captured_chunks.append(chunk)
                if parquet:
                    handle.seek(0)
                    try:
                        parquet_file = pq.ParquetFile(handle)
                        row_count = int(parquet_file.metadata.num_rows)
                        schema = parquet_file.schema_arrow
                    except (OSError, pa.ArrowException) as exc:
                        raise SilverStoreError(f"cannot read Parquet artifact: {path}") from exc
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if digest.hexdigest() != expected_sha256:
            raise SilverStoreError(f"artifact checksum mismatch: {path}")
        try:
            path_after = os.lstat(path)
        except OSError as exc:
            raise SilverStoreError(f"artifact path changed during verification: {path}") from exc
        if _stat_identity(before) != _stat_identity(after) or _stat_identity(after) != (
            _stat_identity(path_after)
        ):
            raise SilverStoreError(f"artifact changed during verification: {path}")
        content = None if captured_chunks is None else b"".join(captured_chunks)
        return row_count, schema, content

    @staticmethod
    def build_output_prefix(intent: BuildIntent) -> str:
        if intent.kind is BuildKind.PREVIEW:
            return f"staging/silver/{intent.table}/build_id={intent.build_id}"
        if intent.kind is BuildKind.FULL:
            return (
                f"silver/schema=v{intent.schema_version}/{intent.domain}/{intent.table}/"
                f"build_id={intent.build_id}"
            )
        raise SilverStoreError("build kind is invalid")

    def _record_build(
        self,
        manifest: BuildManifest,
        *,
        expected_event_sha256: str,
        required_state: WorkflowState,
        next_state: WorkflowState,
        kind: BuildKind,
        actor: str,
        recorded_at: str,
        note: str,
    ) -> WorkflowSnapshot:
        workflow_id = manifest.intent.workflow_id
        with self._workflow_lock(workflow_id):
            current = self._require_current(
                workflow_id,
                expected_event_sha256,
                required_state,
            )
            if manifest.intent.kind is not kind:
                raise SilverStoreError(f"expected a {kind.value} build")
            if _utc_datetime(manifest.started_at, "build started_at") < _utc_datetime(
                current.created_at,
                "current event created_at",
            ):
                raise SilverStoreError("build started before its authorizing workflow state")
            if _utc_datetime(manifest.completed_at, "build completed_at") > _utc_datetime(
                recorded_at,
                "recorded_at",
            ):
                raise SilverStoreError("build was recorded before it completed")
            contract, _ = self.load_workflow_contract(workflow_id)
            if kind is BuildKind.FULL:
                preview, preview_stored = self._latest_build(workflow_id, BuildKind.PREVIEW)
                plan: FullRunPlan | None = None
                if "approved_full_run_plan_id" in current.evidence:
                    plan, plan_stored = self.load_full_run_plan(
                        contract.table,
                        self._evidence_text(current.evidence, "approved_full_run_plan_id"),
                    )
                    if (
                        self._evidence_text(
                            current.evidence,
                            "approved_full_run_plan_sha256",
                        )
                        != plan_stored.sha256
                    ):
                        raise SilverStoreError(
                            "approved full-run event does not bind the plan document"
                        )
                    self._validate_full_run_plan_context(
                        plan,
                        preview,
                        preview_stored,
                        reviewed_preview_event_sha256=(
                            plan.reviewed_preview_event_sha256
                        ),
                        contract=contract,
                    )
                self._validate_full_build_binding(manifest, preview, plan)
            self.verify_build(manifest, contract)
            build_document = self._store_build(manifest)
            record = self._append_event(
                current,
                next_state=next_state,
                actor=actor,
                created_at=recorded_at,
                evidence={
                    "build_id": manifest.build_id,
                    "build_kind": kind.value,
                    "build_manifest_path": build_document.path,
                    "build_manifest_sha256": build_document.sha256,
                },
                note=note,
            )
        return self._snapshot(record)

    def _transition(
        self,
        workflow_id: str,
        *,
        expected_event_sha256: str,
        required_state: WorkflowState,
        next_state: WorkflowState,
        actor: str,
        created_at: str,
        evidence: Mapping[str, object],
        note: str,
    ) -> WorkflowSnapshot:
        with self._workflow_lock(workflow_id):
            current = self._require_current(
                workflow_id,
                expected_event_sha256,
                required_state,
            )
            record = self._append_event(
                current,
                next_state=next_state,
                actor=actor,
                created_at=created_at,
                evidence=evidence,
                note=note,
            )
        return self._snapshot(record)

    def _terminal_transition(
        self,
        workflow_id: str,
        *,
        expected_event_sha256: str,
        next_state: WorkflowState,
        actor: str,
        created_at: str,
        evidence: Mapping[str, object],
        note: str,
    ) -> WorkflowSnapshot:
        with self._workflow_lock(workflow_id):
            current = self._require_current(workflow_id, expected_event_sha256, None)
            if current.state in {
                WorkflowState.PUBLISHED,
                WorkflowState.FAILED,
                WorkflowState.REJECTED,
            }:
                raise SilverStoreError(f"workflow is terminal: {current.state.value}")
            record = self._append_event(
                current,
                next_state=next_state,
                actor=actor,
                created_at=created_at,
                evidence=evidence,
                note=note,
                terminal=True,
            )
        return self._snapshot(record)

    def _require_current(
        self,
        workflow_id: str,
        expected_event_sha256: str,
        required_state: WorkflowState | None,
    ) -> WorkflowSnapshot:
        _digest(expected_event_sha256, "expected_event_sha256")
        current = self.status(workflow_id)
        if current.event_sha256 != expected_event_sha256:
            raise SilverStoreError(
                "stale workflow update: expected_event_sha256 is not the current event"
            )
        if required_state is not None and current.state is not required_state:
            raise SilverStoreError(
                f"workflow must be {required_state.value}, got {current.state.value}"
            )
        return current

    def _append_event(
        self,
        current: WorkflowSnapshot,
        *,
        next_state: WorkflowState,
        actor: str,
        created_at: str,
        evidence: Mapping[str, object],
        note: str,
        terminal: bool = False,
    ) -> WorkflowEventRecord:
        if not terminal:
            self._validate_transition(current.state, next_state)
        if _utc_datetime(created_at, "created_at") < _utc_datetime(
            current.created_at,
            "current event created_at",
        ):
            raise SilverStoreError("workflow event timestamps must be monotonic")
        event = WorkflowEvent(
            workflow_id=current.workflow_id,
            sequence=current.sequence + 1,
            previous_event_sha256=current.event_sha256,
            from_state=current.state,
            to_state=next_state,
            actor=actor,
            created_at=created_at,
            evidence=evidence,
            note=note,
        )
        return self._write_event(event)

    def _write_event(self, event: WorkflowEvent) -> WorkflowEventRecord:
        content = _json_bytes(event.to_dict())
        checksum = _sha256_bytes(content)
        path = (
            self.root
            / "manifests"
            / "silver"
            / "workflows"
            / event.workflow_id
            / "events"
            / f"{event.sequence:06d}-{checksum}.json"
        )
        stored = write_bytes_immutable(self.root, path, content)
        return WorkflowEventRecord(
            event=event,
            event_sha256=str(stored["sha256"]),
            path=str(stored["path"]),
        )

    def _store_build(self, manifest: BuildManifest) -> StoredDocument:
        path = (
            self.root
            / "manifests"
            / "silver"
            / "builds"
            / manifest.intent.table
            / f"build_id={manifest.build_id}"
            / "manifest.json"
        )
        return self._write_document(path, manifest.to_dict())

    def _store_full_run_plan(self, plan: FullRunPlan) -> StoredDocument:
        return self._write_document(self.full_run_plan_path(plan), plan.to_dict())

    def _store_approval(self, receipt: ApprovalReceipt) -> StoredDocument:
        path = self.root / "manifests" / "silver" / "approvals" / f"{receipt.approval_id}.json"
        return self._write_document(path, receipt.to_dict())

    def _store_release(self, release: ReleaseManifest) -> StoredDocument:
        path = (
            self.root
            / "manifests"
            / "silver"
            / "releases"
            / f"release_id={release.release_id}.json"
        )
        return self._write_document(path, release.to_dict())

    def _write_document(self, path: Path, document: Mapping[str, object]) -> StoredDocument:
        stored = write_bytes_immutable(self.root, path, _json_bytes(document))
        return StoredDocument(
            path=str(stored["path"]),
            sha256=str(stored["sha256"]),
            bytes=int(stored["bytes"]),
        )

    def _latest_build(
        self, workflow_id: str, kind: BuildKind
    ) -> tuple[BuildManifest, StoredDocument]:
        state = (
            WorkflowState.PREVIEW_READY if kind is BuildKind.PREVIEW else WorkflowState.FULL_READY
        )
        build_id = self._latest_evidence(workflow_id, state, "build_id")
        contract, _ = self.load_workflow_contract(workflow_id)
        build, stored = self.load_build(contract.table, build_id)
        expected_sha = self._latest_evidence(
            workflow_id,
            state,
            "build_manifest_sha256",
        )
        if stored.sha256 != expected_sha or build.intent.kind is not kind:
            raise SilverStoreError("workflow build evidence does not match the build manifest")
        self.verify_build(build, contract)
        return build, stored

    def _latest_evidence(
        self,
        workflow_id: str,
        state: WorkflowState,
        key: str,
    ) -> str:
        for record in reversed(self.workflow_events(workflow_id)):
            if record.event.to_state is state:
                return self._evidence_text(record.event.evidence, key)
        raise SilverStoreError(f"workflow has no {state.value} evidence")

    def _validate_qa_gate(
        self,
        build: BuildManifest,
        waived_qa_result_ids: tuple[str, ...],
        accepted_quarantine_issue_ids: tuple[str, ...],
    ) -> None:
        for item in waived_qa_result_ids:
            _digest(item, "waived QA result ID")
        if len(set(waived_qa_result_ids)) != len(waived_qa_result_ids):
            raise SilverStoreError("waived QA result IDs must be unique")
        blocking = [item for item in build.qa_checks if item.blocks_publish]
        if blocking:
            labels = [f"{item.table}:{item.partition_key}:{item.check_id}" for item in blocking]
            raise SilverStoreError(f"blocking QA failures cannot be waived: {labels}")
        waivable = {
            item.result_id
            for item in build.qa_checks
            if item.status is QAStatus.WARNING
            or (
                item.status is QAStatus.FAILED
                and item.severity in {QASeverity.MEDIUM, QASeverity.LOW}
            )
        }
        supplied = set(waived_qa_result_ids)
        if supplied != waivable:
            raise SilverStoreError(
                "QA waivers must exactly match all non-blocking warning/failure result IDs: "
                f"missing={sorted(waivable - supplied)}, extra={sorted(supplied - waivable)}"
            )
        critical_issues = set(build.quarantine_issue_ids_by_severity["critical"])
        if critical_issues:
            raise SilverStoreError(
                f"critical quarantine issues cannot be accepted: {sorted(critical_issues)}"
            )
        for issue_id in accepted_quarantine_issue_ids:
            _digest(issue_id, "accepted quarantine issue ID")
        if len(set(accepted_quarantine_issue_ids)) != len(accepted_quarantine_issue_ids):
            raise SilverStoreError("accepted quarantine issue IDs must be unique")
        high_issues = set(build.quarantine_issue_ids_by_severity["high"])
        accepted = set(accepted_quarantine_issue_ids)
        if accepted != high_issues:
            raise SilverStoreError(
                "quarantine acceptance must exactly match all high-severity issue IDs: "
                f"missing={sorted(high_issues - accepted)}, "
                f"extra={sorted(accepted - high_issues)}"
            )

    def _load_stored_json(
        self,
        path_text: str,
        expected_sha256: str,
        label: str,
    ) -> tuple[dict[str, object], StoredDocument]:
        _digest(expected_sha256, f"{label} sha256")
        path = safe_relative_path(self.root, path_text)
        return self._read_document(path, label, expected_sha256=expected_sha256)

    def _read_document(
        self,
        path: Path,
        label: str,
        *,
        expected_sha256: str | None = None,
    ) -> tuple[dict[str, object], StoredDocument]:
        self._reject_symlink_path(path)
        flags = os.O_RDONLY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise SilverStoreError(f"cannot read {label}: {path}") from exc
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                raise SilverStoreError(f"{label} is not a single-link regular file: {path}")
            if before.st_size > 128 * 1024 * 1024:
                raise SilverStoreError(f"{label} exceeds the 128 MiB manifest limit")
            with os.fdopen(descriptor, "rb", closefd=False) as handle:
                content = handle.read()
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if _stat_identity(before) != _stat_identity(after) or len(content) != before.st_size:
            raise SilverStoreError(f"{label} changed while it was being read: {path}")
        checksum = _sha256_bytes(content)
        if expected_sha256 is not None and checksum != expected_sha256:
            raise SilverStoreError(f"{label} checksum does not match its evidence")
        try:
            value = json.loads(content.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SilverStoreError(f"cannot parse {label}: {path}") from exc
        return _object(value, label), StoredDocument(
            path=str(path.relative_to(self.root)),
            sha256=checksum,
            bytes=len(content),
        )

    def _event_files(self, workflow_id: str) -> tuple[Path, ...]:
        directory = self.root / "manifests" / "silver" / "workflows" / workflow_id / "events"
        if not directory.exists():
            return ()
        self._reject_symlink_path(directory)
        entries = tuple(sorted(directory.iterdir()))
        invalid = [
            path for path in entries if not path.is_file() or not _EVENT_FILE.fullmatch(path.name)
        ]
        if invalid:
            raise SilverStoreError(f"workflow contains an invalid event entry: {invalid[0]}")
        return entries

    @contextmanager
    def _workflow_lock(self, workflow_id: str) -> Iterator[None]:
        _digest(workflow_id, "workflow_id")
        directory = self.root / "manifests" / "silver" / "workflows" / workflow_id
        with self._directory_lock(directory, ".lock"):
            yield

    @contextmanager
    def _directory_lock(self, directory: Path, name: str) -> Iterator[None]:
        self._secure_mkdir(directory)
        lock_path = directory / name
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(lock_path, flags, 0o600)
        except OSError as exc:
            raise SilverStoreError(f"cannot safely open registry lock: {lock_path}") from exc
        try:
            file_stat = os.fstat(descriptor)
            if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_nlink != 1:
                raise SilverStoreError(
                    f"registry lock is not a single-link regular file: {lock_path}"
                )
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def _secure_mkdir(self, directory: Path) -> None:
        try:
            relative = directory.relative_to(self.root)
        except ValueError as exc:
            raise SilverStoreError("registry directory escaped data root") from exc
        current = self.root
        if current.is_symlink() or not current.is_dir():
            raise SilverStoreError("data root must be a real directory")
        for part in relative.parts:
            current /= part
            with suppress(FileExistsError):
                current.mkdir()
            if current.is_symlink() or not current.is_dir():
                raise SilverStoreError(f"registry path is not a real directory: {current}")

    def _reject_symlink_path(self, path: Path) -> None:
        try:
            relative = path.relative_to(self.root)
        except ValueError as exc:
            raise SilverStoreError("path escaped data root") from exc
        current = self.root
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                raise SilverStoreError(f"refusing path through symlink: {current}")

    @staticmethod
    def _snapshot(record: WorkflowEventRecord) -> WorkflowSnapshot:
        return WorkflowSnapshot(
            workflow_id=record.event.workflow_id,
            state=record.event.to_state,
            event_sha256=record.event_sha256,
            sequence=record.event.sequence,
            event_path=record.path,
            created_at=record.event.created_at,
            evidence=record.event.evidence,
        )

    @staticmethod
    def _validate_transition(current: WorkflowState, next_state: WorkflowState) -> None:
        if next_state in {WorkflowState.FAILED, WorkflowState.REJECTED} and current not in {
            WorkflowState.PUBLISHED,
            WorkflowState.FAILED,
            WorkflowState.REJECTED,
        }:
            return
        if next_state not in _FORWARD_TRANSITIONS[current]:
            raise SilverStoreError(
                f"illegal workflow transition: {current.value} -> {next_state.value}"
            )

    @staticmethod
    def _evidence_text(evidence: Mapping[str, object], key: str) -> str:
        if key not in evidence:
            raise SilverStoreError(f"workflow evidence is missing {key}")
        return _text(evidence[key], f"workflow evidence {key}")


def _json_bytes(document: Mapping[str, object]) -> bytes:
    return (
        json.dumps(document, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        + b"\n"
    )


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if not _DIGEST.fullmatch(text):
        raise SilverStoreError(f"{label} must be a lowercase SHA-256 digest")
    return text


def _clean_code(value: object) -> str:
    text = _text(value, "code")
    if not re.fullmatch(r"[a-z][a-z0-9_.-]*", text):
        raise SilverStoreError(f"invalid lowercase code: {text!r}")
    return text


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise SilverStoreError(f"{label} must be a positive native int")
    return value


def _text(value: object, label: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise SilverStoreError(f"{label} must be a string")
    if value != value.strip() or (not allow_empty and not value):
        raise SilverStoreError(f"{label} is empty or has surrounding whitespace")
    return value


def _clean_text(
    value: object,
    label: str,
    *,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    text = _text(value, label, allow_empty=allow_empty)
    if len(text) > maximum:
        raise SilverStoreError(f"{label} exceeds {maximum} characters")
    return text


def _utc_text(value: object, label: str) -> str:
    text = _text(value, label)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SilverStoreError(f"{label} is not ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise SilverStoreError(f"{label} must be timezone-aware UTC")
    return text


def _utc_datetime(value: object, label: str) -> datetime:
    return datetime.fromisoformat(_utc_text(value, label).replace("Z", "+00:00"))


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise SilverStoreError(f"{label} must be an object")
    return dict(value)


def _exact_keys(document: Mapping[str, object], expected: set[str], label: str) -> None:
    actual = set(document)
    if actual != expected:
        raise SilverStoreError(
            f"{label} keys mismatch: missing={sorted(expected - actual)}, "
            f"extra={sorted(actual - expected)}"
        )


def _is_relative_to(path_text: str, prefix_text: str) -> bool:
    try:
        Path(path_text).relative_to(Path(prefix_text))
    except ValueError:
        return False
    return True


def _index_upstream_bindings(
    value: object,
    file_bindings: dict[str, set[tuple[str, int | None]]],
    row_bindings: dict[str, set[int]],
) -> None:
    if isinstance(value, Mapping):
        path = value.get("path")
        if isinstance(path, str):
            checksum = value.get("sha256", value.get("stored_sha256"))
            size = value.get("bytes")
            if isinstance(checksum, str) and (type(size) is int or size is None):
                file_bindings.setdefault(path, set()).add((checksum, size))
            row_count = value.get("row_count", value.get("record_count"))
            if type(row_count) is int:
                row_bindings.setdefault(path, set()).add(row_count)
        for child in value.values():
            _index_upstream_bindings(child, file_bindings, row_bindings)
    elif isinstance(value, list):
        for child in value:
            _index_upstream_bindings(child, file_bindings, row_bindings)


def _is_release_manifest_path(path: str) -> bool:
    return bool(
        re.fullmatch(
            r"manifests/silver/releases/release_id=[0-9a-f]{64}\.json",
            path,
        )
    )


def _qa_row_key(row: Mapping[str, object]) -> tuple[str, str, str]:
    return (
        str(row["table_name"]),
        str(row["partition_key"]),
        str(row["check_id"]),
    )


def _artifact_identity_set(artifacts: tuple[ArtifactRef, ...]) -> set[str]:
    return {stable_digest(item.to_dict()) for item in artifacts}


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )
