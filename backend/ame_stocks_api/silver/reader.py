"""Release-only access to fully approved Silver data artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import (
    ApprovalDecision,
    ApprovalStage,
    ArtifactRef,
    ArtifactRole,
    BuildKind,
    BuildManifest,
    ReleaseManifest,
    TableContract,
)
from ame_stocks_api.silver.store import (
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowState,
)


@dataclass(frozen=True, slots=True)
class PublishedRelease:
    release: ReleaseManifest
    contract: TableContract
    build: BuildManifest
    data_paths: tuple[Path, ...]


class PublishedSilverReader:
    """Resolve data only through a release backed by the published event chain."""

    def __init__(self, data_root: Path) -> None:
        self.store = SilverStore(data_root)

    def inspect(self, release_id: str) -> PublishedRelease:
        release, release_document = self.store.load_release(release_id)
        snapshot = self.store.verify_workflow_trust_chain(release.workflow_id)
        events = self.store.workflow_events(release.workflow_id)
        published = events[-1]
        if (
            snapshot.state is not WorkflowState.PUBLISHED
            or published.event.to_state is not WorkflowState.PUBLISHED
        ):
            raise SilverStoreError("release workflow is not published")
        evidence = published.event.evidence
        self._match_evidence(evidence, "release_id", release.release_id)
        self._match_evidence(evidence, "release_path", release_document.path)
        self._match_evidence(evidence, "release_sha256", release_document.sha256)

        contract, _ = self.store.load_workflow_contract(release.workflow_id)
        if (
            release.contract_id != contract.contract_id
            or release.domain != contract.domain
            or release.table != contract.table
            or release.schema_version != contract.schema_version
        ):
            raise SilverStoreError("release does not match its workflow contract")

        build, build_document = self.store.load_build(release.table, release.build_id)
        if build.intent.kind is not BuildKind.FULL:
            raise SilverStoreError("release must reference a full build")
        if build.intent.workflow_id != release.workflow_id:
            raise SilverStoreError("released build belongs to a different workflow")
        self._match_document_sha(
            build_document,
            release.build_manifest_sha256,
            "build manifest",
        )
        self._match_evidence(evidence, "build_id", build.build_id)
        self._match_evidence(
            evidence,
            "build_manifest_sha256",
            build_document.sha256,
        )
        self.store.validate_build_manifest(
            build,
            contract,
            workflow_id=release.workflow_id,
        )

        approval, approval_document = self.store.load_approval(release.approval_id)
        self._match_document_sha(approval_document, release.approval_sha256, "approval")
        self._match_evidence(evidence, "approval_id", approval.approval_id)
        self._match_evidence(evidence, "approval_path", approval_document.path)
        self._match_evidence(evidence, "approval_sha256", approval_document.sha256)
        if (
            approval.workflow_id != release.workflow_id
            or approval.stage is not ApprovalStage.PUBLISH
            or approval.decision is not ApprovalDecision.APPROVED
            or approval.subject_id != build.build_id
            or approval.subject_manifest_sha256 != build_document.sha256
            or approval.expected_event_sha256 != published.event.previous_event_sha256
        ):
            raise SilverStoreError("publish approval is not bound to the released full build")
        if not (release.released_at == approval.decided_at == published.event.created_at):
            raise SilverStoreError("release, approval, and published event timestamps differ")
        self.store.validate_qa_gate(
            build,
            approval.waived_qa_result_ids,
            approval.accepted_quarantine_issue_ids,
        )

        expected_outputs = tuple(
            output for output in build.outputs if output.role is ArtifactRole.DATA
        )
        if self._artifact_set(release.outputs) != self._artifact_set(expected_outputs):
            raise SilverStoreError("release data artifacts differ from the approved build")
        data_paths = tuple(
            self.store.verify_artifact(output, contract=contract) for output in release.outputs
        )
        return PublishedRelease(
            release=release,
            contract=contract,
            build=build,
            data_paths=data_paths,
        )

    def data_files(self, release_id: str) -> tuple[Path, ...]:
        """Return verified data paths; build IDs and arbitrary paths are not accepted."""

        return self.inspect(release_id).data_paths

    @staticmethod
    def _match_evidence(evidence: object, key: str, expected: str) -> None:
        if not isinstance(evidence, Mapping):
            raise SilverStoreError("published workflow evidence is malformed")
        if evidence.get(key) != expected:
            raise SilverStoreError(f"published workflow evidence mismatch for {key}")

    @staticmethod
    def _match_document_sha(document: StoredDocument, expected: str, label: str) -> None:
        if document.sha256 != expected:
            raise SilverStoreError(f"{label} SHA does not match the release")

    @staticmethod
    def _artifact_set(artifacts: tuple[ArtifactRef, ...]) -> set[str]:
        return {stable_digest(item.to_dict()) for item in artifacts}
