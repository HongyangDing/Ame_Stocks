"""Release-only access to fully approved Silver data artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

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
    S4_IDENTITY_ELIGIBILITY_PENDING,
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowState,
)

if TYPE_CHECKING:
    from ame_stocks_api.silver.asset_release_set import AssetReleaseSet


@dataclass(frozen=True, slots=True)
class PublishedRelease:
    release: ReleaseManifest
    contract: TableContract
    build: BuildManifest
    data_paths: tuple[Path, ...]


@dataclass(frozen=True, slots=True)
class PublishedAssetEvidence:
    """Verified S4 data that remains evidence-only until S7 resolves identity."""

    release: ReleaseManifest
    contract: TableContract
    build: BuildManifest
    data_paths: tuple[Path, ...]
    publication_scope: str
    backtest_identity_eligible: bool


class PublishedSilverReader:
    """Resolve data only through a release backed by the published event chain."""

    def __init__(self, data_root: Path) -> None:
        self.store = SilverStore(data_root)

    def inspect(self, release_id: str) -> PublishedRelease:
        published, release_set = self._inspect_with_scope(release_id)
        if release_set is not None:
            raise SilverStoreError(
                f"{S4_IDENTITY_ELIGIBILITY_PENDING}; use "
                "PublishedAssetEvidenceReader for evidence-only access"
            )
        return published

    def _inspect_with_scope(
        self,
        release_id: str,
    ) -> tuple[PublishedRelease, AssetReleaseSet | None]:
        release, release_document = self.store.load_release(release_id)
        # S4 publishes three mutually dependent tables.  Keep this import local:
        # the release-set verifier uses SilverStore to authenticate its marker.
        from ame_stocks_api.silver.asset_release_set import (
            asset_release_requires_set,
            require_asset_release_set_membership,
        )

        release_set = None
        if asset_release_requires_set(release.table):
            # Every path, including the explicit evidence-only reader, authenticates
            # the complete marker here.  There is no boolean/capability bypass for a
            # member that reached seq10 before the all-or-nothing set was committed.
            release_set = require_asset_release_set_membership(
                self.store.root,
                release.release_id,
            )
        controlled_release, contract, build = self._inspect_control_chain(
            release_id,
            loaded_release=(release, release_document),
        )
        if controlled_release != release:
            raise SilverStoreError("release changed while its publication was verified")
        data_paths = tuple(
            self.store.verify_artifact(output, contract=contract) for output in release.outputs
        )
        return (
            PublishedRelease(
                release=release,
                contract=contract,
                build=build,
                data_paths=data_paths,
            ),
            release_set,
        )

    def _inspect_selected_for_identity_preview(
        self,
        release_id: str,
        artifact_paths: Sequence[str],
    ) -> tuple[PublishedRelease, AssetReleaseSet | None, tuple[ArtifactRef, ...]]:
        """Authenticate one release but physically verify only an exact DATA subset.

        This is deliberately private and is used only to mint the source capability for
        an approved bounded S7 preview.  It never grants ordinary published-data access.
        """

        selected_paths = tuple(artifact_paths)
        if (
            any(not isinstance(path, str) or not path for path in selected_paths)
            or tuple(sorted(set(selected_paths))) != selected_paths
        ):
            raise SilverStoreError(
                "identity preview artifact paths must be sorted, unique, nonempty strings"
            )
        release, contract, build = self._inspect_control_chain(release_id)
        from ame_stocks_api.silver.asset_release_set import (
            _require_asset_release_set_control_membership,
            asset_release_requires_set,
        )

        release_set = None
        if asset_release_requires_set(release.table):
            release_set = _require_asset_release_set_control_membership(
                self.store.root,
                release.release_id,
            )
        outputs_by_path = {output.path: output for output in release.outputs}
        if len(outputs_by_path) != len(release.outputs):
            raise SilverStoreError("release contains duplicate DATA artifact paths")
        try:
            selected_outputs = tuple(outputs_by_path[path] for path in selected_paths)
        except KeyError as exc:
            raise SilverStoreError(
                "identity preview selected an artifact outside the exact release"
            ) from exc
        data_paths = tuple(
            self.store.verify_artifact(output, contract=contract)
            for output in selected_outputs
        )
        return (
            PublishedRelease(
                release=release,
                contract=contract,
                build=build,
                data_paths=data_paths,
            ),
            release_set,
            selected_outputs,
        )

    def _inspect_control_chain(
        self,
        release_id: str,
        *,
        loaded_release: tuple[ReleaseManifest, StoredDocument] | None = None,
    ) -> tuple[ReleaseManifest, TableContract, BuildManifest]:
        """Verify publication metadata and approvals without touching release DATA."""

        release, release_document = (
            self.store.load_release(release_id)
            if loaded_release is None
            else loaded_release
        )
        if release.release_id != release_id:
            raise SilverStoreError("loaded release differs from its requested identity")
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
        return release, contract, build

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


class PublishedAssetEvidenceReader:
    """Read protected S4 releases without granting backtest identity eligibility."""

    def __init__(self, data_root: Path) -> None:
        self._reader = PublishedSilverReader(data_root)

    def inspect(self, release_id: str) -> PublishedAssetEvidence:
        from ame_stocks_api.silver.asset_release_set import ASSET_PUBLICATION_SCOPE

        published, release_set = self._reader._inspect_with_scope(release_id)
        if release_set is None:
            raise SilverStoreError(
                "PublishedAssetEvidenceReader accepts only protected S4 asset releases"
            )
        if (
            release_set.publication_scope != ASSET_PUBLICATION_SCOPE
            or release_set.backtest_identity_eligible is not False
        ):
            raise SilverStoreError(
                "S4 release-set marker does not preserve the evidence-only identity boundary"
            )
        return PublishedAssetEvidence(
            release=published.release,
            contract=published.contract,
            build=published.build,
            data_paths=published.data_paths,
            publication_scope=release_set.publication_scope,
            backtest_identity_eligible=False,
        )

    def _inspect_selected_for_identity_preview(
        self,
        release_id: str,
        artifact_paths: Sequence[str],
    ) -> tuple[PublishedAssetEvidence, tuple[ArtifactRef, ...]]:
        """Return only physically verified artifacts selected by the bounded S7 factory."""

        from ame_stocks_api.silver.asset_release_set import ASSET_PUBLICATION_SCOPE

        published, release_set, selected_outputs = (
            self._reader._inspect_selected_for_identity_preview(
                release_id,
                artifact_paths,
            )
        )
        if release_set is None:
            raise SilverStoreError(
                "PublishedAssetEvidenceReader accepts only protected S4 asset releases"
            )
        if (
            release_set.publication_scope != ASSET_PUBLICATION_SCOPE
            or release_set.backtest_identity_eligible is not False
        ):
            raise SilverStoreError(
                "S4 release-set marker does not preserve the evidence-only identity boundary"
            )
        return (
            PublishedAssetEvidence(
                release=published.release,
                contract=published.contract,
                build=published.build,
                data_paths=published.data_paths,
                publication_scope=release_set.publication_scope,
                backtest_identity_eligible=False,
            ),
            selected_outputs,
        )
