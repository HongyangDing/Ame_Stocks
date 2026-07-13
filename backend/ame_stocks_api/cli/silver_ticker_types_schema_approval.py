"""Record only the exact user-approved S2 ticker-type schema decision."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from ame_stocks_api.silver.contracts import (
    ApprovalDecision,
    ApprovalReceipt,
    ApprovalStage,
    SilverContractError,
    TableContract,
)
from ame_stocks_api.silver.store import (
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowSnapshot,
    WorkflowState,
)

CANDIDATE_PATH = "docs/silver/contracts/reference/ticker_type_dim.schema-v1.candidate.json"
APPROVAL_TEXT = (
    "批准 S2 ticker_types schema contract "
    "b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d"
)
APPROVER = "user-approved-s2-ticker-types-contract"


@dataclass(frozen=True, slots=True)
class TickerTypeSchemaApprovalAuthorization:
    """Immutable identities authorized by the user's exact approval."""

    workflow_id: str
    schema_review_event_sha256: str
    contract_id: str
    schema_digest: str
    candidate_sha256: str
    registered_contract_sha256: str
    approval_text: str = APPROVAL_TEXT
    approver: str = APPROVER


CURRENT_AUTHORIZATION = TickerTypeSchemaApprovalAuthorization(
    workflow_id="40cde0fb24a52dbce894b52700f25c21074ad8d97ae5011a0a83cc773cee4b97",
    schema_review_event_sha256=("72411cbb8714609eb91b516dc66771e8a9a1019edddf4db5c0f164c00e96d209"),
    contract_id="b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d",
    schema_digest="b402318f8b67120fd0bf71fe1b67f56acba31b2ec70915d9b7e57acba84b1957",
    candidate_sha256="cd11385be2649e00a7f99938754fe7d58e1fa12f6535786cadcce62c281adbd2",
    registered_contract_sha256=("e7d45dc2f0fba278fe059e374447a33a3aa7dbe7dcc97a073cb509a46ba4476b"),
)


@dataclass(frozen=True, slots=True)
class TickerTypeSchemaApprovalRun:
    workflow: WorkflowSnapshot
    contract: TableContract
    registered_contract: StoredDocument
    approval: ApprovalReceipt
    approval_document: StoredDocument


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-ticker-types-schema-approval",
        description=(
            "Record the exact approved S2 ticker_type_dim schema and stop at code_ready. "
            "This command cannot transform, preview, build, or publish data."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--decided-at", required=True, help="fixed UTC ISO-8601 decision time")
    parser.add_argument(
        "--approval-text",
        required=True,
        help="must exactly match the already recorded user approval",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        repo_root = _verify_git_checkout(arguments.repo_root, arguments.git_commit)
        run = record_ticker_type_schema_approval(
            arguments.data_root,
            repo_root=repo_root,
            approval_text=arguments.approval_text,
            decided_at=arguments.decided_at,
        )
        output = {
            "approval_id": run.approval.approval_id,
            "approval_path": run.approval_document.path,
            "approval_sha256": run.approval_document.sha256,
            "approval_text": run.approval.note,
            "approver": run.approval.approver,
            "candidate_path": CANDIDATE_PATH,
            "candidate_sha256": CURRENT_AUTHORIZATION.candidate_sha256,
            "contract_id": run.contract.contract_id,
            "git_commit": arguments.git_commit,
            "mode": "schema_approval_only",
            "registered_contract_path": run.registered_contract.path,
            "registered_contract_sha256": run.registered_contract.sha256,
            "schema_digest": run.contract.schema_digest,
            "sequence": run.workflow.sequence,
            "state": run.workflow.state.value,
            "workflow_event_path": run.workflow.event_path,
            "workflow_event_sha256": run.workflow.event_sha256,
            "workflow_id": run.workflow.workflow_id,
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (
        json.JSONDecodeError,
        OSError,
        SilverContractError,
        subprocess.SubprocessError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(2, f"ame-silver-ticker-types-schema-approval: {exc}\n")


def record_ticker_type_schema_approval(
    data_root: Path,
    *,
    repo_root: Path,
    approval_text: str,
    decided_at: str,
    authorization: TickerTypeSchemaApprovalAuthorization | None = None,
) -> TickerTypeSchemaApprovalRun:
    """Approve one pinned schema-review event and perform no downstream transition."""

    authorized = CURRENT_AUTHORIZATION if authorization is None else authorization
    if approval_text != authorized.approval_text:
        raise SilverStoreError("approval text does not match the exact user authorization")
    candidate = _load_fixed_candidate(repo_root, authorized)
    store = SilverStore(data_root.expanduser().resolve())
    current = store.verify_workflow_trust_chain(authorized.workflow_id, verify_artifacts=True)
    registered, registered_document = store.load_workflow_contract(authorized.workflow_id)
    if registered != candidate:
        raise SilverStoreError("registered workflow contract differs from the approved candidate")
    if registered_document.sha256 != authorized.registered_contract_sha256:
        raise SilverStoreError("registered workflow contract SHA-256 is not authorized")

    if current.state is WorkflowState.SCHEMA_REVIEW:
        if current.sequence != 2 or current.event_sha256 != authorized.schema_review_event_sha256:
            raise SilverStoreError("current schema-review event identity is not authorized")
        current = store.approve_schema(
            authorized.workflow_id,
            expected_event_sha256=authorized.schema_review_event_sha256,
            approver=authorized.approver,
            decided_at=decided_at,
            note=authorized.approval_text,
        )
    elif current.state is not WorkflowState.CODE_READY:
        raise SilverStoreError(
            f"schema approval refuses workflow state {current.state.value}; expected schema_review"
        )

    verified = store.verify_workflow_trust_chain(authorized.workflow_id, verify_artifacts=True)
    if verified.state is not WorkflowState.CODE_READY or verified.sequence != 3:
        raise SilverStoreError("schema approval did not stop at the exact code_ready gate")
    records = store.workflow_events(authorized.workflow_id)
    if (
        len(records) != 3
        or records[1].event.to_state is not WorkflowState.SCHEMA_REVIEW
        or records[1].event_sha256 != authorized.schema_review_event_sha256
    ):
        raise SilverStoreError("code_ready workflow does not descend from the authorized review")
    approval_id = _approval_id(records[-1].event.evidence)
    approval, approval_document = store.load_approval(approval_id)
    _verify_approval_receipt(
        approval,
        approval_document,
        decided_at=decided_at,
        authorization=authorized,
    )
    return TickerTypeSchemaApprovalRun(
        workflow=verified,
        contract=registered,
        registered_contract=registered_document,
        approval=approval,
        approval_document=approval_document,
    )


def _load_fixed_candidate(
    repo_root: Path,
    authorization: TickerTypeSchemaApprovalAuthorization,
) -> TableContract:
    content = (repo_root / CANDIDATE_PATH).read_bytes()
    if hashlib.sha256(content).hexdigest() != authorization.candidate_sha256:
        raise SilverStoreError("ticker_type_dim candidate SHA-256 is not authorized")
    document = json.loads(content)
    if not isinstance(document, dict):
        raise SilverContractError("ticker_type_dim candidate must be a JSON object")
    contract = TableContract.from_dict(document)
    if (
        contract.contract_id != authorization.contract_id
        or contract.schema_digest != authorization.schema_digest
        or contract.domain != "reference"
        or contract.table != "ticker_type_dim"
        or contract.schema_version != 1
        or contract.source_datasets != ("ticker_types",)
    ):
        raise SilverStoreError("ticker_type_dim candidate identity or scope is not authorized")
    return contract


def _verify_approval_receipt(
    approval: ApprovalReceipt,
    document: StoredDocument,
    *,
    decided_at: str,
    authorization: TickerTypeSchemaApprovalAuthorization,
) -> None:
    if (
        approval.workflow_id != authorization.workflow_id
        or approval.stage is not ApprovalStage.SCHEMA
        or approval.decision is not ApprovalDecision.APPROVED
        or approval.subject_id != authorization.contract_id
        or approval.subject_manifest_sha256 != authorization.registered_contract_sha256
        or approval.expected_event_sha256 != authorization.schema_review_event_sha256
        or approval.approver != authorization.approver
        or approval.decided_at != decided_at
        or approval.note != authorization.approval_text
        or approval.waived_qa_result_ids
        or approval.accepted_quarantine_issue_ids
    ):
        raise SilverStoreError("existing schema approval is not the exact authorized decision")
    if not document.path.endswith(f"/{approval.approval_id}.json"):
        raise SilverStoreError("schema approval receipt path is not canonical")


def _approval_id(evidence: object) -> str:
    if not isinstance(evidence, Mapping) or set(evidence) != {
        "approval_id",
        "approval_path",
        "approval_sha256",
    }:
        raise SilverStoreError("code_ready event has unexpected approval evidence")
    approval_id = evidence["approval_id"]
    if not isinstance(approval_id, str):
        raise SilverStoreError("code_ready approval ID is invalid")
    return approval_id


def _verify_git_checkout(repo_root: Path, git_commit: str) -> Path:
    root = repo_root.expanduser().resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise SilverStoreError("schema-approval CLI is not executing from --repo-root") from exc
    if Path(_git_output(root, "rev-parse", "--show-toplevel")).resolve() != root:
        raise SilverStoreError("--repo-root is not the exact Git top level")
    if _git_output(root, "rev-parse", "HEAD") != git_commit:
        raise SilverStoreError("Git HEAD differs from --git-commit")
    _git_output(root, "ls-files", "--error-unmatch", "--", module_relative)
    _git_output(root, "ls-files", "--error-unmatch", "--", CANDIDATE_PATH)
    if _git_output(root, "status", "--porcelain=v1", "--untracked-files=all"):
        raise SilverStoreError("schema-approval Git checkout is not clean")
    return root


def _git_output(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown Git error"
        raise SilverStoreError(f"cannot verify schema-approval Git checkout: {detail}")
    return completed.stdout.strip()


if __name__ == "__main__":
    raise SystemExit(main())
