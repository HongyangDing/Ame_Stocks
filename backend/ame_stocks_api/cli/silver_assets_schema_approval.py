"""Record only the exact user-approved S4 Assets schema package."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
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

APPROVAL_TEXT = (
    "批准 S4 三个 schema contracts:\n"
    "dd916b8528b9ce1a341e6b8ad897ae80e40d5df118b8e102e4ea1f1ea6e9c045\n"
    "14ce114f5911f7e4d1c15e58f0f42a8307066d6517e859d6233fa23c199616fc\n"
    "9711320ee9227df347224b7cd17a41fe10a352fddf089cd72b758bde7a7f0c58"
)
APPROVAL_TEXT_SHA256 = hashlib.sha256(APPROVAL_TEXT.encode("utf-8")).hexdigest()
APPROVER = "user-approved-s4-assets-contracts"
S4_SCHEMA_DECIDED_AT = "2026-07-13T08:31:00+00:00"


@dataclass(frozen=True, slots=True)
class AssetSchemaApprovalAuthorization:
    """One exact contract and schema-review event covered by the user approval."""

    table: str
    domain: str
    candidate_path: str
    resource_path: str
    candidate_sha256: str
    contract_id: str
    schema_digest: str
    workflow_id: str
    schema_review_event_sha256: str
    registered_contract_sha256: str


CURRENT_AUTHORIZATIONS = (
    AssetSchemaApprovalAuthorization(
        table="asset_observation_daily",
        domain="identity",
        candidate_path=(
            "docs/silver/contracts/identity/asset_observation_daily.schema-v1.candidate.json"
        ),
        resource_path=(
            "backend/ame_stocks_api/silver/schema_resources/asset_observation_daily.schema-v1.json"
        ),
        candidate_sha256="dbe656df1cd0e007498b2f7c3a79c6654a52d8ffa7f4099a1b8f32546ab3eced",
        contract_id="dd916b8528b9ce1a341e6b8ad897ae80e40d5df118b8e102e4ea1f1ea6e9c045",
        schema_digest="402d0ea624dc26e43ea63974572ede5a46ae20e0741e97a3d01d07075a71bc1e",
        workflow_id="c1bae241ed90e49aed1ae8a98b6801f511d6abaac2cef93c66ccba59d33775ec",
        schema_review_event_sha256=(
            "84749ab1a7a1cac80b636dbb4be9fb58af8ce22e2b34656044d7f34ed848d5cd"
        ),
        registered_contract_sha256=(
            "2efd0476eb15b2d39ef0317607a21de5e08551e6c49062c47ca0264e18f2eb24"
        ),
    ),
    AssetSchemaApprovalAuthorization(
        table="asset_observation_version",
        domain="identity",
        candidate_path=(
            "docs/silver/contracts/identity/asset_observation_version.schema-v1.candidate.json"
        ),
        resource_path=(
            "backend/ame_stocks_api/silver/schema_resources/"
            "asset_observation_version.schema-v1.json"
        ),
        candidate_sha256="c3249b8684347e5b491cbe31d44c19f6ce0ddec4568a61c831baebafe3433751",
        contract_id="14ce114f5911f7e4d1c15e58f0f42a8307066d6517e859d6233fa23c199616fc",
        schema_digest="4c797ca373d697078b2061b9a76696dc036a1d2db0a5f8e1fe3ce2dac4b6bb4b",
        workflow_id="989c8c513905e2710714c0b6f94352119e8fb1128147d8c2db9486c1e03df6da",
        schema_review_event_sha256=(
            "c3ff6ef36cc5533bf6838912ee25aac0d9fa30ffc0bda3fbc0b387e90e027911"
        ),
        registered_contract_sha256=(
            "d093c894983436c58b512edbf9e7a63d28cba50ad2c07a34bf95b9a492345b1e"
        ),
    ),
    AssetSchemaApprovalAuthorization(
        table="universe_source_daily",
        domain="reference",
        candidate_path=(
            "docs/silver/contracts/reference/universe_source_daily.schema-v1.candidate.json"
        ),
        resource_path=(
            "backend/ame_stocks_api/silver/schema_resources/universe_source_daily.schema-v1.json"
        ),
        candidate_sha256="49fb584c6109eee6088aaf291773089caa171d02a31d3c159aa474885abd6d2a",
        contract_id="9711320ee9227df347224b7cd17a41fe10a352fddf089cd72b758bde7a7f0c58",
        schema_digest="78b799cd5a2621b5a78e4ed8c23c090f6aea686fcd786366e5c258e81ad278a5",
        workflow_id="918ebc04d2eded87243387804d58fa9f24e4282ee27a8a26ac6ac22f4390b755",
        schema_review_event_sha256=(
            "57f357d158dd9856d0fda46262dee70308d7b9b30f0ce864954fc62c83703dbb"
        ),
        registered_contract_sha256=(
            "141c947595569ddebbbda3a21c9826055d3aed6c69c62fe2e825512a6607adeb"
        ),
    ),
)

_TRACKED_RUNTIME_PATHS = (
    "backend/ame_stocks_api/cli/silver_assets_schema_review.py",
    "backend/ame_stocks_api/silver/asset_contract.py",
    "backend/ame_stocks_api/silver/asset_source.py",
    "backend/ame_stocks_api/silver/assets.py",
    "pyproject.toml",
)


@dataclass(frozen=True, slots=True)
class AssetSchemaApprovalItem:
    authorization: AssetSchemaApprovalAuthorization
    workflow: WorkflowSnapshot
    contract: TableContract
    registered_contract: StoredDocument
    approval: ApprovalReceipt
    approval_document: StoredDocument


@dataclass(frozen=True, slots=True)
class AssetSchemaApprovalRun:
    items: tuple[AssetSchemaApprovalItem, ...]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-assets-schema-approval",
        description=(
            "Record the exact approved S4 Assets schema package and stop all three workflows "
            "at code_ready. This command cannot transform, preview, build, or publish data."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--decided-at", required=True)
    parser.add_argument(
        "--approval-text-sha256",
        required=True,
        help="must match the digest of the exact already-recorded multi-line approval",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        repo_root = _verify_git_checkout(arguments.repo_root, arguments.git_commit)
        run = record_asset_schema_approvals(
            arguments.data_root,
            repo_root=repo_root,
            approval_text_sha256=arguments.approval_text_sha256,
            decided_at=arguments.decided_at,
        )
        output = {
            "approval_text": APPROVAL_TEXT,
            "approval_text_sha256": APPROVAL_TEXT_SHA256,
            "approver": APPROVER,
            "decided_at": arguments.decided_at,
            "git_commit": arguments.git_commit,
            "mode": "schema_approval_only",
            "state": "code_ready",
            "workflows": {
                item.authorization.table: {
                    "approval_id": item.approval.approval_id,
                    "approval_path": item.approval_document.path,
                    "approval_sha256": item.approval_document.sha256,
                    "candidate_path": item.authorization.candidate_path,
                    "candidate_sha256": item.authorization.candidate_sha256,
                    "contract_id": item.contract.contract_id,
                    "registered_contract_path": item.registered_contract.path,
                    "registered_contract_sha256": item.registered_contract.sha256,
                    "schema_digest": item.contract.schema_digest,
                    "schema_review_event_sha256": (item.authorization.schema_review_event_sha256),
                    "sequence": item.workflow.sequence,
                    "state": item.workflow.state.value,
                    "workflow_event_path": item.workflow.event_path,
                    "workflow_event_sha256": item.workflow.event_sha256,
                    "workflow_id": item.workflow.workflow_id,
                }
                for item in run.items
            },
        }
        print(json.dumps(output, indent=2, sort_keys=True, ensure_ascii=False))
        return 0
    except (
        json.JSONDecodeError,
        OSError,
        SilverContractError,
        subprocess.SubprocessError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(2, f"ame-silver-assets-schema-approval: {exc}\n")


def record_asset_schema_approvals(
    data_root: Path,
    *,
    repo_root: Path,
    approval_text_sha256: str,
    decided_at: str,
    authorizations: tuple[AssetSchemaApprovalAuthorization, ...] | None = None,
) -> AssetSchemaApprovalRun:
    """Approve three pinned schema-review events after a complete lockstep preflight."""

    authorized = CURRENT_AUTHORIZATIONS if authorizations is None else authorizations
    if approval_text_sha256 != APPROVAL_TEXT_SHA256:
        raise SilverStoreError("approval text digest does not match the exact user authorization")
    if decided_at != S4_SCHEMA_DECIDED_AT:
        raise SilverStoreError("S4 schema approval decided_at is not authorized")
    if len(authorized) != 3 or len({item.table for item in authorized}) != 3:
        raise SilverStoreError("S4 schema approval requires exactly three table authorizations")

    contracts = tuple(_load_fixed_contract(repo_root, item) for item in authorized)
    store = SilverStore(data_root.expanduser().resolve())

    preflight: list[tuple[AssetSchemaApprovalAuthorization, TableContract, StoredDocument]] = []
    for authorization, contract in zip(authorized, contracts, strict=True):
        current = store.verify_workflow_trust_chain(
            authorization.workflow_id,
            verify_artifacts=True,
        )
        registered, document = store.load_workflow_contract(authorization.workflow_id)
        if registered != contract:
            raise SilverStoreError(
                f"registered {authorization.table} workflow contract differs from approval"
            )
        if document.sha256 != authorization.registered_contract_sha256:
            raise SilverStoreError(
                f"registered {authorization.table} contract SHA-256 is not authorized"
            )
        if current.state is WorkflowState.SCHEMA_REVIEW:
            records = store.workflow_events(authorization.workflow_id)
            if (
                current.sequence != 2
                or current.event_sha256 != authorization.schema_review_event_sha256
                or len(records) != 2
                or records[1].event.to_state is not WorkflowState.SCHEMA_REVIEW
            ):
                raise SilverStoreError(
                    f"current {authorization.table} schema-review event is not authorized"
                )
        elif current.state is WorkflowState.CODE_READY:
            _verify_code_ready(
                store,
                current,
                authorization=authorization,
                decided_at=decided_at,
            )
        else:
            raise SilverStoreError(
                f"schema approval refuses {authorization.table} state {current.state.value}"
            )
        preflight.append((authorization, contract, document))

    for authorization, _, _ in preflight:
        current = store.status(authorization.workflow_id)
        if current.state is WorkflowState.SCHEMA_REVIEW:
            store.approve_schema(
                authorization.workflow_id,
                expected_event_sha256=authorization.schema_review_event_sha256,
                approver=APPROVER,
                decided_at=decided_at,
                note=APPROVAL_TEXT,
            )

    items: list[AssetSchemaApprovalItem] = []
    for authorization, contract, registered_document in preflight:
        current = store.verify_workflow_trust_chain(
            authorization.workflow_id,
            verify_artifacts=True,
        )
        approval, approval_document = _verify_code_ready(
            store,
            current,
            authorization=authorization,
            decided_at=decided_at,
        )
        items.append(
            AssetSchemaApprovalItem(
                authorization=authorization,
                workflow=current,
                contract=contract,
                registered_contract=registered_document,
                approval=approval,
                approval_document=approval_document,
            )
        )
    return AssetSchemaApprovalRun(tuple(items))


def _load_fixed_contract(
    repo_root: Path,
    authorization: AssetSchemaApprovalAuthorization,
) -> TableContract:
    candidate_content = (repo_root / authorization.candidate_path).read_bytes()
    resource_content = (repo_root / authorization.resource_path).read_bytes()
    if hashlib.sha256(candidate_content).hexdigest() != authorization.candidate_sha256:
        raise SilverStoreError(f"{authorization.table} candidate SHA-256 is not authorized")
    if resource_content != candidate_content:
        raise SilverStoreError(f"packaged {authorization.table} resource differs from candidate")
    document = json.loads(candidate_content)
    if not isinstance(document, dict):
        raise SilverContractError(f"{authorization.table} candidate must be a JSON object")
    contract = TableContract.from_dict(document)
    if (
        contract.contract_id != authorization.contract_id
        or contract.schema_digest != authorization.schema_digest
        or contract.domain != authorization.domain
        or contract.table != authorization.table
        or contract.schema_version != 1
        or contract.source_datasets != ("assets",)
        or ASSET_CONTRACTS.get(authorization.table) != contract
    ):
        raise SilverStoreError(f"{authorization.table} candidate identity is not authorized")
    return contract


def _verify_code_ready(
    store: SilverStore,
    current: WorkflowSnapshot,
    *,
    authorization: AssetSchemaApprovalAuthorization,
    decided_at: str,
) -> tuple[ApprovalReceipt, StoredDocument]:
    if current.state is not WorkflowState.CODE_READY or current.sequence != 3:
        raise SilverStoreError(f"{authorization.table} did not stop at code_ready sequence 3")
    records = store.workflow_events(authorization.workflow_id)
    if (
        len(records) != 3
        or records[1].event.to_state is not WorkflowState.SCHEMA_REVIEW
        or records[1].event_sha256 != authorization.schema_review_event_sha256
        or records[2].event.to_state is not WorkflowState.CODE_READY
    ):
        raise SilverStoreError(f"{authorization.table} code_ready chain is not authorized")
    approval_id = _approval_id(records[-1].event.evidence)
    approval, document = store.load_approval(approval_id)
    if (
        approval.workflow_id != authorization.workflow_id
        or approval.stage is not ApprovalStage.SCHEMA
        or approval.decision is not ApprovalDecision.APPROVED
        or approval.subject_id != authorization.contract_id
        or approval.subject_manifest_sha256 != authorization.registered_contract_sha256
        or approval.expected_event_sha256 != authorization.schema_review_event_sha256
        or approval.approver != APPROVER
        or approval.decided_at != decided_at
        or approval.note != APPROVAL_TEXT
        or approval.waived_qa_result_ids
        or approval.accepted_quarantine_issue_ids
    ):
        raise SilverStoreError(f"existing {authorization.table} approval is not authorized")
    if not document.path.endswith(f"/{approval.approval_id}.json"):
        raise SilverStoreError(f"{authorization.table} approval receipt path is not canonical")
    return approval, document


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
    tracked = [module_relative, *_TRACKED_RUNTIME_PATHS]
    tracked.extend(item.candidate_path for item in CURRENT_AUTHORIZATIONS)
    tracked.extend(item.resource_path for item in CURRENT_AUTHORIZATIONS)
    for path in tracked:
        _git_output(root, "ls-files", "--error-unmatch", "--", path)
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
