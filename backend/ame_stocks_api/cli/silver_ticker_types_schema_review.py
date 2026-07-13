"""Register the fixed S2 ticker-type contract and stop at schema review."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import SilverContractError, TableContract
from ame_stocks_api.silver.store import (
    WORKFLOW_EVENT_VERSION,
    SilverStore,
    SilverStoreError,
    WorkflowState,
)

CANDIDATE_PATH = "docs/silver/contracts/reference/ticker_type_dim.schema-v1.candidate.json"
CANDIDATE_SHA256 = "cd11385be2649e00a7f99938754fe7d58e1fa12f6535786cadcce62c281adbd2"
CONTRACT_ID = "b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d"
SCHEMA_DIGEST = "b402318f8b67120fd0bf71fe1b67f56acba31b2ec70915d9b7e57acba84b1957"
ACTOR = "s2-ticker-types-schema-review"
NOTE = "Registered the fixed S2 ticker_type_dim candidate; approval and data work remain gated."


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-ticker-types-schema-review",
        description=(
            "Register the fixed S2 ticker_type_dim candidate and stop at schema_review. "
            "This command cannot approve, transform, preview, build, or publish."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument(
        "--created-at",
        required=True,
        help="fixed UTC ISO-8601 timestamp used for deterministic, idempotent registration",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        repo_root = _verify_git_checkout(arguments.repo_root, arguments.git_commit)
        contract = _load_fixed_candidate(repo_root)
        snapshot = _register_schema_review(
            arguments.data_root,
            contract=contract,
            created_at=arguments.created_at,
        )
        output = {
            "candidate_path": CANDIDATE_PATH,
            "candidate_sha256": CANDIDATE_SHA256,
            "contract_id": contract.contract_id,
            "domain": contract.domain,
            "event_path": snapshot.event_path,
            "event_sha256": snapshot.event_sha256,
            "git_commit": arguments.git_commit,
            "mode": "schema_review_only",
            "schema_digest": contract.schema_digest,
            "schema_version": contract.schema_version,
            "sequence": snapshot.sequence,
            "source_datasets": list(contract.source_datasets),
            "state": snapshot.state.value,
            "table": contract.table,
            "workflow_id": snapshot.workflow_id,
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (
        json.JSONDecodeError,
        OSError,
        SilverContractError,
        SilverStoreError,
        subprocess.TimeoutExpired,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(2, f"ame-silver-ticker-types-schema-review: {exc}\n")


def _verify_git_checkout(repo_root: Path, git_commit: str) -> Path:
    root = repo_root.expanduser().resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise SilverStoreError("schema-review CLI is not executing from --repo-root") from exc
    top_level = _git_output(root, "rev-parse", "--show-toplevel")
    head = _git_output(root, "rev-parse", "HEAD")
    if Path(top_level).resolve() != root:
        raise SilverStoreError("--repo-root is not the exact Git top level")
    if head != git_commit:
        raise SilverStoreError("Git HEAD differs from --git-commit")
    _git_output(root, "ls-files", "--error-unmatch", "--", module_relative)
    _git_output(root, "ls-files", "--error-unmatch", "--", CANDIDATE_PATH)
    status = _git_output(root, "status", "--porcelain=v1", "--untracked-files=all")
    if status:
        raise SilverStoreError("schema-review Git checkout is not clean")
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
        raise SilverStoreError(f"cannot verify schema-review Git checkout: {detail}")
    return completed.stdout.strip()


def _load_fixed_candidate(repo_root: Path) -> TableContract:
    path = repo_root / CANDIDATE_PATH
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != CANDIDATE_SHA256:
        raise SilverStoreError("ticker_type_dim candidate SHA-256 is not authorized")
    document = json.loads(content)
    if not isinstance(document, dict):
        raise SilverContractError("ticker_type_dim candidate must be a JSON object")
    contract = TableContract.from_dict(document)
    if (
        contract.contract_id != CONTRACT_ID
        or contract.schema_digest != SCHEMA_DIGEST
        or contract.domain != "reference"
        or contract.table != "ticker_type_dim"
        or contract.schema_version != 1
        or contract.source_datasets != ("ticker_types",)
    ):
        raise SilverStoreError("ticker_type_dim candidate identity or scope is not authorized")
    return contract


def _register_schema_review(
    data_root: Path,
    *,
    contract: TableContract,
    created_at: str,
):
    root = data_root.expanduser().resolve()
    store = SilverStore(root)
    workflow_id = stable_digest(
        {
            "actor": ACTOR,
            "contract_id": contract.contract_id,
            "created_at": created_at,
            "workflow_event_version": WORKFLOW_EVENT_VERSION,
        }
    )
    events = root / "manifests" / "silver" / "workflows" / workflow_id / "events"
    if events.exists():
        snapshot = store.status(workflow_id)
        registered, _ = store.load_workflow_contract(workflow_id)
        if registered != contract:
            raise SilverStoreError("existing deterministic workflow uses a different contract")
    else:
        snapshot = store.create_workflow(
            contract,
            actor=ACTOR,
            created_at=created_at,
            note=NOTE,
        )
    if snapshot.state is WorkflowState.PLANNED:
        snapshot = store.submit_schema_review(
            workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            actor=ACTOR,
            created_at=created_at,
            note=NOTE,
        )
    if snapshot.state is not WorkflowState.SCHEMA_REVIEW:
        raise SilverStoreError(
            "deterministic S2 workflow already advanced beyond schema_review; refusing mutation"
        )
    return snapshot


if __name__ == "__main__":
    raise SystemExit(main())
