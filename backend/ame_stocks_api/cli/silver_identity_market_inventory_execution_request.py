"""Supersede the non-executable Gate-A v1 plan with one exact v2 request."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ame_stocks_api.artifacts import sha256_file
from ame_stocks_api.silver.contracts import SilverContractError, TableContract
from ame_stocks_api.silver.identity_market_inventory_contract import (
    COMPOSITE_FIGI_INVENTORY_CONTRACT,
    COMPOSITE_FIGI_INVENTORY_CONTRACT_ID,
    COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
    COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_market_inventory_execution_plan import (
    INVENTORY_CONTRACT_CANDIDATE_PATH,
    INVENTORY_CONTRACT_RESOURCE_PATH,
    REQUIRED_EXECUTION_RUNTIME_PATHS,
    REQUIRED_EXECUTION_VERIFICATION_PATHS,
    IdentityMarketInventoryExecutionPlanError,
    IdentityMarketInventoryExecutionPlanStore,
    S7CompositeInventoryExecutionPlanV2,
    S7CompositeInventoryExecutionRequestV2,
    S7InventoryCandidateContractPin,
    S7InventoryRuntimeFilePin,
    S7V1InventoryExecutionBlockedEvent,
    StoredInventoryExecutionDocument,
)


@dataclass(frozen=True, slots=True)
class S7InventoryExecutionRequestRun:
    blocked_event: S7V1InventoryExecutionBlockedEvent
    blocked_event_document: StoredInventoryExecutionDocument
    plan: S7CompositeInventoryExecutionPlanV2
    plan_document: StoredInventoryExecutionDocument
    request: S7CompositeInventoryExecutionRequestV2
    request_document: StoredInventoryExecutionDocument
    all_documents_preexisting: bool


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-identity-market-inventory-execution-request",
        description=(
            "Record why the under-bound v1 literal was not executed and create one "
            "fully executable v2 Plan/Request. This command reads no Parquet and "
            "cannot approve or execute the inventory."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--recorded-at", required=True)
    parser.add_argument("--blocked-recorded-by", required=True)
    parser.add_argument("--plan-created-by", required=True)
    parser.add_argument("--request-created-by", required=True)
    return parser


def create_s7_inventory_execution_request(
    data_root: Path,
    *,
    repo_root: Path,
    git_commit: str,
    recorded_at: str,
    blocked_recorded_by: str,
    plan_created_by: str,
    request_created_by: str,
) -> S7InventoryExecutionRequestRun:
    """Preflight exact executable bytes, then write only blocked/plan/request JSON."""

    instant = _parse_nonfuture_utc(recorded_at)
    repository, execution_tree = _verify_exact_clean_checkout(repo_root, git_commit)
    contract = _verify_contract_bytes(repository)
    runtime_paths = set(REQUIRED_EXECUTION_RUNTIME_PATHS) | {
        INVENTORY_CONTRACT_CANDIDATE_PATH,
        INVENTORY_CONTRACT_RESOURCE_PATH,
    }
    runtime_files = _pin_tracked_files(repository, runtime_paths)
    verification_files = _pin_tracked_files(
        repository,
        set(REQUIRED_EXECUTION_VERIFICATION_PATHS),
    )

    store = IdentityMarketInventoryExecutionPlanStore(data_root)
    store.load_exact_v1_controls()
    blocked = S7V1InventoryExecutionBlockedEvent(
        recorded_by=blocked_recorded_by,
        recorded_at_utc=instant,
    )
    prospective_blocked = StoredInventoryExecutionDocument(
        blocked.relative_path,
        blocked.sha256,
        len(blocked.content),
    )
    plan = S7CompositeInventoryExecutionPlanV2.create(
        created_by=plan_created_by,
        created_at_utc=instant,
        execution_git_commit=git_commit,
        execution_git_tree=execution_tree,
        execution_data_root=str(store.root),
        runtime_files=runtime_files,
        verification_files=verification_files,
        inventory_contract=contract,
        blocked_event=blocked,
        blocked_event_receipt=prospective_blocked,
    )
    prospective_plan = StoredInventoryExecutionDocument(
        plan.relative_path,
        plan.sha256,
        len(plan.content),
    )
    request = S7CompositeInventoryExecutionRequestV2.create(
        plan,
        prospective_plan,
        created_by=request_created_by,
        created_at_utc=instant,
    )

    destinations = tuple(
        (relative, checksum)
        for relative, checksum in (
            (blocked.relative_path, blocked.sha256),
            (plan.relative_path, plan.sha256),
            (request.relative_path, request.sha256),
        )
    )
    preexisting = tuple(_preflight_destination(store.root, *item) for item in destinations)
    blocked_document = store.store_blocked_event(blocked)
    plan_document = store.store_execution_plan_v2(plan)
    request_document = store.store_execution_request_v2(request)
    loaded_blocked, loaded_blocked_document = store.load_blocked_event(
        blocked.event_id,
        expected_sha256=blocked.sha256,
    )
    loaded_plan, loaded_plan_document = store.load_execution_plan_v2(
        plan.plan_id,
        expected_sha256=plan.sha256,
    )
    loaded_request, loaded_request_document = store.load_execution_request_v2(
        request.request_event_id,
        expected_sha256=request.sha256,
    )
    if (
        loaded_blocked != blocked
        or loaded_blocked_document != blocked_document
        or loaded_plan != plan
        or loaded_plan_document != plan_document
        or loaded_request != request
        or loaded_request_document != request_document
    ):
        raise IdentityMarketInventoryExecutionPlanError(
            "v2 execution controls differ on immutable readback"
        )
    return S7InventoryExecutionRequestRun(
        blocked_event=blocked,
        blocked_event_document=blocked_document,
        plan=plan,
        plan_document=plan_document,
        request=request,
        request_document=request_document,
        all_documents_preexisting=all(preexisting),
    )


def _verify_contract_bytes(repo_root: Path) -> S7InventoryCandidateContractPin:
    candidate_path = repo_root / INVENTORY_CONTRACT_CANDIDATE_PATH
    resource_path = repo_root / INVENTORY_CONTRACT_RESOURCE_PATH
    if (
        not candidate_path.is_file()
        or candidate_path.is_symlink()
        or not resource_path.is_file()
        or resource_path.is_symlink()
    ):
        raise IdentityMarketInventoryExecutionPlanError(
            "inventory candidate contract bytes are unavailable or unsafe"
        )
    candidate_bytes = candidate_path.read_bytes()
    resource_bytes = resource_path.read_bytes()
    if candidate_bytes != resource_bytes:
        raise IdentityMarketInventoryExecutionPlanError(
            "inventory candidate and packaged resource bytes differ"
        )
    checksum = hashlib.sha256(candidate_bytes).hexdigest()
    try:
        candidate = TableContract.from_dict(json.loads(candidate_bytes))
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        SilverContractError,
        ValueError,
    ) as exc:
        raise IdentityMarketInventoryExecutionPlanError(
            "inventory candidate contract cannot be parsed"
        ) from exc
    if (
        checksum != COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256
        or candidate != COMPOSITE_FIGI_INVENTORY_CONTRACT
        or candidate.contract_id != COMPOSITE_FIGI_INVENTORY_CONTRACT_ID
        or candidate.schema_digest != COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST
    ):
        raise IdentityMarketInventoryExecutionPlanError(
            "inventory candidate contract identity changed"
        )
    return S7InventoryCandidateContractPin(
        contract_id=candidate.contract_id,
        schema_digest=candidate.schema_digest,
        candidate_sha256=checksum,
        resource_sha256=checksum,
    )


def _verify_exact_clean_checkout(
    repo_root: Path,
    expected_commit: str,
) -> tuple[Path, str]:
    repository = repo_root.expanduser().resolve()
    if not repository.is_dir() or repository.is_symlink():
        raise IdentityMarketInventoryExecutionPlanError("repo_root is unsafe")
    try:
        top = Path(_git(repository, "rev-parse", "--show-toplevel")).resolve()
        head = _git(repository, "rev-parse", "HEAD")
        tree = _git(repository, "rev-parse", "HEAD^{tree}")
        status = _git(repository, "status", "--porcelain", "--untracked-files=all")
    except (OSError, subprocess.SubprocessError) as exc:
        raise IdentityMarketInventoryExecutionPlanError(
            "execution Git checkout cannot be verified"
        ) from exc
    if top != repository or head != expected_commit or status:
        raise IdentityMarketInventoryExecutionPlanError(
            "execution Git checkout is dirty, displaced, or at the wrong commit"
        )
    return repository, tree


def _pin_tracked_files(
    repo_root: Path,
    relative_paths: set[str],
) -> tuple[S7InventoryRuntimeFilePin, ...]:
    pins: list[S7InventoryRuntimeFilePin] = []
    for relative in sorted(relative_paths):
        path = repo_root / relative
        if not path.is_file() or path.is_symlink() or path.resolve() != path:
            raise IdentityMarketInventoryExecutionPlanError(
                f"required tracked execution file is missing or unsafe: {relative}"
            )
        try:
            git_blob = _git(repo_root, "rev-parse", f"HEAD:{relative}")
            working_blob = _git(repo_root, "hash-object", "--no-filters", "--", relative)
        except (OSError, subprocess.SubprocessError) as exc:
            raise IdentityMarketInventoryExecutionPlanError(
                f"required execution file is not tracked at HEAD: {relative}"
            ) from exc
        if git_blob != working_blob:
            raise IdentityMarketInventoryExecutionPlanError(
                f"required execution file differs from HEAD: {relative}"
            )
        pins.append(
            S7InventoryRuntimeFilePin(
                path=relative,
                git_blob=git_blob,
                sha256=sha256_file(path),
                bytes=path.stat().st_size,
            )
        )
    return tuple(pins)


def _preflight_destination(root: Path, relative: str, expected_sha256: str) -> bool:
    path = root / relative
    if not path.exists():
        return False
    if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
        raise IdentityMarketInventoryExecutionPlanError(
            f"immutable v2 control destination conflicts: {relative}"
        )
    return True


def _parse_nonfuture_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise IdentityMarketInventoryExecutionPlanError(
            "recorded_at must be canonical ISO-8601 UTC"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IdentityMarketInventoryExecutionPlanError("recorded_at must include UTC")
    normalized = parsed.astimezone(UTC)
    if parsed.utcoffset().total_seconds() != 0 or normalized.isoformat() != value:
        raise IdentityMarketInventoryExecutionPlanError(
            "recorded_at must be canonical ISO-8601 UTC"
        )
    if normalized > datetime.now(UTC):
        raise IdentityMarketInventoryExecutionPlanError("recorded_at cannot be in the future")
    return normalized


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    return completed.stdout.strip()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        run = create_s7_inventory_execution_request(
            arguments.data_root,
            repo_root=arguments.repo_root,
            git_commit=arguments.git_commit,
            recorded_at=arguments.recorded_at,
            blocked_recorded_by=arguments.blocked_recorded_by,
            plan_created_by=arguments.plan_created_by,
            request_created_by=arguments.request_created_by,
        )
    except (
        IdentityMarketInventoryExecutionPlanError,
        OSError,
        subprocess.SubprocessError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(2, f"ame-silver-identity-market-inventory-execution-request: {exc}\n")
    print(
        json.dumps(
            {
                "all_documents_preexisting": run.all_documents_preexisting,
                "approval_created": False,
                "blocked_v1": {
                    "event_id": run.blocked_event.event_id,
                    "path": run.blocked_event_document.path,
                    "sha256": run.blocked_event_document.sha256,
                    "state": run.blocked_event.state,
                },
                "inventory_executed": False,
                "mode": "superseding_v2_plan_and_request_only",
                "plan": {
                    "algorithm_digest": run.request.algorithm_digest,
                    "candidate_contract_id": run.request.inventory_contract_id,
                    "candidate_schema_digest": run.request.inventory_schema_digest,
                    "input_binding_digest": run.request.input_binding_digest,
                    "path": run.plan_document.path,
                    "plan_id": run.plan.plan_id,
                    "qa_semantics_digest": run.request.qa_semantics_digest,
                    "resource_caps_digest": run.request.resource_caps_digest,
                    "runtime_file_set_digest": run.request.runtime_file_set_digest,
                    "sha256": run.plan_document.sha256,
                    "state": run.plan.plan_state,
                    "verification_file_set_digest": (run.request.verification_file_set_digest),
                },
                "request": {
                    "canonical_approval_literal": run.request.canonical_approval_literal,
                    "path": run.request_document.path,
                    "request_event_id": run.request.request_event_id,
                    "sha256": run.request_document.sha256,
                    "state": run.request.request_state,
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
