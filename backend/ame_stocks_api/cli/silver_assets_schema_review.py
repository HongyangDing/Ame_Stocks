"""Register the three fixed S4 Assets contracts and stop at schema review."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.contracts import SilverContractError, TableContract
from ame_stocks_api.silver.store import (
    WORKFLOW_EVENT_VERSION,
    SilverStore,
    SilverStoreError,
    WorkflowSnapshot,
    WorkflowState,
)

ACTOR_PREFIX = "s4-assets-schema-review"
S4_SCHEMA_CREATED_AT = "2026-07-13T08:30:00+00:00"
NOTE = "Registered one fixed S4 Assets contract; schema approval and all data work remain gated."


@dataclass(frozen=True, slots=True)
class AssetSchemaReviewSpec:
    """Immutable Git and contract identity for one approved S4 candidate."""

    candidate_path: str
    resource_path: str
    candidate_sha256: str
    contract_id: str
    schema_digest: str
    domain: str
    table: str

    @property
    def actor(self) -> str:
        return f"{ACTOR_PREFIX}-{self.table.replace('_', '-')}"


ASSET_SCHEMA_REVIEW_SPECS = (
    AssetSchemaReviewSpec(
        candidate_path=(
            "docs/silver/contracts/identity/asset_observation_daily.schema-v1.candidate.json"
        ),
        resource_path=(
            "backend/ame_stocks_api/silver/schema_resources/asset_observation_daily.schema-v1.json"
        ),
        candidate_sha256=("dbe656df1cd0e007498b2f7c3a79c6654a52d8ffa7f4099a1b8f32546ab3eced"),
        contract_id=("dd916b8528b9ce1a341e6b8ad897ae80e40d5df118b8e102e4ea1f1ea6e9c045"),
        schema_digest=("402d0ea624dc26e43ea63974572ede5a46ae20e0741e97a3d01d07075a71bc1e"),
        domain="identity",
        table="asset_observation_daily",
    ),
    AssetSchemaReviewSpec(
        candidate_path=(
            "docs/silver/contracts/identity/asset_observation_version.schema-v1.candidate.json"
        ),
        resource_path=(
            "backend/ame_stocks_api/silver/schema_resources/"
            "asset_observation_version.schema-v1.json"
        ),
        candidate_sha256=("c3249b8684347e5b491cbe31d44c19f6ce0ddec4568a61c831baebafe3433751"),
        contract_id=("14ce114f5911f7e4d1c15e58f0f42a8307066d6517e859d6233fa23c199616fc"),
        schema_digest=("4c797ca373d697078b2061b9a76696dc036a1d2db0a5f8e1fe3ce2dac4b6bb4b"),
        domain="identity",
        table="asset_observation_version",
    ),
    AssetSchemaReviewSpec(
        candidate_path=(
            "docs/silver/contracts/reference/universe_source_daily.schema-v1.candidate.json"
        ),
        resource_path=(
            "backend/ame_stocks_api/silver/schema_resources/universe_source_daily.schema-v1.json"
        ),
        candidate_sha256=("49fb584c6109eee6088aaf291773089caa171d02a31d3c159aa474885abd6d2a"),
        contract_id=("9711320ee9227df347224b7cd17a41fe10a352fddf089cd72b758bde7a7f0c58"),
        schema_digest=("78b799cd5a2621b5a78e4ed8c23c090f6aea686fcd786366e5c258e81ad278a5"),
        domain="reference",
        table="universe_source_daily",
    ),
)

_TRACKED_SUPPORT_PATH = "backend/ame_stocks_api/silver/asset_contract.py"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-assets-schema-review",
        description=(
            "Register the three fixed S4 Assets contracts in lockstep and stop every "
            "workflow at schema_review. This command cannot approve, transform, preview, "
            "build, or publish data."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument(
        "--created-at",
        required=True,
        help=(
            "must exactly equal the fixed deterministic registration timestamp "
            f"{S4_SCHEMA_CREATED_AT}"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        repo_root = _verify_git_checkout(arguments.repo_root, arguments.git_commit)
        contracts = _load_fixed_contracts(repo_root)
        snapshots = register_asset_schema_reviews(
            arguments.data_root,
            contracts=contracts,
            created_at=arguments.created_at,
        )
        output = {
            "created_at": arguments.created_at,
            "git_commit": arguments.git_commit,
            "mode": "schema_review_only",
            "state": "schema_review",
            "workflows": {
                spec.table: {
                    "candidate_path": spec.candidate_path,
                    "candidate_sha256": spec.candidate_sha256,
                    "contract_id": contract.contract_id,
                    "domain": contract.domain,
                    "event_path": snapshot.event_path,
                    "event_sha256": snapshot.event_sha256,
                    "resource_path": spec.resource_path,
                    "schema_digest": contract.schema_digest,
                    "schema_version": contract.schema_version,
                    "sequence": snapshot.sequence,
                    "source_datasets": list(contract.source_datasets),
                    "state": snapshot.state.value,
                    "workflow_id": snapshot.workflow_id,
                }
                for (spec, contract), snapshot in zip(
                    zip(ASSET_SCHEMA_REVIEW_SPECS, contracts, strict=True),
                    snapshots,
                    strict=True,
                )
            },
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
        parser.exit(2, f"ame-silver-assets-schema-review: {exc}\n")


def register_asset_schema_reviews(
    data_root: Path,
    *,
    contracts: tuple[TableContract, ...],
    created_at: str,
) -> tuple[WorkflowSnapshot, ...]:
    """Register three deterministic workflows after a complete fail-closed preflight."""

    if len(contracts) != len(ASSET_SCHEMA_REVIEW_SPECS):
        raise SilverStoreError("S4 schema review requires exactly three contracts")
    if created_at != S4_SCHEMA_CREATED_AT:
        raise SilverStoreError("S4 schema-review created_at is not authorized")
    root = data_root.expanduser().resolve()
    store = SilverStore(root)

    expected = tuple(zip(ASSET_SCHEMA_REVIEW_SPECS, contracts, strict=True))
    for spec, contract in expected:
        _verify_contract_scope(spec, contract)
        _preflight_contract_registry(store, contract)
        _preflight_workflow(store, spec, contract, created_at)

    snapshots: list[WorkflowSnapshot] = []
    for spec, contract in expected:
        workflow_id = _workflow_id(spec, contract, created_at)
        event_directory = root / "manifests/silver/workflows" / workflow_id / "events"
        if event_directory.exists():
            snapshot = store.verify_workflow_trust_chain(
                workflow_id,
                verify_artifacts=True,
            )
        else:
            snapshot = store.create_workflow(
                contract,
                actor=spec.actor,
                created_at=created_at,
                note=NOTE,
            )
        if snapshot.state is WorkflowState.PLANNED:
            snapshot = store.submit_schema_review(
                workflow_id,
                expected_event_sha256=snapshot.event_sha256,
                actor=spec.actor,
                created_at=created_at,
                note=NOTE,
            )
        snapshots.append(snapshot)

    verified: list[WorkflowSnapshot] = []
    for (spec, contract), snapshot in zip(expected, snapshots, strict=True):
        current = store.verify_workflow_trust_chain(
            snapshot.workflow_id,
            verify_artifacts=True,
        )
        registered, _ = store.load_workflow_contract(snapshot.workflow_id)
        records = store.workflow_events(snapshot.workflow_id)
        if (
            current.state is not WorkflowState.SCHEMA_REVIEW
            or current.sequence != 2
            or len(records) != 2
            or registered != contract
            or records[0].event.to_state is not WorkflowState.PLANNED
            or records[1].event.to_state is not WorkflowState.SCHEMA_REVIEW
        ):
            raise SilverStoreError(
                f"S4 {spec.table} workflow did not stop at the exact schema_review gate"
            )
        verified.append(current)
    return tuple(verified)


def _verify_git_checkout(repo_root: Path, git_commit: str) -> Path:
    root = repo_root.expanduser().resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise SilverStoreError("schema-review CLI is not executing from --repo-root") from exc
    if Path(_git_output(root, "rev-parse", "--show-toplevel")).resolve() != root:
        raise SilverStoreError("--repo-root is not the exact Git top level")
    if _git_output(root, "rev-parse", "HEAD") != git_commit:
        raise SilverStoreError("Git HEAD differs from --git-commit")
    tracked = [module_relative, _TRACKED_SUPPORT_PATH]
    tracked.extend(spec.candidate_path for spec in ASSET_SCHEMA_REVIEW_SPECS)
    tracked.extend(spec.resource_path for spec in ASSET_SCHEMA_REVIEW_SPECS)
    for path in tracked:
        _git_output(root, "ls-files", "--error-unmatch", "--", path)
    if _git_output(root, "status", "--porcelain=v1", "--untracked-files=all"):
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


def _load_fixed_contracts(repo_root: Path) -> tuple[TableContract, ...]:
    contracts: list[TableContract] = []
    for spec in ASSET_SCHEMA_REVIEW_SPECS:
        candidate_content = (repo_root / spec.candidate_path).read_bytes()
        resource_content = (repo_root / spec.resource_path).read_bytes()
        if hashlib.sha256(candidate_content).hexdigest() != spec.candidate_sha256:
            raise SilverStoreError(f"{spec.table} candidate SHA-256 is not authorized")
        if resource_content != candidate_content:
            raise SilverStoreError(f"packaged {spec.table} resource differs from its candidate")
        document = json.loads(candidate_content)
        if not isinstance(document, dict):
            raise SilverContractError(f"{spec.table} candidate must be a JSON object")
        contract = TableContract.from_dict(document)
        _verify_contract_scope(spec, contract)
        if ASSET_CONTRACTS.get(spec.table) != contract:
            raise SilverStoreError(f"imported {spec.table} resource differs from its candidate")
        contracts.append(contract)
    return tuple(contracts)


def _verify_contract_scope(spec: AssetSchemaReviewSpec, contract: TableContract) -> None:
    if (
        contract.contract_id != spec.contract_id
        or contract.schema_digest != spec.schema_digest
        or contract.domain != spec.domain
        or contract.table != spec.table
        or contract.schema_version != 1
        or contract.source_datasets != ("assets",)
    ):
        raise SilverStoreError(f"{spec.table} candidate identity or scope is not authorized")


def _preflight_contract_registry(store: SilverStore, contract: TableContract) -> None:
    expected_path = store.contract_path(contract)
    existing = sorted(expected_path.parent.glob("contract-*.json"))
    unexpected = [path for path in existing if path.name != expected_path.name]
    if unexpected:
        raise SilverStoreError(f"{contract.table} schema version already has a different contract")
    if not expected_path.exists():
        return
    if expected_path.is_symlink() or not expected_path.is_file():
        raise SilverStoreError(f"existing {contract.table} contract path is unsafe")
    document = json.loads(expected_path.read_text(encoding="utf-8"))
    if TableContract.from_dict(document) != contract:
        raise SilverStoreError(f"existing {contract.table} registry contract differs")


def _preflight_workflow(
    store: SilverStore,
    spec: AssetSchemaReviewSpec,
    contract: TableContract,
    created_at: str,
) -> None:
    workflow_id = _workflow_id(spec, contract, created_at)
    event_directory = store.root / "manifests/silver/workflows" / workflow_id / "events"
    if not event_directory.exists():
        return
    snapshot = store.verify_workflow_trust_chain(workflow_id, verify_artifacts=True)
    registered, _ = store.load_workflow_contract(workflow_id)
    if registered != contract:
        raise SilverStoreError(f"existing deterministic {spec.table} workflow changed contract")
    if snapshot.state not in {WorkflowState.PLANNED, WorkflowState.SCHEMA_REVIEW}:
        raise SilverStoreError(
            f"deterministic {spec.table} workflow already advanced beyond schema_review"
        )


def _workflow_id(
    spec: AssetSchemaReviewSpec,
    contract: TableContract,
    created_at: str,
) -> str:
    return stable_digest(
        {
            "actor": spec.actor,
            "contract_id": contract.contract_id,
            "created_at": created_at,
            "workflow_event_version": WORKFLOW_EVENT_VERSION,
        }
    )


if __name__ == "__main__":
    raise SystemExit(main())
