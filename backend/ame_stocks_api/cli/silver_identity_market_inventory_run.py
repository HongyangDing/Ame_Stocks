"""Run one exact approved S7 Composite-FIGI inventory to awaiting review."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40,64}$")


class InventoryRunBootstrapError(RuntimeError):
    """Raised before the capability-bearing runner module is imported."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-identity-market-inventory-run",
        description=(
            "Execute exactly one source-bound S4 Composite-FIGI inventory under "
            "an immutable v2 approval, then stop at awaiting_review."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--approval-id", required=True)
    parser.add_argument("--approval-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        _bootstrap_exact_checkout(
            arguments.data_root,
            plan_id=arguments.plan_id,
            expected_plan_sha256=arguments.plan_sha256,
        )
    except (InventoryRunBootstrapError, OSError, TypeError, ValueError) as exc:
        parser.exit(2, f"ame-silver-identity-market-inventory-run: {exc}\n")
    try:
        run_inventory, runner_error = _load_runner()
    except (ImportError, RuntimeError) as exc:
        parser.exit(2, f"ame-silver-identity-market-inventory-run: {exc}\n")
    try:
        completion = run_inventory(
            arguments.data_root,
            plan_id=arguments.plan_id,
            expected_plan_sha256=arguments.plan_sha256,
            approval_id=arguments.approval_id,
            expected_approval_sha256=arguments.approval_sha256,
        )
    except runner_error as exc:
        parser.exit(2, f"ame-silver-identity-market-inventory-run: {exc}\n")
    print(
        json.dumps(
            {
                "approval_id": completion.approval_id,
                "candidate": {
                    "candidate_id": completion.candidate_id,
                    "data_path": completion.data_path,
                    "manifest_path": completion.candidate_path,
                    "qa_path": completion.qa_path,
                    "state": completion.completion_state,
                },
                "completion": {
                    "completion_id": completion.completion_id,
                    "path": completion.relative_path,
                    "sha256": completion.sha256,
                },
                "counts": {
                    "authority_rows": completion.authority_row_count,
                    "inventory_rows": completion.inventory_row_count,
                    "reconciliation_rows": completion.reconciliation_row_count,
                    "sessions": completion.session_count,
                    "source_artifacts": completion.source_artifact_count,
                    "source_rows": completion.source_row_count,
                },
                "mode": "exact_inventory_execution_to_awaiting_review_only",
                "plan_id": completion.plan_id,
                "resource_measurements": {
                    "maximum_tmp_bytes": completion.maximum_tmp_bytes,
                    "minimum_disk_free_bytes": completion.minimum_disk_free_bytes,
                    "disk_free_warning_triggered": completion.disk_free_warning_triggered,
                    "output_bytes": completion.output_bytes,
                    "peak_rss_bytes": completion.peak_rss_bytes,
                    "wall_clock_seconds": completion.wall_clock_seconds,
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _load_runner() -> tuple[Any, type[Exception]]:
    """Import the runner only after the stdlib-only exact-tree bootstrap passes."""

    from ame_stocks_api.silver.identity_market_inventory_runner import (
        IdentityMarketInventoryRunnerError,
        run_source_bound_composite_inventory,
    )

    return run_source_bound_composite_inventory, IdentityMarketInventoryRunnerError


def _bootstrap_exact_checkout(
    data_root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
) -> None:
    """Verify the exact plan, Git tree and every pin before importing the runner."""

    _require_digest(plan_id, "plan ID")
    _require_digest(expected_plan_sha256, "plan SHA-256")
    if not isinstance(data_root, Path):
        raise InventoryRunBootstrapError("data_root must be a Path")
    expanded = data_root.expanduser()
    if expanded.is_symlink():
        raise InventoryRunBootstrapError("data_root cannot be a symlink")
    root = expanded.resolve()
    if not root.is_dir() or root == Path("/"):
        raise InventoryRunBootstrapError("data_root is unavailable or unsafe")
    plan_path = _safe_path(
        root,
        (
            "manifests/silver/identity/composite-inventory-execution-plans-v2/"
            f"plan_id={plan_id}/manifest.json"
        ),
    )
    if not plan_path.is_file() or plan_path.is_symlink():
        raise InventoryRunBootstrapError("exact v2 plan is missing or unsafe")
    content = plan_path.read_bytes()
    if hashlib.sha256(content).hexdigest() != expected_plan_sha256:
        raise InventoryRunBootstrapError("exact v2 plan SHA-256 differs")
    document = _decode_canonical_json(content, "v2 execution plan")
    if (
        document.get("plan_id") != plan_id
        or document.get("execution_data_root") != str(root)
        or document.get("artifact_type") != "s7_composite_inventory_execution_plan_v2"
        or document.get("plan_state") != "awaiting_exact_execution_approval"
    ):
        raise InventoryRunBootstrapError("v2 plan identity or execution root differs")

    git_binding = _mapping(document.get("git_binding"), "Git binding")
    verification_binding = _mapping(
        document.get("verification_binding"),
        "verification binding",
    )
    commit = _require_git_object(
        git_binding.get("execution_git_commit"),
        "execution Git commit",
    )
    tree = _require_git_object(
        git_binding.get("execution_git_tree"),
        "execution Git tree",
    )
    repository = _repository_root()
    if (
        _git(repository, "rev-parse", "HEAD") != commit
        or _git(repository, "rev-parse", "HEAD^{tree}") != tree
        or _git(repository, "status", "--porcelain=v1", "--untracked-files=all")
    ):
        raise InventoryRunBootstrapError("Git checkout is dirty or differs from the v2 plan")

    runtime = _pin_array(git_binding.get("runtime_files"), "runtime files")
    verification = _pin_array(
        verification_binding.get("verification_files"),
        "verification files",
    )
    if _stable_digest(runtime) != git_binding.get("runtime_file_set_digest") or _stable_digest(
        verification
    ) != verification_binding.get("verification_file_set_digest"):
        raise InventoryRunBootstrapError("runtime or verification pin-set digest differs")
    run_cli_path = "backend/ame_stocks_api/cli/silver_identity_market_inventory_run.py"
    if run_cli_path not in {str(item["path"]) for item in runtime}:
        raise InventoryRunBootstrapError("runner bootstrap file is not pinned")
    for pin in (*runtime, *verification):
        _verify_pin(repository, commit, pin)


def _pin_array(value: object, label: str) -> list[dict[str, object]]:
    if not isinstance(value, list) or not value:
        raise InventoryRunBootstrapError(f"{label} must be a nonempty array")
    pins: list[dict[str, object]] = []
    paths: set[str] = set()
    for raw in value:
        pin = _mapping(raw, "execution file pin")
        if set(pin) != {"bytes", "git_blob", "path", "sha256"}:
            raise InventoryRunBootstrapError("execution file pin schema differs")
        path = pin.get("path")
        if not isinstance(path, str) or path in paths:
            raise InventoryRunBootstrapError("execution file pin path is invalid or duplicated")
        paths.add(path)
        _require_digest(pin.get("sha256"), "execution file SHA-256")
        _require_git_object(pin.get("git_blob"), "execution file Git blob")
        if type(pin.get("bytes")) is not int or int(pin["bytes"]) < 0:
            raise InventoryRunBootstrapError("execution file byte count is invalid")
        pins.append(pin)
    if [str(item["path"]) for item in pins] != sorted(paths):
        raise InventoryRunBootstrapError(f"{label} are not canonical path-sorted")
    return pins


def _verify_pin(repository: Path, commit: str, pin: dict[str, object]) -> None:
    relative = str(pin["path"])
    path = _safe_path(repository, relative)
    if (
        not path.is_file()
        or path.is_symlink()
        or path.stat().st_size != pin["bytes"]
        or _sha256_file(path) != pin["sha256"]
    ):
        raise InventoryRunBootstrapError(f"pinned execution bytes differ: {relative}")
    output = _git(repository, "ls-tree", commit, "--", relative).split()
    if len(output) < 4 or output[1] != "blob" or output[2] != pin["git_blob"]:
        raise InventoryRunBootstrapError(f"pinned Git blob differs: {relative}")


def _repository_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / ".git").exists() and (parent / "pyproject.toml").is_file():
            return parent
    raise InventoryRunBootstrapError("approved Git repository cannot be located")


def _safe_path(root: Path, relative: str) -> Path:
    candidate = Path(relative)
    if candidate.is_absolute() or candidate.as_posix() != relative or ".." in candidate.parts:
        raise InventoryRunBootstrapError("execution path is not a safe relative path")
    path = Path(os.path.abspath(root / candidate))
    try:
        parts = path.relative_to(root).parts
    except ValueError as exc:
        raise InventoryRunBootstrapError("execution path escaped its root") from exc
    current = root
    for part in parts:
        current /= part
        if current.is_symlink():
            raise InventoryRunBootstrapError("execution path traverses a symlink")
    return path


def _decode_canonical_json(content: bytes, label: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise InventoryRunBootstrapError(f"{label} has duplicate keys")
            result[key] = value
        return result

    try:
        value = json.loads(content, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InventoryRunBootstrapError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict) or _canonical_bytes(value) != content:
        raise InventoryRunBootstrapError(f"{label} is not canonical JSON")
    return value


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise InventoryRunBootstrapError(f"{label} must be an object")
    return value


def _canonical_bytes(value: dict[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _stable_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise InventoryRunBootstrapError(f"{label} must be lowercase SHA-256")
    return value


def _require_git_object(value: object, label: str) -> str:
    if not isinstance(value, str) or _GIT_OBJECT.fullmatch(value) is None:
        raise InventoryRunBootstrapError(f"{label} is invalid")
    return value


def _git(root: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(
            ("git", "-C", str(root), *arguments),
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise InventoryRunBootstrapError("Git bootstrap verification failed") from exc
    return completed.stdout.strip()


if __name__ == "__main__":
    raise SystemExit(main())
