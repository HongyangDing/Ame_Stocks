"""Run the exact paired S3 condition-code lifecycle through publication."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from ame_stocks_api.silver.condition_code_lifecycle import (
    complete_condition_code_lifecycle,
)
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.store import SilverStoreError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-condition-codes-lifecycle",
        description=(
            "Advance the two manifest-pinned S3 condition-code workflows in lockstep "
            "through bounded previews, review-bound full builds, and publication."
        ),
        epilog=(
            "The Bronze page, 94/123 cardinalities, S1 exchange release, user delegation, "
            "and zero-exception approvals are hard pinned and cannot be overridden."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--git-commit", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        run = complete_condition_code_lifecycle(
            arguments.data_root,
            repo_root=arguments.repo_root,
            git_commit=arguments.git_commit,
        )
        output = {
            "bridge_parent_coverage": "exact",
            "exchange_ids": list(run.exchange_ids),
            "inventory_id": run.inventory.inventory_id,
            "inventory_manifest_path": run.inventory_document.path,
            "inventory_manifest_sha256": run.inventory_document.sha256,
            "state": "published",
            "tables": {
                item.contract.table: {
                    "contract_id": item.contract.contract_id,
                    "data_files": [
                        str(path.relative_to(arguments.data_root.expanduser().resolve()))
                        for path in item.published.data_paths
                    ],
                    "full_build_id": item.full.build_id,
                    "full_manifest_sha256": item.full_document.sha256,
                    "preview_build_id": item.preview.build_id,
                    "preview_manifest_sha256": item.preview_document.sha256,
                    "qa_checks": [check.to_dict() for check in item.full.qa_checks],
                    "release_id": item.release.release_id,
                    "release_manifest_sha256": item.release_document.sha256,
                    "row_funnel": item.full.row_funnel.to_dict(),
                    "state": item.workflow.state.value,
                    "workflow_event_sha256": item.workflow.event_sha256,
                    "workflow_id": item.workflow.workflow_id,
                }
                for item in (run.dim, run.bridge)
            },
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (
        OSError,
        subprocess.SubprocessError,
        SilverContractError,
        SilverStoreError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(2, f"ame-silver-condition-codes-lifecycle: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
