"""Explicit CLI for completing the exact reviewed S1 exchanges workflow."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from ame_stocks_api.silver.contracts import SilverContractError, thaw_json
from ame_stocks_api.silver.exchange_release import complete_exchange_release
from ame_stocks_api.silver.store import SilverStoreError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-exchanges-release",
        description=(
            "Advance the exact reviewed 27-row exchange_dim preview through a "
            "review-bound full build and immutable published release."
        ),
        epilog=(
            "Interrupted runs must resume from the exact same --runner-git-commit; "
            "runtime provenance is immutable within the deterministic full build ID."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--workflow-id", required=True)
    parser.add_argument("--expected-event-sha256", required=True)
    parser.add_argument("--reviewed-preview-build-id", required=True)
    parser.add_argument("--reviewed-preview-manifest-sha256", required=True)
    parser.add_argument("--runner-git-commit", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--approver", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        run = complete_exchange_release(
            arguments.data_root,
            workflow_id=arguments.workflow_id,
            expected_event_sha256=arguments.expected_event_sha256,
            reviewed_preview_build_id=arguments.reviewed_preview_build_id,
            reviewed_preview_manifest_sha256=arguments.reviewed_preview_manifest_sha256,
            repo_root=arguments.repo_root,
            runner_git_commit=arguments.runner_git_commit,
            actor=arguments.actor,
            approver=arguments.approver,
        )
        output = {
            "build_id": run.full.build_id,
            "build_manifest_path": run.full_document.path,
            "build_manifest_sha256": run.full_document.sha256,
            "contract_id": run.full.intent.contract_id,
            "data_files": [
                str(path.relative_to(arguments.data_root.expanduser().resolve()))
                for path in run.published.data_paths
            ],
            "full_intent": thaw_json(run.full.intent.to_dict()),
            "outputs": [item.to_dict() for item in run.full.outputs],
            "preview_build_id": run.preview.build_id,
            "preview_manifest_sha256": run.preview_document.sha256,
            "qa_checks": [item.to_dict() for item in run.full.qa_checks],
            "release_id": run.release.release_id,
            "release_manifest_path": run.release_document.path,
            "release_manifest_sha256": run.release_document.sha256,
            "row_funnel": run.full.row_funnel.to_dict(),
            "state": run.workflow.state.value,
            "workflow_event_path": run.workflow.event_path,
            "workflow_event_sha256": run.workflow.event_sha256,
            "workflow_id": run.workflow.workflow_id,
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
        parser.exit(2, f"ame-silver-exchanges-release: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
