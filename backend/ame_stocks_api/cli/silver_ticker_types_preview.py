"""Explicit mutating CLI for the bounded S2 ticker-types preview only."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ame_stocks_api.silver.contracts import SilverContractError, thaw_json
from ame_stocks_api.silver.store import SilverStoreError
from ame_stocks_api.silver.ticker_type_preview import run_ticker_type_preview


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-ticker-types-preview",
        description=(
            "Build one manifest-bound ticker_type_dim preview and stop at awaiting_review. "
            "This command cannot run a full build or publish."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument(
        "--repo-root",
        type=Path,
        required=True,
        help="clean Git checkout whose HEAD must equal --git-commit",
    )
    parser.add_argument("--workflow-id", required=True)
    parser.add_argument("--expected-event-sha256", required=True)
    parser.add_argument(
        "--manifest",
        action="append",
        required=True,
        help="the one production-authorized relative Bronze manifest path",
    )
    parser.add_argument("--expected-manifest-sha256", required=True)
    parser.add_argument("--expected-input-rows", type=int, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--actor", default="s2-ticker-types-preview-runner")
    parser.add_argument("--calendar-name", choices=("XNYS",), default="XNYS")
    parser.add_argument("--sample-limit", type=int, default=100)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        run = run_ticker_type_preview(
            arguments.data_root,
            workflow_id=arguments.workflow_id,
            expected_event_sha256=arguments.expected_event_sha256,
            manifest_paths=tuple(arguments.manifest),
            expected_manifest_sha256=arguments.expected_manifest_sha256,
            expected_input_rows=arguments.expected_input_rows,
            git_commit=arguments.git_commit,
            repo_root=arguments.repo_root,
            actor=arguments.actor,
            calendar_name=arguments.calendar_name,
            sample_limit=arguments.sample_limit,
        )
        preview = run.build.preview
        if preview is None:  # pragma: no cover - BuildManifest enforces this
            raise SilverStoreError("registered ticker-type preview has no preview metadata")
        output = {
            "build_id": run.build.build_id,
            "build_manifest_path": run.build_document.path,
            "build_manifest_sha256": run.build_document.sha256,
            "contract_id": run.build.intent.contract_id,
            "input_sample_path": preview.input_sample_path,
            "intent": thaw_json(run.build.intent.to_dict()),
            "inventory_id": run.inventory.inventory_id,
            "inventory_path": run.inventory_document.path,
            "output_sample_path": preview.output_sample_path,
            "outputs": [item.to_dict() for item in run.build.outputs],
            "preview": thaw_json(preview.to_dict()),
            "qa_checks": [item.to_dict() for item in run.build.qa_checks],
            "row_funnel": run.build.row_funnel.to_dict(),
            "state": run.workflow.state.value,
            "workflow_event_path": run.workflow.event_path,
            "workflow_event_sha256": run.workflow.event_sha256,
            "workflow_id": run.workflow.workflow_id,
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (OSError, SilverContractError, SilverStoreError, TypeError, ValueError) as exc:
        parser.exit(2, f"ame-silver-ticker-types-preview: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
