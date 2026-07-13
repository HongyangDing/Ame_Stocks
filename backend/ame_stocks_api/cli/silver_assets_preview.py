"""Explicit mutating CLI for the exact bounded S4 assets preview only."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ame_stocks_api.silver.asset_preview import run_asset_preview
from ame_stocks_api.silver.contracts import SilverContractError, thaw_json
from ame_stocks_api.silver.store import SilverStoreError

_TABLE_ARGUMENTS = (
    "asset_observation_daily",
    "asset_observation_version",
    "universe_source_daily",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-assets-preview",
        description=(
            "Build the exact 2026-05-11 active/inactive assets preview for three tables, "
            "then stop every workflow at awaiting_review. This command cannot run a full "
            "build or publish."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument(
        "--repo-root",
        type=Path,
        required=True,
        help="clean Git checkout whose HEAD must equal --git-commit",
    )
    for table in _TABLE_ARGUMENTS:
        option = table.replace("_", "-")
        parser.add_argument(f"--{option}-workflow-id", required=True)
        parser.add_argument(f"--{option}-expected-event-sha256", required=True)
    parser.add_argument(
        "--manifest",
        action="append",
        required=True,
        help="production-authorized relative Bronze assets manifest path; pass exactly twice",
    )
    parser.add_argument(
        "--expected-manifest-sha256",
        action="append",
        required=True,
        metavar="PATH=SHA256",
        help="expected digest for each --manifest path; pass exactly twice",
    )
    parser.add_argument("--expected-input-rows", type=int, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--actor", default="s4-assets-preview-runner")
    parser.add_argument("--calendar-name", choices=("XNYS",), default="XNYS")
    parser.add_argument("--sample-limit", type=int, default=100)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        workflow_ids = {
            table: getattr(arguments, f"{table}_workflow_id") for table in _TABLE_ARGUMENTS
        }
        expected_events = {
            table: getattr(arguments, f"{table}_expected_event_sha256")
            for table in _TABLE_ARGUMENTS
        }
        expected_manifest_sha = _parse_path_digests(
            arguments.expected_manifest_sha256,
            parser=parser,
        )
        run = run_asset_preview(
            arguments.data_root,
            workflow_ids=workflow_ids,
            expected_event_sha256_by_table=expected_events,
            manifest_paths=tuple(arguments.manifest),
            expected_manifest_sha256_by_path=expected_manifest_sha,
            expected_input_rows=arguments.expected_input_rows,
            git_commit=arguments.git_commit,
            repo_root=arguments.repo_root,
            actor=arguments.actor,
            calendar_name=arguments.calendar_name,
            sample_limit=arguments.sample_limit,
        )
        output = {
            "inventory_id": run.inventory.inventory_id,
            "inventory_path": run.inventory_document.path,
            "inventory_sha256": run.inventory_document.sha256,
            "tables": {
                item.build.intent.table: {
                    "build_id": item.build.build_id,
                    "build_manifest_path": item.build_document.path,
                    "build_manifest_sha256": item.build_document.sha256,
                    "intent": thaw_json(item.build.intent.to_dict()),
                    "outputs": [artifact.to_dict() for artifact in item.build.outputs],
                    "preview": (
                        None
                        if item.build.preview is None
                        else thaw_json(item.build.preview.to_dict())
                    ),
                    "qa_checks": [check.to_dict() for check in item.build.qa_checks],
                    "row_funnel": item.build.row_funnel.to_dict(),
                    "state": item.workflow.state.value,
                    "workflow_event_path": item.workflow.event_path,
                    "workflow_event_sha256": item.workflow.event_sha256,
                    "workflow_id": item.workflow.workflow_id,
                }
                for item in run.table_runs
            },
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (OSError, SilverContractError, SilverStoreError, TypeError, ValueError) as exc:
        parser.exit(2, f"ame-silver-assets-preview: {exc}\n")


def _parse_path_digests(
    values: list[str],
    *,
    parser: argparse.ArgumentParser,
) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        path, separator, digest = value.partition("=")
        if not separator or not path or not digest:
            parser.error("--expected-manifest-sha256 must use PATH=SHA256")
        if path in parsed:
            parser.error(f"duplicate expected manifest path: {path}")
        parsed[path] = digest
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
