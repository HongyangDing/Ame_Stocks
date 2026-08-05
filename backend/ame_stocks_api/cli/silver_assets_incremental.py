"""Run one exact, no-publish S4 Assets session increment."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from ame_stocks_api.silver.asset_incremental import (
    S4AssetIncrementalError,
    load_completed_s4_asset_session_run,
    load_current_s4_reference_binding,
    prepare_s4_asset_session_run_spec,
    run_s4_asset_session_incremental,
    verify_s4_incremental_git_checkout,
)
from ame_stocks_api.silver.asset_incremental_contract import (
    S4ParentFrontierPin,
    S4ParentKind,
)
from ame_stocks_api.silver.incremental_contract import ArtifactPin


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize exactly one canonical Massive assets session into immutable S4 "
            "partitions. This command does not publish a release."
        )
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--session", type=date.fromisoformat, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument(
        "--parent-kind",
        choices=tuple(item.value for item in S4ParentKind),
        required=True,
    )
    parser.add_argument("--parent-terminal-session", type=date.fromisoformat, required=True)
    parser.add_argument("--parent-terminal-receipt-id", required=True)
    parser.add_argument("--parent-artifact-path", required=True)
    parser.add_argument("--parent-artifact-sha256", required=True)
    parser.add_argument("--parent-artifact-bytes", type=int, required=True)
    parser.add_argument("--calendar-artifact-id", required=True)
    parser.add_argument("--calendar-artifact-sha256", required=True)
    parser.add_argument("--receipt-available-session", type=date.fromisoformat, required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    verify_s4_incremental_git_checkout(args.repo_root, args.git_commit)
    parent = S4ParentFrontierPin(
        parent_kind=S4ParentKind(args.parent_kind),
        terminal_session=args.parent_terminal_session,
        terminal_receipt_id=args.parent_terminal_receipt_id,
        artifact=ArtifactPin(
            path=args.parent_artifact_path,
            sha256=args.parent_artifact_sha256,
            bytes=args.parent_artifact_bytes,
        ),
    )
    run = load_completed_s4_asset_session_run(args.data_root, args.session)
    if run is not None:
        if (
            run.run_spec.parent_frontier != parent
            or run.run_spec.calendar_artifact_id != args.calendar_artifact_id
            or run.run_spec.calendar_artifact.sha256 != args.calendar_artifact_sha256
            or run.run_spec.receipt_available_session != args.receipt_available_session
        ):
            raise S4AssetIncrementalError(
                "correction_required: completed session differs from requested controls"
            )
    else:
        reference = load_current_s4_reference_binding(args.data_root)
        run_spec = prepare_s4_asset_session_run_spec(
            args.data_root,
            session_date=args.session,
            parent_frontier=parent,
            calendar_artifact_id=args.calendar_artifact_id,
            calendar_artifact_sha256=args.calendar_artifact_sha256,
            reference_binding=reference,
            receipt_available_session=args.receipt_available_session,
            writer_git_commit=args.git_commit,
        )
        run = run_s4_asset_session_incremental(args.data_root, run_spec)
    print(
        json.dumps(
            {
                "idempotent": run.idempotent,
                "receipt": run.receipt.to_dict(),
                "receipt_artifact": run.receipt_artifact.to_dict(),
                "run_spec": run.run_spec.to_dict(),
                "run_spec_artifact": run.run_spec_artifact.to_dict(),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
