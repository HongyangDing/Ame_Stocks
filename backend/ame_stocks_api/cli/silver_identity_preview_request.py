"""Freeze one exact S7 detector-preview plan and request human approval."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import date
from pathlib import Path

from ame_stocks_api.artifacts import ArtifactError
from ame_stocks_api.silver.calendar_artifact import XNYSCalendarArtifactError
from ame_stocks_api.silver.identity_preview_plan import (
    IdentityPreviewPlanError,
    S7DetectorPreviewResourceCaps,
)
from ame_stocks_api.silver.identity_preview_request import (
    create_s7_detector_preview_request,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-identity-preview-request",
        description=(
            "Create and read back only the full frozen XNYS calendar, exact ticker "
            "allowlist, bounded S7 detector-preview plan, and approval-request event. "
            "This command cannot approve, execute a detector, materialize candidates, "
            "or publish."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument(
        "--recorded-at",
        required=True,
        help="exact nonfuture canonical UTC ISO-8601 timestamp",
    )
    parser.add_argument("--plan-created-by", required=True)
    parser.add_argument("--request-created-by", required=True)
    parser.add_argument(
        "--ticker",
        action="append",
        required=True,
        help="one exact case-sensitive ticker; repeat in already sorted unique order",
    )
    parser.add_argument("--expected-ticker-count", type=int, required=True)
    parser.add_argument("--start-session", required=True)
    parser.add_argument("--end-session", required=True)
    parser.add_argument("--expected-session-count", type=int, required=True)
    parser.add_argument("--selected-row-cap", type=int, required=True)
    parser.add_argument("--universe-scanned-row-cap", type=int, required=True)
    parser.add_argument("--asset-parent-scanned-row-cap", type=int, required=True)
    parser.add_argument("--total-scanned-row-cap", type=int, required=True)
    parser.add_argument("--source-artifact-cap", type=int, required=True)
    parser.add_argument("--source-bytes-cap", type=int, required=True)
    parser.add_argument("--case-cap", type=int, required=True)
    parser.add_argument("--batch-size", type=int, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        start_session = _exact_date(arguments.start_session, "start_session")
        end_session = _exact_date(arguments.end_session, "end_session")
        caps = S7DetectorPreviewResourceCaps(
            selected_row_cap=arguments.selected_row_cap,
            universe_scanned_row_cap=arguments.universe_scanned_row_cap,
            asset_parent_scanned_row_cap=arguments.asset_parent_scanned_row_cap,
            total_scanned_row_cap=arguments.total_scanned_row_cap,
            source_artifact_cap=arguments.source_artifact_cap,
            source_bytes_cap=arguments.source_bytes_cap,
            case_cap=arguments.case_cap,
            batch_size=arguments.batch_size,
        )
        run = create_s7_detector_preview_request(
            arguments.data_root,
            repo_root=arguments.repo_root,
            git_commit=arguments.git_commit,
            recorded_at=arguments.recorded_at,
            plan_created_by=arguments.plan_created_by,
            request_created_by=arguments.request_created_by,
            tickers=tuple(arguments.ticker),
            expected_ticker_count=arguments.expected_ticker_count,
            start_session=start_session,
            end_session=end_session,
            expected_session_count=arguments.expected_session_count,
            resource_caps=caps,
        )
        output = {
            "all_documents_preexisting": run.all_documents_preexisting,
            "approval_created": False,
            "approval_request": {
                "authorized_action": run.approval_request.authorized_action,
                "canonical_approval_literal": (
                    run.approval_request.canonical_approval_literal
                ),
                "path": run.approval_request_document.path,
                "request_event_id": run.approval_request.request_event_id,
                "sha256": run.approval_request_document.sha256,
                "state": run.approval_request.request_state,
            },
            "calendar": {
                "calendar_artifact_id": run.calendar.calendar_artifact_id,
                "end_session": run.calendar.end_session.isoformat(),
                "path": run.calendar_document.path,
                "session_count": len(run.calendar.sessions),
                "sha256": run.calendar_document.sha256,
                "start_session": run.calendar.start_session.isoformat(),
            },
            "candidate_artifacts_created": False,
            "detector_preview_executed": False,
            "git_commit": run.plan.git_commit,
            "mode": "plan_and_approval_request_only",
            "plan": {
                "end_session": run.plan.end_session.isoformat(),
                "path": run.plan_document.path,
                "plan_id": run.plan.plan_id,
                "resource_caps": run.plan.resource_caps.to_dict(),
                "resource_caps_digest": run.plan.resource_caps.digest,
                "session_count": run.plan.session_count,
                "sha256": run.plan_document.sha256,
                "start_session": run.plan.start_session.isoformat(),
                "state": run.plan.plan_state,
                "ticker_count": run.plan.ticker_count,
            },
            "publication_executed": False,
            "selected_sources": [item.to_dict() for item in run.selected_sources],
            "ticker_allowlist": {
                "path": run.ticker_allowlist_document.path,
                "sha256": run.ticker_allowlist_document.sha256,
                "ticker_allowlist_id": run.ticker_allowlist.ticker_allowlist_id,
                "ticker_count": run.ticker_allowlist.ticker_count,
                "tickers": list(run.ticker_allowlist.tickers),
            },
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (
        ArtifactError,
        IdentityPreviewPlanError,
        OSError,
        subprocess.SubprocessError,
        TypeError,
        ValueError,
        XNYSCalendarArtifactError,
    ) as exc:
        parser.exit(2, f"ame-silver-identity-preview-request: {exc}\n")


def _exact_date(value: str, label: str) -> date:
    if not isinstance(value, str):
        raise IdentityPreviewPlanError(f"{label} must be an exact ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise IdentityPreviewPlanError(f"{label} must be an exact ISO date") from exc
    if parsed.isoformat() != value:
        raise IdentityPreviewPlanError(f"{label} must be an exact ISO date")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
