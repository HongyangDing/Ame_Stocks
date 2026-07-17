"""Record the exact S7 schema/evidence receipt and freeze Gate-A controls."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from ame_stocks_api.artifacts import ArtifactError
from ame_stocks_api.silver.calendar_artifact import XNYSCalendarArtifactError
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.identity_market_inventory_plan import (
    APPROVAL_TEXT,
    APPROVAL_TEXT_SHA256,
    IdentityMarketInventoryPlanError,
)
from ame_stocks_api.silver.identity_market_inventory_request import (
    create_s7_market_inventory_request,
)
from ame_stocks_api.silver.identity_preview_runner import IdentityPreviewRunnerError
from ame_stocks_api.silver.identity_provider_evidence import ProviderEvidenceError
from ame_stocks_api.silver.identity_streaming_preview import IdentityStreamingPreviewError
from ame_stocks_api.silver.store import SilverStoreError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-identity-market-inventory-request",
        description=(
            "Revalidate the exact S7 schema/evidence, release-manifest, preview-lineage "
            "and XNYS-calendar bindings; then record only the aggregate approval receipt, "
            "Gate-A Composite-inventory plan and literal approval request. This command "
            "cannot read Parquet, access a network, approve or execute inventory work, "
            "classify markets, adjudicate, materialize tables, run Full, or publish."
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
    parser.add_argument("--approval-recorded-by", required=True)
    parser.add_argument("--plan-created-by", required=True)
    parser.add_argument("--request-created-by", required=True)
    parser.add_argument(
        "--approval-text-sha256",
        required=True,
        help="must equal the SHA-256 of the exact already-recorded schema/evidence approval",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        run = create_s7_market_inventory_request(
            arguments.data_root,
            repo_root=arguments.repo_root,
            git_commit=arguments.git_commit,
            recorded_at=arguments.recorded_at,
            approval_recorded_by=arguments.approval_recorded_by,
            plan_created_by=arguments.plan_created_by,
            request_created_by=arguments.request_created_by,
            approval_text_sha256=arguments.approval_text_sha256,
        )
        output = {
            "all_documents_preexisting": run.all_documents_preexisting,
            "approval_text": APPROVAL_TEXT,
            "approval_text_sha256": APPROVAL_TEXT_SHA256,
            "authorization_flags": {
                "adjudication_authorized": False,
                "adjudication_plan_authorized": False,
                "api_call_authorized": False,
                "derived_table_materialization_authorized": False,
                "full_run_authorized": False,
                "identity_market_consistency_scan_authorized": False,
                "inventory_execution_authorized": False,
                "market_classification_authorized": False,
                "network_access_authorized": False,
                "parquet_read_authorized": False,
                "publish_authorized": False,
                "registry_release_authorized": False,
            },
            "calendar": {
                "calendar_artifact_id": run.calendar.calendar_artifact_id,
                "end_session": run.calendar.end_session.isoformat(),
                "path": run.calendar.relative_path,
                "session_count": len(run.calendar.sessions),
                "sha256": run.calendar.sha256,
                "start_session": run.calendar.start_session.isoformat(),
                "written_by_this_command": False,
            },
            "execution_results": {
                "adjudication_created": False,
                "adjudication_executed": False,
                "adjudication_plan_created": False,
                "api_called": False,
                "derived_tables_materialized": False,
                "full_run_plan_created": False,
                "identity_market_consistency_scan_executed": False,
                "inventory_executed": False,
                "market_classification_executed": False,
                "network_accessed": False,
                "parquet_opened": False,
                "publish_plan_created": False,
                "registry_release_created": False,
            },
            "git_commit": arguments.git_commit,
            "mode": "schema_evidence_receipt_and_inventory_plan_request_only",
            "plan": {
                "canonical_approval_literal": (
                    run.approval_request.canonical_approval_literal
                ),
                "input_binding_digest": run.plan.input_binding_digest,
                "path": run.plan_document.path,
                "plan_id": run.plan.plan_id,
                "resource_caps": run.plan.resource_caps.to_dict(),
                "resource_caps_digest": run.plan.resource_caps.digest,
                "sha256": run.plan_document.sha256,
                "state": run.plan.plan_state,
            },
            "preview_lineage": {
                "case_evidence_set_digest": (
                    run.preview_lineage.case_evidence_set_digest
                ),
                "completion_path": run.preview_lineage.completion_path,
                "provider_evidence_manifest_count": (
                    run.preview_lineage.provider_evidence_manifest_count
                ),
            },
            "release_manifest_preflight": {
                "daily_artifact_count": run.releases.daily_artifact_count,
                "daily_row_count": run.releases.daily_row_count,
                "daily_source_bytes": run.releases.daily_source_bytes,
                "release_manifest_count": run.releases.release_manifest_count,
            },
            "request": {
                "authorized_action": run.approval_request.authorized_action,
                "canonical_approval_literal": (
                    run.approval_request.canonical_approval_literal
                ),
                "path": run.approval_request_document.path,
                "request_event_id": run.approval_request.request_event_id,
                "sha256": run.approval_request_document.sha256,
                "state": run.approval_request.request_state,
            },
            "schema_evidence_approval": {
                "approval_id": run.schema_approval.approval_id,
                "approval_scope": run.schema_approval.logical_payload()[
                    "approval_scope"
                ],
                "package_digest": run.schema_approval.package_digest,
                "path": run.schema_approval_document.path,
                "sha256": run.schema_approval_document.sha256,
            },
        }
        print(json.dumps(output, indent=2, sort_keys=True, ensure_ascii=False))
        return 0
    except (
        ArtifactError,
        IdentityMarketInventoryPlanError,
        IdentityPreviewRunnerError,
        IdentityStreamingPreviewError,
        json.JSONDecodeError,
        OSError,
        ProviderEvidenceError,
        SilverContractError,
        SilverStoreError,
        subprocess.SubprocessError,
        TypeError,
        ValueError,
        XNYSCalendarArtifactError,
    ) as exc:
        parser.exit(2, f"ame-silver-identity-market-inventory-request: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
