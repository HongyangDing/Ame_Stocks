"""Freeze local-only S7 directional raw-preview scope, Plan, and Request JSON."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from ame_stocks_api.artifacts import ArtifactError
from ame_stocks_api.silver import identity_directional_raw_preview_request as request_module
from ame_stocks_api.silver.identity_directional_raw_preview_plan import (
    IdentityDirectionalRawPreviewPlanError,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-identity-directional-raw-preview-request",
        description=(
            "Verify one exact clean local Git checkout and freeze only the fixed "
            "eleven-pair scope, non-executing Plan, and human approval Request JSON. "
            "This command cannot read Parquet, access a network, approve or execute "
            "the preview, load registries, adjudicate, materialize, run Full, or publish."
        ),
    )
    parser.add_argument("--control-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--recorded-at", required=True)
    parser.add_argument("--scope-created-by", required=True)
    parser.add_argument("--plan-created-by", required=True)
    parser.add_argument("--request-created-by", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        run = request_module.create_s7_directional_raw_preview_request(
            arguments.control_root,
            repo_root=arguments.repo_root,
            git_commit=arguments.git_commit,
            recorded_at=arguments.recorded_at,
            scope_created_by=arguments.scope_created_by,
            plan_created_by=arguments.plan_created_by,
            request_created_by=arguments.request_created_by,
        )
    except (
        ArtifactError,
        IdentityDirectionalRawPreviewPlanError,
        request_module.IdentityDirectionalRawPreviewRequestError,
        OSError,
        subprocess.SubprocessError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(
            2,
            f"ame-silver-identity-directional-raw-preview-request: {exc}\n",
        )

    print(
        json.dumps(
            {
                "all_documents_preexisting": run.all_documents_preexisting,
                "authorization_flags": {
                    "adjudication_authorized": False,
                    "approval_created": False,
                    "canonical_identity_materialization_authorized": False,
                    "exact_group_history_read_authorized": False,
                    "external_evidence_capture_authorized": False,
                    "forced_liquidation_authorized": False,
                    "full_run_authorized": False,
                    "inventory_rerun_authorized": False,
                    "materialization_authorized": False,
                    "network_access_authorized": False,
                    "parquet_read_authorized": False,
                    "preview_execution_authorized": False,
                    "publication_authorized": False,
                    "registry_evaluation_authorized": False,
                    "s5_s6_identity_confirmation_authorized": False,
                },
                "execution_results": {
                    "adjudication_created": False,
                    "approval_created": False,
                    "canonical_identity_materialized": False,
                    "exact_group_history_read": False,
                    "external_evidence_captured": False,
                    "forced_liquidation_triggered": False,
                    "full_run_created": False,
                    "inventory_rerun": False,
                    "materialized": False,
                    "network_accessed": False,
                    "parquet_opened": False,
                    "preview_executed": False,
                    "published": False,
                    "registry_evaluated": False,
                    "s5_s6_identity_confirmation_performed": False,
                },
                "git_commit": arguments.git_commit,
                "mode": "local_scope_plan_request_only",
                "plan": {
                    "input_binding_digest": run.plan.input_binding_digest,
                    "path": run.plan_document.path,
                    "plan_id": run.plan.plan_id,
                    "resource_caps_digest": run.plan.resource_caps.digest,
                    "runtime_file_set_digest": run.plan.runtime_file_set_digest,
                    "sha256": run.plan_document.sha256,
                    "state": run.plan.plan_state,
                    "verification_file_set_digest": (run.plan.verification_file_set_digest),
                },
                "request": {
                    "canonical_approval_literal": run.request.canonical_approval_literal,
                    "path": run.request_document.path,
                    "request_event_id": run.request.request_event_id,
                    "sha256": run.request_document.sha256,
                    "state": run.request.request_state,
                },
                "scope_set": {
                    "path": run.scope_document.path,
                    "scope_set_id": run.scope.scope_set_id,
                    "sha256": run.scope_document.sha256,
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
