"""Freeze only the S7 manifest-preflight authorization, Plan, and Request."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from ame_stocks_api.artifacts import ArtifactError
from ame_stocks_api.silver import (
    identity_directional_raw_preview_manifest_request as request_module,
)
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan import (
    IdentityDirectionalRawPreviewManifestPlanError,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-identity-directional-manifest-request",
        description=(
            "Verify the approved preparation lineage and a clean Git checkout, then "
            "freeze exactly three local JSON controls for a future manifest-only "
            "source-binding preflight. This command never opens release manifests, "
            "Parquet, registries, or a network connection."
        ),
    )
    parser.add_argument("--control-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--recorded-at", required=True)
    parser.add_argument("--preparation-authorization-recorded-by", required=True)
    parser.add_argument("--plan-created-by", required=True)
    parser.add_argument("--request-created-by", required=True)
    parser.add_argument("--future-manifest-reader-actor", required=True)
    parser.add_argument("--future-execution-plan-actor", required=True)
    parser.add_argument("--future-execution-request-actor", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        run = request_module.create_s7_directional_raw_preview_manifest_request(
            arguments.control_root,
            repo_root=arguments.repo_root,
            git_commit=arguments.git_commit,
            recorded_at=arguments.recorded_at,
            preparation_authorization_recorded_by=(arguments.preparation_authorization_recorded_by),
            plan_created_by=arguments.plan_created_by,
            request_created_by=arguments.request_created_by,
            future_manifest_reader_actor=arguments.future_manifest_reader_actor,
            future_execution_plan_actor=arguments.future_execution_plan_actor,
            future_execution_request_actor=arguments.future_execution_request_actor,
        )
    except (
        ArtifactError,
        IdentityDirectionalRawPreviewManifestPlanError,
        request_module.IdentityDirectionalRawPreviewManifestRequestError,
        OSError,
        subprocess.SubprocessError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(2, f"ame-silver-identity-directional-manifest-request: {exc}\n")

    print(
        json.dumps(
            {
                "all_documents_preexisting": run.all_documents_preexisting,
                "authorization_flags_now": {
                    "adjudication": False,
                    "approval_receipt_creation": False,
                    "full_run": False,
                    "manifest_read": False,
                    "network_access": False,
                    "parquet_read": False,
                    "preview_execution": False,
                    "publication": False,
                    "registry_evaluation": False,
                    "runner": False,
                    "source_binding_creation": False,
                },
                "execution_results": {
                    "data_file_opened": False,
                    "directory_discovered": False,
                    "inventory_manifest_opened": False,
                    "manifest_opened": False,
                    "network_accessed": False,
                    "parquet_lstat_performed": False,
                    "preview_executed": False,
                    "source_binding_created": False,
                },
                "git_commit": arguments.git_commit,
                "mode": "local_manifest_preflight_controls_only",
                "plan": {
                    "input_binding_digest": run.plan.input_binding_digest,
                    "path": run.plan_document.path,
                    "plan_id": run.plan.plan_id,
                    "resource_caps_digest": run.plan.resource_caps.digest,
                    "runtime_file_set_digest": run.plan.runtime_file_set_digest,
                    "sha256": run.plan_document.sha256,
                    "state": run.plan.plan_state,
                    "verification_file_set_digest": run.plan.verification_file_set_digest,
                },
                "preparation_authorization": {
                    "authorization_id": run.preparation_authorization.authorization_id,
                    "path": run.preparation_authorization_document.path,
                    "sha256": run.preparation_authorization_document.sha256,
                },
                "request": {
                    "canonical_approval_literal": run.request.canonical_approval_literal,
                    "path": run.request_document.path,
                    "request_event_id": run.request.request_event_id,
                    "sha256": run.request_document.sha256,
                    "state": run.request.request_state,
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
