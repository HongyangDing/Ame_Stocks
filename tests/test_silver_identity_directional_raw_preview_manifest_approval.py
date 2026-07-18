from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest

from ame_stocks_api.silver.identity_directional_raw_preview_manifest_approval import (
    IdentityDirectionalRawPreviewManifestApprovalError,
    S7DirectionalRawPreviewManifestApproval,
)
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan import (
    MANIFEST_EXECUTION_DATA_ROOT,
    REQUIRED_MANIFEST_RUNTIME_PATHS,
    REQUIRED_MANIFEST_VERIFICATION_PATHS,
    S7DirectionalRawPreviewManifestFilePin,
    S7DirectionalRawPreviewManifestPreflightPlan,
    S7DirectionalRawPreviewPreparationAuthorizationReceipt,
    StoredDirectionalRawPreviewManifestControl,
)
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_request import (
    S7DirectionalRawPreviewManifestPreflightRequest,
)

NOW = datetime.now(UTC) - timedelta(hours=1)


def _controls():
    authorization = S7DirectionalRawPreviewPreparationAuthorizationReceipt("receipt-recorder", NOW)
    authorization_receipt = StoredDirectionalRawPreviewManifestControl(
        authorization.relative_path, authorization.sha256, len(authorization.content)
    )
    pin = lambda path: S7DirectionalRawPreviewManifestFilePin(  # noqa: E731
        path=path, git_blob="a" * 40, sha256="b" * 64, bytes=1
    )
    plan = S7DirectionalRawPreviewManifestPreflightPlan.create(
        created_by="plan-author",
        created_at_utc=NOW,
        future_manifest_reader_actor="future-reader",
        future_execution_plan_actor="future-execution-plan-author",
        future_execution_request_actor="future-execution-request-author",
        git_commit="c" * 40,
        git_tree="d" * 40,
        execution_data_root=MANIFEST_EXECUTION_DATA_ROOT,
        runtime_files=tuple(pin(path) for path in REQUIRED_MANIFEST_RUNTIME_PATHS),
        verification_files=tuple(pin(path) for path in REQUIRED_MANIFEST_VERIFICATION_PATHS),
        preparation_authorization=authorization,
        preparation_authorization_receipt=authorization_receipt,
    )
    plan_receipt = StoredDirectionalRawPreviewManifestControl(
        plan.relative_path, plan.sha256, len(plan.content)
    )
    request = S7DirectionalRawPreviewManifestPreflightRequest.create(
        plan,
        plan_receipt,
        created_by="request-author",
        created_at_utc=NOW,
    )
    request_receipt = StoredDirectionalRawPreviewManifestControl(
        request.relative_path, request.sha256, len(request.content)
    )
    return plan, request, plan_receipt, request_receipt, request.canonical_approval_literal


def test_exact_literal_records_manifest_only_approval_and_roundtrips(tmp_path) -> None:
    plan, request, plan_receipt, request_receipt, literal = _controls()
    approval = S7DirectionalRawPreviewManifestApproval.create(
        plan,
        request,
        plan_receipt,
        request_receipt,
        approval_literal=literal,
        approved_by="human-reviewer",
        approved_at_utc=NOW + timedelta(seconds=1),
        approval_note="Exact manifest-only preflight approved.",
    )
    assert approval.approval_literal_sha256 == hashlib.sha256(literal.encode()).hexdigest()
    assert approval.document["capabilities"]["manifest_only_source_binding"] is True
    assert approval.document["capabilities"]["parquet_content_read"] is False
    replayed = S7DirectionalRawPreviewManifestApproval.from_dict(json.loads(approval.content))
    assert replayed == approval


def test_literal_or_actor_reuse_fails_closed() -> None:
    plan, request, plan_receipt, request_receipt, literal = _controls()
    with pytest.raises(IdentityDirectionalRawPreviewManifestApprovalError, match="differs"):
        S7DirectionalRawPreviewManifestApproval.create(
            plan,
            request,
            plan_receipt,
            request_receipt,
            approval_literal=literal + " ",
            approved_by="human-reviewer",
            approved_at_utc=NOW + timedelta(seconds=1),
            approval_note="reviewed",
        )
    with pytest.raises(IdentityDirectionalRawPreviewManifestApprovalError, match="separate"):
        S7DirectionalRawPreviewManifestApproval.create(
            plan,
            request,
            plan_receipt,
            request_receipt,
            approval_literal=literal,
            approved_by="human-reviewer",
            approved_at_utc=datetime(2999, 1, 1, tzinfo=UTC),
            approval_note="reviewed",
        )
    with pytest.raises(IdentityDirectionalRawPreviewManifestApprovalError, match="separate"):
        S7DirectionalRawPreviewManifestApproval.create(
            plan,
            request,
            plan_receipt,
            request_receipt,
            approval_literal=literal,
            approved_by=request.created_by,
            approved_at_utc=NOW + timedelta(seconds=1),
            approval_note="reviewed",
        )
