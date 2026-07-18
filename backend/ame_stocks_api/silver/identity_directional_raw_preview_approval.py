"""Exact execution approval receipt for the bounded S7 directional preview.

The recorder accepts only the byte-for-byte literal emitted by one immutable
execution Request.  It records a receipt and performs none of the approved
work.  In particular, this module has no runner import, Parquet reader, source
discovery, ticker/date override, network client, registry evaluator,
adjudicator, materializer, Full-run path, or publisher.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.identity_directional_raw_preview_execution_plan import (
    EXECUTION_AUTHORIZED_ACTION,
    EXECUTION_LITERAL_VERSION,
    EXECUTION_SCOPE,
    DirectionalRawPreviewExecutionPlanStore,
    IdentityDirectionalRawPreviewExecutionPlanError,
    S7DirectionalRawPreviewExecutionPlan,
    S7DirectionalRawPreviewExecutionRequest,
    StoredDirectionalRawPreviewExecutionDocument,
    directional_raw_preview_execution_plan_path,
    directional_raw_preview_execution_request_path,
)

EXECUTION_APPROVAL_SCHEMA_VERSION: Final = 1
EXECUTION_APPROVAL_RULE_VERSION: Final = (
    "s7_directional_raw_preview_execution_approval_v1"
)
EXECUTION_APPROVAL_STAGE: Final = "s7_directional_raw_preview_exact_bounded_preview"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class IdentityDirectionalRawPreviewExecutionApprovalError(
    IdentityDirectionalRawPreviewExecutionPlanError
):
    """Raised when an exact directional-preview approval cannot be trusted."""


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise IdentityDirectionalRawPreviewExecutionApprovalError(
            f"{label} must be lowercase 64-hex"
        )
    return value


def _safe_text(
    value: object,
    label: str,
    *,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise IdentityDirectionalRawPreviewExecutionApprovalError(f"{label} must be text")
    lowered = value.casefold()
    if (
        (not value and not allow_empty)
        or len(value) > maximum
        or value.strip() != value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
        or any(token in lowered for token in ("api_key", "password", "secret", "token="))
    ):
        raise IdentityDirectionalRawPreviewExecutionApprovalError(f"{label} is unsafe")
    return value


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityDirectionalRawPreviewExecutionApprovalError(
            f"{label} must be timezone-aware"
        )
    if value.utcoffset().total_seconds() != 0:
        raise IdentityDirectionalRawPreviewExecutionApprovalError(f"{label} must be UTC")
    return value.astimezone(UTC)


def _parse_utc(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise IdentityDirectionalRawPreviewExecutionApprovalError(f"{label} must be text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IdentityDirectionalRawPreviewExecutionApprovalError(
            f"{label} must be ISO-8601"
        ) from exc
    normalized = _utc(parsed, label)
    if normalized.isoformat() != value:
        raise IdentityDirectionalRawPreviewExecutionApprovalError(
            f"{label} must be canonical UTC"
        )
    return normalized


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise IdentityDirectionalRawPreviewExecutionApprovalError(
            f"{label} must be an object"
        )
    return dict(value)


def _literal(
    *,
    algorithm_digest: str,
    authorized_action: str,
    contract_candidate_sha256: str,
    contract_id: str,
    contract_schema_digest: str,
    execution_data_root: str,
    input_binding_digest: str,
    inventory_completion_id: str,
    inventory_completion_sha256: str,
    plan_id: str,
    plan_sha256: str,
    preparation_approval_literal_sha256: str,
    qa_semantics_digest: str,
    registry_semantics_digest: str,
    request_event_id: str,
    request_event_sha256: str,
    resource_caps_digest: str,
    runtime_file_set_digest: str,
    scope_set_id: str,
    scope_set_sha256: str,
    source_artifact_set_digest: str,
    source_binding_manifest_id: str,
    source_binding_manifest_sha256: str,
    manifest_preflight_intent_id: str,
    manifest_preflight_intent_path: str,
    manifest_preflight_intent_sha256: str,
    verification_file_set_digest: str,
) -> str:
    return json.dumps(
        {
            "algorithm_digest": algorithm_digest,
            "authorized_action": authorized_action,
            "contract_candidate_sha256": contract_candidate_sha256,
            "contract_id": contract_id,
            "contract_schema_digest": contract_schema_digest,
            "execution_data_root": execution_data_root,
            "input_binding_digest": input_binding_digest,
            "inventory_completion_id": inventory_completion_id,
            "inventory_completion_sha256": inventory_completion_sha256,
            "literal_version": EXECUTION_LITERAL_VERSION,
            "plan_id": plan_id,
            "plan_sha256": plan_sha256,
            "preparation_approval_literal_sha256": preparation_approval_literal_sha256,
            "qa_semantics_digest": qa_semantics_digest,
            "registry_semantics_digest": registry_semantics_digest,
            "request_event_id": request_event_id,
            "request_event_sha256": request_event_sha256,
            "resource_caps_digest": resource_caps_digest,
            "runtime_file_set_digest": runtime_file_set_digest,
            "scope_set_id": scope_set_id,
            "scope_set_sha256": scope_set_sha256,
            "source_artifact_set_digest": source_artifact_set_digest,
            "source_binding_manifest_id": source_binding_manifest_id,
            "source_binding_manifest_sha256": source_binding_manifest_sha256,
            "manifest_preflight_intent_id": manifest_preflight_intent_id,
            "manifest_preflight_intent_path": manifest_preflight_intent_path,
            "manifest_preflight_intent_sha256": manifest_preflight_intent_sha256,
            "verification_file_set_digest": verification_file_set_digest,
        },
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewExecutionApproval:
    request_event_id: str
    request_event_path: str
    request_event_sha256: str
    plan_id: str
    plan_path: str
    plan_sha256: str
    execution_data_root: str
    input_binding_digest: str
    resource_caps_digest: str
    runtime_file_set_digest: str
    verification_file_set_digest: str
    source_binding_manifest_id: str
    source_binding_manifest_sha256: str
    manifest_preflight_intent_id: str
    manifest_preflight_intent_path: str
    manifest_preflight_intent_sha256: str
    source_artifact_set_digest: str
    inventory_completion_id: str
    inventory_completion_sha256: str
    scope_set_id: str
    scope_set_sha256: str
    contract_id: str
    contract_schema_digest: str
    contract_candidate_sha256: str
    algorithm_digest: str
    qa_semantics_digest: str
    registry_semantics_digest: str
    preparation_approval_literal_sha256: str
    approval_literal: str
    approval_literal_sha256: str
    approved_by: str
    approved_at_utc: datetime
    approval_note: str
    decision: str = "approved"
    approval_stage: str = EXECUTION_APPROVAL_STAGE
    authorized_action: str = EXECUTION_AUTHORIZED_ACTION
    execution_scope: str = EXECUTION_SCOPE
    preview_execution_authorized: bool = field(default=True, init=False)
    data_read_authorized: bool = field(default=True, init=False)
    parquet_read_authorized: bool = field(default=True, init=False)
    once_to_awaiting_review: bool = field(default=True, init=False)
    source_discovery_authorized: bool = field(default=False, init=False)
    caller_scope_override_authorized: bool = field(default=False, init=False)
    exact_group_history_read_authorized: bool = field(default=False, init=False)
    network_access_authorized: bool = field(default=False, init=False)
    external_evidence_capture_authorized: bool = field(default=False, init=False)
    registry_evaluation_authorized: bool = field(default=False, init=False)
    adjudication_authorized: bool = field(default=False, init=False)
    table_materialization_authorized: bool = field(default=False, init=False)
    full_run_authorized: bool = field(default=False, init=False)
    publication_authorized: bool = field(default=False, init=False)
    forced_liquidation_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        for label, value in (
            ("request event ID", self.request_event_id),
            ("request event SHA", self.request_event_sha256),
            ("plan ID", self.plan_id),
            ("plan SHA", self.plan_sha256),
            ("input binding digest", self.input_binding_digest),
            ("resource caps digest", self.resource_caps_digest),
            ("runtime file-set digest", self.runtime_file_set_digest),
            ("verification file-set digest", self.verification_file_set_digest),
            ("source-binding ID", self.source_binding_manifest_id),
            ("source-binding SHA", self.source_binding_manifest_sha256),
            ("manifest preflight intent ID", self.manifest_preflight_intent_id),
            ("manifest preflight intent SHA", self.manifest_preflight_intent_sha256),
            ("source artifact-set digest", self.source_artifact_set_digest),
            ("inventory completion ID", self.inventory_completion_id),
            ("inventory completion SHA", self.inventory_completion_sha256),
            ("scope-set ID", self.scope_set_id),
            ("scope-set SHA", self.scope_set_sha256),
            ("contract ID", self.contract_id),
            ("contract schema digest", self.contract_schema_digest),
            ("contract candidate SHA", self.contract_candidate_sha256),
            ("algorithm digest", self.algorithm_digest),
            ("QA semantics digest", self.qa_semantics_digest),
            ("registry semantics digest", self.registry_semantics_digest),
            ("preparation literal SHA", self.preparation_approval_literal_sha256),
            ("approval literal SHA", self.approval_literal_sha256),
        ):
            _digest(value, label)
        if self.request_event_path != directional_raw_preview_execution_request_path(
            self.request_event_id
        ):
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "approval request path is not canonical"
            )
        if self.plan_path != directional_raw_preview_execution_plan_path(self.plan_id):
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "approval plan path is not canonical"
            )
        intent_path = Path(self.manifest_preflight_intent_path)
        if (
            intent_path.is_absolute()
            or ".." in intent_path.parts
            or intent_path.as_posix() != self.manifest_preflight_intent_path
        ):
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "manifest preflight intent path is not canonical"
            )
        if not Path(self.execution_data_root).is_absolute() or str(
            Path(self.execution_data_root)
        ) != self.execution_data_root:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "approval execution root is not canonical and absolute"
            )
        if not isinstance(self.approval_literal, str) or hashlib.sha256(
            self.approval_literal.encode("utf-8")
        ).hexdigest() != self.approval_literal_sha256:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "approval literal SHA-256 does not reproduce"
            )
        expected = _literal(
            algorithm_digest=self.algorithm_digest,
            authorized_action=self.authorized_action,
            contract_candidate_sha256=self.contract_candidate_sha256,
            contract_id=self.contract_id,
            contract_schema_digest=self.contract_schema_digest,
            execution_data_root=self.execution_data_root,
            input_binding_digest=self.input_binding_digest,
            inventory_completion_id=self.inventory_completion_id,
            inventory_completion_sha256=self.inventory_completion_sha256,
            plan_id=self.plan_id,
            plan_sha256=self.plan_sha256,
            preparation_approval_literal_sha256=self.preparation_approval_literal_sha256,
            qa_semantics_digest=self.qa_semantics_digest,
            registry_semantics_digest=self.registry_semantics_digest,
            request_event_id=self.request_event_id,
            request_event_sha256=self.request_event_sha256,
            resource_caps_digest=self.resource_caps_digest,
            runtime_file_set_digest=self.runtime_file_set_digest,
            scope_set_id=self.scope_set_id,
            scope_set_sha256=self.scope_set_sha256,
            source_artifact_set_digest=self.source_artifact_set_digest,
            source_binding_manifest_id=self.source_binding_manifest_id,
            source_binding_manifest_sha256=self.source_binding_manifest_sha256,
            manifest_preflight_intent_id=self.manifest_preflight_intent_id,
            manifest_preflight_intent_path=self.manifest_preflight_intent_path,
            manifest_preflight_intent_sha256=self.manifest_preflight_intent_sha256,
            verification_file_set_digest=self.verification_file_set_digest,
        )
        if self.approval_literal != expected:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "approval literal is not the exact execution request literal"
            )
        _safe_text(self.approved_by, "approved_by", maximum=200)
        object.__setattr__(
            self, "approved_at_utc", _utc(self.approved_at_utc, "approved_at_utc")
        )
        if self.approved_at_utc > datetime.now(UTC):
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "approved_at_utc cannot be in the future"
            )
        _safe_text(
            self.approval_note,
            "approval_note",
            maximum=1_000,
            allow_empty=True,
        )
        if (
            self.decision != "approved"
            or self.approval_stage != EXECUTION_APPROVAL_STAGE
            or self.authorized_action != EXECUTION_AUTHORIZED_ACTION
            or self.execution_scope != EXECUTION_SCOPE
            or not all(
                (
                    self.preview_execution_authorized,
                    self.data_read_authorized,
                    self.parquet_read_authorized,
                    self.once_to_awaiting_review,
                )
            )
            or any(
                (
                    self.source_discovery_authorized,
                    self.caller_scope_override_authorized,
                    self.exact_group_history_read_authorized,
                    self.network_access_authorized,
                    self.external_evidence_capture_authorized,
                    self.registry_evaluation_authorized,
                    self.adjudication_authorized,
                    self.table_materialization_authorized,
                    self.full_run_authorized,
                    self.publication_authorized,
                    self.forced_liquidation_authorized,
                )
            )
        ):
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "execution approval scope is invalid or too broad"
            )

    @classmethod
    def create(
        cls,
        request: S7DirectionalRawPreviewExecutionRequest,
        request_receipt: StoredDirectionalRawPreviewExecutionDocument,
        *,
        plan: S7DirectionalRawPreviewExecutionPlan,
        approval_literal: str,
        approved_by: str,
        approved_at_utc: datetime,
        approval_note: str = "",
    ) -> S7DirectionalRawPreviewExecutionApproval:
        if not isinstance(request, S7DirectionalRawPreviewExecutionRequest) or not isinstance(
            plan, S7DirectionalRawPreviewExecutionPlan
        ):
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "approval controls have wrong type"
            )
        if (
            request_receipt.path != request.relative_path
            or request_receipt.sha256 != request.sha256
            or request_receipt.bytes != len(request.content)
        ):
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "stored execution request receipt differs"
            )
        if request.plan_id != plan.plan_id or request.plan_sha256 != plan.sha256:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "execution request crosses supplied plan"
            )
        approved_at = _utc(approved_at_utc, "approved_at_utc")
        if approved_at <= request.created_at_utc:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "execution request must strictly predate approval"
            )
        if approved_by in {
            request.created_by,
            plan.created_by,
            plan.source_binding_created_by,
        }:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "approval actor must be separate from source, plan, and request actors"
            )
        if approval_literal != request.canonical_approval_literal:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "approval literal differs from the exact execution request"
            )
        return cls(
            request_event_id=request.request_event_id,
            request_event_path=request.relative_path,
            request_event_sha256=request.sha256,
            plan_id=plan.plan_id,
            plan_path=plan.relative_path,
            plan_sha256=plan.sha256,
            execution_data_root=request.execution_data_root,
            input_binding_digest=request.input_binding_digest,
            resource_caps_digest=request.resource_caps_digest,
            runtime_file_set_digest=request.runtime_file_set_digest,
            verification_file_set_digest=request.verification_file_set_digest,
            source_binding_manifest_id=request.source_binding_manifest_id,
            source_binding_manifest_sha256=request.source_binding_manifest_sha256,
            manifest_preflight_intent_id=request.manifest_preflight_intent_id,
            manifest_preflight_intent_path=request.manifest_preflight_intent_path,
            manifest_preflight_intent_sha256=request.manifest_preflight_intent_sha256,
            source_artifact_set_digest=request.source_artifact_set_digest,
            inventory_completion_id=request.inventory_completion_id,
            inventory_completion_sha256=request.inventory_completion_sha256,
            scope_set_id=request.scope_set_id,
            scope_set_sha256=request.scope_set_sha256,
            contract_id=request.contract_id,
            contract_schema_digest=request.contract_schema_digest,
            contract_candidate_sha256=request.contract_candidate_sha256,
            algorithm_digest=request.algorithm_digest,
            qa_semantics_digest=request.qa_semantics_digest,
            registry_semantics_digest=request.registry_semantics_digest,
            preparation_approval_literal_sha256=(
                request.preparation_approval_literal_sha256
            ),
            approval_literal=approval_literal,
            approval_literal_sha256=hashlib.sha256(
                approval_literal.encode("utf-8")
            ).hexdigest(),
            approved_by=approved_by,
            approved_at_utc=approved_at,
            approval_note=approval_note,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "adjudication_authorized": self.adjudication_authorized,
            "algorithm_digest": self.algorithm_digest,
            "approval_literal": self.approval_literal,
            "approval_literal_sha256": self.approval_literal_sha256,
            "approval_note": self.approval_note,
            "approval_rule_version": EXECUTION_APPROVAL_RULE_VERSION,
            "approval_stage": self.approval_stage,
            "approved_at_utc": self.approved_at_utc.isoformat(),
            "approved_by": self.approved_by,
            "artifact_type": "s7_directional_raw_preview_execution_approval",
            "authorized_action": self.authorized_action,
            "caller_scope_override_authorized": self.caller_scope_override_authorized,
            "contract_id": self.contract_id,
            "contract_schema_digest": self.contract_schema_digest,
            "contract_candidate_sha256": self.contract_candidate_sha256,
            "data_read_authorized": self.data_read_authorized,
            "decision": self.decision,
            "exact_group_history_read_authorized": (
                self.exact_group_history_read_authorized
            ),
            "execution_data_root": self.execution_data_root,
            "execution_scope": self.execution_scope,
            "external_evidence_capture_authorized": (
                self.external_evidence_capture_authorized
            ),
            "forced_liquidation_authorized": self.forced_liquidation_authorized,
            "full_run_authorized": self.full_run_authorized,
            "input_binding_digest": self.input_binding_digest,
            "inventory_completion_id": self.inventory_completion_id,
            "inventory_completion_sha256": self.inventory_completion_sha256,
            "network_access_authorized": self.network_access_authorized,
            "once_to_awaiting_review": self.once_to_awaiting_review,
            "parquet_read_authorized": self.parquet_read_authorized,
            "plan_id": self.plan_id,
            "plan_path": self.plan_path,
            "plan_sha256": self.plan_sha256,
            "preparation_approval_literal_sha256": (
                self.preparation_approval_literal_sha256
            ),
            "preview_execution_authorized": self.preview_execution_authorized,
            "publication_authorized": self.publication_authorized,
            "qa_semantics_digest": self.qa_semantics_digest,
            "registry_evaluation_authorized": self.registry_evaluation_authorized,
            "registry_semantics_digest": self.registry_semantics_digest,
            "request_event_id": self.request_event_id,
            "request_event_path": self.request_event_path,
            "request_event_sha256": self.request_event_sha256,
            "research_table_materialization_authorized": (
                self.table_materialization_authorized
            ),
            "resource_caps_digest": self.resource_caps_digest,
            "runtime_file_set_digest": self.runtime_file_set_digest,
            "schema_version": EXECUTION_APPROVAL_SCHEMA_VERSION,
            "scope_set_id": self.scope_set_id,
            "scope_set_sha256": self.scope_set_sha256,
            "source_artifact_set_digest": self.source_artifact_set_digest,
            "source_binding_manifest_id": self.source_binding_manifest_id,
            "source_binding_manifest_sha256": self.source_binding_manifest_sha256,
            "manifest_preflight_intent_id": self.manifest_preflight_intent_id,
            "manifest_preflight_intent_path": self.manifest_preflight_intent_path,
            "manifest_preflight_intent_sha256": self.manifest_preflight_intent_sha256,
            "source_discovery_authorized": self.source_discovery_authorized,
            "verification_file_set_digest": self.verification_file_set_digest,
        }

    @property
    def approval_id(self) -> str:
        return stable_digest(
            {
                "approval_literal_sha256": self.approval_literal_sha256,
                "approval_rule_version": EXECUTION_APPROVAL_RULE_VERSION,
                "authorized_action": self.authorized_action,
                "execution_scope": self.execution_scope,
                "plan_id": self.plan_id,
                "plan_sha256": self.plan_sha256,
                "request_event_id": self.request_event_id,
                "request_event_sha256": self.request_event_sha256,
            }
        )

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "approval_id": self.approval_id})

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return directional_raw_preview_execution_approval_path(self.approval_id)

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewExecutionApproval:
        document = _mapping(value, "directional raw-preview execution approval")
        approval = cls(
            request_event_id=str(document.get("request_event_id")),
            request_event_path=str(document.get("request_event_path")),
            request_event_sha256=str(document.get("request_event_sha256")),
            plan_id=str(document.get("plan_id")),
            plan_path=str(document.get("plan_path")),
            plan_sha256=str(document.get("plan_sha256")),
            execution_data_root=str(document.get("execution_data_root")),
            input_binding_digest=str(document.get("input_binding_digest")),
            resource_caps_digest=str(document.get("resource_caps_digest")),
            runtime_file_set_digest=str(document.get("runtime_file_set_digest")),
            verification_file_set_digest=str(
                document.get("verification_file_set_digest")
            ),
            source_binding_manifest_id=str(document.get("source_binding_manifest_id")),
            source_binding_manifest_sha256=str(
                document.get("source_binding_manifest_sha256")
            ),
            manifest_preflight_intent_id=str(
                document.get("manifest_preflight_intent_id")
            ),
            manifest_preflight_intent_path=str(
                document.get("manifest_preflight_intent_path")
            ),
            manifest_preflight_intent_sha256=str(
                document.get("manifest_preflight_intent_sha256")
            ),
            source_artifact_set_digest=str(document.get("source_artifact_set_digest")),
            inventory_completion_id=str(document.get("inventory_completion_id")),
            inventory_completion_sha256=str(
                document.get("inventory_completion_sha256")
            ),
            scope_set_id=str(document.get("scope_set_id")),
            scope_set_sha256=str(document.get("scope_set_sha256")),
            contract_id=str(document.get("contract_id")),
            contract_schema_digest=str(document.get("contract_schema_digest")),
            contract_candidate_sha256=str(document.get("contract_candidate_sha256")),
            algorithm_digest=str(document.get("algorithm_digest")),
            qa_semantics_digest=str(document.get("qa_semantics_digest")),
            registry_semantics_digest=str(document.get("registry_semantics_digest")),
            preparation_approval_literal_sha256=str(
                document.get("preparation_approval_literal_sha256")
            ),
            approval_literal=str(document.get("approval_literal")),
            approval_literal_sha256=str(document.get("approval_literal_sha256")),
            approved_by=str(document.get("approved_by")),
            approved_at_utc=_parse_utc(document.get("approved_at_utc"), "approved_at_utc"),
            approval_note=str(document.get("approval_note")),
            decision=str(document.get("decision")),
            approval_stage=str(document.get("approval_stage")),
            authorized_action=str(document.get("authorized_action")),
            execution_scope=str(document.get("execution_scope")),
        )
        if _canonical_bytes(document) != approval.content:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "execution approval does not reproduce canonical bytes"
            )
        return approval


def directional_raw_preview_execution_approval_path(approval_id: str) -> str:
    _digest(approval_id, "execution approval ID")
    return (
        "manifests/silver/identity/directional-raw-preview-execution-approvals/"
        f"approval_id={approval_id}/manifest.json"
    )


class DirectionalRawPreviewExecutionApprovalStore:
    """Immutable approval store layered over the execution control store."""

    def __init__(self, data_root: Path) -> None:
        self.plan_store = DirectionalRawPreviewExecutionPlanStore(data_root)
        self.root = self.plan_store.root

    def store_approval(
        self, approval: S7DirectionalRawPreviewExecutionApproval
    ) -> StoredDirectionalRawPreviewExecutionDocument:
        request, _ = self.plan_store.load_execution_request(
            approval.request_event_id,
            expected_sha256=approval.request_event_sha256,
        )
        plan, _ = self.plan_store.load_execution_plan(
            approval.plan_id,
            expected_sha256=approval.plan_sha256,
        )
        _verify_approval_bindings(approval, request, plan)
        try:
            path = safe_relative_path(self.root, approval.relative_path)
            receipt = write_bytes_immutable(self.root, path, approval.content)
        except ArtifactError as exc:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(str(exc)) from exc
        return StoredDirectionalRawPreviewExecutionDocument(
            path=str(receipt["path"]),
            sha256=str(receipt["sha256"]),
            bytes=int(receipt["bytes"]),
        )

    def load_approval(
        self, approval_id: str, *, expected_sha256: str
    ) -> tuple[
        S7DirectionalRawPreviewExecutionApproval,
        StoredDirectionalRawPreviewExecutionDocument,
    ]:
        relative = directional_raw_preview_execution_approval_path(approval_id)
        try:
            path = safe_relative_path(self.root, relative)
        except ArtifactError as exc:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(str(exc)) from exc
        if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "exact execution approval is missing or altered"
            )
        content = path.read_bytes()
        try:
            document = json.loads(content, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "execution approval is not JSON"
            ) from exc
        if not isinstance(document, dict) or _canonical_bytes(document) != content:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "execution approval is not canonical JSON"
            )
        approval = S7DirectionalRawPreviewExecutionApproval.from_dict(document)
        if approval.approval_id != approval_id or approval.sha256 != expected_sha256:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "execution approval identity differs"
            )
        request, _ = self.plan_store.load_execution_request(
            approval.request_event_id,
            expected_sha256=approval.request_event_sha256,
        )
        plan, _ = self.plan_store.load_execution_plan(
            approval.plan_id,
            expected_sha256=approval.plan_sha256,
        )
        _verify_approval_bindings(approval, request, plan)
        return approval, StoredDirectionalRawPreviewExecutionDocument(
            relative, expected_sha256, len(content)
        )


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewExecutionApprovalRun:
    plan: S7DirectionalRawPreviewExecutionPlan
    plan_document: StoredDirectionalRawPreviewExecutionDocument
    request: S7DirectionalRawPreviewExecutionRequest
    request_document: StoredDirectionalRawPreviewExecutionDocument
    approval: S7DirectionalRawPreviewExecutionApproval
    approval_document: StoredDirectionalRawPreviewExecutionDocument
    approval_document_preexisting: bool


def record_s7_directional_raw_preview_execution_approval(
    data_root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    request_event_id: str,
    expected_request_event_sha256: str,
    approval_literal: str,
    approved_by: str,
    approved_at: str,
    approval_note: str = "",
) -> S7DirectionalRawPreviewExecutionApprovalRun:
    """Record the exact receipt only; never execute the approved preview."""

    store = DirectionalRawPreviewExecutionApprovalStore(data_root)
    plan, plan_document = store.plan_store.load_execution_plan(
        plan_id, expected_sha256=expected_plan_sha256
    )
    request, request_document = store.plan_store.load_execution_request(
        request_event_id, expected_sha256=expected_request_event_sha256
    )
    if request.plan_id != plan.plan_id or request.plan_sha256 != plan.sha256:
        raise IdentityDirectionalRawPreviewExecutionApprovalError(
            "execution request crosses supplied plan"
        )
    approval = S7DirectionalRawPreviewExecutionApproval.create(
        request,
        request_document,
        plan=plan,
        approval_literal=approval_literal,
        approved_by=approved_by,
        approved_at_utc=_parse_utc(approved_at, "approved_at"),
        approval_note=approval_note,
    )
    destination = safe_relative_path(store.root, approval.relative_path)
    preexisting = destination.exists()
    approval_document = store.store_approval(approval)
    loaded, loaded_document = store.load_approval(
        approval.approval_id, expected_sha256=approval.sha256
    )
    if loaded != approval or loaded_document != approval_document:
        raise IdentityDirectionalRawPreviewExecutionApprovalError(
            "execution approval immutable readback differs"
        )
    return S7DirectionalRawPreviewExecutionApprovalRun(
        plan=plan,
        plan_document=plan_document,
        request=request,
        request_document=request_document,
        approval=approval,
        approval_document=approval_document,
        approval_document_preexisting=preexisting,
    )


def _verify_approval_bindings(
    approval: S7DirectionalRawPreviewExecutionApproval,
    request: S7DirectionalRawPreviewExecutionRequest,
    plan: S7DirectionalRawPreviewExecutionPlan,
) -> None:
    if (
        approval.request_event_id != request.request_event_id
        or approval.request_event_path != request.relative_path
        or approval.request_event_sha256 != request.sha256
        or approval.plan_id != plan.plan_id
        or approval.plan_path != plan.relative_path
        or approval.plan_sha256 != plan.sha256
        or request.plan_id != plan.plan_id
        or request.plan_sha256 != plan.sha256
        or approval.execution_data_root != plan.execution_data_root
        or approval.input_binding_digest != plan.input_binding_digest
        or approval.resource_caps_digest != plan.resource_caps.digest
        or approval.runtime_file_set_digest != plan.runtime_file_set_digest
        or approval.verification_file_set_digest != plan.verification_file_set_digest
        or approval.source_binding_manifest_id != plan.source_binding_manifest_id
        or approval.source_binding_manifest_sha256 != plan.source_binding_manifest_sha256
        or approval.manifest_preflight_intent_id != plan.manifest_preflight_intent_id
        or approval.manifest_preflight_intent_path != plan.manifest_preflight_intent_path
        or approval.manifest_preflight_intent_sha256
        != plan.manifest_preflight_intent_sha256
        or request.manifest_preflight_intent_id != plan.manifest_preflight_intent_id
        or request.manifest_preflight_intent_path != plan.manifest_preflight_intent_path
        or request.manifest_preflight_intent_sha256
        != plan.manifest_preflight_intent_sha256
        or approval.source_artifact_set_digest != plan.source_artifact_set_digest
        or approval.inventory_completion_id != plan.inventory_completion_id
        or approval.inventory_completion_sha256 != plan.inventory_completion_sha256
        or approval.scope_set_id != plan.scope_set_id
        or approval.scope_set_sha256 != plan.scope_set_sha256
        or approval.contract_id != plan.contract_id
        or approval.contract_schema_digest != plan.contract_schema_digest
        or approval.contract_candidate_sha256 != plan.contract_candidate_sha256
        or approval.algorithm_digest != plan.algorithm_digest
        or approval.qa_semantics_digest != plan.qa_semantics_digest
        or approval.registry_semantics_digest != request.registry_semantics_digest
        or approval.preparation_approval_literal_sha256
        != request.preparation_approval_literal_sha256
        or approval.approval_literal != request.canonical_approval_literal
        or approval.approved_at_utc <= request.created_at_utc
        or approval.approved_by
        in {request.created_by, plan.created_by, plan.source_binding_created_by}
    ):
        raise IdentityDirectionalRawPreviewExecutionApprovalError(
            "execution approval crosses request or plan bindings"
        )


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise IdentityDirectionalRawPreviewExecutionApprovalError(
                "execution approval contains duplicate JSON keys"
            )
        output[key] = value
    return output


__all__ = [
    "EXECUTION_APPROVAL_RULE_VERSION",
    "EXECUTION_APPROVAL_STAGE",
    "DirectionalRawPreviewExecutionApprovalStore",
    "IdentityDirectionalRawPreviewExecutionApprovalError",
    "S7DirectionalRawPreviewExecutionApproval",
    "S7DirectionalRawPreviewExecutionApprovalRun",
    "directional_raw_preview_execution_approval_path",
    "record_s7_directional_raw_preview_execution_approval",
]
