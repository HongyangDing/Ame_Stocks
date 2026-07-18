"""Record one exact manifest-only preflight approval without reading sources."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan import (
    MANIFEST_AUTHORIZED_ACTION,
    MANIFEST_LITERAL_VERSION,
    IdentityDirectionalRawPreviewManifestStore,
    S7DirectionalRawPreviewManifestPreflightPlan,
    StoredDirectionalRawPreviewManifestControl,
)
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_request import (
    S7DirectionalRawPreviewManifestPreflightRequest,
    load_manifest_preflight_request,
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class IdentityDirectionalRawPreviewManifestApprovalError(RuntimeError):
    pass


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise IdentityDirectionalRawPreviewManifestApprovalError(f"{label} is not 64-hex")
    return value


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityDirectionalRawPreviewManifestApprovalError(f"{label} must include UTC")
    if value.utcoffset().total_seconds() != 0:
        raise IdentityDirectionalRawPreviewManifestApprovalError(f"{label} must be UTC")
    return value.astimezone(UTC)


def _safe_text(value: object, label: str, maximum: int = 400) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or value.strip() != value
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise IdentityDirectionalRawPreviewManifestApprovalError(f"{label} is unsafe")
    return value


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            dict(value), allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode()
        + b"\n"
    )


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewManifestApproval:
    plan_id: str
    plan_sha256: str
    request_event_id: str
    request_event_sha256: str
    input_binding_digest: str
    resource_caps_digest: str
    runtime_file_set_digest: str
    verification_file_set_digest: str
    selection_semantics_digest: str
    approval_literal: str
    approval_literal_sha256: str
    approved_by: str
    approved_at_utc: datetime
    approval_note: str

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA", self.plan_sha256),
            ("request ID", self.request_event_id),
            ("request SHA", self.request_event_sha256),
            ("input binding", self.input_binding_digest),
            ("caps", self.resource_caps_digest),
            ("runtime set", self.runtime_file_set_digest),
            ("verification set", self.verification_file_set_digest),
            ("selection semantics", self.selection_semantics_digest),
            ("literal SHA", self.approval_literal_sha256),
        ):
            _digest(value, label)
        if (
            hashlib.sha256(self.approval_literal.encode()).hexdigest()
            != self.approval_literal_sha256
        ):
            raise IdentityDirectionalRawPreviewManifestApprovalError("literal SHA differs")
        literal = json.loads(self.approval_literal)
        if (
            not isinstance(literal, dict)
            or json.dumps(literal, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            != self.approval_literal
            or literal.get("authorized_action") != MANIFEST_AUTHORIZED_ACTION
            or literal.get("literal_version") != MANIFEST_LITERAL_VERSION
        ):
            raise IdentityDirectionalRawPreviewManifestApprovalError("literal is not exact")
        _safe_text(self.approved_by, "approved_by", 200)
        object.__setattr__(self, "approved_at_utc", _utc(self.approved_at_utc, "approved_at"))
        if self.approved_at_utc > datetime.now(UTC):
            raise IdentityDirectionalRawPreviewManifestApprovalError(
                "approved_at cannot be in the future"
            )
        _safe_text(self.approval_note, "approval_note")

    @classmethod
    def create(
        cls,
        plan: S7DirectionalRawPreviewManifestPreflightPlan,
        request: S7DirectionalRawPreviewManifestPreflightRequest,
        plan_receipt: StoredDirectionalRawPreviewManifestControl,
        request_receipt: StoredDirectionalRawPreviewManifestControl,
        *,
        approval_literal: str,
        approved_by: str,
        approved_at_utc: datetime,
        approval_note: str,
    ) -> S7DirectionalRawPreviewManifestApproval:
        if not isinstance(plan, S7DirectionalRawPreviewManifestPreflightPlan) or not isinstance(
            request, S7DirectionalRawPreviewManifestPreflightRequest
        ):
            raise IdentityDirectionalRawPreviewManifestApprovalError(
                "approval requires exact Plan and Request types"
            )
        if (
            plan_receipt.path != plan.relative_path
            or plan_receipt.sha256 != plan.sha256
            or plan_receipt.bytes != len(plan.content)
            or request_receipt.path != request.relative_path
            or request_receipt.sha256 != request.sha256
            or request_receipt.bytes != len(request.content)
        ):
            raise IdentityDirectionalRawPreviewManifestApprovalError(
                "persisted Plan or Request receipt differs"
            )
        _verify_request_plan(request, plan)
        if approval_literal != request.canonical_approval_literal:
            raise IdentityDirectionalRawPreviewManifestApprovalError("literal differs from request")
        instant = _utc(approved_at_utc, "approved_at")
        if (
            instant <= request.created_at_utc
            or instant > datetime.now(UTC)
            or approved_by
            in {
                request.created_by,
                plan.created_by,
                plan.future_manifest_reader_actor,
                plan.future_execution_plan_actor,
                plan.future_execution_request_actor,
            }
        ):
            raise IdentityDirectionalRawPreviewManifestApprovalError(
                "approval must follow request and use a separate actor"
            )
        return cls(
            plan_id=plan.plan_id,
            plan_sha256=plan.sha256,
            request_event_id=request.request_event_id,
            request_event_sha256=request.sha256,
            input_binding_digest=plan.input_binding_digest,
            resource_caps_digest=plan.resource_caps.digest,
            runtime_file_set_digest=plan.runtime_file_set_digest,
            verification_file_set_digest=plan.verification_file_set_digest,
            selection_semantics_digest=request.selection_semantics_digest,
            approval_literal=approval_literal,
            approval_literal_sha256=hashlib.sha256(approval_literal.encode()).hexdigest(),
            approved_by=approved_by,
            approved_at_utc=instant,
            approval_note=approval_note,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_literal": self.approval_literal,
            "approval_literal_sha256": self.approval_literal_sha256,
            "approval_note": self.approval_note,
            "approved_at_utc": self.approved_at_utc.isoformat(),
            "approved_by": self.approved_by,
            "artifact_type": "s7_directional_raw_preview_manifest_preflight_approval",
            "authorized_action": MANIFEST_AUTHORIZED_ACTION,
            "capabilities": {
                "adjudication": False,
                "full_run": False,
                "manifest_only_source_binding": True,
                "network_access": False,
                "parquet_content_read": False,
                "preview_execution": False,
                "publication": False,
                "registry_evaluation": False,
            },
            "exact_authorized_counts": {
                "json_manifest_reads": 4,
                "output_json_documents": 5,
                "parquet_content_read_bytes": 0,
                "parquet_lstats": 22,
            },
            "input_binding_digest": self.input_binding_digest,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "resource_caps_digest": self.resource_caps_digest,
            "runtime_file_set_digest": self.runtime_file_set_digest,
            "schema_version": 1,
            "selection_semantics_digest": self.selection_semantics_digest,
            "state": "approved_manifest_only_once",
            "verification_file_set_digest": self.verification_file_set_digest,
        }

    @property
    def approval_id(self) -> str:
        return stable_digest(self.logical_payload())

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
        return manifest_preflight_approval_path(self.approval_id)

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewManifestApproval:
        if not isinstance(value, dict):
            raise IdentityDirectionalRawPreviewManifestApprovalError("approval must be object")
        result = cls(
            plan_id=str(value.get("plan_id")),
            plan_sha256=str(value.get("plan_sha256")),
            request_event_id=str(value.get("request_event_id")),
            request_event_sha256=str(value.get("request_event_sha256")),
            input_binding_digest=str(value.get("input_binding_digest")),
            resource_caps_digest=str(value.get("resource_caps_digest")),
            runtime_file_set_digest=str(value.get("runtime_file_set_digest")),
            verification_file_set_digest=str(value.get("verification_file_set_digest")),
            selection_semantics_digest=str(value.get("selection_semantics_digest")),
            approval_literal=str(value.get("approval_literal")),
            approval_literal_sha256=str(value.get("approval_literal_sha256")),
            approved_by=str(value.get("approved_by")),
            approved_at_utc=datetime.fromisoformat(str(value.get("approved_at_utc"))),
            approval_note=str(value.get("approval_note")),
        )
        if _canonical_bytes(value) != result.content:
            raise IdentityDirectionalRawPreviewManifestApprovalError("approval is not canonical")
        return result


def manifest_preflight_approval_path(approval_id: str) -> str:
    _digest(approval_id, "approval ID")
    return (
        "manifests/silver/identity/directional-raw-preview-manifest-preflight-approvals/"
        f"approval_id={approval_id}/manifest.json"
    )


class DirectionalRawPreviewManifestApprovalStore:
    def __init__(self, control_root: Path) -> None:
        expanded = control_root.expanduser()
        if expanded.is_symlink():
            raise IdentityDirectionalRawPreviewManifestApprovalError("control root is symlink")
        self.root = expanded.resolve()
        if not self.root.is_dir():
            raise IdentityDirectionalRawPreviewManifestApprovalError("control root must exist")

    def store(
        self, value: S7DirectionalRawPreviewManifestApproval
    ) -> StoredDirectionalRawPreviewManifestControl:
        self._verify_controls(value)
        try:
            receipt = write_bytes_immutable(
                self.root, safe_relative_path(self.root, value.relative_path), value.content
            )
        except ArtifactError as exc:
            raise IdentityDirectionalRawPreviewManifestApprovalError(str(exc)) from exc
        return StoredDirectionalRawPreviewManifestControl(
            str(receipt["path"]), str(receipt["sha256"]), int(receipt["bytes"])
        )

    def load_approval(
        self, approval_id: str, *, expected_sha256: str
    ) -> tuple[
        S7DirectionalRawPreviewManifestApproval,
        StoredDirectionalRawPreviewManifestControl,
    ]:
        relative = manifest_preflight_approval_path(approval_id)
        path = safe_relative_path(self.root, relative)
        if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
            raise IdentityDirectionalRawPreviewManifestApprovalError("approval missing or altered")
        content = path.read_bytes()
        value = json.loads(content, object_pairs_hook=_reject_duplicates)
        if not isinstance(value, dict) or _canonical_bytes(value) != content:
            raise IdentityDirectionalRawPreviewManifestApprovalError("approval not canonical")
        approval = S7DirectionalRawPreviewManifestApproval.from_dict(value)
        if approval.approval_id != approval_id:
            raise IdentityDirectionalRawPreviewManifestApprovalError("approval ID differs")
        self._verify_controls(approval)
        return approval, StoredDirectionalRawPreviewManifestControl(
            relative, expected_sha256, len(content)
        )

    def _verify_controls(self, approval: S7DirectionalRawPreviewManifestApproval) -> None:
        plan, _ = IdentityDirectionalRawPreviewManifestStore(self.root).load_plan(
            approval.plan_id, expected_sha256=approval.plan_sha256
        )
        request, _ = load_manifest_preflight_request(
            self.root,
            approval.request_event_id,
            expected_sha256=approval.request_event_sha256,
        )
        _verify_request_plan(request, plan)
        if (
            approval.approval_literal != request.canonical_approval_literal
            or approval.input_binding_digest != plan.input_binding_digest
            or approval.resource_caps_digest != plan.resource_caps.digest
            or approval.runtime_file_set_digest != plan.runtime_file_set_digest
            or approval.verification_file_set_digest != plan.verification_file_set_digest
            or approval.selection_semantics_digest != request.selection_semantics_digest
            or approval.approved_at_utc <= request.created_at_utc
            or approval.approved_at_utc > datetime.now(UTC)
            or approval.approved_by
            in {
                plan.created_by,
                request.created_by,
                plan.future_manifest_reader_actor,
                plan.future_execution_plan_actor,
                plan.future_execution_request_actor,
            }
        ):
            raise IdentityDirectionalRawPreviewManifestApprovalError(
                "approval differs from persisted controls"
            )


def record_s7_directional_raw_preview_manifest_approval(
    control_root: Path,
    *,
    plan_id: str,
    plan_sha256: str,
    request_event_id: str,
    request_event_sha256: str,
    approval_literal: str,
    approved_by: str,
    approved_at_utc: datetime,
    approval_note: str,
) -> tuple[S7DirectionalRawPreviewManifestApproval, StoredDirectionalRawPreviewManifestControl]:
    controls = IdentityDirectionalRawPreviewManifestStore(control_root)
    plan, plan_receipt = controls.load_plan(plan_id, expected_sha256=plan_sha256)
    request, request_receipt = load_manifest_preflight_request(
        controls.root, request_event_id, expected_sha256=request_event_sha256
    )
    approval = S7DirectionalRawPreviewManifestApproval.create(
        plan,
        request,
        plan_receipt,
        request_receipt,
        approval_literal=approval_literal,
        approved_by=approved_by,
        approved_at_utc=approved_at_utc,
        approval_note=approval_note,
    )
    store = DirectionalRawPreviewManifestApprovalStore(controls.root)
    receipt = store.store(approval)
    loaded, loaded_receipt = store.load_approval(
        approval.approval_id, expected_sha256=approval.sha256
    )
    if loaded != approval or loaded_receipt != receipt:
        raise IdentityDirectionalRawPreviewManifestApprovalError("approval readback differs")
    return approval, receipt


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IdentityDirectionalRawPreviewManifestApprovalError("duplicate JSON key")
        result[key] = value
    return result


def _verify_request_plan(
    request: S7DirectionalRawPreviewManifestPreflightRequest,
    plan: S7DirectionalRawPreviewManifestPreflightPlan,
) -> None:
    if (
        request.plan_id != plan.plan_id
        or request.plan_path != plan.relative_path
        or request.plan_sha256 != plan.sha256
        or request.input_binding_digest != plan.input_binding_digest
        or request.resource_caps_digest != plan.resource_caps.digest
        or request.runtime_file_set_digest != plan.runtime_file_set_digest
        or request.verification_file_set_digest != plan.verification_file_set_digest
        or request.execution_data_root != plan.execution_data_root
        or request.future_manifest_reader_actor != plan.future_manifest_reader_actor
        or request.future_execution_plan_actor != plan.future_execution_plan_actor
        or request.future_execution_request_actor != plan.future_execution_request_actor
        or request.preparation_authorization_id != plan.preparation_authorization_id
        or request.preparation_authorization_sha256 != plan.preparation_authorization_sha256
        or request.created_at_utc < plan.created_at_utc
        or request.created_by == plan.created_by
    ):
        raise IdentityDirectionalRawPreviewManifestApprovalError(
            "request crosses persisted Plan bindings"
        )


__all__ = [
    "DirectionalRawPreviewManifestApprovalStore",
    "IdentityDirectionalRawPreviewManifestApprovalError",
    "S7DirectionalRawPreviewManifestApproval",
    "manifest_preflight_approval_path",
    "record_s7_directional_raw_preview_manifest_approval",
]
