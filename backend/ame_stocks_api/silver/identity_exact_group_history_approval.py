"""Immutable execution approval for the S7 exact-group history review.

This module records one byte-exact human approval receipt.  It deliberately
contains no Parquet reader, source-bundle opener, review engine, registry,
adjudicator, materializer, Full-run entry point, or publisher.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)

EXACT_GROUP_HISTORY_EXECUTION_APPROVAL_SCHEMA_VERSION: Final = 1
EXACT_GROUP_HISTORY_EXECUTION_APPROVAL_RULE_VERSION: Final = (
    "s7_exact_group_history_execution_approval_v1"
)
EXACT_GROUP_HISTORY_EXECUTION_AUTHORIZED_ACTION: Final = (
    "execute_exact_s7_three_group_full_s4_history_once_to_awaiting_review"
)
EXACT_GROUP_HISTORY_EXECUTION_SCOPE: Final = (
    "exact_three_group_5026_artifact_full_s4_observed_history_once_to_"
    "awaiting_review_no_registry_no_adjudication_no_full_no_publish"
)
EXACT_GROUP_HISTORY_EXECUTION_APPROVAL_STAGE: Final = (
    "s7_exact_group_history_full_s4_bounded_review"
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class IdentityExactGroupHistoryApprovalError(RuntimeError):
    """Raised when an exact-group execution approval cannot be trusted."""


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise IdentityExactGroupHistoryApprovalError(f"{label} must be lowercase 64-hex")
    return value


def _safe_text(
    value: object,
    label: str,
    *,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise IdentityExactGroupHistoryApprovalError(f"{label} must be text")
    lowered = value.casefold()
    if (
        (not value and not allow_empty)
        or len(value) > maximum
        or value.strip() != value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
        or any(token in lowered for token in ("api_key", "password", "secret", "token="))
    ):
        raise IdentityExactGroupHistoryApprovalError(f"{label} is unsafe")
    return value


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityExactGroupHistoryApprovalError(f"{label} must be timezone-aware")
    if value.utcoffset().total_seconds() != 0:
        raise IdentityExactGroupHistoryApprovalError(f"{label} must be UTC")
    return value.astimezone(UTC)


def _parse_utc(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise IdentityExactGroupHistoryApprovalError(f"{label} must be text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IdentityExactGroupHistoryApprovalError(f"{label} must be ISO-8601") from exc
    normalized = _utc(parsed, label)
    if normalized.isoformat() != value:
        raise IdentityExactGroupHistoryApprovalError(f"{label} must be canonical UTC")
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


def _request_literal(request: object) -> str:
    literal = getattr(request, "canonical_approval_literal", None)
    if not isinstance(literal, str):
        literal = getattr(request, "approval_literal", None)
    if not isinstance(literal, str):
        raise IdentityExactGroupHistoryApprovalError(
            "execution Request does not expose a canonical approval literal"
        )
    try:
        parsed = json.loads(literal)
    except json.JSONDecodeError as exc:
        raise IdentityExactGroupHistoryApprovalError(
            "execution Request approval literal is not JSON"
        ) from exc
    if (
        not isinstance(parsed, dict)
        or json.dumps(
            parsed,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        != literal
    ):
        raise IdentityExactGroupHistoryApprovalError(
            "execution Request approval literal is not canonical"
        )
    return literal


def exact_group_history_execution_approval_path(approval_id: str) -> str:
    _digest(approval_id, "approval ID")
    return (
        "manifests/silver/identity/exact-group-history-execution-approvals/"
        f"approval_id={approval_id}/manifest.json"
    )


@dataclass(frozen=True, slots=True)
class StoredExactGroupHistoryApprovalDocument:
    path: str
    sha256: str
    bytes: int


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryExecutionApproval:
    request_event_id: str
    request_event_path: str
    request_event_sha256: str
    plan_id: str
    plan_path: str
    plan_sha256: str
    execution_data_root: str
    source_binding_id: str
    source_binding_sha256: str
    source_artifact_set_digest: str
    normalized_source_artifact_set_digest: str
    approval_literal: str
    approval_literal_sha256: str
    approved_by: str
    approved_at_utc: datetime
    approval_note: str
    decision: str = "approved"
    approval_stage: str = EXACT_GROUP_HISTORY_EXECUTION_APPROVAL_STAGE
    authorized_action: str = EXACT_GROUP_HISTORY_EXECUTION_AUTHORIZED_ACTION
    execution_scope: str = EXACT_GROUP_HISTORY_EXECUTION_SCOPE
    exact_group_history_execution_authorized: bool = field(default=True, init=False)
    source_read_authorized: bool = field(default=True, init=False)
    parquet_read_authorized: bool = field(default=True, init=False)
    once_to_awaiting_review: bool = field(default=True, init=False)
    source_discovery_authorized: bool = field(default=False, init=False)
    caller_scope_override_authorized: bool = field(default=False, init=False)
    share_class_filter_authorized: bool = field(default=False, init=False)
    network_access_authorized: bool = field(default=False, init=False)
    external_evidence_capture_authorized: bool = field(default=False, init=False)
    registry_evaluation_authorized: bool = field(default=False, init=False)
    adjudication_authorized: bool = field(default=False, init=False)
    override_generation_authorized: bool = field(default=False, init=False)
    table_materialization_authorized: bool = field(default=False, init=False)
    full_run_authorized: bool = field(default=False, init=False)
    publication_authorized: bool = field(default=False, init=False)
    membership_mutation_authorized: bool = field(default=False, init=False)
    forced_liquidation_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        for label, value in (
            ("request event ID", self.request_event_id),
            ("request event SHA", self.request_event_sha256),
            ("plan ID", self.plan_id),
            ("plan SHA", self.plan_sha256),
            ("source binding ID", self.source_binding_id),
            ("source binding SHA", self.source_binding_sha256),
            ("source artifact-set digest", self.source_artifact_set_digest),
            (
                "normalized source artifact-set digest",
                self.normalized_source_artifact_set_digest,
            ),
            ("approval literal SHA", self.approval_literal_sha256),
        ):
            _digest(value, label)
        if (
            not Path(self.execution_data_root).is_absolute()
            or str(Path(self.execution_data_root)) != self.execution_data_root
        ):
            raise IdentityExactGroupHistoryApprovalError(
                "approval execution root must be canonical and absolute"
            )
        for value, label in (
            (self.request_event_path, "request event path"),
            (self.plan_path, "plan path"),
        ):
            path = Path(value)
            if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
                raise IdentityExactGroupHistoryApprovalError(f"{label} is not canonical")
        if hashlib.sha256(self.approval_literal.encode("utf-8")).hexdigest() != (
            self.approval_literal_sha256
        ):
            raise IdentityExactGroupHistoryApprovalError("approval literal SHA does not reproduce")
        _safe_text(self.approved_by, "approved_by", maximum=200)
        object.__setattr__(self, "approved_at_utc", _utc(self.approved_at_utc, "approved_at_utc"))
        if self.approved_at_utc > datetime.now(UTC):
            raise IdentityExactGroupHistoryApprovalError("approved_at_utc cannot be in the future")
        _safe_text(
            self.approval_note,
            "approval_note",
            maximum=1_000,
            allow_empty=True,
        )
        if (
            self.decision != "approved"
            or self.approval_stage != EXACT_GROUP_HISTORY_EXECUTION_APPROVAL_STAGE
            or self.authorized_action != EXACT_GROUP_HISTORY_EXECUTION_AUTHORIZED_ACTION
            or self.execution_scope != EXACT_GROUP_HISTORY_EXECUTION_SCOPE
            or not all(
                (
                    self.exact_group_history_execution_authorized,
                    self.source_read_authorized,
                    self.parquet_read_authorized,
                    self.once_to_awaiting_review,
                )
            )
            or any(
                (
                    self.source_discovery_authorized,
                    self.caller_scope_override_authorized,
                    self.share_class_filter_authorized,
                    self.network_access_authorized,
                    self.external_evidence_capture_authorized,
                    self.registry_evaluation_authorized,
                    self.adjudication_authorized,
                    self.override_generation_authorized,
                    self.table_materialization_authorized,
                    self.full_run_authorized,
                    self.publication_authorized,
                    self.membership_mutation_authorized,
                    self.forced_liquidation_authorized,
                )
            )
        ):
            raise IdentityExactGroupHistoryApprovalError(
                "execution approval capability is invalid or too broad"
            )

    @classmethod
    def create(
        cls,
        request: object,
        request_receipt: object,
        *,
        plan: object,
        approval_literal: str,
        approved_by: str,
        approved_at_utc: datetime,
        approval_note: str = "",
    ) -> S7ExactGroupHistoryExecutionApproval:
        request_path = str(request.relative_path)
        request_sha = str(request.sha256)
        request_content = request.content
        if (
            getattr(request_receipt, "path", None) != request_path
            or getattr(request_receipt, "sha256", None) != request_sha
            or getattr(request_receipt, "bytes", None) != len(request_content)
        ):
            raise IdentityExactGroupHistoryApprovalError("stored execution Request receipt differs")
        if (
            getattr(request, "plan_id", None) != getattr(plan, "plan_id", None)
            or getattr(request, "plan_sha256", None) != getattr(plan, "sha256", None)
            or not _request_plan_projection_matches(request, plan)
        ):
            raise IdentityExactGroupHistoryApprovalError("execution Request crosses supplied Plan")
        literal = _request_literal(request)
        if approval_literal != literal:
            raise IdentityExactGroupHistoryApprovalError(
                "approval literal differs from exact execution Request"
            )
        request_created = _utc(request.created_at_utc, "request created_at_utc")
        approved = _utc(approved_at_utc, "approved_at_utc")
        if approved <= request_created:
            raise IdentityExactGroupHistoryApprovalError(
                "execution Request must strictly predate approval"
            )
        actors = {
            str(getattr(request, "created_by", "")),
            str(getattr(plan, "created_by", "")),
            str(getattr(plan, "source_binding_created_by", "")),
        }
        if approved_by in actors:
            raise IdentityExactGroupHistoryApprovalError(
                "approval actor must be separate from source, Plan, and Request actors"
            )
        return cls(
            request_event_id=str(request.request_event_id),
            request_event_path=request_path,
            request_event_sha256=request_sha,
            plan_id=str(plan.plan_id),
            plan_path=str(plan.relative_path),
            plan_sha256=str(plan.sha256),
            execution_data_root=str(plan.execution_data_root),
            source_binding_id=str(plan.source_binding_id),
            source_binding_sha256=str(plan.source_binding_sha256),
            source_artifact_set_digest=str(plan.source_artifact_set_digest),
            normalized_source_artifact_set_digest=str(plan.normalized_source_artifact_set_digest),
            approval_literal=literal,
            approval_literal_sha256=hashlib.sha256(literal.encode("utf-8")).hexdigest(),
            approved_by=approved_by,
            approved_at_utc=approved,
            approval_note=approval_note,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "adjudication_authorized": self.adjudication_authorized,
            "approval_literal": self.approval_literal,
            "approval_literal_sha256": self.approval_literal_sha256,
            "approval_note": self.approval_note,
            "approval_rule_version": EXACT_GROUP_HISTORY_EXECUTION_APPROVAL_RULE_VERSION,
            "approval_stage": self.approval_stage,
            "approved_at_utc": self.approved_at_utc.isoformat(),
            "approved_by": self.approved_by,
            "artifact_type": "s7_exact_group_history_execution_approval",
            "authorized_action": self.authorized_action,
            "caller_scope_override_authorized": self.caller_scope_override_authorized,
            "decision": self.decision,
            "exact_group_history_execution_authorized": (
                self.exact_group_history_execution_authorized
            ),
            "execution_data_root": self.execution_data_root,
            "execution_scope": self.execution_scope,
            "external_evidence_capture_authorized": (self.external_evidence_capture_authorized),
            "forced_liquidation_authorized": self.forced_liquidation_authorized,
            "full_run_authorized": self.full_run_authorized,
            "membership_mutation_authorized": self.membership_mutation_authorized,
            "network_access_authorized": self.network_access_authorized,
            "normalized_source_artifact_set_digest": (self.normalized_source_artifact_set_digest),
            "once_to_awaiting_review": self.once_to_awaiting_review,
            "override_generation_authorized": self.override_generation_authorized,
            "parquet_read_authorized": self.parquet_read_authorized,
            "plan_id": self.plan_id,
            "plan_path": self.plan_path,
            "plan_sha256": self.plan_sha256,
            "publication_authorized": self.publication_authorized,
            "registry_evaluation_authorized": self.registry_evaluation_authorized,
            "request_event_id": self.request_event_id,
            "request_event_path": self.request_event_path,
            "request_event_sha256": self.request_event_sha256,
            "schema_version": EXACT_GROUP_HISTORY_EXECUTION_APPROVAL_SCHEMA_VERSION,
            "share_class_filter_authorized": self.share_class_filter_authorized,
            "source_artifact_set_digest": self.source_artifact_set_digest,
            "source_binding_id": self.source_binding_id,
            "source_binding_sha256": self.source_binding_sha256,
            "source_discovery_authorized": self.source_discovery_authorized,
            "source_read_authorized": self.source_read_authorized,
            "table_materialization_authorized": self.table_materialization_authorized,
        }

    def approval_slot_payload(self) -> dict[str, object]:
        """Return the immutable at-most-once lane independent of receipt metadata."""

        return {
            "approval_literal_sha256": self.approval_literal_sha256,
            "approval_rule_version": EXACT_GROUP_HISTORY_EXECUTION_APPROVAL_RULE_VERSION,
            "authorized_action": self.authorized_action,
            "execution_scope": self.execution_scope,
            "namespace": "ame_stocks.s7.exact_group_history.execution_approval_slot.v1",
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
        }

    @property
    def approval_id(self) -> str:
        return stable_digest(self.approval_slot_payload())

    @property
    def content(self) -> bytes:
        return _canonical_bytes({**self.logical_payload(), "approval_id": self.approval_id})

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return exact_group_history_execution_approval_path(self.approval_id)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryExecutionApproval:
        if not isinstance(value, Mapping):
            raise IdentityExactGroupHistoryApprovalError("approval must be an object")
        document = dict(value)
        approval_id = document.pop("approval_id", None)
        if document.get("schema_version") != 1:
            raise IdentityExactGroupHistoryApprovalError("unsupported approval schema")
        capabilities = {
            "exact_group_history_execution_authorized",
            "source_read_authorized",
            "parquet_read_authorized",
            "once_to_awaiting_review",
            "source_discovery_authorized",
            "caller_scope_override_authorized",
            "share_class_filter_authorized",
            "network_access_authorized",
            "external_evidence_capture_authorized",
            "registry_evaluation_authorized",
            "adjudication_authorized",
            "override_generation_authorized",
            "table_materialization_authorized",
            "full_run_authorized",
            "publication_authorized",
            "membership_mutation_authorized",
            "forced_liquidation_authorized",
        }
        required = {
            "adjudication_authorized",
            "approval_literal",
            "approval_literal_sha256",
            "approval_note",
            "approval_rule_version",
            "approval_stage",
            "approved_at_utc",
            "approved_by",
            "artifact_type",
            "authorized_action",
            "caller_scope_override_authorized",
            "decision",
            "exact_group_history_execution_authorized",
            "execution_data_root",
            "execution_scope",
            "external_evidence_capture_authorized",
            "forced_liquidation_authorized",
            "full_run_authorized",
            "membership_mutation_authorized",
            "network_access_authorized",
            "normalized_source_artifact_set_digest",
            "once_to_awaiting_review",
            "override_generation_authorized",
            "parquet_read_authorized",
            "plan_id",
            "plan_path",
            "plan_sha256",
            "publication_authorized",
            "registry_evaluation_authorized",
            "request_event_id",
            "request_event_path",
            "request_event_sha256",
            "schema_version",
            "share_class_filter_authorized",
            "source_artifact_set_digest",
            "source_binding_id",
            "source_binding_sha256",
            "source_discovery_authorized",
            "source_read_authorized",
            "table_materialization_authorized",
        }
        if set(document) != required:
            raise IdentityExactGroupHistoryApprovalError("approval schema is not exact")
        if any(type(document[name]) is not bool for name in capabilities):
            raise IdentityExactGroupHistoryApprovalError(
                "approval capabilities must be native booleans"
            )
        approval = cls(
            request_event_id=str(document["request_event_id"]),
            request_event_path=str(document["request_event_path"]),
            request_event_sha256=str(document["request_event_sha256"]),
            plan_id=str(document["plan_id"]),
            plan_path=str(document["plan_path"]),
            plan_sha256=str(document["plan_sha256"]),
            execution_data_root=str(document["execution_data_root"]),
            source_binding_id=str(document["source_binding_id"]),
            source_binding_sha256=str(document["source_binding_sha256"]),
            source_artifact_set_digest=str(document["source_artifact_set_digest"]),
            normalized_source_artifact_set_digest=str(
                document["normalized_source_artifact_set_digest"]
            ),
            approval_literal=str(document["approval_literal"]),
            approval_literal_sha256=str(document["approval_literal_sha256"]),
            approved_by=str(document["approved_by"]),
            approved_at_utc=_parse_utc(document["approved_at_utc"], "approved_at_utc"),
            approval_note=str(document["approval_note"]),
        )
        if (
            document != approval.logical_payload()
            or approval_id != approval.approval_id
            or value != {**document, "approval_id": approval.approval_id}
        ):
            raise IdentityExactGroupHistoryApprovalError(
                "approval canonical identity does not reproduce"
            )
        return approval


class ExactGroupHistoryExecutionApprovalStore:
    """Immutable approval store that revalidates its Plan and Request."""

    def __init__(self, data_root: Path) -> None:
        if not isinstance(data_root, Path):
            raise IdentityExactGroupHistoryApprovalError("data_root must be a Path")
        expanded = data_root.expanduser()
        if expanded.is_symlink():
            raise IdentityExactGroupHistoryApprovalError("data_root cannot be a symlink")
        self.root = expanded.resolve()
        if not self.root.is_dir():
            raise IdentityExactGroupHistoryApprovalError("data_root must exist")

    def store_approval(
        self, approval: S7ExactGroupHistoryExecutionApproval
    ) -> StoredExactGroupHistoryApprovalDocument:
        plan, request, _ = _load_manifest_controls(
            self.root,
            plan_id=approval.plan_id,
            plan_sha256=approval.plan_sha256,
            request_event_id=approval.request_event_id,
            request_event_sha256=approval.request_event_sha256,
        )
        _verify_bindings(approval, request, plan)
        try:
            destination = safe_relative_path(self.root, approval.relative_path)
            receipt = write_bytes_immutable(self.root, destination, approval.content)
        except ArtifactError as exc:
            raise IdentityExactGroupHistoryApprovalError(str(exc)) from exc
        return StoredExactGroupHistoryApprovalDocument(
            path=str(receipt["path"]),
            sha256=str(receipt["sha256"]),
            bytes=int(receipt["bytes"]),
        )

    def load_approval(
        self, approval_id: str, *, expected_sha256: str
    ) -> tuple[
        S7ExactGroupHistoryExecutionApproval,
        StoredExactGroupHistoryApprovalDocument,
    ]:
        _digest(approval_id, "approval ID")
        _digest(expected_sha256, "expected approval SHA")
        relative = exact_group_history_execution_approval_path(approval_id)
        path = safe_relative_path(self.root, relative)
        if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
            raise IdentityExactGroupHistoryApprovalError("execution approval is missing or altered")
        content = path.read_bytes()
        try:
            raw = json.loads(content, object_pairs_hook=_reject_duplicate_keys)
            approval = S7ExactGroupHistoryExecutionApproval.from_dict(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise IdentityExactGroupHistoryApprovalError("execution approval is not JSON") from exc
        if (
            approval.approval_id != approval_id
            or approval.sha256 != expected_sha256
            or approval.content != content
            or approval.relative_path != relative
        ):
            raise IdentityExactGroupHistoryApprovalError(
                "execution approval canonical readback differs"
            )
        plan, request, _ = _load_manifest_controls(
            self.root,
            plan_id=approval.plan_id,
            plan_sha256=approval.plan_sha256,
            request_event_id=approval.request_event_id,
            request_event_sha256=approval.request_event_sha256,
        )
        _verify_bindings(approval, request, plan)
        return approval, StoredExactGroupHistoryApprovalDocument(
            relative, expected_sha256, len(content)
        )


def record_s7_exact_group_history_execution_approval(
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
) -> tuple[
    object,
    object,
    S7ExactGroupHistoryExecutionApproval,
    StoredExactGroupHistoryApprovalDocument,
]:
    """Record and read back one exact approval without executing the review."""

    root = data_root.expanduser().resolve()
    plan, request, request_receipt = _load_manifest_controls(
        root,
        plan_id=plan_id,
        plan_sha256=expected_plan_sha256,
        request_event_id=request_event_id,
        request_event_sha256=expected_request_event_sha256,
    )
    approval = S7ExactGroupHistoryExecutionApproval.create(
        request,
        request_receipt,
        plan=plan,
        approval_literal=approval_literal,
        approved_by=approved_by,
        approved_at_utc=_parse_utc(approved_at, "approved_at"),
        approval_note=approval_note,
    )
    store = ExactGroupHistoryExecutionApprovalStore(root)
    receipt = store.store_approval(approval)
    loaded, loaded_receipt = store.load_approval(
        approval.approval_id, expected_sha256=approval.sha256
    )
    if loaded != approval or loaded_receipt != receipt:
        raise IdentityExactGroupHistoryApprovalError(
            "execution approval immutable readback differs"
        )
    return plan, request, approval, receipt


def _load_manifest_controls(
    root: Path,
    *,
    plan_id: str,
    plan_sha256: str,
    request_event_id: str,
    request_event_sha256: str,
) -> tuple[object, object, object]:
    try:
        from ame_stocks_api.silver.identity_exact_group_history_manifest import (
            ExactGroupHistoryManifestStore,
        )
    except ImportError as exc:
        raise IdentityExactGroupHistoryApprovalError(
            "exact-group manifest controls are unavailable"
        ) from exc
    store = ExactGroupHistoryManifestStore(root)
    try:
        plan, _ = store.load_execution_plan(plan_id, expected_sha256=plan_sha256)
        request, receipt = store.load_execution_request(
            request_event_id, expected_sha256=request_event_sha256
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise IdentityExactGroupHistoryApprovalError(
            "exact execution Plan or Request cannot be loaded"
        ) from exc
    return plan, request, receipt


def _verify_bindings(approval: object, request: object, plan: object) -> None:
    if (
        approval.plan_id != getattr(plan, "plan_id", None)
        or approval.plan_path != getattr(plan, "relative_path", None)
        or approval.plan_sha256 != getattr(plan, "sha256", None)
        or approval.request_event_id != getattr(request, "request_event_id", None)
        or approval.request_event_path != getattr(request, "relative_path", None)
        or approval.request_event_sha256 != getattr(request, "sha256", None)
        or getattr(request, "plan_id", None) != getattr(plan, "plan_id", None)
        or getattr(request, "plan_sha256", None) != getattr(plan, "sha256", None)
        or not _request_plan_projection_matches(request, plan)
        or approval.authorized_action != getattr(plan, "document", {}).get("authorized_action")
        or approval.authorized_action != getattr(request, "document", {}).get("authorized_action")
        or approval.execution_data_root != getattr(plan, "execution_data_root", None)
        or approval.source_binding_id != getattr(plan, "source_binding_id", None)
        or approval.source_binding_sha256 != getattr(plan, "source_binding_sha256", None)
        or approval.source_artifact_set_digest != getattr(plan, "source_artifact_set_digest", None)
        or approval.normalized_source_artifact_set_digest
        != getattr(plan, "normalized_source_artifact_set_digest", None)
        or approval.approval_literal != _request_literal(request)
        or approval.approved_at_utc <= getattr(request, "created_at_utc", None)
        or approval.approved_by
        in {
            getattr(request, "created_by", None),
            getattr(plan, "created_by", None),
            getattr(plan, "source_binding_created_by", None),
        }
    ):
        raise IdentityExactGroupHistoryApprovalError(
            "execution approval crosses exact Plan or Request"
        )


def _request_plan_projection_matches(request: object, plan: object) -> bool:
    """Require every immutable Request source/control projection to match its Plan."""

    pairs = (
        ("input_binding_digest", "input_binding_digest"),
        ("manifest_plan_id", "manifest_plan_id"),
        ("manifest_plan_sha256", "manifest_plan_sha256"),
        ("manifest_approval_id", "manifest_approval_id"),
        ("manifest_approval_sha256", "manifest_approval_sha256"),
        ("source_binding_id", "source_binding_id"),
        ("source_binding_sha256", "source_binding_sha256"),
        ("raw_source_artifact_set_digest", "raw_source_artifact_set_digest"),
        ("inventory_projection_set_digest", "inventory_projection_set_digest"),
        (
            "normalized_source_artifact_set_digest",
            "normalized_source_artifact_set_digest",
        ),
    )
    return all(
        getattr(request, request_field, None) == getattr(plan, plan_field, None)
        for request_field, plan_field in pairs
    ) and getattr(request, "resource_caps_digest", None) == getattr(
        getattr(plan, "execution_resource_caps", None), "digest", None
    )


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise IdentityExactGroupHistoryApprovalError(
                "control document contains duplicate JSON keys"
            )
        output[key] = value
    return output


__all__ = [
    "EXACT_GROUP_HISTORY_EXECUTION_APPROVAL_RULE_VERSION",
    "EXACT_GROUP_HISTORY_EXECUTION_AUTHORIZED_ACTION",
    "EXACT_GROUP_HISTORY_EXECUTION_SCOPE",
    "ExactGroupHistoryExecutionApprovalStore",
    "IdentityExactGroupHistoryApprovalError",
    "S7ExactGroupHistoryExecutionApproval",
    "StoredExactGroupHistoryApprovalDocument",
    "exact_group_history_execution_approval_path",
    "record_s7_exact_group_history_execution_approval",
]
