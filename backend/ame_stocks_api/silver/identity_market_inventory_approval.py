"""Exact literal approval receipt for the executable S7 Gate-A inventory v2.

This module records one immutable approval for one already-persisted v2
request.  It has no Parquet reader, inventory engine, network client,
classification, adjudication, materialization, release, or publication path.
No plan/request identity is compiled into the code: the caller must supply the
exact IDs, checksums, and byte-for-byte literal that the request emitted.
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
from ame_stocks_api.silver.identity_market_inventory_execution_plan import (
    EXECUTION_AUTHORIZED_ACTION,
    EXECUTION_LITERAL_VERSION,
    EXECUTION_SCOPE,
    IdentityMarketInventoryExecutionPlanError,
    IdentityMarketInventoryExecutionPlanStore,
    S7CompositeInventoryExecutionPlanV2,
    S7CompositeInventoryExecutionRequestV2,
    StoredInventoryExecutionDocument,
    execution_plan_v2_path,
    execution_request_v2_path,
)

EXECUTION_APPROVAL_SCHEMA_VERSION: Final = 2
EXECUTION_APPROVAL_RULE_VERSION: Final = "s7_composite_inventory_execution_approval_v2"
EXECUTION_APPROVAL_STAGE: Final = "gate_a_composite_inventory_v2"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class IdentityMarketInventoryExecutionApprovalError(IdentityMarketInventoryExecutionPlanError):
    """Raised when an exact v2 execution approval cannot be trusted."""


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise IdentityMarketInventoryExecutionApprovalError(f"{label} must be lowercase 64-hex")
    return value


def _safe_text(
    value: object,
    label: str,
    *,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise IdentityMarketInventoryExecutionApprovalError(f"{label} must be text")
    if (
        (not value and not allow_empty)
        or len(value) > maximum
        or value.strip() != value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise IdentityMarketInventoryExecutionApprovalError(f"{label} is unsafe")
    return value


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityMarketInventoryExecutionApprovalError(f"{label} must be timezone-aware")
    if value.utcoffset().total_seconds() != 0:
        raise IdentityMarketInventoryExecutionApprovalError(f"{label} must be UTC")
    return value.astimezone(UTC)


def _parse_utc(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise IdentityMarketInventoryExecutionApprovalError(f"{label} must be text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IdentityMarketInventoryExecutionApprovalError(f"{label} is not ISO-8601") from exc
    normalized = _utc(parsed, label)
    if normalized.isoformat() != value:
        raise IdentityMarketInventoryExecutionApprovalError(f"{label} is not canonical UTC")
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
        raise IdentityMarketInventoryExecutionApprovalError(f"{label} must be an object")
    return dict(value)


def _decode_json(content: bytes, label: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in pairs:
            if key in output:
                raise IdentityMarketInventoryExecutionApprovalError(
                    f"{label} contains duplicate JSON keys"
                )
            output[key] = value
        return output

    try:
        decoded = json.loads(content, object_pairs_hook=reject_duplicates)
    except IdentityMarketInventoryExecutionApprovalError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityMarketInventoryExecutionApprovalError(f"{label} is not valid JSON") from exc
    return _mapping(decoded, label)


def _approval_literal(
    *,
    algorithm_digest: str,
    authorized_action: str,
    blocked_event_id: str,
    execution_data_root: str,
    input_binding_digest: str,
    inventory_contract_id: str,
    inventory_schema_digest: str,
    plan_id: str,
    plan_sha256: str,
    qa_semantics_digest: str,
    request_event_id: str,
    request_event_sha256: str,
    resource_caps_digest: str,
    runtime_file_set_digest: str,
    verification_file_set_digest: str,
) -> str:
    return json.dumps(
        {
            "algorithm_digest": algorithm_digest,
            "authorized_action": authorized_action,
            "blocked_event_id": blocked_event_id,
            "execution_data_root": execution_data_root,
            "input_binding_digest": input_binding_digest,
            "inventory_contract_id": inventory_contract_id,
            "inventory_schema_digest": inventory_schema_digest,
            "literal_version": EXECUTION_LITERAL_VERSION,
            "plan_id": plan_id,
            "plan_sha256": plan_sha256,
            "qa_semantics_digest": qa_semantics_digest,
            "request_event_id": request_event_id,
            "request_event_sha256": request_event_sha256,
            "resource_caps_digest": resource_caps_digest,
            "runtime_file_set_digest": runtime_file_set_digest,
            "verification_file_set_digest": verification_file_set_digest,
        },
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


@dataclass(frozen=True, slots=True)
class S7CompositeInventoryExecutionApprovalV2:
    """One byte-exact human approval of one immutable v2 request."""

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
    inventory_contract_id: str
    inventory_candidate_sha256: str
    inventory_schema_digest: str
    algorithm_digest: str
    qa_semantics_digest: str
    blocked_event_id: str
    blocked_event_sha256: str
    approval_literal: str
    approval_literal_sha256: str
    approved_by: str
    approved_at_utc: datetime
    approval_note: str
    decision: str = "approved"
    approval_stage: str = EXECUTION_APPROVAL_STAGE
    authorized_action: str = EXECUTION_AUTHORIZED_ACTION
    execution_scope: str = EXECUTION_SCOPE
    execution_authorized: bool = field(default=True, init=False)
    market_classification_authorized: bool = field(default=False, init=False)
    adjudication_authorized: bool = field(default=False, init=False)
    table_materialization_authorized: bool = field(default=False, init=False)
    publication_authorized: bool = field(default=False, init=False)
    network_access_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        for label, value in (
            ("request event ID", self.request_event_id),
            ("request event SHA-256", self.request_event_sha256),
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("input binding digest", self.input_binding_digest),
            ("resource caps digest", self.resource_caps_digest),
            ("runtime file set digest", self.runtime_file_set_digest),
            ("verification file set digest", self.verification_file_set_digest),
            ("inventory contract ID", self.inventory_contract_id),
            ("inventory candidate SHA-256", self.inventory_candidate_sha256),
            ("inventory schema digest", self.inventory_schema_digest),
            ("algorithm digest", self.algorithm_digest),
            ("QA semantics digest", self.qa_semantics_digest),
            ("blocked event ID", self.blocked_event_id),
            ("blocked event SHA-256", self.blocked_event_sha256),
            ("approval literal SHA-256", self.approval_literal_sha256),
        ):
            _digest(value, label)
        if self.request_event_path != execution_request_v2_path(self.request_event_id):
            raise IdentityMarketInventoryExecutionApprovalError(
                "approval request path is not canonical"
            )
        if self.plan_path != execution_plan_v2_path(self.plan_id):
            raise IdentityMarketInventoryExecutionApprovalError(
                "approval plan path is not canonical"
            )
        if (
            self.execution_data_root != str(Path(self.execution_data_root))
            or not Path(self.execution_data_root).is_absolute()
        ):
            raise IdentityMarketInventoryExecutionApprovalError(
                "approval execution_data_root is not canonical and absolute"
            )
        if not isinstance(self.approval_literal, str):
            raise IdentityMarketInventoryExecutionApprovalError(
                "approval literal must be exact text"
            )
        if (
            hashlib.sha256(self.approval_literal.encode("utf-8")).hexdigest()
            != self.approval_literal_sha256
        ):
            raise IdentityMarketInventoryExecutionApprovalError(
                "approval literal SHA-256 does not reproduce"
            )
        expected_literal = _approval_literal(
            algorithm_digest=self.algorithm_digest,
            authorized_action=self.authorized_action,
            blocked_event_id=self.blocked_event_id,
            execution_data_root=self.execution_data_root,
            input_binding_digest=self.input_binding_digest,
            inventory_contract_id=self.inventory_contract_id,
            inventory_schema_digest=self.inventory_schema_digest,
            plan_id=self.plan_id,
            plan_sha256=self.plan_sha256,
            qa_semantics_digest=self.qa_semantics_digest,
            request_event_id=self.request_event_id,
            request_event_sha256=self.request_event_sha256,
            resource_caps_digest=self.resource_caps_digest,
            runtime_file_set_digest=self.runtime_file_set_digest,
            verification_file_set_digest=self.verification_file_set_digest,
        )
        if self.approval_literal != expected_literal:
            raise IdentityMarketInventoryExecutionApprovalError(
                "approval literal is not the exact canonical v2 literal"
            )
        _safe_text(self.approved_by, "approved_by", maximum=200)
        object.__setattr__(
            self,
            "approved_at_utc",
            _utc(self.approved_at_utc, "approved_at_utc"),
        )
        if self.approved_at_utc > datetime.now(UTC):
            raise IdentityMarketInventoryExecutionApprovalError(
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
            or self.execution_authorized is not True
            or self.market_classification_authorized is not False
            or self.adjudication_authorized is not False
            or self.table_materialization_authorized is not False
            or self.publication_authorized is not False
            or self.network_access_authorized is not False
        ):
            raise IdentityMarketInventoryExecutionApprovalError(
                "execution approval scope is invalid or too broad"
            )

    @classmethod
    def create(
        cls,
        request: S7CompositeInventoryExecutionRequestV2,
        request_receipt: StoredInventoryExecutionDocument,
        *,
        approval_literal: str,
        approved_by: str,
        approved_at_utc: datetime,
        approval_note: str = "",
    ) -> S7CompositeInventoryExecutionApprovalV2:
        if not isinstance(request, S7CompositeInventoryExecutionRequestV2):
            raise IdentityMarketInventoryExecutionApprovalError("execution request has wrong type")
        if (
            request_receipt.path != request.relative_path
            or request_receipt.sha256 != request.sha256
            or request_receipt.bytes != len(request.content)
        ):
            raise IdentityMarketInventoryExecutionApprovalError(
                "stored execution request receipt differs"
            )
        approved_at = _utc(approved_at_utc, "approved_at_utc")
        if approved_at <= request.created_at_utc:
            raise IdentityMarketInventoryExecutionApprovalError(
                "execution request must strictly predate approval"
            )
        if approval_literal != request.canonical_approval_literal:
            raise IdentityMarketInventoryExecutionApprovalError(
                "approval literal differs from the exact request"
            )
        return cls(
            request_event_id=request.request_event_id,
            request_event_path=request_receipt.path,
            request_event_sha256=request_receipt.sha256,
            plan_id=request.plan_id,
            plan_path=request.plan_path,
            plan_sha256=request.plan_sha256,
            execution_data_root=request.execution_data_root,
            input_binding_digest=request.input_binding_digest,
            resource_caps_digest=request.resource_caps_digest,
            runtime_file_set_digest=request.runtime_file_set_digest,
            verification_file_set_digest=request.verification_file_set_digest,
            inventory_contract_id=request.inventory_contract_id,
            inventory_candidate_sha256=request.inventory_candidate_sha256,
            inventory_schema_digest=request.inventory_schema_digest,
            algorithm_digest=request.algorithm_digest,
            qa_semantics_digest=request.qa_semantics_digest,
            blocked_event_id=request.blocked_event_id,
            blocked_event_sha256=request.blocked_event_sha256,
            approval_literal=approval_literal,
            approval_literal_sha256=hashlib.sha256(approval_literal.encode("utf-8")).hexdigest(),
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
            "artifact_type": "s7_composite_inventory_execution_approval_v2",
            "authorized_action": self.authorized_action,
            "blocked_event_id": self.blocked_event_id,
            "blocked_event_sha256": self.blocked_event_sha256,
            "decision": self.decision,
            "execution_authorized": self.execution_authorized,
            "execution_data_root": self.execution_data_root,
            "execution_scope": self.execution_scope,
            "input_binding_digest": self.input_binding_digest,
            "inventory_candidate_sha256": self.inventory_candidate_sha256,
            "inventory_contract_id": self.inventory_contract_id,
            "inventory_schema_digest": self.inventory_schema_digest,
            "market_classification_authorized": self.market_classification_authorized,
            "network_access_authorized": self.network_access_authorized,
            "plan_id": self.plan_id,
            "plan_path": self.plan_path,
            "plan_sha256": self.plan_sha256,
            "publication_authorized": self.publication_authorized,
            "qa_semantics_digest": self.qa_semantics_digest,
            "request_event_id": self.request_event_id,
            "request_event_path": self.request_event_path,
            "request_event_sha256": self.request_event_sha256,
            "resource_caps_digest": self.resource_caps_digest,
            "runtime_file_set_digest": self.runtime_file_set_digest,
            "schema_version": EXECUTION_APPROVAL_SCHEMA_VERSION,
            "table_materialization_authorized": self.table_materialization_authorized,
            "verification_file_set_digest": self.verification_file_set_digest,
        }

    @property
    def approval_id(self) -> str:
        return stable_digest(
            {
                "approval_literal_sha256": self.approval_literal_sha256,
                "approval_rule_version": EXECUTION_APPROVAL_RULE_VERSION,
                "authorized_action": self.authorized_action,
                "execution_data_root": self.execution_data_root,
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
        return composite_inventory_execution_approval_path(self.approval_id)

    @classmethod
    def from_dict(cls, value: object) -> S7CompositeInventoryExecutionApprovalV2:
        document = _mapping(value, "v2 execution approval")
        expected = {
            "adjudication_authorized",
            "algorithm_digest",
            "approval_id",
            "approval_literal",
            "approval_literal_sha256",
            "approval_note",
            "approval_rule_version",
            "approval_stage",
            "approved_at_utc",
            "approved_by",
            "artifact_type",
            "authorized_action",
            "blocked_event_id",
            "blocked_event_sha256",
            "decision",
            "execution_authorized",
            "execution_data_root",
            "execution_scope",
            "input_binding_digest",
            "inventory_candidate_sha256",
            "inventory_contract_id",
            "inventory_schema_digest",
            "market_classification_authorized",
            "network_access_authorized",
            "plan_id",
            "plan_path",
            "plan_sha256",
            "publication_authorized",
            "qa_semantics_digest",
            "request_event_id",
            "request_event_path",
            "request_event_sha256",
            "resource_caps_digest",
            "runtime_file_set_digest",
            "schema_version",
            "table_materialization_authorized",
            "verification_file_set_digest",
        }
        if set(document) != expected:
            raise IdentityMarketInventoryExecutionApprovalError(
                "v2 execution approval schema is not exact"
            )
        if (
            document["artifact_type"] != "s7_composite_inventory_execution_approval_v2"
            or document["schema_version"] != EXECUTION_APPROVAL_SCHEMA_VERSION
            or document["approval_rule_version"] != EXECUTION_APPROVAL_RULE_VERSION
        ):
            raise IdentityMarketInventoryExecutionApprovalError(
                "v2 execution approval fixed fields are invalid"
            )
        approval = cls(
            request_event_id=str(document["request_event_id"]),
            request_event_path=str(document["request_event_path"]),
            request_event_sha256=str(document["request_event_sha256"]),
            plan_id=str(document["plan_id"]),
            plan_path=str(document["plan_path"]),
            plan_sha256=str(document["plan_sha256"]),
            execution_data_root=str(document["execution_data_root"]),
            input_binding_digest=str(document["input_binding_digest"]),
            resource_caps_digest=str(document["resource_caps_digest"]),
            runtime_file_set_digest=str(document["runtime_file_set_digest"]),
            verification_file_set_digest=str(document["verification_file_set_digest"]),
            inventory_contract_id=str(document["inventory_contract_id"]),
            inventory_candidate_sha256=str(document["inventory_candidate_sha256"]),
            inventory_schema_digest=str(document["inventory_schema_digest"]),
            algorithm_digest=str(document["algorithm_digest"]),
            qa_semantics_digest=str(document["qa_semantics_digest"]),
            blocked_event_id=str(document["blocked_event_id"]),
            blocked_event_sha256=str(document["blocked_event_sha256"]),
            approval_literal=str(document["approval_literal"]),
            approval_literal_sha256=str(document["approval_literal_sha256"]),
            approved_by=str(document["approved_by"]),
            approved_at_utc=_parse_utc(document["approved_at_utc"], "approved_at_utc"),
            approval_note=str(document["approval_note"]),
            decision=str(document["decision"]),
            approval_stage=str(document["approval_stage"]),
            authorized_action=str(document["authorized_action"]),
            execution_scope=str(document["execution_scope"]),
        )
        if (
            document["approval_id"] != approval.approval_id
            or _canonical_bytes(document) != approval.content
        ):
            raise IdentityMarketInventoryExecutionApprovalError(
                "v2 execution approval does not reproduce"
            )
        return approval


def composite_inventory_execution_approval_path(approval_id: str) -> str:
    _digest(approval_id, "execution approval ID")
    return (
        "manifests/silver/identity/composite-inventory-execution-approvals/"
        f"approval_id={approval_id}/manifest.json"
    )


class IdentityMarketInventoryExecutionApprovalStore:
    """Strict content-addressed store for v2 execution approvals."""

    def __init__(self, data_root: Path) -> None:
        self.plan_store = IdentityMarketInventoryExecutionPlanStore(data_root)
        self.root = self.plan_store.root

    def store_approval(
        self,
        approval: S7CompositeInventoryExecutionApprovalV2,
    ) -> StoredInventoryExecutionDocument:
        if not isinstance(approval, S7CompositeInventoryExecutionApprovalV2):
            raise IdentityMarketInventoryExecutionApprovalError("execution approval has wrong type")
        request, _ = self.plan_store.load_execution_request_v2(
            approval.request_event_id,
            expected_sha256=approval.request_event_sha256,
        )
        plan, _ = self.plan_store.load_execution_plan_v2(
            approval.plan_id,
            expected_sha256=approval.plan_sha256,
        )
        _verify_approval_bindings(approval, request, plan)
        try:
            target = safe_relative_path(self.root, approval.relative_path)
            stored = write_bytes_immutable(self.root, target, approval.content)
        except ArtifactError as exc:
            raise IdentityMarketInventoryExecutionApprovalError(str(exc)) from exc
        return StoredInventoryExecutionDocument(
            path=str(stored["path"]),
            sha256=str(stored["sha256"]),
            bytes=int(stored["bytes"]),
        )

    def load_approval(
        self,
        approval_id: str,
        *,
        expected_sha256: str,
    ) -> tuple[
        S7CompositeInventoryExecutionApprovalV2,
        StoredInventoryExecutionDocument,
    ]:
        _digest(approval_id, "execution approval ID")
        _digest(expected_sha256, "execution approval expected SHA-256")
        relative = composite_inventory_execution_approval_path(approval_id)
        try:
            path = safe_relative_path(self.root, relative)
        except ArtifactError as exc:
            raise IdentityMarketInventoryExecutionApprovalError(str(exc)) from exc
        if not path.is_file() or path.is_symlink():
            raise IdentityMarketInventoryExecutionApprovalError(
                "exact execution approval is missing or unsafe"
            )
        content = path.read_bytes()
        if sha256_file(path) != expected_sha256:
            raise IdentityMarketInventoryExecutionApprovalError(
                "execution approval SHA-256 differs"
            )
        document = _decode_json(content, relative)
        if _canonical_bytes(document) != content:
            raise IdentityMarketInventoryExecutionApprovalError(
                "execution approval is not canonical JSON"
            )
        approval = S7CompositeInventoryExecutionApprovalV2.from_dict(document)
        if (
            approval.approval_id != approval_id
            or approval.sha256 != expected_sha256
            or approval.relative_path != relative
            or approval.content != content
        ):
            raise IdentityMarketInventoryExecutionApprovalError(
                "execution approval path/ID/SHA/content binding differs"
            )
        request, _ = self.plan_store.load_execution_request_v2(
            approval.request_event_id,
            expected_sha256=approval.request_event_sha256,
        )
        plan, _ = self.plan_store.load_execution_plan_v2(
            approval.plan_id,
            expected_sha256=approval.plan_sha256,
        )
        _verify_approval_bindings(approval, request, plan)
        return approval, StoredInventoryExecutionDocument(
            path=relative,
            sha256=expected_sha256,
            bytes=len(content),
        )


@dataclass(frozen=True, slots=True)
class S7CompositeInventoryExecutionApprovalRun:
    plan: S7CompositeInventoryExecutionPlanV2
    plan_document: StoredInventoryExecutionDocument
    request: S7CompositeInventoryExecutionRequestV2
    request_document: StoredInventoryExecutionDocument
    approval: S7CompositeInventoryExecutionApprovalV2
    approval_document: StoredInventoryExecutionDocument
    approval_document_preexisting: bool


def record_s7_composite_inventory_execution_approval(
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
) -> S7CompositeInventoryExecutionApprovalRun:
    """Record one exact approval receipt and perform none of the approved scan."""

    store = IdentityMarketInventoryExecutionApprovalStore(data_root)
    plan, plan_document = store.plan_store.load_execution_plan_v2(
        plan_id,
        expected_sha256=expected_plan_sha256,
    )
    request, request_document = store.plan_store.load_execution_request_v2(
        request_event_id,
        expected_sha256=expected_request_event_sha256,
    )
    if request.plan_id != plan.plan_id or request.plan_sha256 != plan.sha256:
        raise IdentityMarketInventoryExecutionApprovalError(
            "execution request crosses the supplied plan"
        )
    approval = S7CompositeInventoryExecutionApprovalV2.create(
        request,
        request_document,
        approval_literal=approval_literal,
        approved_by=approved_by,
        approved_at_utc=_parse_utc(approved_at, "approved_at"),
        approval_note=approval_note,
    )
    approval_path = safe_relative_path(store.root, approval.relative_path)
    preexisting = approval_path.exists()
    approval_document = store.store_approval(approval)
    loaded, loaded_document = store.load_approval(
        approval.approval_id,
        expected_sha256=approval.sha256,
    )
    if loaded != approval or loaded_document != approval_document:
        raise IdentityMarketInventoryExecutionApprovalError("execution approval readback differs")
    return S7CompositeInventoryExecutionApprovalRun(
        plan=plan,
        plan_document=plan_document,
        request=request,
        request_document=request_document,
        approval=approval,
        approval_document=approval_document,
        approval_document_preexisting=preexisting,
    )


def _verify_approval_bindings(
    approval: S7CompositeInventoryExecutionApprovalV2,
    request: S7CompositeInventoryExecutionRequestV2,
    plan: S7CompositeInventoryExecutionPlanV2,
) -> None:
    if (
        approval.request_event_id != request.request_event_id
        or approval.request_event_path != request.relative_path
        or approval.request_event_sha256 != request.sha256
        or approval.plan_id != plan.plan_id
        or approval.plan_path != plan.relative_path
        or approval.plan_sha256 != plan.sha256
        or approval.execution_data_root != plan.execution_data_root
        or approval.execution_data_root != request.execution_data_root
        or request.plan_id != plan.plan_id
        or request.plan_sha256 != plan.sha256
        or approval.input_binding_digest != request.input_binding_digest
        or approval.resource_caps_digest != request.resource_caps_digest
        or approval.runtime_file_set_digest != request.runtime_file_set_digest
        or approval.verification_file_set_digest != request.verification_file_set_digest
        or approval.inventory_contract_id != request.inventory_contract_id
        or approval.inventory_candidate_sha256 != request.inventory_candidate_sha256
        or approval.inventory_schema_digest != request.inventory_schema_digest
        or approval.algorithm_digest != request.algorithm_digest
        or approval.qa_semantics_digest != request.qa_semantics_digest
        or approval.blocked_event_id != request.blocked_event_id
        or approval.blocked_event_sha256 != request.blocked_event_sha256
        or approval.approval_literal != request.canonical_approval_literal
        or approval.approved_at_utc <= request.created_at_utc
    ):
        raise IdentityMarketInventoryExecutionApprovalError(
            "execution approval crosses request or plan bindings"
        )


__all__ = [
    "EXECUTION_APPROVAL_RULE_VERSION",
    "EXECUTION_APPROVAL_STAGE",
    "IdentityMarketInventoryExecutionApprovalError",
    "IdentityMarketInventoryExecutionApprovalStore",
    "S7CompositeInventoryExecutionApprovalRun",
    "S7CompositeInventoryExecutionApprovalV2",
    "composite_inventory_execution_approval_path",
    "record_s7_composite_inventory_execution_approval",
]
