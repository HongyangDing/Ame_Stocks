"""Structured publication gates for S7.5 incremental releases.

QA is an embedded run receipt, not an opaque digest.  Exceptional correction
authorization is one exact, content-addressed decision envelope.  Neither adds
a normal-path workflow layer: clean deltas have no authorization object, while
every release is evaluated against one explicit QA policy.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import PurePosixPath

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import SilverContractError

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_TOKEN = re.compile(r"^[a-z][a-z0-9_]*$")
CORRECTION_AUTHORIZATION_LITERAL_VERSION = "s7_5_correction_authorization_literal_v1"


class IncrementalGateError(SilverContractError):
    """Raised when QA or correction authority is incomplete or unsafe."""


class QaSeverity(StrEnum):
    """Closed severity policy for deterministic QA checks."""

    CRITICAL = "critical"
    HIGH = "high"
    WARNING = "warning"
    INFO = "info"


class CorrectionAuthorizedAction(StrEnum):
    """The only action granted by an S7.5 correction authorization."""

    PUBLISH_EXACT_CORRECTION = "publish_exact_s7_5_correction"


@dataclass(frozen=True, slots=True)
class GateArtifactPin:
    """Exact evidence/authorization artifact locator used by gate receipts."""

    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative_path(self.path, "gate artifact path")
        _digest(self.sha256, "gate artifact SHA-256")
        if type(self.bytes) is not int or self.bytes <= 0:
            raise IncrementalGateError("gate artifact bytes must be positive")

    def to_dict(self) -> dict[str, object]:
        return {"bytes": self.bytes, "path": self.path, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class GateEvidencePin:
    """Exact correction evidence plus when it became usable by the project."""

    artifact: GateArtifactPin
    available_session: date

    def __post_init__(self) -> None:
        if not isinstance(self.artifact, GateArtifactPin):
            raise IncrementalGateError("correction evidence artifact pin is invalid")
        _session(self.available_session, "correction evidence availability")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "available_session": self.available_session.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class QaCheckPolicy:
    """One required check and its non-overridable publish severity."""

    check_id: str
    severity: QaSeverity
    semantics_digest: str
    max_publish_failure_count: int

    def __post_init__(self) -> None:
        _token(self.check_id, "QA check ID")
        if not isinstance(self.severity, QaSeverity):
            raise IncrementalGateError("QA severity is invalid")
        _digest(self.semantics_digest, "QA check semantics digest")
        _nonnegative_int(
            self.max_publish_failure_count,
            "QA maximum publish failure count",
        )
        if (
            self.severity in {QaSeverity.CRITICAL, QaSeverity.HIGH}
            and self.max_publish_failure_count != 0
        ):
            raise IncrementalGateError(
                "Critical and High QA checks require a zero publish failure limit"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "check_id": self.check_id,
            "max_publish_failure_count": self.max_publish_failure_count,
            "semantics_digest": self.semantics_digest,
            "severity": self.severity.value,
        }


@dataclass(frozen=True, slots=True)
class QaPolicy:
    """Exact required QA check set for one release family."""

    checks: tuple[QaCheckPolicy, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.checks, tuple) or not self.checks:
            raise IncrementalGateError("QA policy requires at least one check")
        if any(not isinstance(item, QaCheckPolicy) for item in self.checks):
            raise IncrementalGateError("QA policy checks have invalid types")
        check_ids = [item.check_id for item in self.checks]
        if check_ids != sorted(set(check_ids)):
            raise IncrementalGateError("QA policy checks must be sorted and unique")
        severities = {item.severity for item in self.checks}
        if QaSeverity.CRITICAL not in severities or QaSeverity.HIGH not in severities:
            raise IncrementalGateError("QA policy requires Critical and High checks")

    @property
    def qa_policy_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "checks": [item.to_dict() for item in self.checks],
            "namespace": "ame_stocks.silver.incremental_qa_policy",
            "rule_version": "s7_5_incremental_qa_policy_v1",
        }

    def to_dict(self) -> dict[str, object]:
        return {"qa_policy_id": self.qa_policy_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True)
class QaCheckResult:
    """Observed and failed counts for one required QA check."""

    check_id: str
    semantics_digest: str
    observed_count: int
    failure_count: int
    details_artifact: GateArtifactPin

    def __post_init__(self) -> None:
        _token(self.check_id, "QA check ID")
        _digest(self.semantics_digest, "QA result semantics digest")
        _nonnegative_int(self.observed_count, "QA observed count")
        _nonnegative_int(self.failure_count, "QA failure count")
        if self.failure_count > self.observed_count:
            raise IncrementalGateError("QA failures cannot exceed observed rows")
        if not isinstance(self.details_artifact, GateArtifactPin):
            raise IncrementalGateError("QA details artifact pin is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "check_id": self.check_id,
            "details_artifact": self.details_artifact.to_dict(),
            "failure_count": self.failure_count,
            "observed_count": self.observed_count,
            "semantics_digest": self.semantics_digest,
        }


@dataclass(frozen=True, slots=True)
class QaReceipt:
    """Structured QA output bound to exact inputs and exact logical changes."""

    qa_policy_id: str
    run_spec_id: str
    source_binding_digest: str
    change_set_digest: str
    qa_available_session: date
    results: tuple[QaCheckResult, ...]

    def __post_init__(self) -> None:
        _digest(self.qa_policy_id, "QA policy ID")
        _digest(self.run_spec_id, "QA run-spec ID")
        _digest(self.source_binding_digest, "QA source-binding digest")
        _digest(self.change_set_digest, "QA change-set digest")
        _session(self.qa_available_session, "QA availability session")
        if not isinstance(self.results, tuple) or not self.results:
            raise IncrementalGateError("QA receipt requires check results")
        if any(not isinstance(item, QaCheckResult) for item in self.results):
            raise IncrementalGateError("QA receipt results have invalid types")
        check_ids = [item.check_id for item in self.results]
        if check_ids != sorted(set(check_ids)):
            raise IncrementalGateError("QA receipt results must be sorted and unique")

    @property
    def qa_receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "change_set_digest": self.change_set_digest,
            "qa_policy_id": self.qa_policy_id,
            "qa_available_session": self.qa_available_session.isoformat(),
            "results": [item.to_dict() for item in self.results],
            "run_spec_id": self.run_spec_id,
            "source_binding_digest": self.source_binding_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {"qa_receipt_id": self.qa_receipt_id, **self.logical_payload()}


def validate_qa_for_publish(
    policy: QaPolicy,
    receipt: QaReceipt,
    *,
    run_spec_id: str,
    source_binding_digest: str,
    change_set_digest: str,
    availability_cutoff_session: date,
) -> None:
    """Recompute the complete check set and publish verdict.

    Critical and High checks always have a zero limit. Warning checks may have
    one immutable, policy-defined limit, so an ordinary warning does not create
    a daily approval object. Informational observations are nonblocking. There
    is no per-run waiver flag.
    """

    if not isinstance(policy, QaPolicy) or not isinstance(receipt, QaReceipt):
        raise IncrementalGateError("QA publish validation requires policy and receipt")
    _digest(run_spec_id, "expected QA run-spec ID")
    _digest(source_binding_digest, "expected QA source-binding digest")
    _digest(change_set_digest, "expected QA change-set digest")
    if receipt.qa_policy_id != policy.qa_policy_id:
        raise IncrementalGateError("QA receipt belongs to another policy")
    if receipt.run_spec_id != run_spec_id:
        raise IncrementalGateError("QA receipt belongs to another run spec")
    if receipt.source_binding_digest != source_binding_digest:
        raise IncrementalGateError("QA receipt source binding differs")
    if receipt.change_set_digest != change_set_digest:
        raise IncrementalGateError("QA receipt change set differs")
    cutoff = _session(availability_cutoff_session, "QA availability cutoff session")
    if receipt.qa_available_session > cutoff:
        raise IncrementalGateError("QA receipt was unavailable at the run cutoff")
    policy_by_id = {item.check_id: item for item in policy.checks}
    result_by_id = {item.check_id: item for item in receipt.results}
    if set(result_by_id) != set(policy_by_id):
        raise IncrementalGateError("QA receipt does not cover the exact required check set")
    semantics_mismatch = sorted(
        check_id
        for check_id, result in result_by_id.items()
        if result.semantics_digest != policy_by_id[check_id].semantics_digest
    )
    if semantics_mismatch:
        raise IncrementalGateError(
            "QA result semantics differ from policy: " + ", ".join(semantics_mismatch)
        )
    blocking = [
        check_id
        for check_id, result in result_by_id.items()
        if policy_by_id[check_id].severity is not QaSeverity.INFO
        and result.failure_count > policy_by_id[check_id].max_publish_failure_count
    ]
    if blocking:
        raise IncrementalGateError(
            "QA publish gate has blocking failures: " + ", ".join(sorted(blocking))
        )


@dataclass(frozen=True, slots=True)
class CorrectionAuthorization:
    """Exact exceptional authority for one proposed correction change set."""

    authorized_action: CorrectionAuthorizedAction
    literal_version: str
    parent_release_id: str
    expected_change_set_digest: str
    source_binding_digest: str
    schema_digest: str
    transform_semantics_digest: str
    calendar_digest: str
    identity_policy_before_id: str
    identity_policy_after_id: str
    scope_digest: str
    approval_event_id: str
    approval_event_sha256: str
    approver_id: str
    approval_available_session: date
    evidence_pins: tuple[GateEvidencePin, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.authorized_action, CorrectionAuthorizedAction):
            raise IncrementalGateError("correction authorized action is invalid")
        _token(self.literal_version, "correction literal version")
        if self.literal_version != CORRECTION_AUTHORIZATION_LITERAL_VERSION:
            raise IncrementalGateError(
                "correction literal version is not the frozen Gate A version"
            )
        for value, label in (
            (self.parent_release_id, "correction parent release ID"),
            (self.expected_change_set_digest, "correction change-set digest"),
            (self.source_binding_digest, "correction source-binding digest"),
            (self.schema_digest, "correction schema digest"),
            (self.transform_semantics_digest, "correction transform digest"),
            (self.calendar_digest, "correction calendar digest"),
            (self.identity_policy_before_id, "correction prior identity-policy ID"),
            (self.identity_policy_after_id, "correction target identity-policy ID"),
            (self.scope_digest, "correction scope digest"),
            (self.approval_event_id, "correction approval event ID"),
            (self.approval_event_sha256, "correction approval event SHA-256"),
        ):
            _digest(value, label)
        _token(self.approver_id, "correction approver ID")
        _session(self.approval_available_session, "correction approval availability")
        if not isinstance(self.evidence_pins, tuple) or not self.evidence_pins:
            raise IncrementalGateError("correction authorization requires evidence pins")
        if any(not isinstance(item, GateEvidencePin) for item in self.evidence_pins):
            raise IncrementalGateError("correction evidence pins have invalid types")
        paths = [item.artifact.path for item in self.evidence_pins]
        if paths != sorted(set(paths)):
            raise IncrementalGateError(
                "correction evidence pins require sorted and unique artifact paths"
            )
        keys = [
            (
                item.artifact.path,
                item.artifact.sha256,
                item.artifact.bytes,
                item.available_session,
            )
            for item in self.evidence_pins
        ]
        if keys != sorted(set(keys)):
            raise IncrementalGateError("correction evidence pins must be sorted and unique")
        if any(
            item.available_session > self.approval_available_session for item in self.evidence_pins
        ):
            raise IncrementalGateError(
                "correction evidence availability exceeds approval availability"
            )

    @property
    def authorization_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_available_session": self.approval_available_session.isoformat(),
            "approval_event_id": self.approval_event_id,
            "approval_event_sha256": self.approval_event_sha256,
            "approver_id": self.approver_id,
            "authorized_action": self.authorized_action.value,
            "calendar_digest": self.calendar_digest,
            "evidence_pins": [item.to_dict() for item in self.evidence_pins],
            "expected_change_set_digest": self.expected_change_set_digest,
            "identity_policy_after_id": self.identity_policy_after_id,
            "identity_policy_before_id": self.identity_policy_before_id,
            "literal_version": self.literal_version,
            "parent_release_id": self.parent_release_id,
            "schema_digest": self.schema_digest,
            "scope_digest": self.scope_digest,
            "source_binding_digest": self.source_binding_digest,
            "transform_semantics_digest": self.transform_semantics_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {"authorization_id": self.authorization_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True)
class PinnedCorrectionAuthorization:
    """Authorization body plus an exact immutable body pin."""

    authorization: CorrectionAuthorization
    artifact: GateArtifactPin

    def __post_init__(self) -> None:
        if not isinstance(self.authorization, CorrectionAuthorization):
            raise IncrementalGateError("correction authorization body is invalid")
        if not isinstance(self.artifact, GateArtifactPin):
            raise IncrementalGateError("correction authorization artifact is invalid")
        content = _canonical_json_bytes(self.authorization.to_dict())
        if self.artifact.sha256 != hashlib.sha256(content).hexdigest():
            raise IncrementalGateError("correction authorization SHA-256 does not reproduce")
        if self.artifact.bytes != len(content):
            raise IncrementalGateError("correction authorization byte count does not reproduce")

    @classmethod
    def freeze(
        cls,
        authorization: CorrectionAuthorization,
        *,
        path: str,
    ) -> PinnedCorrectionAuthorization:
        """Canonical-serialize one authorization and bind its exact locator."""

        if not isinstance(authorization, CorrectionAuthorization):
            raise IncrementalGateError("correction authorization body is invalid")
        content = _canonical_json_bytes(authorization.to_dict())
        return cls(
            authorization=authorization,
            artifact=GateArtifactPin(
                path=path,
                sha256=hashlib.sha256(content).hexdigest(),
                bytes=len(content),
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "authorization": self.authorization.to_dict(),
        }


def validate_correction_authorization(
    pinned: PinnedCorrectionAuthorization,
    *,
    parent_release_id: str,
    change_set_digest: str,
    source_binding_digest: str,
    schema_digest: str,
    transform_semantics_digest: str,
    calendar_digest: str,
    identity_policy_before_id: str,
    identity_policy_after_id: str,
    scope_digest: str,
    availability_cutoff_session: date,
) -> None:
    """Verify an exact structural correction candidate.

    This does not attest that the referenced approval event occurred. Gate A
    therefore keeps correction reader capability disabled until I2/I3 verifies
    the event in a trusted append-only ledger.
    """

    if not isinstance(pinned, PinnedCorrectionAuthorization):
        raise IncrementalGateError("correction authorization pin is invalid")
    authorization = pinned.authorization
    expected = (
        (authorization.parent_release_id, parent_release_id, "parent release"),
        (authorization.expected_change_set_digest, change_set_digest, "change set"),
        (authorization.source_binding_digest, source_binding_digest, "source binding"),
        (authorization.schema_digest, schema_digest, "schema"),
        (
            authorization.transform_semantics_digest,
            transform_semantics_digest,
            "transform semantics",
        ),
        (authorization.calendar_digest, calendar_digest, "calendar"),
        (
            authorization.identity_policy_before_id,
            identity_policy_before_id,
            "prior identity policy",
        ),
        (
            authorization.identity_policy_after_id,
            identity_policy_after_id,
            "target identity policy",
        ),
        (authorization.scope_digest, scope_digest, "scope"),
    )
    for actual, required, label in expected:
        if actual != required:
            raise IncrementalGateError(f"correction authorization {label} differs")
    cutoff = _session(availability_cutoff_session, "availability cutoff session")
    if authorization.approval_available_session > cutoff:
        raise IncrementalGateError("correction authorization was unavailable at the run cutoff")


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise IncrementalGateError(f"{label} must be a lowercase SHA-256")
    return value


def _token(value: object, label: str) -> str:
    if not isinstance(value, str) or _TOKEN.fullmatch(value) is None:
        raise IncrementalGateError(f"{label} must be a lowercase token")
    return value


def _relative_path(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise IncrementalGateError(f"{label} must be trimmed nonempty text")
    path = PurePosixPath(value)
    if path.is_absolute() or path.as_posix() != value or ".." in path.parts:
        raise IncrementalGateError(f"{label} must be a normalized relative path")
    return value


def _session(value: object, label: str) -> date:
    if type(value) is not date:
        raise IncrementalGateError(f"{label} must be a date")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IncrementalGateError(f"{label} must be a nonnegative integer")
    return value
