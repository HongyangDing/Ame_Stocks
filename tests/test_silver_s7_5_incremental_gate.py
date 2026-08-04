from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.incremental_gate import (
    CORRECTION_AUTHORIZATION_LITERAL_VERSION,
    CorrectionAuthorization,
    CorrectionAuthorizedAction,
    GateArtifactPin,
    GateEvidencePin,
    IncrementalGateError,
    PinnedCorrectionAuthorization,
    QaCheckPolicy,
    QaCheckResult,
    QaPolicy,
    QaReceipt,
    QaSeverity,
    validate_correction_authorization,
    validate_qa_for_publish,
)

_APPROVAL_AVAILABLE_SESSION = date(2026, 8, 3)
_PUBLISH_CUTOFF_SESSION = date(2026, 8, 4)


def _digest(label: str) -> str:
    return stable_digest({"fixture": label})


def _qa_policy() -> QaPolicy:
    return QaPolicy(
        checks=(
            QaCheckPolicy(
                "critical_integrity",
                QaSeverity.CRITICAL,
                _digest("critical-integrity-semantics"),
                0,
            ),
            QaCheckPolicy(
                "high_identity",
                QaSeverity.HIGH,
                _digest("high-identity-semantics"),
                0,
            ),
            QaCheckPolicy(
                "info_metrics",
                QaSeverity.INFO,
                _digest("info-metrics-semantics"),
                0,
            ),
            QaCheckPolicy(
                "warning_review",
                QaSeverity.WARNING,
                _digest("warning-review-semantics"),
                0,
            ),
        )
    )


def _qa_result(
    check_id: str,
    *,
    semantics_digest: str | None = None,
    observed_count: int = 10,
    failure_count: int = 0,
) -> QaCheckResult:
    return QaCheckResult(
        check_id=check_id,
        semantics_digest=semantics_digest or _digest(f"qa-semantics-{check_id}"),
        observed_count=observed_count,
        failure_count=failure_count,
        details_artifact=GateArtifactPin(
            path=f"qa/{check_id}.json",
            sha256=_digest(f"qa-details-{check_id}-{failure_count}"),
            bytes=100 + observed_count,
        ),
    )


def _qa_receipt(
    policy: QaPolicy | None = None,
    *,
    source_binding_digest: str | None = None,
    change_set_digest: str | None = None,
    result_overrides: dict[str, tuple[int, int]] | None = None,
) -> QaReceipt:
    selected_policy = policy or _qa_policy()
    overrides = result_overrides or {}
    return QaReceipt(
        qa_policy_id=selected_policy.qa_policy_id,
        run_spec_id=_digest("run-spec"),
        source_binding_digest=source_binding_digest or _digest("source-binding"),
        change_set_digest=change_set_digest or _digest("change-set"),
        qa_available_session=_APPROVAL_AVAILABLE_SESSION,
        results=tuple(
            _qa_result(
                item.check_id,
                semantics_digest=item.semantics_digest,
                observed_count=overrides.get(item.check_id, (10, 0))[0],
                failure_count=overrides.get(item.check_id, (10, 0))[1],
            )
            for item in selected_policy.checks
        ),
    )


def _validate_qa(policy: QaPolicy, receipt: QaReceipt) -> None:
    validate_qa_for_publish(
        policy,
        receipt,
        run_spec_id=_digest("run-spec"),
        source_binding_digest=_digest("source-binding"),
        change_set_digest=_digest("change-set"),
        availability_cutoff_session=_PUBLISH_CUTOFF_SESSION,
    )


def _evidence(
    label: str,
    *,
    available_session: date = _APPROVAL_AVAILABLE_SESSION,
) -> GateEvidencePin:
    return GateEvidencePin(
        artifact=GateArtifactPin(
            path=f"evidence/{label}.json",
            sha256=_digest(f"evidence-{label}"),
            bytes=100 + len(label),
        ),
        available_session=available_session,
    )


def _authorization(**changes: object) -> CorrectionAuthorization:
    values: dict[str, object] = {
        "authorized_action": CorrectionAuthorizedAction.PUBLISH_EXACT_CORRECTION,
        "literal_version": CORRECTION_AUTHORIZATION_LITERAL_VERSION,
        "parent_release_id": _digest("parent-release"),
        "expected_change_set_digest": _digest("change-set"),
        "source_binding_digest": _digest("source-binding"),
        "schema_digest": _digest("schema"),
        "transform_semantics_digest": _digest("transform"),
        "calendar_digest": _digest("calendar"),
        "identity_policy_before_id": _digest("identity-policy-before"),
        "identity_policy_after_id": _digest("identity-policy-after"),
        "scope_digest": _digest("exact-correction-scope"),
        "approval_event_id": _digest("approval-event"),
        "approval_event_sha256": _digest("approval-event-bytes"),
        "approver_id": "joe",
        "approval_available_session": _APPROVAL_AVAILABLE_SESSION,
        "evidence_pins": (_evidence("openfigi"), _evidence("sec")),
    }
    values.update(changes)
    return CorrectionAuthorization(**values)  # type: ignore[arg-type]


def _pinned_authorization(
    authorization: CorrectionAuthorization | None = None,
) -> PinnedCorrectionAuthorization:
    return PinnedCorrectionAuthorization.freeze(
        authorization or _authorization(),
        path="control/correction-authorization.json",
    )


def _authorization_expectations() -> dict[str, object]:
    return {
        "parent_release_id": _digest("parent-release"),
        "change_set_digest": _digest("change-set"),
        "source_binding_digest": _digest("source-binding"),
        "schema_digest": _digest("schema"),
        "transform_semantics_digest": _digest("transform"),
        "calendar_digest": _digest("calendar"),
        "identity_policy_before_id": _digest("identity-policy-before"),
        "identity_policy_after_id": _digest("identity-policy-after"),
        "scope_digest": _digest("exact-correction-scope"),
        "availability_cutoff_session": _PUBLISH_CUTOFF_SESSION,
    }


def test_qa_policy_and_receipt_ids_bind_complete_structured_payloads() -> None:
    policy = _qa_policy()
    receipt = _qa_receipt(policy)

    assert policy.qa_policy_id == stable_digest(policy.logical_payload())
    assert receipt.qa_receipt_id == stable_digest(receipt.logical_payload())
    assert policy.to_dict()["qa_policy_id"] == policy.qa_policy_id
    assert receipt.to_dict()["qa_receipt_id"] == receipt.qa_receipt_id
    assert receipt.qa_policy_id == policy.qa_policy_id
    _validate_qa(policy, receipt)

    changed_policy = QaPolicy(
        checks=tuple(
            replace(item, severity=QaSeverity.WARNING) if item.check_id == "info_metrics" else item
            for item in policy.checks
        )
    )
    changed_receipt = replace(
        receipt,
        results=tuple(
            replace(item, observed_count=item.observed_count + 1)
            if item.check_id == "info_metrics"
            else item
            for item in receipt.results
        ),
    )

    assert changed_policy.qa_policy_id != policy.qa_policy_id
    assert changed_receipt.qa_receipt_id != receipt.qa_receipt_id
    semantics_changed = QaPolicy(
        checks=tuple(
            replace(item, semantics_digest=_digest("changed-check-semantics"))
            if item.check_id == "info_metrics"
            else item
            for item in policy.checks
        )
    )
    assert semantics_changed.qa_policy_id != policy.qa_policy_id

    artifact_changed = replace(
        receipt,
        results=tuple(
            replace(
                item,
                details_artifact=replace(
                    item.details_artifact,
                    sha256=_digest("changed-details-bytes"),
                ),
            )
            if item.check_id == "info_metrics"
            else item
            for item in receipt.results
        ),
    )
    assert artifact_changed.qa_receipt_id != receipt.qa_receipt_id


def test_qa_result_checker_semantics_must_match_frozen_policy() -> None:
    policy = _qa_policy()
    receipt = _qa_receipt(policy)
    mismatched = replace(
        receipt,
        results=tuple(
            replace(item, semantics_digest=_digest("wrong-checker-semantics"))
            if item.check_id == "high_identity"
            else item
            for item in receipt.results
        ),
    )

    with pytest.raises(IncrementalGateError, match="semantics differ"):
        _validate_qa(policy, mismatched)


def test_qa_details_are_one_exact_artifact_pin_not_a_second_free_digest() -> None:
    result = _qa_receipt().results[0]

    assert set(result.to_dict()) == {
        "check_id",
        "details_artifact",
        "failure_count",
        "observed_count",
        "semantics_digest",
    }
    with pytest.raises(IncrementalGateError, match="normalized relative path"):
        replace(
            result,
            details_artifact=replace(result.details_artifact, path="../qa.json"),
        )


@pytest.mark.parametrize(
    "check_id",
    ("critical_integrity", "high_identity", "warning_review"),
)
def test_qa_critical_high_and_warning_fail_closed(check_id: str) -> None:
    policy = _qa_policy()
    receipt = _qa_receipt(policy, result_overrides={check_id: (10, 1)})

    with pytest.raises(IncrementalGateError, match="blocking failures"):
        _validate_qa(policy, receipt)


def test_qa_info_nonzero_failure_count_is_nonblocking_metric() -> None:
    policy = _qa_policy()
    receipt = _qa_receipt(
        policy,
        result_overrides={"info_metrics": (7, 7)},
    )

    _validate_qa(policy, receipt)


def test_qa_contract_predefines_bounded_ordinary_warning_without_daily_waiver() -> None:
    base_policy = _qa_policy()
    policy = QaPolicy(
        checks=tuple(
            replace(item, max_publish_failure_count=2)
            if item.check_id == "warning_review"
            else item
            for item in base_policy.checks
        )
    )
    _validate_qa(
        policy,
        _qa_receipt(policy, result_overrides={"warning_review": (10, 2)}),
    )
    with pytest.raises(IncrementalGateError, match="blocking failures"):
        _validate_qa(
            policy,
            _qa_receipt(policy, result_overrides={"warning_review": (10, 3)}),
        )


def test_critical_and_high_policy_limits_are_always_zero() -> None:
    with pytest.raises(IncrementalGateError, match="require a zero"):
        replace(
            _qa_policy().checks[0],
            max_publish_failure_count=1,
        )


@pytest.mark.parametrize("omit_check_id", ("critical_integrity", "info_metrics"))
def test_qa_missing_required_check_fails_closed(omit_check_id: str) -> None:
    policy = _qa_policy()
    receipt = _qa_receipt(policy)
    incomplete = replace(
        receipt,
        results=tuple(item for item in receipt.results if item.check_id != omit_check_id),
    )

    with pytest.raises(IncrementalGateError, match="exact required check set"):
        _validate_qa(policy, incomplete)


def test_qa_extra_unapproved_check_fails_closed() -> None:
    policy = _qa_policy()
    receipt = _qa_receipt(policy)
    extra = replace(
        receipt,
        results=(*receipt.results, _qa_result("zz_unapproved_check")),
    )

    with pytest.raises(IncrementalGateError, match="exact required check set"):
        _validate_qa(policy, extra)


def test_qa_receipt_from_another_policy_fails_closed() -> None:
    policy = _qa_policy()
    receipt = replace(_qa_receipt(policy), qa_policy_id=_digest("forged-policy"))

    with pytest.raises(IncrementalGateError, match="another policy"):
        _validate_qa(policy, receipt)


def test_qa_receipt_from_another_run_or_future_session_fails_closed() -> None:
    policy = _qa_policy()
    with pytest.raises(IncrementalGateError, match="another run spec"):
        _validate_qa(
            policy,
            replace(_qa_receipt(policy), run_spec_id=_digest("other-run-spec")),
        )
    with pytest.raises(IncrementalGateError, match="unavailable at the run cutoff"):
        _validate_qa(
            policy,
            replace(
                _qa_receipt(policy),
                qa_available_session=date(2026, 8, 5),
            ),
        )


@pytest.mark.parametrize(
    ("field", "error"),
    (
        ("source_binding_digest", "source binding differs"),
        ("change_set_digest", "change set differs"),
    ),
)
def test_qa_source_and_change_set_forgery_fail_closed(
    field: str,
    error: str,
) -> None:
    policy = _qa_policy()
    forged = replace(_qa_receipt(policy), **{field: _digest(f"forged-{field}")})

    with pytest.raises(IncrementalGateError, match=error):
        _validate_qa(policy, forged)


def test_qa_policy_and_receipt_require_sorted_unique_complete_shapes() -> None:
    with pytest.raises(IncrementalGateError, match="sorted and unique"):
        QaPolicy(
            checks=(
                QaCheckPolicy(
                    "high_identity",
                    QaSeverity.HIGH,
                    _digest("high-identity-semantics"),
                    0,
                ),
                QaCheckPolicy(
                    "critical_integrity",
                    QaSeverity.CRITICAL,
                    _digest("critical-integrity-semantics"),
                    0,
                ),
            )
        )
    with pytest.raises(IncrementalGateError, match="Critical and High"):
        QaPolicy(
            checks=(
                QaCheckPolicy(
                    "critical_integrity",
                    QaSeverity.CRITICAL,
                    _digest("critical-integrity-semantics"),
                    0,
                ),
            )
        )

    receipt = _qa_receipt()
    with pytest.raises(IncrementalGateError, match="sorted and unique"):
        replace(receipt, results=tuple(reversed(receipt.results)))


def test_correction_authorization_freeze_is_an_exact_canonical_body_pin() -> None:
    authorization = _authorization()
    pinned = _pinned_authorization(authorization)

    assert pinned.authorization == authorization
    assert pinned.authorization.authorization_id == stable_digest(authorization.logical_payload())
    assert pinned.artifact.path == "control/correction-authorization.json"
    assert pinned.artifact.bytes > 0
    assert pinned.to_dict()["artifact"] == pinned.artifact.to_dict()
    validate_correction_authorization(pinned, **_authorization_expectations())

    for artifact in (
        replace(pinned.artifact, sha256=_digest("forged-body-sha")),
        replace(pinned.artifact, bytes=pinned.artifact.bytes + 1),
    ):
        with pytest.raises(IncrementalGateError, match="does not reproduce"):
            replace(pinned, artifact=artifact)


@pytest.mark.parametrize(
    ("expected_field", "error"),
    (
        ("parent_release_id", "parent release differs"),
        ("change_set_digest", "change set differs"),
        ("source_binding_digest", "source binding differs"),
        ("schema_digest", "schema differs"),
        ("transform_semantics_digest", "transform semantics differs"),
        ("calendar_digest", "calendar differs"),
        ("identity_policy_before_id", "prior identity policy differs"),
        ("identity_policy_after_id", "target identity policy differs"),
        ("scope_digest", "scope differs"),
    ),
)
def test_correction_authorization_exact_runtime_binding_fails_closed(
    expected_field: str,
    error: str,
) -> None:
    expectations = _authorization_expectations()
    expectations[expected_field] = _digest(f"forged-{expected_field}")

    with pytest.raises(IncrementalGateError, match=error):
        validate_correction_authorization(
            _pinned_authorization(),
            **expectations,
        )


@pytest.mark.parametrize(
    "authorization_change",
    (
        {"scope_digest": _digest("broadened-scope")},
        {"evidence_pins": (_evidence("different-evidence"),)},
        {"approval_event_id": _digest("different-approval")},
        {"approval_event_sha256": _digest("different-approval-bytes")},
        {"approver_id": "different_approver"},
    ),
)
def test_correction_scope_evidence_and_approval_cannot_change_under_existing_pin(
    authorization_change: dict[str, object],
) -> None:
    pinned = _pinned_authorization()
    altered = replace(pinned.authorization, **authorization_change)

    with pytest.raises(IncrementalGateError, match="does not reproduce"):
        replace(pinned, authorization=altered)


def test_refrozen_scope_or_evidence_change_produces_a_distinct_authority() -> None:
    original = _pinned_authorization()
    scope_changed = _pinned_authorization(
        replace(original.authorization, scope_digest=_digest("broadened-scope"))
    )
    evidence_changed = _pinned_authorization(
        replace(
            original.authorization,
            evidence_pins=(_evidence("different-evidence"),),
        )
    )

    assert scope_changed.authorization.authorization_id != (original.authorization.authorization_id)
    assert scope_changed.artifact.sha256 != original.artifact.sha256
    assert evidence_changed.authorization.authorization_id != (
        original.authorization.authorization_id
    )
    assert evidence_changed.artifact.sha256 != original.artifact.sha256


def test_correction_approval_availability_is_inclusive_and_cannot_be_backdated() -> None:
    pinned = _pinned_authorization()
    exact_cutoff = _authorization_expectations()
    exact_cutoff["availability_cutoff_session"] = _APPROVAL_AVAILABLE_SESSION

    validate_correction_authorization(pinned, **exact_cutoff)

    unavailable = _authorization_expectations()
    unavailable["availability_cutoff_session"] = date(2026, 8, 2)
    with pytest.raises(IncrementalGateError, match="unavailable at the run cutoff"):
        validate_correction_authorization(pinned, **unavailable)


def test_correction_literal_version_is_frozen() -> None:
    with pytest.raises(IncrementalGateError, match="frozen Gate A version"):
        _authorization(literal_version="s7_5_correction_authorization_literal_v2")


def test_correction_evidence_pins_must_be_nonempty_sorted_and_unique() -> None:
    with pytest.raises(IncrementalGateError, match="requires evidence"):
        _authorization(evidence_pins=())
    with pytest.raises(IncrementalGateError, match="sorted and unique"):
        _authorization(evidence_pins=(_evidence("sec"), _evidence("openfigi")))
    with pytest.raises(IncrementalGateError, match="sorted and unique"):
        _authorization(evidence_pins=(_evidence("sec"), _evidence("sec")))
    first = _evidence("same-path")
    second = replace(
        first,
        artifact=replace(first.artifact, sha256=_digest("different-evidence-bytes")),
    )
    with pytest.raises(IncrementalGateError, match="unique artifact paths"):
        _authorization(evidence_pins=(first, second))
    with pytest.raises(IncrementalGateError, match="exceeds approval availability"):
        _authorization(
            evidence_pins=(_evidence("future", available_session=_PUBLISH_CUTOFF_SESSION),)
        )
