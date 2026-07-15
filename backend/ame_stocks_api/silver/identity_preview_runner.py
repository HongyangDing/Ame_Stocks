"""Source-bound, bounded S7 detector preview runner.

The public entry accepts only an exact data root plus plan/approval ID/SHA pairs.
It cannot accept caller-provided rows, bundles, artifacts, or corroboration.  Outputs
remain ``awaiting_review`` and are structurally incapable of entering the production
candidate, adjudication, derived-table, full-run, or publish paths.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Final

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.calendar_artifact import (
    XNYSCalendarArtifact,
    XNYSCalendarArtifactError,
    load_xnys_calendar_artifact,
)
from ame_stocks_api.silver.identity_bounce import BounceCase, IdentityObservation, SourceSession
from ame_stocks_api.silver.identity_preview_plan import (
    IdentityPreviewPlanError,
    IdentityPreviewPlanStore,
    S7DetectorPreviewPlan,
    S7DetectorPreviewPlanApproval,
    S7TickerAllowlist,
)
from ame_stocks_api.silver.identity_provider_evidence import (
    ProviderEvidenceError,
    ProviderRowAttestation,
    S4BounceProviderEvidenceManifest,
    _build_s4_bounce_provider_evidence_manifest_for_runner,
    _issue_runner_evidence_authority,
    _ProviderReplaySession,
    _rebuild_s4_bounce_provider_evidence_manifest_for_completion,
    _RunnerEvidenceAuthority,
    _write_s4_bounce_provider_evidence_manifest_from_official_bundle,
    attest_provider_rows,
    build_s4_bounce_case_evidence_usage,
    validate_s4_observation_parent_pair,
)
from ame_stocks_api.silver.identity_source import (
    S7_S4_RELEASE_SET_ID,
    S7_S4_RELEASE_SET_MANIFEST_SHA256,
    S7_SIX_RELEASE_BINDING_ID,
    IdentitySourceArtifact,
    IdentitySourceBundle,
    IdentitySourceError,
    open_approved_identity_preview_source_bundle,
)
from ame_stocks_api.silver.identity_streaming_preview import (
    BoundedIdentityPreviewArtifact,
    BoundedIdentityPreviewEngine,
    BoundedIdentityPreviewLimits,
    IdentityStreamingPreviewError,
    build_bounded_identity_preview_artifact,
    read_bounded_identity_preview_artifact,
    write_bounded_identity_preview_artifact,
)

S7_DETECTOR_PREVIEW_COMPLETION_SCHEMA_VERSION: Final = 1
S7_DETECTOR_PREVIEW_COMPLETION_RULE_VERSION: Final = (
    "s7_source_bound_detector_preview_completion_v1"
)
S7_DETECTOR_PREVIEW_COMPLETION_SCOPE: Final = (
    "bounded_s4_source_attested_preview_no_corroboration_no_candidate_no_adjudication"
)
S7_DETECTOR_PREVIEW_SOURCE_ATTESTATION_SCOPE: Final = (
    "s4_asset_observation_and_universe_membership_only"
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")


class IdentityPreviewRunnerError(RuntimeError):
    """Raised before completion when an exact bounded preview cannot be trusted."""


@dataclass(frozen=True, slots=True, order=True)
class PreviewSourceArtifactRef:
    """One selected daily DATA artifact included in the bounded physical scan."""

    table: str
    session_date: date
    path: str
    sha256: str
    bytes: int
    row_count: int

    def __post_init__(self) -> None:
        if self.table not in {"asset_observation_daily", "universe_source_daily"}:
            raise IdentityPreviewRunnerError("preview source artifact table is outside S4")
        _native_date(self.session_date, "source artifact session")
        _relative_path(self.path, "source artifact path")
        _digest(self.sha256, "source artifact SHA-256")
        _positive_int(self.bytes, "source artifact bytes")
        _nonnegative_int(self.row_count, "source artifact row count")

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "path": self.path,
            "row_count": self.row_count,
            "session_date": self.session_date.isoformat(),
            "sha256": self.sha256,
            "table": self.table,
        }

    @classmethod
    def from_dict(cls, value: object) -> PreviewSourceArtifactRef:
        item = _exact_mapping(
            value,
            {"bytes", "path", "row_count", "session_date", "sha256", "table"},
            "preview source artifact",
        )
        return cls(
            table=_string(item, "table"),
            session_date=_parse_date(_string(item, "session_date"), "session_date"),
            path=_string(item, "path"),
            sha256=_string(item, "sha256"),
            bytes=_positive_int(item["bytes"], "source artifact bytes"),
            row_count=_nonnegative_int(item["row_count"], "source artifact row count"),
        )


@dataclass(frozen=True, slots=True, order=True)
class PreviewCaseEvidenceRef:
    """One exact physically replayable evidence manifest for one preview case."""

    identity_case_id: str
    manifest_id: str
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _digest(self.identity_case_id, "identity case ID")
        _digest(self.manifest_id, "case evidence manifest ID")
        _relative_path(self.path, "case evidence path")
        _digest(self.sha256, "case evidence SHA-256")
        _positive_int(self.bytes, "case evidence bytes")

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "identity_case_id": self.identity_case_id,
            "manifest_id": self.manifest_id,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> PreviewCaseEvidenceRef:
        item = _exact_mapping(
            value,
            {"bytes", "identity_case_id", "manifest_id", "path", "sha256"},
            "preview case evidence ref",
        )
        return cls(
            identity_case_id=_string(item, "identity_case_id"),
            manifest_id=_string(item, "manifest_id"),
            path=_string(item, "path"),
            sha256=_string(item, "sha256"),
            bytes=_positive_int(item["bytes"], "case evidence bytes"),
        )


@dataclass(frozen=True, slots=True)
class S7DetectorPreviewCompletion:
    """Immutable terminal artifact for one review-only, source-attested preview."""

    plan_id: str
    plan_sha256: str
    approval_id: str
    approval_sha256: str
    request_event_id: str
    request_event_sha256: str
    git_commit: str
    calendar_artifact_id: str
    calendar_artifact_sha256: str
    ticker_allowlist_id: str
    ticker_allowlist_sha256: str
    start_session: date
    end_session: date
    session_count: int
    ticker_count: int
    source_artifacts: tuple[PreviewSourceArtifactRef, ...]
    preview_artifact_id: str
    preview_artifact_path: str
    preview_artifact_sha256: str
    preview_artifact_bytes: int
    case_evidence: tuple[PreviewCaseEvidenceRef, ...]
    selected_observation_count: int
    valid_active_observation_count: int
    scanned_row_count: int
    scanned_artifact_count: int
    scanned_bytes: int
    case_count: int
    suspected_provider_figi_bounce_rows: int
    created_at_utc: datetime
    completion_available_session: date
    six_release_binding_id: str = S7_SIX_RELEASE_BINDING_ID
    s4_release_set_id: str = S7_S4_RELEASE_SET_ID
    s4_release_set_manifest_sha256: str = S7_S4_RELEASE_SET_MANIFEST_SHA256
    status: str = field(default="awaiting_review", init=False)
    scope_kind: str = field(default=S7_DETECTOR_PREVIEW_COMPLETION_SCOPE, init=False)
    source_attested: bool = field(default=True, init=False)
    source_attestation_scope: str = field(
        default=S7_DETECTOR_PREVIEW_SOURCE_ATTESTATION_SCOPE,
        init=False,
    )
    corroboration_evaluation_state: str = field(default="not_evaluated", init=False)
    support_absence_verified: bool = field(default=False, init=False)
    adjudication_eligible: bool = field(default=False, init=False)
    canonical_candidate_eligible: bool = field(default=False, init=False)
    backtest_identity_eligible: bool = field(default=False, init=False)
    publication_eligible: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("approval ID", self.approval_id),
            ("approval SHA-256", self.approval_sha256),
            ("request event ID", self.request_event_id),
            ("request event SHA-256", self.request_event_sha256),
            ("calendar artifact ID", self.calendar_artifact_id),
            ("calendar artifact SHA-256", self.calendar_artifact_sha256),
            ("ticker allowlist ID", self.ticker_allowlist_id),
            ("ticker allowlist SHA-256", self.ticker_allowlist_sha256),
            ("preview artifact ID", self.preview_artifact_id),
            ("preview artifact SHA-256", self.preview_artifact_sha256),
        ):
            _digest(value, label)
        if not _GIT_COMMIT.fullmatch(self.git_commit):
            raise IdentityPreviewRunnerError("completion Git commit is invalid")
        if self.six_release_binding_id != S7_SIX_RELEASE_BINDING_ID:
            raise IdentityPreviewRunnerError("completion source binding changed")
        if (
            self.s4_release_set_id != S7_S4_RELEASE_SET_ID
            or self.s4_release_set_manifest_sha256 != S7_S4_RELEASE_SET_MANIFEST_SHA256
        ):
            raise IdentityPreviewRunnerError("completion S4 release-set binding changed")
        _native_date(self.start_session, "start session")
        _native_date(self.end_session, "end session")
        if self.start_session > self.end_session:
            raise IdentityPreviewRunnerError("completion session range is reversed")
        _positive_int(self.session_count, "session count")
        _positive_int(self.ticker_count, "ticker count")
        _relative_path(self.preview_artifact_path, "preview artifact path")
        expected_preview_path = (
            "manifests/silver/identity-bounce-bounded-previews/"
            f"preview_artifact_id={self.preview_artifact_id}/manifest.json"
        )
        if self.preview_artifact_path != expected_preview_path:
            raise IdentityPreviewRunnerError("completion preview path is not canonical")
        _positive_int(self.preview_artifact_bytes, "preview artifact bytes")
        source = tuple(sorted(self.source_artifacts))
        evidence = tuple(sorted(self.case_evidence))
        if not source or len(source) != self.session_count * 2:
            raise IdentityPreviewRunnerError("completion source artifact scope is incomplete")
        if len({(item.table, item.session_date) for item in source}) != len(source):
            raise IdentityPreviewRunnerError("completion repeats a daily source artifact")
        if len({item.path for item in source}) != len(source):
            raise IdentityPreviewRunnerError("completion repeats a source artifact path")
        if len({item.identity_case_id for item in evidence}) != len(evidence):
            raise IdentityPreviewRunnerError("completion repeats case evidence")
        object.__setattr__(self, "source_artifacts", source)
        object.__setattr__(self, "case_evidence", evidence)
        for label, value in (
            ("selected observation count", self.selected_observation_count),
            ("valid active observation count", self.valid_active_observation_count),
            ("scanned row count", self.scanned_row_count),
            ("scanned artifact count", self.scanned_artifact_count),
            ("scanned bytes", self.scanned_bytes),
            ("case count", self.case_count),
            ("suspected provider FIGI bounce rows", self.suspected_provider_figi_bounce_rows),
        ):
            _nonnegative_int(value, label)
        if (
            self.valid_active_observation_count > self.selected_observation_count
            or self.selected_observation_count > self.scanned_row_count
            or self.scanned_artifact_count != len(source)
            or self.scanned_row_count != sum(item.row_count for item in source)
            or self.scanned_bytes != sum(item.bytes for item in source)
            or self.case_count != len(evidence)
        ):
            raise IdentityPreviewRunnerError("completion counts do not reproduce")
        object.__setattr__(self, "created_at_utc", _utc_datetime(self.created_at_utc))
        _native_date(self.completion_available_session, "completion available session")
        if (
            self.status != "awaiting_review"
            or self.scope_kind != S7_DETECTOR_PREVIEW_COMPLETION_SCOPE
            or self.source_attested is not True
            or self.source_attestation_scope != S7_DETECTOR_PREVIEW_SOURCE_ATTESTATION_SCOPE
            or self.corroboration_evaluation_state != "not_evaluated"
            or self.support_absence_verified is not False
            or self.adjudication_eligible is not False
            or self.canonical_candidate_eligible is not False
            or self.backtest_identity_eligible is not False
            or self.publication_eligible is not False
        ):
            raise IdentityPreviewRunnerError("completion fail-closed markers changed")

    @property
    def source_artifact_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.source_artifacts])

    @property
    def case_evidence_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.case_evidence])

    def logical_payload(self) -> dict[str, object]:
        return {
            "adjudication_eligible": False,
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "artifact_type": "s7_source_bound_detector_preview_completion",
            "backtest_identity_eligible": False,
            "calendar_artifact_id": self.calendar_artifact_id,
            "calendar_artifact_sha256": self.calendar_artifact_sha256,
            "canonical_candidate_eligible": False,
            "case_evidence": [item.to_dict() for item in self.case_evidence],
            "case_evidence_set_digest": self.case_evidence_set_digest,
            "completion_available_session": self.completion_available_session.isoformat(),
            "corroboration_evaluation_state": "not_evaluated",
            "created_at_utc": _utc_text(self.created_at_utc),
            "git_commit": self.git_commit,
            "input_summary": {
                "case_count": self.case_count,
                "scanned_artifact_count": self.scanned_artifact_count,
                "scanned_bytes": self.scanned_bytes,
                "scanned_row_count": self.scanned_row_count,
                "selected_observation_count": self.selected_observation_count,
                "valid_active_observation_count": self.valid_active_observation_count,
            },
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "preview_artifact_bytes": self.preview_artifact_bytes,
            "preview_artifact_id": self.preview_artifact_id,
            "preview_artifact_path": self.preview_artifact_path,
            "preview_artifact_sha256": self.preview_artifact_sha256,
            "publication_eligible": False,
            "qa": {
                "suspected_provider_contamination_eligible_rows": 0,
                "suspected_provider_figi_bounce_rows": (self.suspected_provider_figi_bounce_rows),
                "unapproved_canonical_identity_override_rows": 0,
            },
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "rule_version": S7_DETECTOR_PREVIEW_COMPLETION_RULE_VERSION,
            "s4_release_set_id": self.s4_release_set_id,
            "s4_release_set_manifest_sha256": self.s4_release_set_manifest_sha256,
            "schema_version": S7_DETECTOR_PREVIEW_COMPLETION_SCHEMA_VERSION,
            "scope": {
                "end_session": self.end_session.isoformat(),
                "session_count": self.session_count,
                "start_session": self.start_session.isoformat(),
                "ticker_count": self.ticker_count,
            },
            "scope_kind": self.scope_kind,
            "six_release_binding_id": self.six_release_binding_id,
            "source_artifact_set_digest": self.source_artifact_set_digest,
            "source_artifacts": [item.to_dict() for item in self.source_artifacts],
            "source_attested": True,
            "source_attestation_scope": self.source_attestation_scope,
            "status": "awaiting_review",
            "support_absence_verified": False,
            "ticker_allowlist_id": self.ticker_allowlist_id,
            "ticker_allowlist_sha256": self.ticker_allowlist_sha256,
        }

    @property
    def completion_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "completion_id": self.completion_id})

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return detector_preview_completion_path(self.plan_id, self.approval_id)

    @classmethod
    def from_dict(cls, value: object) -> S7DetectorPreviewCompletion:
        item = _exact_mapping(
            value,
            {
                "adjudication_eligible",
                "approval_id",
                "approval_sha256",
                "artifact_type",
                "backtest_identity_eligible",
                "calendar_artifact_id",
                "calendar_artifact_sha256",
                "canonical_candidate_eligible",
                "case_evidence",
                "case_evidence_set_digest",
                "completion_available_session",
                "completion_id",
                "corroboration_evaluation_state",
                "created_at_utc",
                "git_commit",
                "input_summary",
                "plan_id",
                "plan_sha256",
                "preview_artifact_bytes",
                "preview_artifact_id",
                "preview_artifact_path",
                "preview_artifact_sha256",
                "publication_eligible",
                "qa",
                "request_event_id",
                "request_event_sha256",
                "rule_version",
                "s4_release_set_id",
                "s4_release_set_manifest_sha256",
                "schema_version",
                "scope",
                "scope_kind",
                "six_release_binding_id",
                "source_artifact_set_digest",
                "source_artifacts",
                "source_attested",
                "source_attestation_scope",
                "status",
                "support_absence_verified",
                "ticker_allowlist_id",
                "ticker_allowlist_sha256",
            },
            "S7 detector preview completion",
        )
        if (
            item["artifact_type"] != "s7_source_bound_detector_preview_completion"
            or item["schema_version"] != S7_DETECTOR_PREVIEW_COMPLETION_SCHEMA_VERSION
            or item["rule_version"] != S7_DETECTOR_PREVIEW_COMPLETION_RULE_VERSION
        ):
            raise IdentityPreviewRunnerError("completion fixed fields are invalid")
        summary = _exact_mapping(
            item["input_summary"],
            {
                "case_count",
                "scanned_artifact_count",
                "scanned_bytes",
                "scanned_row_count",
                "selected_observation_count",
                "valid_active_observation_count",
            },
            "completion input summary",
        )
        scope = _exact_mapping(
            item["scope"],
            {"end_session", "session_count", "start_session", "ticker_count"},
            "completion scope",
        )
        qa = _exact_mapping(
            item["qa"],
            {
                "suspected_provider_contamination_eligible_rows",
                "suspected_provider_figi_bounce_rows",
                "unapproved_canonical_identity_override_rows",
            },
            "completion QA",
        )
        if (
            qa["suspected_provider_contamination_eligible_rows"] != 0
            or qa["unapproved_canonical_identity_override_rows"] != 0
        ):
            raise IdentityPreviewRunnerError("completion Critical QA is nonzero")
        source_raw = _array(item["source_artifacts"], "source artifacts")
        evidence_raw = _array(item["case_evidence"], "case evidence")
        completion = cls(
            plan_id=_string(item, "plan_id"),
            plan_sha256=_string(item, "plan_sha256"),
            approval_id=_string(item, "approval_id"),
            approval_sha256=_string(item, "approval_sha256"),
            request_event_id=_string(item, "request_event_id"),
            request_event_sha256=_string(item, "request_event_sha256"),
            git_commit=_string(item, "git_commit"),
            calendar_artifact_id=_string(item, "calendar_artifact_id"),
            calendar_artifact_sha256=_string(item, "calendar_artifact_sha256"),
            ticker_allowlist_id=_string(item, "ticker_allowlist_id"),
            ticker_allowlist_sha256=_string(item, "ticker_allowlist_sha256"),
            start_session=_parse_date(_string(scope, "start_session"), "start_session"),
            end_session=_parse_date(_string(scope, "end_session"), "end_session"),
            session_count=_positive_int(scope["session_count"], "session count"),
            ticker_count=_positive_int(scope["ticker_count"], "ticker count"),
            source_artifacts=tuple(PreviewSourceArtifactRef.from_dict(row) for row in source_raw),
            preview_artifact_id=_string(item, "preview_artifact_id"),
            preview_artifact_path=_string(item, "preview_artifact_path"),
            preview_artifact_sha256=_string(item, "preview_artifact_sha256"),
            preview_artifact_bytes=_positive_int(
                item["preview_artifact_bytes"], "preview artifact bytes"
            ),
            case_evidence=tuple(PreviewCaseEvidenceRef.from_dict(row) for row in evidence_raw),
            selected_observation_count=_nonnegative_int(
                summary["selected_observation_count"], "selected observation count"
            ),
            valid_active_observation_count=_nonnegative_int(
                summary["valid_active_observation_count"], "valid active observation count"
            ),
            scanned_row_count=_nonnegative_int(summary["scanned_row_count"], "scanned row count"),
            scanned_artifact_count=_nonnegative_int(
                summary["scanned_artifact_count"], "scanned artifact count"
            ),
            scanned_bytes=_nonnegative_int(summary["scanned_bytes"], "scanned bytes"),
            case_count=_nonnegative_int(summary["case_count"], "case count"),
            suspected_provider_figi_bounce_rows=_nonnegative_int(
                qa["suspected_provider_figi_bounce_rows"],
                "suspected provider FIGI bounce rows",
            ),
            created_at_utc=_parse_utc(_string(item, "created_at_utc")),
            completion_available_session=_parse_date(
                _string(item, "completion_available_session"),
                "completion_available_session",
            ),
            six_release_binding_id=_string(item, "six_release_binding_id"),
            s4_release_set_id=_string(item, "s4_release_set_id"),
            s4_release_set_manifest_sha256=_string(item, "s4_release_set_manifest_sha256"),
        )
        expected = completion.to_dict()
        if dict(item) != expected:
            raise IdentityPreviewRunnerError("completion canonical fields do not reproduce")
        return completion

    def to_dict(self) -> dict[str, object]:
        return dict(self.document)


@dataclass(frozen=True, slots=True)
class _LoadedControls:
    plan: S7DetectorPreviewPlan
    approval: S7DetectorPreviewPlanApproval
    allowlist: S7TickerAllowlist
    calendar: XNYSCalendarArtifact
    sessions: tuple[date, ...]


def detector_preview_completion_path(plan_id: str, approval_id: str) -> str:
    _digest(plan_id, "plan ID")
    _digest(approval_id, "approval ID")
    return (
        "manifests/silver/identity/detector-preview-completions/"
        f"plan_id={plan_id}/approval_id={approval_id}/manifest.json"
    )


def run_source_bound_identity_streaming_preview(
    data_root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> S7DetectorPreviewCompletion:
    """Execute only one exact approved bounded preview and stop at review."""

    if not isinstance(data_root, Path):
        raise IdentityPreviewRunnerError("source-bound preview data_root must be a Path")
    for label, value in (
        ("plan ID", plan_id),
        ("plan SHA-256", expected_plan_sha256),
        ("approval ID", approval_id),
        ("approval SHA-256", expected_approval_sha256),
    ):
        _digest(value, label)
    root = data_root.expanduser().resolve()
    if not root.is_dir() or root.is_symlink():
        raise IdentityPreviewRunnerError("source-bound preview data_root is unsafe")
    controls = _load_controls(
        root,
        plan_id=plan_id,
        expected_plan_sha256=expected_plan_sha256,
        approval_id=approval_id,
        expected_approval_sha256=expected_approval_sha256,
    )
    _verify_git_checkout(controls.plan.git_commit)
    existing = _completion_file(root, plan_id, approval_id)
    if existing.is_file():
        return _read_and_revalidate_completion(root, existing, controls)
    if existing.exists() or existing.is_symlink():
        raise IdentityPreviewRunnerError("completion slot is not a safe regular file")

    try:
        bundle = open_approved_identity_preview_source_bundle(
            root,
            plan_id=controls.plan.plan_id,
            expected_plan_sha256=controls.plan.sha256,
            approval_id=controls.approval.approval_id,
            expected_approval_sha256=controls.approval.sha256,
        )
        bundle.require_official()
        bundle.require_approved_preview_scope(
            plan_id=controls.plan.plan_id,
            plan_sha256=controls.plan.sha256,
            approval_id=controls.approval.approval_id,
            approval_sha256=controls.approval.sha256,
            sessions=controls.sessions,
        )
    except (IdentitySourceError, OSError) as exc:
        raise IdentityPreviewRunnerError("exact S7 source bundle cannot be opened") from exc
    try:
        asset_artifacts = bundle.daily_partition_artifacts(
            "asset_observation_daily", controls.sessions
        )
        universe_artifacts = bundle.daily_partition_artifacts(
            "universe_source_daily", controls.sessions
        )
    except IdentitySourceError as exc:
        raise IdentityPreviewRunnerError("exact daily S4 source scope cannot be selected") from exc
    source_refs = _preflight_source_scope(
        controls.plan,
        controls.sessions,
        asset_artifacts,
        universe_artifacts,
    )
    try:
        preview, pairs, created_at = _scan_preview(
            bundle,
            controls,
            asset_artifacts=asset_artifacts,
            universe_artifacts=universe_artifacts,
            source_refs=source_refs,
            preview_available_session=None,
        )
    except (IdentitySourceError, ProviderEvidenceError, IdentityStreamingPreviewError) as exc:
        raise IdentityPreviewRunnerError("bounded physical S4 scan failed") from exc
    if created_at is None or created_at < controls.approval.approved_at_utc:
        raise IdentityPreviewRunnerError("preview finalization predates its exact approval")
    cases = _preview_cases(preview)
    case_attestations = _case_attestations(cases, pairs)
    result = _mapping(preview.document["result"], "bounded preview result")
    completion_available = _parse_date(
        _string(result, "preview_manifest_available_session"),
        "preview manifest availability",
    )
    try:
        evidence_authority = _issue_runner_evidence_authority(
            data_root=root,
            bundle=bundle,
            plan=controls.plan,
            approval=controls.approval,
            calendar=controls.calendar,
            created_at_utc=created_at,
        )
    except ProviderEvidenceError as exc:
        raise IdentityPreviewRunnerError("live evidence authority cannot be issued") from exc
    try:
        preview_stored = write_bounded_identity_preview_artifact(root, preview)
    except IdentityStreamingPreviewError as exc:
        raise IdentityPreviewRunnerError("bounded preview artifact cannot be stored") from exc

    case_refs: list[PreviewCaseEvidenceRef] = []
    replay_session = _ProviderReplaySession(bundle=bundle, calendar=controls.calendar)
    if case_attestations:
        replay_session.replay(case_attestations)
    for case in cases:
        record_ids = (
            case.left_outer_source_record_id,
            *case.episode_source_record_ids,
            case.right_outer_source_record_id,
        )
        try:
            selected_pairs = tuple(pairs[item] for item in record_ids)
        except KeyError as exc:
            raise IdentityPreviewRunnerError(
                "detected case is missing a physically attested S4 pair"
            ) from exc
        usages = tuple(
            build_s4_bounce_case_evidence_usage(
                case,
                plan=controls.plan,
                preview=preview,
                asset_observation=asset,
                universe_membership=universe,
                calendar=controls.calendar,
            )
            for asset, universe in selected_pairs
        )
        attestations = tuple(item for pair in selected_pairs for item in pair)
        try:
            manifest = _build_s4_bounce_provider_evidence_manifest_for_runner(
                data_root=root,
                bundle=bundle,
                plan=controls.plan,
                approval=controls.approval,
                preview=preview,
                case=case,
                attestations=attestations,
                usages=usages,
                calendar=controls.calendar,
                _authority=evidence_authority,
                _replay_session=replay_session,
            )
            stored = _write_s4_bounce_provider_evidence_manifest_from_official_bundle(
                root,
                manifest,
                bundle=bundle,
                calendar=controls.calendar,
                _authority=evidence_authority,
                _replay_session=replay_session,
            )
        except ProviderEvidenceError as exc:
            raise IdentityPreviewRunnerError(
                "case provider evidence cannot be physically replayed"
            ) from exc
        case_refs.append(
            PreviewCaseEvidenceRef(
                identity_case_id=case.identity_case_id,
                manifest_id=manifest.manifest_id,
                path=manifest.relative_path,
                sha256=manifest.sha256,
                bytes=int(stored["bytes"]),
            )
        )

    result = _mapping(preview.document["result"], "bounded preview result")
    summary = _mapping(result["input_summary"], "bounded preview input summary")
    completion = S7DetectorPreviewCompletion(
        plan_id=controls.plan.plan_id,
        plan_sha256=controls.plan.sha256,
        approval_id=controls.approval.approval_id,
        approval_sha256=controls.approval.sha256,
        request_event_id=controls.approval.request_event_id,
        request_event_sha256=controls.approval.request_event_sha256,
        git_commit=controls.plan.git_commit,
        calendar_artifact_id=controls.calendar.calendar_artifact_id,
        calendar_artifact_sha256=controls.calendar.sha256,
        ticker_allowlist_id=controls.allowlist.ticker_allowlist_id,
        ticker_allowlist_sha256=controls.allowlist.sha256,
        start_session=controls.plan.start_session,
        end_session=controls.plan.end_session,
        session_count=controls.plan.session_count,
        ticker_count=controls.plan.ticker_count,
        source_artifacts=source_refs,
        preview_artifact_id=preview.preview_artifact_id,
        preview_artifact_path=preview.relative_path,
        preview_artifact_sha256=preview.sha256,
        preview_artifact_bytes=int(preview_stored["bytes"]),
        case_evidence=tuple(case_refs),
        selected_observation_count=_nonnegative_int(
            summary["selected_observation_count"], "selected observation count"
        ),
        valid_active_observation_count=_nonnegative_int(
            summary["valid_active_observation_count"], "valid active observation count"
        ),
        scanned_row_count=_nonnegative_int(summary["scanned_row_count"], "scanned rows"),
        scanned_artifact_count=_nonnegative_int(
            summary["scanned_artifact_count"], "scanned artifacts"
        ),
        scanned_bytes=_nonnegative_int(summary["scanned_bytes"], "scanned bytes"),
        case_count=len(case_refs),
        suspected_provider_figi_bounce_rows=_nonnegative_int(
            result["suspected_provider_figi_bounce_rows"],
            "suspected provider FIGI bounce rows",
        ),
        created_at_utc=created_at,
        completion_available_session=completion_available,
    )
    _write_completion(
        root,
        completion,
        evidence_authority=evidence_authority,
        calendar=controls.calendar,
    )
    return completion


def _load_controls(
    root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> _LoadedControls:
    store = IdentityPreviewPlanStore(root)
    try:
        plan, _ = store.load_plan(plan_id, expected_sha256=expected_plan_sha256)
        approval, _ = store.load_approval(
            approval_id,
            expected_sha256=expected_approval_sha256,
        )
        allowlist, _ = store.load_ticker_allowlist(
            plan.ticker_allowlist_id,
            expected_sha256=plan.ticker_allowlist_sha256,
        )
        calendar = load_xnys_calendar_artifact(
            root,
            calendar_artifact_id=plan.calendar_artifact_id,
            expected_sha256=plan.calendar_artifact_sha256,
        )
    except (ArtifactError, IdentityPreviewPlanError, XNYSCalendarArtifactError) as exc:
        raise IdentityPreviewRunnerError("exact preview controls cannot be loaded") from exc
    if (
        approval.plan_id != plan.plan_id
        or approval.plan_sha256 != plan.sha256
        or allowlist.ticker_count != plan.ticker_count
    ):
        raise IdentityPreviewRunnerError("preview approval or allowlist crosses plans")
    sessions = tuple(
        item.session_date
        for item in calendar.sessions
        if plan.start_session <= item.session_date <= plan.end_session
    )
    if (
        len(sessions) != plan.session_count
        or not sessions
        or sessions[0] != plan.start_session
        or sessions[-1] != plan.end_session
    ):
        raise IdentityPreviewRunnerError("preview session spine does not reproduce")
    return _LoadedControls(plan, approval, allowlist, calendar, sessions)


def _preflight_source_scope(
    plan: S7DetectorPreviewPlan,
    sessions: tuple[date, ...],
    asset_artifacts: tuple[IdentitySourceArtifact, ...],
    universe_artifacts: tuple[IdentitySourceArtifact, ...],
) -> tuple[PreviewSourceArtifactRef, ...]:
    if len(asset_artifacts) != len(sessions) or len(universe_artifacts) != len(sessions):
        raise IdentityPreviewRunnerError("daily source artifact scope is incomplete")
    refs = tuple(
        sorted(
            (
                *(
                    _source_ref("asset_observation_daily", session, artifact)
                    for session, artifact in zip(sessions, asset_artifacts, strict=True)
                ),
                *(
                    _source_ref("universe_source_daily", session, artifact)
                    for session, artifact in zip(sessions, universe_artifacts, strict=True)
                ),
            )
        )
    )
    caps = plan.resource_caps
    asset_rows = sum(item.row_count for item in refs if item.table == "asset_observation_daily")
    universe_rows = sum(item.row_count for item in refs if item.table == "universe_source_daily")
    total_rows = asset_rows + universe_rows
    total_bytes = sum(item.bytes for item in refs)
    if asset_rows > caps.asset_parent_scanned_row_cap:
        raise IdentityPreviewRunnerError("asset-parent preflight row cap exceeded")
    if universe_rows > caps.universe_scanned_row_cap:
        raise IdentityPreviewRunnerError("universe preflight row cap exceeded")
    if total_rows > caps.total_scanned_row_cap:
        raise IdentityPreviewRunnerError("total preflight row cap exceeded")
    if len(refs) > caps.source_artifact_cap:
        raise IdentityPreviewRunnerError("source artifact preflight cap exceeded")
    if total_bytes > caps.source_bytes_cap:
        raise IdentityPreviewRunnerError("source byte preflight cap exceeded")
    return refs


def _source_ref(
    table: str,
    session: date,
    artifact: IdentitySourceArtifact,
) -> PreviewSourceArtifactRef:
    artifact.require_official()
    if artifact.table != table or artifact.ref.row_count is None:
        raise IdentityPreviewRunnerError("daily artifact metadata is invalid")
    return PreviewSourceArtifactRef(
        table=table,
        session_date=session,
        path=artifact.ref.path,
        sha256=artifact.ref.sha256,
        bytes=artifact.ref.bytes,
        row_count=artifact.ref.row_count,
    )


def _scan_preview(
    bundle: IdentitySourceBundle,
    controls: _LoadedControls,
    *,
    asset_artifacts: tuple[IdentitySourceArtifact, ...],
    universe_artifacts: tuple[IdentitySourceArtifact, ...],
    source_refs: tuple[PreviewSourceArtifactRef, ...],
    preview_available_session: date | None,
) -> tuple[
    BoundedIdentityPreviewArtifact,
    dict[str, tuple[ProviderRowAttestation, ProviderRowAttestation]],
    datetime | None,
]:
    caps = controls.plan.resource_caps
    engine = BoundedIdentityPreviewEngine(
        six_release_binding_id=controls.plan.six_release_binding_id,
        preview_manifest_available_session=(preview_available_session or controls.plan.end_session),
        scoped_tickers=controls.allowlist.tickers,
        limits=BoundedIdentityPreviewLimits(
            max_sessions=controls.plan.session_count,
            max_tickers=controls.plan.ticker_count,
            max_selected_rows=caps.selected_row_cap,
            max_scanned_rows=caps.total_scanned_row_cap,
            max_artifacts=caps.source_artifact_cap,
            max_bytes=caps.source_bytes_cap,
            max_cases=caps.case_cap,
        ),
    )
    ticker_scope = frozenset(controls.allowlist.tickers)
    evidence_pairs: dict[str, tuple[ProviderRowAttestation, ProviderRowAttestation]] = {}
    seen_source_ids: set[str] = set()
    ref_by_key = {(item.table, item.session_date): item for item in source_refs}
    for session, asset_artifact, universe_artifact in zip(
        controls.sessions,
        asset_artifacts,
        universe_artifacts,
        strict=True,
    ):
        observations: list[IdentityObservation] = []
        universe_attestations: dict[str, ProviderRowAttestation] = {}
        seen_tickers: set[str] = set()
        for physical in bundle.iter_physical_batches(
            "universe_source_daily",
            batch_size=caps.batch_size,
            artifacts=(universe_artifact,),
        ):
            rows = physical.batch.to_pylist()
            indices = tuple(
                index for index, row in enumerate(rows) if row["ticker"] in ticker_scope
            )
            if not indices:
                continue
            attestations = attest_provider_rows(
                physical,
                row_indices_in_batch=indices,
                calendar=controls.calendar,
            )
            for index, attestation in zip(indices, attestations, strict=True):
                row = rows[index]
                ticker = row["ticker"]
                if ticker in seen_tickers:
                    raise IdentityPreviewRunnerError(
                        "universe scan contains duplicate ticker/session membership"
                    )
                seen_tickers.add(ticker)
                source_id = row["selected_source_record_id"]
                if source_id in universe_attestations or source_id in seen_source_ids:
                    raise IdentityPreviewRunnerError(
                        "universe scan reuses a selected source record"
                    )
                universe_attestations[source_id] = attestation
                observations.append(
                    IdentityObservation(
                        session_date=row["session_date"],
                        ticker=ticker,
                        observed_composite_figi=row["composite_figi"],
                        source_record_id=source_id,
                        source_available_session=row["source_available_session"],
                        active_on_date=row["active_on_date"],
                    )
                )
        wanted = frozenset(universe_attestations)
        asset_attestations: dict[str, ProviderRowAttestation] = {}
        for physical in bundle.iter_physical_batches(
            "asset_observation_daily",
            batch_size=caps.batch_size,
            artifacts=(asset_artifact,),
        ):
            rows = physical.batch.to_pylist()
            indices = tuple(
                index for index, row in enumerate(rows) if row["source_record_id"] in wanted
            )
            if not indices:
                continue
            attestations = attest_provider_rows(
                physical,
                row_indices_in_batch=indices,
                calendar=controls.calendar,
            )
            for index, attestation in zip(indices, attestations, strict=True):
                source_id = rows[index]["source_record_id"]
                if source_id in asset_attestations:
                    raise IdentityPreviewRunnerError("asset parent scan contains duplicates")
                asset_attestations[source_id] = attestation
        if set(asset_attestations) != set(universe_attestations):
            raise IdentityPreviewRunnerError("selected universe rows lack exact asset parents")
        for source_id, universe in universe_attestations.items():
            asset = asset_attestations[source_id]
            try:
                validate_s4_observation_parent_pair(asset, universe)
            except ProviderEvidenceError as exc:
                raise IdentityPreviewRunnerError("S4 parent lineage mismatch") from exc
            seen_source_ids.add(source_id)
            if universe.full_row_snapshot["active_on_date"] is True:
                evidence_pairs[source_id] = (asset, universe)
        asset_ref = ref_by_key[("asset_observation_daily", session)]
        universe_ref = ref_by_key[("universe_source_daily", session)]
        engine.consume_session(
            SourceSession(session_date=session, source_complete=True),
            tuple(sorted(observations, key=lambda item: item.ticker)),
            scanned_row_count=asset_ref.row_count + universe_ref.row_count,
            scanned_artifact_count=2,
            scanned_bytes=asset_ref.bytes + universe_ref.bytes,
        )
    finalized_at: datetime | None = None
    available_session = preview_available_session
    if available_session is None:
        finalized_at = _utc_now()
        try:
            available_session, _ = controls.calendar.first_open_after(finalized_at)
        except XNYSCalendarArtifactError as exc:
            raise IdentityPreviewRunnerError(str(exc)) from exc
    try:
        result = engine.finalize(
            preview_manifest_available_session=available_session,
        )
        return build_bounded_identity_preview_artifact(result), evidence_pairs, finalized_at
    except IdentityStreamingPreviewError as exc:
        raise IdentityPreviewRunnerError("bounded streaming detector failed") from exc


def _preview_cases(preview: BoundedIdentityPreviewArtifact) -> tuple[BounceCase, ...]:
    from ame_stocks_api.silver.identity_provider_evidence import _bounce_case_from_snapshot

    result = _mapping(preview.document["result"], "bounded preview result")
    raw = _array(result["cases"], "bounded preview cases")
    cases = tuple(_bounce_case_from_snapshot(item) for item in raw)
    if tuple(item.identity_case_id for item in cases) != tuple(
        sorted(item.identity_case_id for item in cases)
    ):
        raise IdentityPreviewRunnerError("bounded preview cases are not deterministically sorted")
    return cases


def _case_attestations(
    cases: tuple[BounceCase, ...],
    pairs: Mapping[
        str,
        tuple[ProviderRowAttestation, ProviderRowAttestation],
    ],
) -> tuple[ProviderRowAttestation, ...]:
    """Collect each physical case row once so replay can batch across all cases."""

    selected: dict[str, ProviderRowAttestation] = {}
    for case in cases:
        record_ids = (
            case.left_outer_source_record_id,
            *case.episode_source_record_ids,
            case.right_outer_source_record_id,
        )
        for record_id in record_ids:
            try:
                pair = pairs[record_id]
            except KeyError as exc:
                raise IdentityPreviewRunnerError(
                    "detected case is missing a physically attested S4 pair"
                ) from exc
            for attestation in pair:
                prior = selected.setdefault(attestation.row_attestation_id, attestation)
                if prior != attestation:
                    raise IdentityPreviewRunnerError(
                        "one physical attestation ID resolves to different rows"
                    )
    return tuple(selected[key] for key in sorted(selected))


def _write_completion(
    root: Path,
    completion: S7DetectorPreviewCompletion,
    *,
    evidence_authority: _RunnerEvidenceAuthority,
    calendar: XNYSCalendarArtifact,
) -> None:
    if type(evidence_authority) is not _RunnerEvidenceAuthority:
        raise IdentityPreviewRunnerError("completion lacks live runner evidence authority")
    try:
        evidence_authority.require_live_write(calendar=calendar)
    except ProviderEvidenceError as exc:
        raise IdentityPreviewRunnerError("completion evidence authority is no longer live") from exc
    try:
        target = safe_relative_path(root, completion.relative_path)
        stored = write_bytes_immutable(root, target, completion.content)
    except ArtifactError as exc:
        raise IdentityPreviewRunnerError("completion cannot be stored immutably") from exc
    if stored["sha256"] != completion.sha256 or stored["bytes"] != len(completion.content):
        raise IdentityPreviewRunnerError("completion storage receipt differs")


def _completion_file(root: Path, plan_id: str, approval_id: str) -> Path:
    try:
        return safe_relative_path(root, detector_preview_completion_path(plan_id, approval_id))
    except ArtifactError as exc:
        raise IdentityPreviewRunnerError(str(exc)) from exc


def _read_and_revalidate_completion(
    root: Path,
    path: Path,
    controls: _LoadedControls,
) -> S7DetectorPreviewCompletion:
    if path.is_symlink():
        raise IdentityPreviewRunnerError("completion is an unsafe symlink")
    content = path.read_bytes()
    try:
        raw = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityPreviewRunnerError("completion is not valid JSON") from exc
    if not isinstance(raw, dict) or _canonical_bytes(raw) != content:
        raise IdentityPreviewRunnerError("completion bytes are not canonical")
    completion = S7DetectorPreviewCompletion.from_dict(raw)
    if (
        completion.relative_path != str(path.relative_to(root))
        or completion.plan_id != controls.plan.plan_id
        or completion.plan_sha256 != controls.plan.sha256
        or completion.approval_id != controls.approval.approval_id
        or completion.approval_sha256 != controls.approval.sha256
        or completion.request_event_id != controls.approval.request_event_id
        or completion.request_event_sha256 != controls.approval.request_event_sha256
        or completion.calendar_artifact_id != controls.calendar.calendar_artifact_id
        or completion.calendar_artifact_sha256 != controls.calendar.sha256
        or completion.ticker_allowlist_id != controls.allowlist.ticker_allowlist_id
        or completion.ticker_allowlist_sha256 != controls.allowlist.sha256
        or completion.git_commit != controls.plan.git_commit
        or completion.start_session != controls.plan.start_session
        or completion.end_session != controls.plan.end_session
        or completion.session_count != controls.plan.session_count
        or completion.ticker_count != controls.plan.ticker_count
    ):
        raise IdentityPreviewRunnerError("completion crosses exact control artifacts")
    if completion.created_at_utc < controls.approval.approved_at_utc:
        raise IdentityPreviewRunnerError("completion predates its exact approval")
    if completion.created_at_utc > _utc_now():
        raise IdentityPreviewRunnerError("completion creation time is in the future")
    try:
        controls.calendar.require_first_open_session(
            completion.created_at_utc,
            completion.completion_available_session,
            label="S7 detector preview completion availability",
        )
        preview = read_bounded_identity_preview_artifact(
            root,
            preview_artifact_id=completion.preview_artifact_id,
            expected_sha256=completion.preview_artifact_sha256,
        )
    except (IdentityStreamingPreviewError, XNYSCalendarArtifactError) as exc:
        raise IdentityPreviewRunnerError("completion preview cannot be revalidated") from exc
    _validate_completion_preview(completion, preview)
    try:
        bundle = open_approved_identity_preview_source_bundle(
            root,
            plan_id=controls.plan.plan_id,
            expected_plan_sha256=controls.plan.sha256,
            approval_id=controls.approval.approval_id,
            expected_approval_sha256=controls.approval.sha256,
        )
        bundle.require_official()
        bundle.require_approved_preview_scope(
            plan_id=controls.plan.plan_id,
            plan_sha256=controls.plan.sha256,
            approval_id=controls.approval.approval_id,
            approval_sha256=controls.approval.sha256,
            sessions=controls.sessions,
        )
        asset_artifacts = bundle.daily_partition_artifacts(
            "asset_observation_daily", controls.sessions
        )
        universe_artifacts = bundle.daily_partition_artifacts(
            "universe_source_daily", controls.sessions
        )
        current_source_refs = _preflight_source_scope(
            controls.plan,
            controls.sessions,
            asset_artifacts,
            universe_artifacts,
        )
    except (IdentitySourceError, OSError) as exc:
        raise IdentityPreviewRunnerError("completion source bundle cannot be reopened") from exc
    if current_source_refs != completion.source_artifacts:
        raise IdentityPreviewRunnerError("completion source artifact set changed")
    try:
        regenerated, pairs, finalized_at = _scan_preview(
            bundle,
            controls,
            asset_artifacts=asset_artifacts,
            universe_artifacts=universe_artifacts,
            source_refs=current_source_refs,
            preview_available_session=completion.completion_available_session,
        )
    except (IdentitySourceError, ProviderEvidenceError, IdentityStreamingPreviewError) as exc:
        raise IdentityPreviewRunnerError("completion physical detector replay failed") from exc
    if finalized_at is not None:
        raise IdentityPreviewRunnerError("completion replay unexpectedly minted a live timestamp")
    if regenerated.content != preview.content:
        raise IdentityPreviewRunnerError(
            "stored preview does not reproduce the bounded physical scan"
        )
    source_by_path = {item.path: item for item in completion.source_artifacts}
    declared_by_case = {item.identity_case_id: item for item in completion.case_evidence}
    reproduced_refs: list[PreviewCaseEvidenceRef] = []
    replay_session = _ProviderReplaySession(bundle=bundle, calendar=controls.calendar)
    cases = _preview_cases(regenerated)
    case_attestations = _case_attestations(cases, pairs)
    if case_attestations:
        replay_session.replay(case_attestations)
    for case in cases:
        try:
            ref = declared_by_case[case.identity_case_id]
            record_ids = (
                case.left_outer_source_record_id,
                *case.episode_source_record_ids,
                case.right_outer_source_record_id,
            )
            selected_pairs = tuple(pairs[item] for item in record_ids)
        except KeyError as exc:
            raise IdentityPreviewRunnerError(
                "completion omitted physically reproduced case evidence"
            ) from exc
        stored_manifest = _read_case_evidence_bytes(root, ref)
        usages = tuple(
            build_s4_bounce_case_evidence_usage(
                case,
                plan=controls.plan,
                preview=regenerated,
                asset_observation=asset,
                universe_membership=universe,
                calendar=controls.calendar,
            )
            for asset, universe in selected_pairs
        )
        attestations = tuple(item for pair in selected_pairs for item in pair)
        try:
            manifest = _rebuild_s4_bounce_provider_evidence_manifest_for_completion(
                data_root=root,
                bundle=bundle,
                plan=controls.plan,
                approval=controls.approval,
                preview=regenerated,
                case=case,
                attestations=attestations,
                usages=usages,
                created_at_utc=completion.created_at_utc,
                calendar=controls.calendar,
                _replay_session=replay_session,
            )
        except ProviderEvidenceError as exc:
            raise IdentityPreviewRunnerError(
                "completion physical case evidence cannot be reproduced"
            ) from exc
        reproduced = PreviewCaseEvidenceRef(
            identity_case_id=case.identity_case_id,
            manifest_id=manifest.manifest_id,
            path=manifest.relative_path,
            sha256=manifest.sha256,
            bytes=len(manifest.content),
        )
        reproduced_refs.append(reproduced)
        if (
            reproduced != ref
            or manifest.content != stored_manifest.content
            or manifest.plan_id != completion.plan_id
            or manifest.plan_sha256 != completion.plan_sha256
            or manifest.approval_id != completion.approval_id
            or manifest.approval_sha256 != completion.approval_sha256
            or manifest.preview_artifact_id != completion.preview_artifact_id
            or manifest.preview_artifact_sha256 != completion.preview_artifact_sha256
            or _string(manifest.case_snapshot, "identity_case_id") != ref.identity_case_id
        ):
            raise IdentityPreviewRunnerError("completion case evidence crosses bindings")
        for attestation in manifest.row_attestations:
            source = source_by_path.get(attestation.silver_artifact_path)
            if source is None or source.sha256 != attestation.silver_artifact_sha256:
                raise IdentityPreviewRunnerError(
                    "completion evidence references an unscanned source artifact"
                )
    if tuple(sorted(reproduced_refs)) != completion.case_evidence:
        raise IdentityPreviewRunnerError("completion case set differs from physical replay")
    return completion


def _validate_completion_preview(
    completion: S7DetectorPreviewCompletion,
    preview: BoundedIdentityPreviewArtifact,
) -> None:
    result = _mapping(preview.document["result"], "bounded preview result")
    summary = _mapping(result["input_summary"], "bounded preview input summary")
    cases = _array(result["cases"], "bounded preview cases")
    case_ids = tuple(sorted(_string(_mapping(item, "case"), "identity_case_id") for item in cases))
    if (
        completion.preview_artifact_bytes != len(preview.content)
        or result["corroboration_evaluation_state"] != "not_evaluated"
        or result["support_absence_verified"] is not False
        or completion.selected_observation_count != summary["selected_observation_count"]
        or completion.valid_active_observation_count != summary["valid_active_observation_count"]
        or completion.scanned_row_count != summary["scanned_row_count"]
        or completion.scanned_artifact_count != summary["scanned_artifact_count"]
        or completion.scanned_bytes != summary["scanned_bytes"]
        or completion.case_count != len(cases)
        or completion.suspected_provider_figi_bounce_rows
        != result["suspected_provider_figi_bounce_rows"]
        or tuple(item.identity_case_id for item in completion.case_evidence) != case_ids
    ):
        raise IdentityPreviewRunnerError("completion differs from its exact preview")


def _read_case_evidence_bytes(
    root: Path,
    ref: PreviewCaseEvidenceRef,
) -> S4BounceProviderEvidenceManifest:
    try:
        path = safe_relative_path(root, ref.path)
    except ArtifactError as exc:
        raise IdentityPreviewRunnerError(str(exc)) from exc
    if (
        not path.is_file()
        or path.is_symlink()
        or path.stat().st_size != ref.bytes
        or sha256_file(path) != ref.sha256
    ):
        raise IdentityPreviewRunnerError("completion case evidence bytes changed")
    content = path.read_bytes()
    try:
        raw = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityPreviewRunnerError("case evidence is not valid JSON") from exc
    if not isinstance(raw, dict):
        raise IdentityPreviewRunnerError("case evidence must be an object")
    manifest = S4BounceProviderEvidenceManifest.from_dict(raw)
    if (
        manifest.manifest_id != ref.manifest_id
        or manifest.sha256 != ref.sha256
        or manifest.relative_path != ref.path
        or manifest.content != content
    ):
        raise IdentityPreviewRunnerError("case evidence trust chain changed")
    return manifest


def _verify_git_checkout(expected_commit: str) -> None:
    if not _GIT_COMMIT.fullmatch(expected_commit):
        raise IdentityPreviewRunnerError("expected Git commit is invalid")
    root = Path(__file__).resolve().parents[3]
    try:
        top = Path(_git_output(root, "rev-parse", "--show-toplevel")).resolve()
        head = _git_output(root, "rev-parse", "HEAD")
        status = _git_output(root, "status", "--porcelain", "--untracked-files=all")
    except (OSError, subprocess.SubprocessError) as exc:
        raise IdentityPreviewRunnerError("Git checkout cannot be verified") from exc
    if top != root or head != expected_commit or status:
        raise IdentityPreviewRunnerError("Git checkout is dirty, displaced, or at the wrong HEAD")


def _git_output(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    return completed.stdout.strip()


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(dict(value), allow_nan=False, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        + b"\n"
    )


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise IdentityPreviewRunnerError(f"{label} must be an object")
    return value


def _exact_mapping(
    value: object,
    expected: set[str],
    label: str,
) -> Mapping[str, object]:
    item = _mapping(value, label)
    if set(item) != expected:
        raise IdentityPreviewRunnerError(f"{label} fields are not exact")
    return item


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise IdentityPreviewRunnerError(f"{label} must be an array")
    return value


def _string(item: Mapping[str, object], field: str) -> str:
    value = item.get(field)
    if not isinstance(value, str) or not value:
        raise IdentityPreviewRunnerError(f"{field} must be nonempty text")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise IdentityPreviewRunnerError(f"{label} must be lowercase SHA-256")
    return value


def _relative_path(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise IdentityPreviewRunnerError(f"{label} must be nonempty text")
    path = Path(value)
    if path.is_absolute() or path.as_posix() != value or ".." in path.parts:
        raise IdentityPreviewRunnerError(f"{label} must be a normalized relative path")
    return value


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IdentityPreviewRunnerError(f"{label} must be a positive native int")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IdentityPreviewRunnerError(f"{label} must be a nonnegative native int")
    return value


def _native_date(value: object, label: str) -> date:
    if type(value) is not date:
        raise IdentityPreviewRunnerError(f"{label} must be a native date")
    return value


def _parse_date(value: str, label: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise IdentityPreviewRunnerError(f"{label} is not an ISO date") from exc
    if parsed.isoformat() != value:
        raise IdentityPreviewRunnerError(f"{label} is not canonical")
    return parsed


def _utc_datetime(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise IdentityPreviewRunnerError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    return _utc_datetime(value).isoformat()


def _parse_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IdentityPreviewRunnerError("timestamp is not ISO-8601") from exc
    result = _utc_datetime(parsed)
    if result.isoformat() != value:
        raise IdentityPreviewRunnerError("timestamp is not canonical UTC")
    return result


__all__ = [
    "S7_DETECTOR_PREVIEW_COMPLETION_RULE_VERSION",
    "S7_DETECTOR_PREVIEW_COMPLETION_SCHEMA_VERSION",
    "S7_DETECTOR_PREVIEW_COMPLETION_SCOPE",
    "S7_DETECTOR_PREVIEW_SOURCE_ATTESTATION_SCOPE",
    "IdentityPreviewRunnerError",
    "PreviewCaseEvidenceRef",
    "PreviewSourceArtifactRef",
    "S7DetectorPreviewCompletion",
    "detector_preview_completion_path",
    "run_source_bound_identity_streaming_preview",
]
