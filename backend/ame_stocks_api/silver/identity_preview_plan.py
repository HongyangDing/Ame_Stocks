"""Fail-closed control artifacts for one bounded S7 detector preview.

This module deliberately contains no detector runner, CLI, adjudication, table
materialization, or publication path.  It only freezes an exact ticker allowlist,
an exact bounded preview plan, a deterministic preapproval request event, and a
separate human approval receipt.  Every artifact is canonical JSON, content
addressed, immutable, and loaded only by an explicit ID/SHA pair; ``latest``
discovery is impossible.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
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
from ame_stocks_api.silver.calendar_artifact import (
    XNYSCalendarArtifactError,
    load_xnys_calendar_artifact,
)
from ame_stocks_api.silver.identity_source import (
    S7_S4_RELEASE_SET_ID,
    S7_S4_RELEASE_SET_MANIFEST_SHA256,
    S7_SIX_RELEASE_BINDING_ID,
    S7_SOURCE_PINS,
    IdentitySourcePin,
)

TICKER_ALLOWLIST_SCHEMA_VERSION: Final = 1
DETECTOR_PREVIEW_PLAN_SCHEMA_VERSION: Final = 1
DETECTOR_PREVIEW_APPROVAL_REQUEST_SCHEMA_VERSION: Final = 1
DETECTOR_PREVIEW_APPROVAL_SCHEMA_VERSION: Final = 2

TICKER_ALLOWLIST_RULE_VERSION: Final = "s7_detector_preview_ticker_allowlist_v1"
DETECTOR_PREVIEW_PLAN_RULE_VERSION: Final = "s7_detector_preview_plan_v1"
DETECTOR_PREVIEW_APPROVAL_REQUEST_RULE_VERSION: Final = (
    "s7_detector_preview_plan_approval_request_v1"
)
DETECTOR_PREVIEW_APPROVAL_RULE_VERSION: Final = "s7_detector_preview_plan_approval_v2"
DETECTOR_PREVIEW_APPROVAL_LITERAL_VERSION: Final = "s7_detector_preview_approval_literal_v2"

DETECTOR_PREVIEW_SCOPE: Final = (
    "bounded_source_attested_preview_only_no_adjudication_no_materialization"
)
DETECTOR_PREVIEW_PLAN_STATE: Final = "awaiting_exact_plan_approval"
DETECTOR_PREVIEW_APPROVAL_STAGE: Final = "s7_detector_preview_plan"
DETECTOR_PREVIEW_APPROVAL_REQUEST_STATE: Final = "awaiting_literal_human_approval"
DETECTOR_PREVIEW_AUTHORIZED_ACTION: Final = (
    "execute_exact_bounded_detector_preview_once_to_awaiting_review"
)

MAX_PREVIEW_SESSIONS: Final = 25
MAX_PREVIEW_TICKERS: Final = 250
MAX_SELECTED_ROWS: Final = 6_250
MAX_UNIVERSE_SCANNED_ROWS: Final = 1_000_000
MAX_ASSET_PARENT_SCANNED_ROWS: Final = 1_000_000
MAX_TOTAL_SCANNED_ROWS: Final = 2_100_000
MAX_SOURCE_ARTIFACTS: Final = 80
MAX_SOURCE_BYTES: Final = 512 * 1024 * 1024
MAX_CASES: Final = 2_500
MAX_BATCH_SIZE: Final = 65_536

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SENSITIVE_TOKENS = (
    "access_token",
    "api_key",
    "apikey",
    "authorization",
    "password",
    "secret",
    "signed_url",
)


class IdentityPreviewPlanError(RuntimeError):
    """Raised when an S7 preview control artifact is unsafe or inconsistent."""


@dataclass(frozen=True, slots=True)
class StoredIdentityPreviewDocument:
    """Exact immutable document receipt returned by the control store."""

    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative_path(self.path, "stored document path")
        _digest(self.sha256, "stored document SHA-256")
        _positive_int(self.bytes, "stored document bytes")


@dataclass(frozen=True, slots=True)
class S7PreviewSourcePin:
    """Serializable copy of one exact production source pin."""

    table: str
    release_id: str
    release_manifest_sha256: str
    build_id: str
    artifact_count: int
    row_count: int
    evidence_only_s4: bool

    @classmethod
    def from_source_pin(cls, pin: IdentitySourcePin) -> S7PreviewSourcePin:
        return cls(
            table=pin.table,
            release_id=pin.release_id,
            release_manifest_sha256=pin.release_manifest_sha256,
            build_id=pin.build_id,
            artifact_count=pin.artifact_count,
            row_count=pin.row_count,
            evidence_only_s4=pin.evidence_only_s4,
        )

    def __post_init__(self) -> None:
        _identifier(self.table, "source pin table")
        _digest(self.release_id, "source pin release_id")
        _digest(self.release_manifest_sha256, "source pin release manifest SHA-256")
        _digest(self.build_id, "source pin build_id")
        _positive_int(self.artifact_count, "source pin artifact_count")
        _nonnegative_int(self.row_count, "source pin row_count")
        _native_bool(self.evidence_only_s4, "source pin evidence_only_s4")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact_count": self.artifact_count,
            "build_id": self.build_id,
            "evidence_only_s4": self.evidence_only_s4,
            "release_id": self.release_id,
            "release_manifest_sha256": self.release_manifest_sha256,
            "row_count": self.row_count,
            "table": self.table,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7PreviewSourcePin:
        document = _mapping(value, "source pin")
        _expect_keys(
            document,
            {
                "artifact_count",
                "build_id",
                "evidence_only_s4",
                "release_id",
                "release_manifest_sha256",
                "row_count",
                "table",
            },
            "source pin",
        )
        return cls(
            table=_string(document["table"], "source pin table"),
            release_id=_string(document["release_id"], "source pin release_id"),
            release_manifest_sha256=_string(
                document["release_manifest_sha256"],
                "source pin release manifest SHA-256",
            ),
            build_id=_string(document["build_id"], "source pin build_id"),
            artifact_count=_positive_int(document["artifact_count"], "source pin artifact_count"),
            row_count=_nonnegative_int(document["row_count"], "source pin row_count"),
            evidence_only_s4=_native_bool(
                document["evidence_only_s4"], "source pin evidence_only_s4"
            ),
        )


@dataclass(frozen=True, slots=True)
class S7TickerAllowlist:
    """Canonical case-sensitive ticker allowlist used by exactly one bounded plan."""

    ticker_allowlist_id: str
    tickers: tuple[str, ...]
    content: bytes
    sha256: str

    def __post_init__(self) -> None:
        _digest(self.ticker_allowlist_id, "ticker_allowlist_id")
        _validate_tickers(self.tickers)
        if not isinstance(self.content, bytes):
            raise IdentityPreviewPlanError("ticker allowlist content must be bytes")
        _digest(self.sha256, "ticker allowlist SHA-256")
        if hashlib.sha256(self.content).hexdigest() != self.sha256:
            raise IdentityPreviewPlanError("ticker allowlist SHA-256 differs from its bytes")
        expected = _ticker_allowlist_document(self.tickers)
        if expected["ticker_allowlist_id"] != self.ticker_allowlist_id:
            raise IdentityPreviewPlanError("ticker allowlist ID differs from its logical payload")
        if _canonical_bytes(expected) != self.content:
            raise IdentityPreviewPlanError("ticker allowlist bytes are not canonical")

    @property
    def relative_path(self) -> str:
        return ticker_allowlist_path(self.ticker_allowlist_id)

    @property
    def ticker_count(self) -> int:
        return len(self.tickers)

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType(_ticker_allowlist_document(self.tickers))


def build_s7_ticker_allowlist(tickers: Sequence[str]) -> S7TickerAllowlist:
    """Build an exact sorted-unique ticker artifact without silently normalizing input."""

    if isinstance(tickers, (str, bytes)):
        raise IdentityPreviewPlanError("ticker allowlist must be a sequence of ticker strings")
    values = tuple(tickers)
    _validate_tickers(values)
    document = _ticker_allowlist_document(values)
    content = _canonical_bytes(document)
    return S7TickerAllowlist(
        ticker_allowlist_id=str(document["ticker_allowlist_id"]),
        tickers=values,
        content=content,
        sha256=hashlib.sha256(content).hexdigest(),
    )


def ticker_allowlist_path(ticker_allowlist_id: str) -> str:
    _digest(ticker_allowlist_id, "ticker_allowlist_id")
    return (
        "manifests/silver/identity/detector-preview-ticker-allowlists/"
        f"ticker_allowlist_id={ticker_allowlist_id}/manifest.json"
    )


@dataclass(frozen=True, slots=True)
class S7DetectorPreviewResourceCaps:
    """All execution limits that an eventual runner must enforce before output."""

    selected_row_cap: int
    universe_scanned_row_cap: int
    asset_parent_scanned_row_cap: int
    total_scanned_row_cap: int
    source_artifact_cap: int
    source_bytes_cap: int
    case_cap: int
    batch_size: int

    def __post_init__(self) -> None:
        limits = (
            ("selected_row_cap", self.selected_row_cap, MAX_SELECTED_ROWS),
            (
                "universe_scanned_row_cap",
                self.universe_scanned_row_cap,
                MAX_UNIVERSE_SCANNED_ROWS,
            ),
            (
                "asset_parent_scanned_row_cap",
                self.asset_parent_scanned_row_cap,
                MAX_ASSET_PARENT_SCANNED_ROWS,
            ),
            ("total_scanned_row_cap", self.total_scanned_row_cap, MAX_TOTAL_SCANNED_ROWS),
            ("source_artifact_cap", self.source_artifact_cap, MAX_SOURCE_ARTIFACTS),
            ("source_bytes_cap", self.source_bytes_cap, MAX_SOURCE_BYTES),
            ("case_cap", self.case_cap, MAX_CASES),
            ("batch_size", self.batch_size, MAX_BATCH_SIZE),
        )
        for label, value, hard_limit in limits:
            _positive_int(value, label)
            if value > hard_limit:
                raise IdentityPreviewPlanError(f"{label} exceeds its hard safety limit")
        if self.total_scanned_row_cap < max(
            self.universe_scanned_row_cap,
            self.asset_parent_scanned_row_cap,
        ):
            raise IdentityPreviewPlanError(
                "total_scanned_row_cap cannot be below either component cap"
            )
        if self.total_scanned_row_cap > (
            self.universe_scanned_row_cap + self.asset_parent_scanned_row_cap
        ):
            raise IdentityPreviewPlanError(
                "total_scanned_row_cap cannot exceed the declared component-cap sum"
            )
        if self.case_cap > self.selected_row_cap:
            raise IdentityPreviewPlanError("case_cap cannot exceed selected_row_cap")

    def to_dict(self) -> dict[str, int]:
        return {
            "asset_parent_scanned_row_cap": self.asset_parent_scanned_row_cap,
            "batch_size": self.batch_size,
            "case_cap": self.case_cap,
            "selected_row_cap": self.selected_row_cap,
            "source_artifact_cap": self.source_artifact_cap,
            "source_bytes_cap": self.source_bytes_cap,
            "total_scanned_row_cap": self.total_scanned_row_cap,
            "universe_scanned_row_cap": self.universe_scanned_row_cap,
        }

    @property
    def digest(self) -> str:
        """Stable digest bound into the request event and approval literal."""

        return stable_digest(self.to_dict())

    @classmethod
    def from_dict(cls, value: object) -> S7DetectorPreviewResourceCaps:
        document = _mapping(value, "detector preview resource caps")
        expected = {
            "asset_parent_scanned_row_cap",
            "batch_size",
            "case_cap",
            "selected_row_cap",
            "source_artifact_cap",
            "source_bytes_cap",
            "total_scanned_row_cap",
            "universe_scanned_row_cap",
        }
        _expect_keys(document, expected, "detector preview resource caps")
        return cls(
            selected_row_cap=_positive_int(document["selected_row_cap"], "selected_row_cap"),
            universe_scanned_row_cap=_positive_int(
                document["universe_scanned_row_cap"], "universe_scanned_row_cap"
            ),
            asset_parent_scanned_row_cap=_positive_int(
                document["asset_parent_scanned_row_cap"],
                "asset_parent_scanned_row_cap",
            ),
            total_scanned_row_cap=_positive_int(
                document["total_scanned_row_cap"], "total_scanned_row_cap"
            ),
            source_artifact_cap=_positive_int(
                document["source_artifact_cap"], "source_artifact_cap"
            ),
            source_bytes_cap=_positive_int(document["source_bytes_cap"], "source_bytes_cap"),
            case_cap=_positive_int(document["case_cap"], "case_cap"),
            batch_size=_positive_int(document["batch_size"], "batch_size"),
        )


@dataclass(frozen=True, slots=True)
class S7DetectorPreviewPlan:
    """One exact, non-executable-until-approved bounded detector plan."""

    created_by: str
    created_at_utc: datetime
    git_commit: str
    clean_checkout_required: bool
    six_release_binding_id: str
    s4_release_set_id: str
    s4_release_set_manifest_sha256: str
    source_pins: tuple[S7PreviewSourcePin, ...]
    calendar_artifact_id: str
    calendar_artifact_sha256: str
    start_session: date
    end_session: date
    session_count: int
    ticker_allowlist_id: str
    ticker_allowlist_path: str
    ticker_allowlist_sha256: str
    ticker_count: int
    resource_caps: S7DetectorPreviewResourceCaps
    execution_scope: str = DETECTOR_PREVIEW_SCOPE
    plan_state: str = DETECTOR_PREVIEW_PLAN_STATE

    def __post_init__(self) -> None:
        _safe_text(self.created_by, "created_by", maximum=200)
        object.__setattr__(
            self, "created_at_utc", _utc_datetime(self.created_at_utc, "created_at_utc")
        )
        if not _GIT_COMMIT.fullmatch(self.git_commit):
            raise IdentityPreviewPlanError("git_commit must be an exact lowercase 40-hex commit")
        if self.clean_checkout_required is not True:
            raise IdentityPreviewPlanError("S7 preview always requires a clean Git checkout")
        if self.six_release_binding_id != S7_SIX_RELEASE_BINDING_ID:
            raise IdentityPreviewPlanError(
                "S7 preview six-release binding is not the reviewed bundle"
            )
        if self.s4_release_set_id != S7_S4_RELEASE_SET_ID:
            raise IdentityPreviewPlanError("S7 preview S4 release-set ID is not exact")
        if self.s4_release_set_manifest_sha256 != S7_S4_RELEASE_SET_MANIFEST_SHA256:
            raise IdentityPreviewPlanError("S7 preview S4 release-set SHA-256 is not exact")
        pins = tuple(self.source_pins)
        if pins != _EXACT_SOURCE_PINS:
            raise IdentityPreviewPlanError(
                "S7 preview source pins differ from the exact six releases"
            )
        object.__setattr__(self, "source_pins", pins)
        _digest(self.calendar_artifact_id, "calendar_artifact_id")
        _digest(self.calendar_artifact_sha256, "calendar artifact SHA-256")
        start = _native_date(self.start_session, "start_session")
        end = _native_date(self.end_session, "end_session")
        if start > end:
            raise IdentityPreviewPlanError("start_session cannot follow end_session")
        _positive_int(self.session_count, "session_count")
        if self.session_count > MAX_PREVIEW_SESSIONS:
            raise IdentityPreviewPlanError("session_count exceeds the hard preview limit")
        _digest(self.ticker_allowlist_id, "ticker_allowlist_id")
        expected_allowlist_path = ticker_allowlist_path(self.ticker_allowlist_id)
        if self.ticker_allowlist_path != expected_allowlist_path:
            raise IdentityPreviewPlanError("ticker allowlist path is not canonical")
        _digest(self.ticker_allowlist_sha256, "ticker allowlist SHA-256")
        _positive_int(self.ticker_count, "ticker_count")
        if self.ticker_count > MAX_PREVIEW_TICKERS:
            raise IdentityPreviewPlanError("ticker_count exceeds the hard preview limit")
        if not isinstance(self.resource_caps, S7DetectorPreviewResourceCaps):
            raise IdentityPreviewPlanError("resource_caps has the wrong concrete type")
        theoretical_rows = self.session_count * self.ticker_count
        if self.resource_caps.selected_row_cap > theoretical_rows:
            raise IdentityPreviewPlanError(
                "selected_row_cap exceeds session_count times ticker_count"
            )
        if self.execution_scope != DETECTOR_PREVIEW_SCOPE:
            raise IdentityPreviewPlanError("detector preview execution scope is not fail-closed")
        if self.plan_state != DETECTOR_PREVIEW_PLAN_STATE:
            raise IdentityPreviewPlanError("detector preview plan state is invalid")

    @classmethod
    def create(
        cls,
        *,
        created_by: str,
        created_at_utc: datetime,
        git_commit: str,
        calendar_artifact_id: str,
        calendar_artifact_sha256: str,
        start_session: date,
        end_session: date,
        session_count: int,
        ticker_allowlist: S7TickerAllowlist,
        resource_caps: S7DetectorPreviewResourceCaps,
    ) -> S7DetectorPreviewPlan:
        if not isinstance(ticker_allowlist, S7TickerAllowlist):
            raise IdentityPreviewPlanError("ticker_allowlist has the wrong concrete type")
        return cls(
            created_by=created_by,
            created_at_utc=created_at_utc,
            git_commit=git_commit,
            clean_checkout_required=True,
            six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
            s4_release_set_id=S7_S4_RELEASE_SET_ID,
            s4_release_set_manifest_sha256=S7_S4_RELEASE_SET_MANIFEST_SHA256,
            source_pins=_EXACT_SOURCE_PINS,
            calendar_artifact_id=calendar_artifact_id,
            calendar_artifact_sha256=calendar_artifact_sha256,
            start_session=start_session,
            end_session=end_session,
            session_count=session_count,
            ticker_allowlist_id=ticker_allowlist.ticker_allowlist_id,
            ticker_allowlist_path=ticker_allowlist.relative_path,
            ticker_allowlist_sha256=ticker_allowlist.sha256,
            ticker_count=ticker_allowlist.ticker_count,
            resource_caps=resource_caps,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_detector_preview_plan",
            "calendar_binding": {
                "calendar_artifact_id": self.calendar_artifact_id,
                "calendar_artifact_sha256": self.calendar_artifact_sha256,
                "calendar_name": "XNYS",
            },
            "created_at_utc": _utc_text(self.created_at_utc),
            "created_by": self.created_by,
            "execution_scope": self.execution_scope,
            "git_binding": {
                "clean_checkout_required": self.clean_checkout_required,
                "git_commit": self.git_commit,
            },
            "plan_rule_version": DETECTOR_PREVIEW_PLAN_RULE_VERSION,
            "plan_state": self.plan_state,
            "resource_caps": self.resource_caps.to_dict(),
            "schema_version": DETECTOR_PREVIEW_PLAN_SCHEMA_VERSION,
            "selection": {
                "end_session": self.end_session.isoformat(),
                "session_count": self.session_count,
                "start_session": self.start_session.isoformat(),
                "ticker_allowlist_id": self.ticker_allowlist_id,
                "ticker_allowlist_path": self.ticker_allowlist_path,
                "ticker_allowlist_sha256": self.ticker_allowlist_sha256,
                "ticker_count": self.ticker_count,
                "ticker_match_rule": "exact_case_sensitive_allowlist_only",
            },
            "source_binding": {
                "s4_release_set_id": self.s4_release_set_id,
                "s4_release_set_manifest_sha256": self.s4_release_set_manifest_sha256,
                "six_release_binding_id": self.six_release_binding_id,
                "source_pins": [item.to_dict() for item in self.source_pins],
            },
        }

    @property
    def plan_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "plan_id": self.plan_id})

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return detector_preview_plan_path(self.plan_id)

    @classmethod
    def from_dict(cls, value: object) -> S7DetectorPreviewPlan:
        document = _mapping(value, "detector preview plan")
        _expect_keys(
            document,
            {
                "artifact_type",
                "calendar_binding",
                "created_at_utc",
                "created_by",
                "execution_scope",
                "git_binding",
                "plan_id",
                "plan_rule_version",
                "plan_state",
                "resource_caps",
                "schema_version",
                "selection",
                "source_binding",
            },
            "detector preview plan",
        )
        if (
            document["artifact_type"] != "s7_detector_preview_plan"
            or document["schema_version"] != DETECTOR_PREVIEW_PLAN_SCHEMA_VERSION
            or document["plan_rule_version"] != DETECTOR_PREVIEW_PLAN_RULE_VERSION
        ):
            raise IdentityPreviewPlanError("detector preview plan fixed fields are invalid")
        calendar = _mapping(document["calendar_binding"], "calendar binding")
        _expect_keys(
            calendar,
            {"calendar_artifact_id", "calendar_artifact_sha256", "calendar_name"},
            "calendar binding",
        )
        if calendar["calendar_name"] != "XNYS":
            raise IdentityPreviewPlanError("detector preview calendar must be XNYS")
        git = _mapping(document["git_binding"], "Git binding")
        _expect_keys(git, {"clean_checkout_required", "git_commit"}, "Git binding")
        selection = _mapping(document["selection"], "selection")
        _expect_keys(
            selection,
            {
                "end_session",
                "session_count",
                "start_session",
                "ticker_allowlist_id",
                "ticker_allowlist_path",
                "ticker_allowlist_sha256",
                "ticker_count",
                "ticker_match_rule",
            },
            "selection",
        )
        if selection["ticker_match_rule"] != "exact_case_sensitive_allowlist_only":
            raise IdentityPreviewPlanError("ticker match rule is not exact and case-sensitive")
        source = _mapping(document["source_binding"], "source binding")
        _expect_keys(
            source,
            {
                "s4_release_set_id",
                "s4_release_set_manifest_sha256",
                "six_release_binding_id",
                "source_pins",
            },
            "source binding",
        )
        raw_pins = source["source_pins"]
        if not isinstance(raw_pins, list):
            raise IdentityPreviewPlanError("source_pins must be an array")
        plan = cls(
            created_by=_string(document["created_by"], "created_by"),
            created_at_utc=_parse_utc(document["created_at_utc"], "created_at_utc"),
            git_commit=_string(git["git_commit"], "git_commit"),
            clean_checkout_required=_native_bool(
                git["clean_checkout_required"], "clean_checkout_required"
            ),
            six_release_binding_id=_string(
                source["six_release_binding_id"], "six_release_binding_id"
            ),
            s4_release_set_id=_string(source["s4_release_set_id"], "s4_release_set_id"),
            s4_release_set_manifest_sha256=_string(
                source["s4_release_set_manifest_sha256"],
                "s4_release_set_manifest_sha256",
            ),
            source_pins=tuple(S7PreviewSourcePin.from_dict(item) for item in raw_pins),
            calendar_artifact_id=_string(calendar["calendar_artifact_id"], "calendar_artifact_id"),
            calendar_artifact_sha256=_string(
                calendar["calendar_artifact_sha256"], "calendar_artifact_sha256"
            ),
            start_session=_parse_date(selection["start_session"], "start_session"),
            end_session=_parse_date(selection["end_session"], "end_session"),
            session_count=_positive_int(selection["session_count"], "session_count"),
            ticker_allowlist_id=_string(selection["ticker_allowlist_id"], "ticker_allowlist_id"),
            ticker_allowlist_path=_string(
                selection["ticker_allowlist_path"], "ticker_allowlist_path"
            ),
            ticker_allowlist_sha256=_string(
                selection["ticker_allowlist_sha256"], "ticker_allowlist_sha256"
            ),
            ticker_count=_positive_int(selection["ticker_count"], "ticker_count"),
            resource_caps=S7DetectorPreviewResourceCaps.from_dict(document["resource_caps"]),
            execution_scope=_string(document["execution_scope"], "execution_scope"),
            plan_state=_string(document["plan_state"], "plan_state"),
        )
        if document["plan_id"] != plan.plan_id:
            raise IdentityPreviewPlanError("detector preview plan ID does not reproduce")
        return plan


def detector_preview_plan_path(plan_id: str) -> str:
    _digest(plan_id, "plan_id")
    return f"manifests/silver/identity/detector-preview-plans/plan_id={plan_id}/manifest.json"


@dataclass(frozen=True, slots=True)
class S7DetectorPreviewApprovalRequest:
    """Immutable request event shown to a human before any approval exists."""

    plan_id: str
    plan_path: str
    plan_sha256: str
    resource_caps_digest: str
    created_by: str
    created_at_utc: datetime
    authorized_action: str = DETECTOR_PREVIEW_AUTHORIZED_ACTION
    execution_scope: str = DETECTOR_PREVIEW_SCOPE
    request_state: str = DETECTOR_PREVIEW_APPROVAL_REQUEST_STATE

    def __post_init__(self) -> None:
        _digest(self.plan_id, "approval request plan_id")
        if self.plan_path != detector_preview_plan_path(self.plan_id):
            raise IdentityPreviewPlanError("approval request plan path is not canonical")
        _digest(self.plan_sha256, "approval request plan SHA-256")
        _digest(self.resource_caps_digest, "approval request resource caps digest")
        _safe_text(self.created_by, "approval request created_by", maximum=200)
        object.__setattr__(
            self,
            "created_at_utc",
            _utc_datetime(self.created_at_utc, "approval request created_at_utc"),
        )
        if self.authorized_action != DETECTOR_PREVIEW_AUTHORIZED_ACTION:
            raise IdentityPreviewPlanError("detector preview approval request action is too broad")
        if self.execution_scope != DETECTOR_PREVIEW_SCOPE:
            raise IdentityPreviewPlanError("detector preview approval request scope is too broad")
        if self.request_state != DETECTOR_PREVIEW_APPROVAL_REQUEST_STATE:
            raise IdentityPreviewPlanError("detector preview approval request state is invalid")

    @classmethod
    def create(
        cls,
        plan: S7DetectorPreviewPlan,
        stored_plan: StoredIdentityPreviewDocument,
        *,
        created_by: str,
        created_at_utc: datetime,
    ) -> S7DetectorPreviewApprovalRequest:
        if not isinstance(plan, S7DetectorPreviewPlan):
            raise IdentityPreviewPlanError("approval request plan has the wrong concrete type")
        _verify_stored_document(stored_plan, plan, "stored plan receipt")
        created_at = _utc_datetime(created_at_utc, "approval request created_at_utc")
        if created_at < plan.created_at_utc:
            raise IdentityPreviewPlanError("approval request cannot predate plan creation")
        return cls(
            plan_id=plan.plan_id,
            plan_path=stored_plan.path,
            plan_sha256=stored_plan.sha256,
            resource_caps_digest=plan.resource_caps.digest,
            created_by=created_by,
            created_at_utc=created_at,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_detector_preview_approval_request",
            "authorized_action": self.authorized_action,
            "created_at_utc": _utc_text(self.created_at_utc),
            "created_by": self.created_by,
            "execution_scope": self.execution_scope,
            "plan_id": self.plan_id,
            "plan_path": self.plan_path,
            "plan_sha256": self.plan_sha256,
            "request_rule_version": DETECTOR_PREVIEW_APPROVAL_REQUEST_RULE_VERSION,
            "request_state": self.request_state,
            "resource_caps_digest": self.resource_caps_digest,
            "schema_version": DETECTOR_PREVIEW_APPROVAL_REQUEST_SCHEMA_VERSION,
        }

    @property
    def request_event_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType(
            {**self.logical_payload(), "request_event_id": self.request_event_id}
        )

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return detector_preview_approval_request_path(self.request_event_id)

    @property
    def canonical_approval_literal(self) -> str:
        return _detector_preview_approval_literal(
            request_event_id=self.request_event_id,
            request_event_sha256=self.sha256,
            plan_id=self.plan_id,
            plan_sha256=self.plan_sha256,
            resource_caps_digest=self.resource_caps_digest,
            authorized_action=self.authorized_action,
        )

    @classmethod
    def from_dict(cls, value: object) -> S7DetectorPreviewApprovalRequest:
        document = _mapping(value, "detector preview approval request")
        _expect_keys(
            document,
            {
                "artifact_type",
                "authorized_action",
                "created_at_utc",
                "created_by",
                "execution_scope",
                "plan_id",
                "plan_path",
                "plan_sha256",
                "request_event_id",
                "request_rule_version",
                "request_state",
                "resource_caps_digest",
                "schema_version",
            },
            "detector preview approval request",
        )
        if (
            document["artifact_type"] != "s7_detector_preview_approval_request"
            or document["schema_version"] != DETECTOR_PREVIEW_APPROVAL_REQUEST_SCHEMA_VERSION
            or document["request_rule_version"] != DETECTOR_PREVIEW_APPROVAL_REQUEST_RULE_VERSION
        ):
            raise IdentityPreviewPlanError(
                "detector preview approval request fixed fields are invalid"
            )
        request = cls(
            plan_id=_string(document["plan_id"], "approval request plan_id"),
            plan_path=_string(document["plan_path"], "approval request plan path"),
            plan_sha256=_string(document["plan_sha256"], "approval request plan SHA-256"),
            resource_caps_digest=_string(
                document["resource_caps_digest"],
                "approval request resource caps digest",
            ),
            created_by=_string(document["created_by"], "approval request created_by"),
            created_at_utc=_parse_utc(
                document["created_at_utc"], "approval request created_at_utc"
            ),
            authorized_action=_string(document["authorized_action"], "authorized_action"),
            execution_scope=_string(document["execution_scope"], "execution_scope"),
            request_state=_string(document["request_state"], "request_state"),
        )
        if document["request_event_id"] != request.request_event_id:
            raise IdentityPreviewPlanError("approval request event ID does not reproduce")
        return request


def detector_preview_approval_request_path(request_event_id: str) -> str:
    _digest(request_event_id, "request_event_id")
    return (
        "manifests/silver/identity/detector-preview-approval-requests/"
        f"request_event_id={request_event_id}/manifest.json"
    )


@dataclass(frozen=True, slots=True)
class S7DetectorPreviewPlanApproval:
    """V2 literal approval for one exact, previously stored request event."""

    request_event_id: str
    request_event_path: str
    request_event_sha256: str
    plan_id: str
    plan_path: str
    plan_sha256: str
    resource_caps_digest: str
    approval_literal: str
    approval_literal_sha256: str
    approved_by: str
    approved_at_utc: datetime
    approval_note: str
    decision: str = "approved"
    approval_stage: str = DETECTOR_PREVIEW_APPROVAL_STAGE
    authorized_action: str = DETECTOR_PREVIEW_AUTHORIZED_ACTION
    execution_scope: str = DETECTOR_PREVIEW_SCOPE

    def __post_init__(self) -> None:
        _digest(self.request_event_id, "approval request_event_id")
        if self.request_event_path != detector_preview_approval_request_path(self.request_event_id):
            raise IdentityPreviewPlanError("approval request event path is not canonical")
        _digest(self.request_event_sha256, "approval request event SHA-256")
        _digest(self.plan_id, "approval plan_id")
        if self.plan_path != detector_preview_plan_path(self.plan_id):
            raise IdentityPreviewPlanError("approval plan path is not canonical")
        _digest(self.plan_sha256, "approval plan SHA-256")
        _digest(self.resource_caps_digest, "approval resource caps digest")
        if not isinstance(self.approval_literal, str):
            raise IdentityPreviewPlanError("approval_literal must be exact text")
        _digest(self.approval_literal_sha256, "approval literal SHA-256")
        literal_sha256 = hashlib.sha256(self.approval_literal.encode("utf-8")).hexdigest()
        if self.approval_literal_sha256 != literal_sha256:
            raise IdentityPreviewPlanError("approval literal SHA-256 does not reproduce")
        expected_literal = _detector_preview_approval_literal(
            request_event_id=self.request_event_id,
            request_event_sha256=self.request_event_sha256,
            plan_id=self.plan_id,
            plan_sha256=self.plan_sha256,
            resource_caps_digest=self.resource_caps_digest,
            authorized_action=self.authorized_action,
        )
        if self.approval_literal != expected_literal:
            raise IdentityPreviewPlanError("approval_literal is not the exact canonical literal")
        _safe_text(self.approved_by, "approved_by", maximum=200)
        object.__setattr__(
            self,
            "approved_at_utc",
            _utc_datetime(self.approved_at_utc, "approved_at_utc"),
        )
        _safe_text(self.approval_note, "approval_note", maximum=1_000, allow_empty=True)
        if self.decision != "approved":
            raise IdentityPreviewPlanError("detector preview plan receipt must be approved")
        if self.approval_stage != DETECTOR_PREVIEW_APPROVAL_STAGE:
            raise IdentityPreviewPlanError(
                "detector preview approval cannot use schema, full-run, or publish stage"
            )
        if self.authorized_action != DETECTOR_PREVIEW_AUTHORIZED_ACTION:
            raise IdentityPreviewPlanError("detector preview approval action is too broad")
        if self.execution_scope != DETECTOR_PREVIEW_SCOPE:
            raise IdentityPreviewPlanError("detector preview approval scope is too broad")

    @classmethod
    def create(
        cls,
        request: S7DetectorPreviewApprovalRequest,
        stored_request: StoredIdentityPreviewDocument,
        *,
        approval_literal: str,
        approved_by: str,
        approved_at_utc: datetime,
        approval_note: str = "",
    ) -> S7DetectorPreviewPlanApproval:
        if not isinstance(request, S7DetectorPreviewApprovalRequest):
            raise IdentityPreviewPlanError("approval request has the wrong concrete type")
        _verify_stored_document(stored_request, request, "stored approval request receipt")
        approved_at = _utc_datetime(approved_at_utc, "approved_at_utc")
        if approved_at <= request.created_at_utc:
            raise IdentityPreviewPlanError("approval request event must predate approval")
        if approval_literal != request.canonical_approval_literal:
            raise IdentityPreviewPlanError("approval_literal is not the exact canonical literal")
        return cls(
            request_event_id=request.request_event_id,
            request_event_path=stored_request.path,
            request_event_sha256=stored_request.sha256,
            plan_id=request.plan_id,
            plan_path=request.plan_path,
            plan_sha256=request.plan_sha256,
            resource_caps_digest=request.resource_caps_digest,
            approval_literal=approval_literal,
            approval_literal_sha256=hashlib.sha256(approval_literal.encode("utf-8")).hexdigest(),
            approved_by=approved_by,
            approved_at_utc=approved_at,
            approval_note=approval_note,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_literal": self.approval_literal,
            "approval_literal_sha256": self.approval_literal_sha256,
            "approval_note": self.approval_note,
            "approval_rule_version": DETECTOR_PREVIEW_APPROVAL_RULE_VERSION,
            "approval_stage": self.approval_stage,
            "approved_at_utc": _utc_text(self.approved_at_utc),
            "approved_by": self.approved_by,
            "artifact_type": "s7_detector_preview_plan_approval",
            "authorized_action": self.authorized_action,
            "decision": self.decision,
            "execution_scope": self.execution_scope,
            "plan_id": self.plan_id,
            "plan_path": self.plan_path,
            "plan_sha256": self.plan_sha256,
            "request_event_id": self.request_event_id,
            "request_event_path": self.request_event_path,
            "request_event_sha256": self.request_event_sha256,
            "resource_caps_digest": self.resource_caps_digest,
            "schema_version": DETECTOR_PREVIEW_APPROVAL_SCHEMA_VERSION,
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
        return detector_preview_plan_approval_path(self.approval_id)

    @classmethod
    def from_dict(cls, value: object) -> S7DetectorPreviewPlanApproval:
        document = _mapping(value, "detector preview plan approval")
        _expect_keys(
            document,
            {
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
                "decision",
                "execution_scope",
                "plan_id",
                "plan_path",
                "plan_sha256",
                "request_event_id",
                "request_event_path",
                "request_event_sha256",
                "resource_caps_digest",
                "schema_version",
            },
            "detector preview plan approval",
        )
        if (
            document["artifact_type"] != "s7_detector_preview_plan_approval"
            or document["schema_version"] != DETECTOR_PREVIEW_APPROVAL_SCHEMA_VERSION
            or document["approval_rule_version"] != DETECTOR_PREVIEW_APPROVAL_RULE_VERSION
        ):
            raise IdentityPreviewPlanError("detector preview approval fixed fields are invalid")
        approval = cls(
            request_event_id=_string(document["request_event_id"], "request_event_id"),
            request_event_path=_string(document["request_event_path"], "request_event_path"),
            request_event_sha256=_string(document["request_event_sha256"], "request_event_sha256"),
            plan_id=_string(document["plan_id"], "approval plan_id"),
            plan_path=_string(document["plan_path"], "approval plan path"),
            plan_sha256=_string(document["plan_sha256"], "approval plan SHA-256"),
            resource_caps_digest=_string(
                document["resource_caps_digest"], "approval resource caps digest"
            ),
            approval_literal=_string(document["approval_literal"], "approval_literal"),
            approval_literal_sha256=_string(
                document["approval_literal_sha256"], "approval_literal_sha256"
            ),
            approved_by=_string(document["approved_by"], "approved_by"),
            approved_at_utc=_parse_utc(document["approved_at_utc"], "approved_at_utc"),
            approval_note=_string(document["approval_note"], "approval_note"),
            decision=_string(document["decision"], "approval decision"),
            approval_stage=_string(document["approval_stage"], "approval_stage"),
            authorized_action=_string(document["authorized_action"], "authorized_action"),
            execution_scope=_string(document["execution_scope"], "execution_scope"),
        )
        if document["approval_id"] != approval.approval_id:
            raise IdentityPreviewPlanError("detector preview approval ID does not reproduce")
        return approval


def detector_preview_plan_approval_path(approval_id: str) -> str:
    _digest(approval_id, "approval_id")
    return (
        "manifests/silver/identity/detector-preview-plan-approvals/"
        f"approval_id={approval_id}/manifest.json"
    )


class IdentityPreviewPlanStore:
    """Strict canonical store for plans, request events, and V2 approvals."""

    def __init__(self, data_root: Path) -> None:
        self.root = data_root.expanduser().resolve()

    def store_ticker_allowlist(self, allowlist: S7TickerAllowlist) -> StoredIdentityPreviewDocument:
        if not isinstance(allowlist, S7TickerAllowlist):
            raise IdentityPreviewPlanError("ticker allowlist has the wrong concrete type")
        return self._write(allowlist.relative_path, allowlist.content)

    def load_ticker_allowlist(
        self,
        ticker_allowlist_id: str,
        *,
        expected_sha256: str,
    ) -> tuple[S7TickerAllowlist, StoredIdentityPreviewDocument]:
        _digest(ticker_allowlist_id, "ticker_allowlist_id")
        document, content, stored = self._read(
            ticker_allowlist_path(ticker_allowlist_id),
            expected_sha256=expected_sha256,
            label="ticker allowlist",
        )
        tickers = document.get("tickers")
        if not isinstance(tickers, list):
            raise IdentityPreviewPlanError("ticker allowlist tickers must be an array")
        values = tuple(tickers)
        allowlist = build_s7_ticker_allowlist(values)
        _expect_keys(
            document,
            {
                "artifact_type",
                "rule_version",
                "schema_version",
                "ticker_allowlist_id",
                "ticker_count",
                "tickers",
            },
            "ticker allowlist",
        )
        if (
            document["artifact_type"] != "s7_detector_preview_ticker_allowlist"
            or document["schema_version"] != TICKER_ALLOWLIST_SCHEMA_VERSION
            or document["rule_version"] != TICKER_ALLOWLIST_RULE_VERSION
            or document["ticker_count"] != len(values)
            or document["ticker_allowlist_id"] != ticker_allowlist_id
            or allowlist.ticker_allowlist_id != ticker_allowlist_id
            or allowlist.content != content
        ):
            raise IdentityPreviewPlanError("ticker allowlist binding does not reproduce")
        return allowlist, stored

    def store_plan(self, plan: S7DetectorPreviewPlan) -> StoredIdentityPreviewDocument:
        if not isinstance(plan, S7DetectorPreviewPlan):
            raise IdentityPreviewPlanError("detector preview plan has the wrong concrete type")
        self._verify_plan_dependencies(plan)
        return self._write(plan.relative_path, plan.content)

    def load_plan(
        self,
        plan_id: str,
        *,
        expected_sha256: str,
    ) -> tuple[S7DetectorPreviewPlan, StoredIdentityPreviewDocument]:
        _digest(plan_id, "plan_id")
        document, content, stored = self._read(
            detector_preview_plan_path(plan_id),
            expected_sha256=expected_sha256,
            label="detector preview plan",
        )
        plan = S7DetectorPreviewPlan.from_dict(document)
        if plan.plan_id != plan_id or plan.content != content:
            raise IdentityPreviewPlanError("detector preview plan path/bytes binding differs")
        self._verify_plan_dependencies(plan)
        return plan, stored

    def store_approval_request(
        self, request: S7DetectorPreviewApprovalRequest
    ) -> StoredIdentityPreviewDocument:
        if not isinstance(request, S7DetectorPreviewApprovalRequest):
            raise IdentityPreviewPlanError(
                "detector preview approval request has the wrong concrete type"
            )
        plan, _ = self.load_plan(request.plan_id, expected_sha256=request.plan_sha256)
        self._verify_approval_request_plan(request, plan)
        return self._write(request.relative_path, request.content)

    def load_approval_request(
        self,
        request_event_id: str,
        *,
        expected_sha256: str,
    ) -> tuple[S7DetectorPreviewApprovalRequest, StoredIdentityPreviewDocument]:
        _digest(request_event_id, "request_event_id")
        document, content, stored = self._read(
            detector_preview_approval_request_path(request_event_id),
            expected_sha256=expected_sha256,
            label="detector preview approval request",
        )
        request = S7DetectorPreviewApprovalRequest.from_dict(document)
        if request.request_event_id != request_event_id or request.content != content:
            raise IdentityPreviewPlanError("approval request path/bytes binding differs")
        plan, _ = self.load_plan(request.plan_id, expected_sha256=request.plan_sha256)
        self._verify_approval_request_plan(request, plan)
        return request, stored

    def store_approval(
        self, approval: S7DetectorPreviewPlanApproval
    ) -> StoredIdentityPreviewDocument:
        if not isinstance(approval, S7DetectorPreviewPlanApproval):
            raise IdentityPreviewPlanError("detector preview approval has the wrong concrete type")
        request, _ = self.load_approval_request(
            approval.request_event_id,
            expected_sha256=approval.request_event_sha256,
        )
        self._verify_approval_request(approval, request)
        return self._write(approval.relative_path, approval.content)

    def load_approval(
        self,
        approval_id: str,
        *,
        expected_sha256: str,
    ) -> tuple[S7DetectorPreviewPlanApproval, StoredIdentityPreviewDocument]:
        _digest(approval_id, "approval_id")
        document, content, stored = self._read(
            detector_preview_plan_approval_path(approval_id),
            expected_sha256=expected_sha256,
            label="detector preview plan approval",
        )
        approval = S7DetectorPreviewPlanApproval.from_dict(document)
        if approval.approval_id != approval_id or approval.content != content:
            raise IdentityPreviewPlanError("detector preview approval path/bytes binding differs")
        request, _ = self.load_approval_request(
            approval.request_event_id,
            expected_sha256=approval.request_event_sha256,
        )
        self._verify_approval_request(approval, request)
        return approval, stored

    def _verify_plan_dependencies(self, plan: S7DetectorPreviewPlan) -> None:
        allowlist, stored = self.load_ticker_allowlist(
            plan.ticker_allowlist_id,
            expected_sha256=plan.ticker_allowlist_sha256,
        )
        if stored.path != plan.ticker_allowlist_path or allowlist.ticker_count != plan.ticker_count:
            raise IdentityPreviewPlanError("plan ticker allowlist binding differs")
        try:
            calendar = load_xnys_calendar_artifact(
                self.root,
                calendar_artifact_id=plan.calendar_artifact_id,
                expected_sha256=plan.calendar_artifact_sha256,
            )
        except (ArtifactError, XNYSCalendarArtifactError) as exc:
            raise IdentityPreviewPlanError("plan calendar binding cannot be verified") from exc
        selected = tuple(
            item.session_date
            for item in calendar.sessions
            if plan.start_session <= item.session_date <= plan.end_session
        )
        if (
            not selected
            or selected[0] != plan.start_session
            or selected[-1] != plan.end_session
            or len(selected) != plan.session_count
        ):
            raise IdentityPreviewPlanError(
                "plan date range/count does not match the exact calendar artifact"
            )

    @staticmethod
    def _verify_approval_request_plan(
        request: S7DetectorPreviewApprovalRequest,
        plan: S7DetectorPreviewPlan,
    ) -> None:
        if (
            request.plan_id != plan.plan_id
            or request.plan_path != plan.relative_path
            or request.plan_sha256 != plan.sha256
            or request.resource_caps_digest != plan.resource_caps.digest
        ):
            raise IdentityPreviewPlanError(
                "approval request does not bind the exact plan and resource caps"
            )
        if request.created_at_utc < plan.created_at_utc:
            raise IdentityPreviewPlanError("approval request cannot predate plan creation")

    @staticmethod
    def _verify_approval_request(
        approval: S7DetectorPreviewPlanApproval,
        request: S7DetectorPreviewApprovalRequest,
    ) -> None:
        if (
            approval.request_event_id != request.request_event_id
            or approval.request_event_path != request.relative_path
            or approval.request_event_sha256 != request.sha256
            or approval.plan_id != request.plan_id
            or approval.plan_path != request.plan_path
            or approval.plan_sha256 != request.plan_sha256
            or approval.resource_caps_digest != request.resource_caps_digest
            or approval.authorized_action != request.authorized_action
            or approval.execution_scope != request.execution_scope
        ):
            raise IdentityPreviewPlanError(
                "approval does not bind the exact request event and plan subject"
            )
        if approval.approval_literal != request.canonical_approval_literal:
            raise IdentityPreviewPlanError("approval literal differs from the exact request event")
        if request.created_at_utc >= approval.approved_at_utc:
            raise IdentityPreviewPlanError("approval request event must predate approval")

    def _write(self, relative: str, content: bytes) -> StoredIdentityPreviewDocument:
        try:
            path = safe_relative_path(self.root, relative)
            stored = write_bytes_immutable(self.root, path, content)
        except ArtifactError as exc:
            raise IdentityPreviewPlanError(str(exc)) from exc
        return StoredIdentityPreviewDocument(
            path=str(stored["path"]),
            sha256=str(stored["sha256"]),
            bytes=int(stored["bytes"]),
        )

    def _read(
        self,
        relative: str,
        *,
        expected_sha256: str,
        label: str,
    ) -> tuple[dict[str, object], bytes, StoredIdentityPreviewDocument]:
        _digest(expected_sha256, f"{label} expected SHA-256")
        try:
            path = safe_relative_path(self.root, relative)
        except ArtifactError as exc:
            raise IdentityPreviewPlanError(str(exc)) from exc
        if not path.is_file() or path.is_symlink():
            raise IdentityPreviewPlanError(f"exact {label} is missing")
        if sha256_file(path) != expected_sha256:
            raise IdentityPreviewPlanError(f"{label} SHA-256 mismatch")
        content = path.read_bytes()
        document = _decode_json(content, label)
        if _canonical_bytes(document) != content:
            raise IdentityPreviewPlanError(f"{label} bytes are not canonical JSON")
        return (
            document,
            content,
            StoredIdentityPreviewDocument(
                path=relative,
                sha256=expected_sha256,
                bytes=len(content),
            ),
        )


def _verify_stored_document(
    stored: StoredIdentityPreviewDocument,
    artifact: S7DetectorPreviewPlan | S7DetectorPreviewApprovalRequest,
    label: str,
) -> None:
    if not isinstance(stored, StoredIdentityPreviewDocument):
        raise IdentityPreviewPlanError(f"{label} has the wrong concrete type")
    if (
        stored.path != artifact.relative_path
        or stored.sha256 != artifact.sha256
        or stored.bytes != len(artifact.content)
    ):
        raise IdentityPreviewPlanError(f"{label} does not match the exact artifact")


def _detector_preview_approval_literal(
    *,
    request_event_id: str,
    request_event_sha256: str,
    plan_id: str,
    plan_sha256: str,
    resource_caps_digest: str,
    authorized_action: str,
) -> str:
    """Return the one byte-for-byte literal a reviewer must approve."""

    literal = {
        "authorized_action": authorized_action,
        "literal_version": DETECTOR_PREVIEW_APPROVAL_LITERAL_VERSION,
        "plan_id": plan_id,
        "plan_sha256": plan_sha256,
        "request_event_id": request_event_id,
        "request_event_sha256": request_event_sha256,
        "resource_caps_digest": resource_caps_digest,
    }
    return json.dumps(literal, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _ticker_allowlist_document(tickers: tuple[str, ...]) -> dict[str, object]:
    logical: dict[str, object] = {
        "artifact_type": "s7_detector_preview_ticker_allowlist",
        "rule_version": TICKER_ALLOWLIST_RULE_VERSION,
        "schema_version": TICKER_ALLOWLIST_SCHEMA_VERSION,
        "ticker_count": len(tickers),
        "tickers": list(tickers),
    }
    return {**logical, "ticker_allowlist_id": stable_digest(logical)}


def _validate_tickers(tickers: tuple[object, ...]) -> None:
    if not tickers:
        raise IdentityPreviewPlanError("ticker allowlist cannot be empty")
    if len(tickers) > MAX_PREVIEW_TICKERS:
        raise IdentityPreviewPlanError("ticker allowlist exceeds the hard preview limit")
    if any(not isinstance(item, str) for item in tickers):
        raise IdentityPreviewPlanError("ticker allowlist values must be strings")
    values = tuple(str(item) for item in tickers)
    if values != tuple(sorted(set(values))):
        raise IdentityPreviewPlanError(
            "ticker allowlist must already be exact sorted unique case-sensitive text"
        )
    for ticker in values:
        if (
            not ticker
            or len(ticker) > 64
            or ticker.strip() != ticker
            or ticker in {"*", ".*"}
            or any(ord(character) < 32 or ord(character) == 127 for character in ticker)
        ):
            raise IdentityPreviewPlanError("ticker allowlist contains unsafe ticker text")


def _canonical_bytes(document: Mapping[str, object]) -> bytes:
    return (
        json.dumps(dict(document), allow_nan=False, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        + b"\n"
    )


def _decode_json(content: bytes, label: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in pairs:
            if key in output:
                raise IdentityPreviewPlanError(f"{label} contains duplicate JSON keys")
            output[key] = value
        return output

    try:
        value = json.loads(content, object_pairs_hook=reject_duplicates)
    except IdentityPreviewPlanError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityPreviewPlanError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise IdentityPreviewPlanError(f"{label} must be a JSON object")
    return value


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise IdentityPreviewPlanError(f"{label} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise IdentityPreviewPlanError(f"{label} keys must be strings")
    return dict(value)


def _expect_keys(document: Mapping[str, object], expected: set[str], label: str) -> None:
    actual = set(document)
    if actual != expected:
        raise IdentityPreviewPlanError(
            f"{label} schema is not exact: missing={sorted(expected - actual)}, "
            f"extra={sorted(actual - expected)}"
        )


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,127}", value):
        raise IdentityPreviewPlanError(f"{label} is invalid")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise IdentityPreviewPlanError(f"{label} must be a lowercase SHA-256")
    return value


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IdentityPreviewPlanError(f"{label} must be a positive native int")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IdentityPreviewPlanError(f"{label} must be a nonnegative native int")
    return value


def _native_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise IdentityPreviewPlanError(f"{label} must be a native bool")
    return value


def _native_date(value: object, label: str) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise IdentityPreviewPlanError(f"{label} must be a date")
    return value


def _parse_date(value: object, label: str) -> date:
    if not isinstance(value, str):
        raise IdentityPreviewPlanError(f"{label} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise IdentityPreviewPlanError(f"{label} must be an ISO date") from exc
    if parsed.isoformat() != value:
        raise IdentityPreviewPlanError(f"{label} is not canonical")
    return parsed


def _utc_datetime(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityPreviewPlanError(f"{label} must be timezone-aware")
    if value.utcoffset().total_seconds() != 0:
        raise IdentityPreviewPlanError(f"{label} must use UTC")
    return value.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    return _utc_datetime(value, "UTC timestamp").isoformat()


def _parse_utc(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise IdentityPreviewPlanError(f"{label} must be an ISO UTC datetime")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IdentityPreviewPlanError(f"{label} must be an ISO UTC datetime") from exc
    normalized = _utc_datetime(parsed, label)
    if normalized.isoformat() != value:
        raise IdentityPreviewPlanError(f"{label} is not canonical UTC text")
    return normalized


def _safe_text(value: object, label: str, *, maximum: int, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or len(value) > maximum or (not value and not allow_empty):
        raise IdentityPreviewPlanError(f"{label} is invalid")
    if value.strip() != value or any(
        ord(character) < 32 and character not in "\t" for character in value
    ):
        raise IdentityPreviewPlanError(f"{label} contains unsafe whitespace")
    lowered = value.casefold()
    if any(token in lowered for token in _SENSITIVE_TOKENS):
        raise IdentityPreviewPlanError(f"{label} may contain sensitive material")
    return value


def _string(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityPreviewPlanError(f"{label} must be text")
    return value


def _relative_path(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityPreviewPlanError(f"{label} must be text")
    path = Path(value)
    if path.is_absolute() or path.as_posix() != value or ".." in path.parts:
        raise IdentityPreviewPlanError(f"{label} must be a normalized relative path")
    return value


_EXACT_SOURCE_PINS: Final[tuple[S7PreviewSourcePin, ...]] = tuple(
    S7PreviewSourcePin.from_source_pin(S7_SOURCE_PINS[table]) for table in sorted(S7_SOURCE_PINS)
)


__all__ = [
    "DETECTOR_PREVIEW_APPROVAL_STAGE",
    "DETECTOR_PREVIEW_AUTHORIZED_ACTION",
    "DETECTOR_PREVIEW_SCOPE",
    "MAX_ASSET_PARENT_SCANNED_ROWS",
    "MAX_BATCH_SIZE",
    "MAX_CASES",
    "MAX_PREVIEW_SESSIONS",
    "MAX_PREVIEW_TICKERS",
    "MAX_SELECTED_ROWS",
    "MAX_SOURCE_ARTIFACTS",
    "MAX_SOURCE_BYTES",
    "MAX_TOTAL_SCANNED_ROWS",
    "MAX_UNIVERSE_SCANNED_ROWS",
    "IdentityPreviewPlanError",
    "IdentityPreviewPlanStore",
    "S7DetectorPreviewApprovalRequest",
    "S7DetectorPreviewPlan",
    "S7DetectorPreviewPlanApproval",
    "S7DetectorPreviewResourceCaps",
    "S7PreviewSourcePin",
    "S7TickerAllowlist",
    "StoredIdentityPreviewDocument",
    "build_s7_ticker_allowlist",
    "detector_preview_approval_request_path",
    "detector_preview_plan_approval_path",
    "detector_preview_plan_path",
    "ticker_allowlist_path",
]
