"""Create only an immutable S7 detector-preview plan and approval request.

This orchestration boundary deliberately has no approval or detector execution
capability.  It verifies an exact clean checkout, constructs every document in
memory, preflights every destination, and only then writes and reads back the
calendar, ticker allowlist, plan, and human approval-request event.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from datetime import UTC, date, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Final

from ame_stocks_api.artifacts import ArtifactError, safe_relative_path, sha256_file
from ame_stocks_api.silver.calendar_artifact import (
    XNYSCalendarArtifact,
    build_xnys_calendar_artifact,
    load_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.contracts import ArtifactRef, ArtifactRole
from ame_stocks_api.silver.identity_preview_plan import (
    IdentityPreviewPlanError,
    IdentityPreviewPlanStore,
    S7DetectorPreviewApprovalRequest,
    S7DetectorPreviewPlan,
    S7DetectorPreviewResourceCaps,
    S7TickerAllowlist,
    StoredIdentityPreviewDocument,
    build_s7_ticker_allowlist,
)
from ame_stocks_api.silver.identity_source import S7_SOURCE_PINS
from ame_stocks_api.silver.store import SilverStore, SilverStoreError

FULL_XNYS_CALENDAR_START: Final = date(2016, 7, 11)
FULL_XNYS_CALENDAR_END: Final = date(2026, 12, 31)

_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SESSION_PARTITION = re.compile(r"(?:^|/)session_date=(\d{4}-\d{2}-\d{2})(?:/|$)")
_REQUIRED_EXCHANGE_CALENDARS_VERSION: Final = "4.13.2"
_PREVIEW_SOURCE_TABLES: Final = (
    "asset_observation_daily",
    "universe_source_daily",
)
_TRACKED_ENTRYPOINTS: Final = (
    "pyproject.toml",
    "backend/ame_stocks_api/artifacts.py",
    "backend/ame_stocks_api/silver/calendar_artifact.py",
    "backend/ame_stocks_api/silver/contracts.py",
    "backend/ame_stocks_api/silver/identity_preview_plan.py",
    "backend/ame_stocks_api/silver/identity_preview_request.py",
    "backend/ame_stocks_api/silver/identity_source.py",
    "backend/ame_stocks_api/silver/store.py",
    "backend/ame_stocks_api/cli/silver_identity_preview_request.py",
)


@dataclass(frozen=True, slots=True)
class S7DetectorPreviewRequestRun:
    """Verified receipts for one plan/request-only orchestration run."""

    calendar: XNYSCalendarArtifact
    calendar_document: StoredIdentityPreviewDocument
    ticker_allowlist: S7TickerAllowlist
    ticker_allowlist_document: StoredIdentityPreviewDocument
    plan: S7DetectorPreviewPlan
    plan_document: StoredIdentityPreviewDocument
    approval_request: S7DetectorPreviewApprovalRequest
    approval_request_document: StoredIdentityPreviewDocument
    selected_sources: tuple[S7SelectedPreviewSource, ...]
    all_documents_preexisting: bool


@dataclass(frozen=True, slots=True, order=True)
class S7SelectedPreviewSource:
    """Manifest-only metadata for one exact daily source selected by the plan."""

    table: str
    session_date: date
    release_id: str
    release_manifest_sha256: str
    path: str
    sha256: str
    bytes: int
    row_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "path": self.path,
            "release_id": self.release_id,
            "release_manifest_sha256": self.release_manifest_sha256,
            "row_count": self.row_count,
            "session_date": self.session_date.isoformat(),
            "sha256": self.sha256,
            "table": self.table,
        }


def create_s7_detector_preview_request(
    data_root: Path,
    *,
    repo_root: Path,
    git_commit: str,
    recorded_at: str,
    plan_created_by: str,
    request_created_by: str,
    tickers: tuple[str, ...],
    expected_ticker_count: int,
    start_session: date,
    end_session: date,
    expected_session_count: int,
    resource_caps: S7DetectorPreviewResourceCaps,
) -> S7DetectorPreviewRequestRun:
    """Write only the four exact control artifacts after a complete preflight."""

    root = _data_root(data_root)
    _verify_git_checkout(repo_root, git_commit)
    instant = _recorded_at(recorded_at)
    _require_exchange_calendars_version()
    calendar = build_xnys_calendar_artifact(
        FULL_XNYS_CALENDAR_START,
        FULL_XNYS_CALENDAR_END,
    )
    selected_sessions = _selected_sessions(
        calendar,
        start_session=start_session,
        end_session=end_session,
        expected_session_count=expected_session_count,
    )
    allowlist = build_s7_ticker_allowlist(tickers)
    if type(expected_ticker_count) is not int or expected_ticker_count <= 0:
        raise IdentityPreviewPlanError("expected_ticker_count must be a positive native int")
    if allowlist.ticker_count != expected_ticker_count:
        raise IdentityPreviewPlanError(
            "expected_ticker_count differs from the exact ticker allowlist"
        )
    if not isinstance(resource_caps, S7DetectorPreviewResourceCaps):
        raise IdentityPreviewPlanError("resource_caps has the wrong concrete type")
    theoretical_selected_rows = len(selected_sessions) * allowlist.ticker_count
    if resource_caps.selected_row_cap != theoretical_selected_rows:
        raise IdentityPreviewPlanError(
            "selected_row_cap must exactly equal session_count times ticker_count"
        )
    selected_sources = _preflight_source_manifests(
        root,
        sessions=selected_sessions,
        resource_caps=resource_caps,
    )

    plan = S7DetectorPreviewPlan.create(
        created_by=plan_created_by,
        created_at_utc=instant,
        git_commit=git_commit,
        calendar_artifact_id=calendar.calendar_artifact_id,
        calendar_artifact_sha256=calendar.sha256,
        start_session=selected_sessions[0],
        end_session=selected_sessions[-1],
        session_count=len(selected_sessions),
        ticker_allowlist=allowlist,
        resource_caps=resource_caps,
    )
    prospective_plan_receipt = StoredIdentityPreviewDocument(
        path=plan.relative_path,
        sha256=plan.sha256,
        bytes=len(plan.content),
    )
    request = S7DetectorPreviewApprovalRequest.create(
        plan,
        prospective_plan_receipt,
        created_by=request_created_by,
        created_at_utc=instant,
    )

    documents = (
        (calendar.relative_path, calendar.content),
        (allowlist.relative_path, allowlist.content),
        (plan.relative_path, plan.content),
        (request.relative_path, request.content),
    )
    preexisting = tuple(
        _preflight_immutable_document(root, relative_path, content)
        for relative_path, content in documents
    )

    calendar_stored = _stored_document(
        write_xnys_calendar_artifact(root, calendar),
        expected_path=calendar.relative_path,
        expected_sha256=calendar.sha256,
        expected_bytes=len(calendar.content),
        label="calendar",
    )
    store = IdentityPreviewPlanStore(root)
    allowlist_stored = store.store_ticker_allowlist(allowlist)
    plan_stored = store.store_plan(plan)
    request_stored = store.store_approval_request(request)

    loaded_calendar = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=calendar.calendar_artifact_id,
        expected_sha256=calendar.sha256,
    )
    loaded_allowlist, loaded_allowlist_stored = store.load_ticker_allowlist(
        allowlist.ticker_allowlist_id,
        expected_sha256=allowlist.sha256,
    )
    loaded_plan, loaded_plan_stored = store.load_plan(
        plan.plan_id,
        expected_sha256=plan.sha256,
    )
    loaded_request, loaded_request_stored = store.load_approval_request(
        request.request_event_id,
        expected_sha256=request.sha256,
    )
    if loaded_calendar != calendar:
        raise IdentityPreviewPlanError("calendar readback differs from preflight bytes")
    if loaded_allowlist != allowlist or loaded_allowlist_stored != allowlist_stored:
        raise IdentityPreviewPlanError("ticker allowlist readback differs from write receipt")
    if loaded_plan != plan or loaded_plan_stored != plan_stored:
        raise IdentityPreviewPlanError("detector preview plan readback differs from write receipt")
    if loaded_request != request or loaded_request_stored != request_stored:
        raise IdentityPreviewPlanError("approval request readback differs from write receipt")

    return S7DetectorPreviewRequestRun(
        calendar=calendar,
        calendar_document=calendar_stored,
        ticker_allowlist=allowlist,
        ticker_allowlist_document=allowlist_stored,
        plan=plan,
        plan_document=plan_stored,
        approval_request=request,
        approval_request_document=request_stored,
        selected_sources=selected_sources,
        all_documents_preexisting=all(preexisting),
    )


def _data_root(value: Path) -> Path:
    if not isinstance(value, Path):
        raise IdentityPreviewPlanError("data_root must be a Path")
    expanded = value.expanduser()
    if expanded.is_symlink():
        raise IdentityPreviewPlanError("data_root cannot be a symlink")
    root = expanded.resolve()
    if not root.is_dir():
        raise IdentityPreviewPlanError("data_root must be an existing directory")
    return root


def _recorded_at(value: str) -> datetime:
    if not isinstance(value, str):
        raise IdentityPreviewPlanError("recorded_at must be canonical UTC text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IdentityPreviewPlanError("recorded_at must be canonical UTC text") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IdentityPreviewPlanError("recorded_at must be timezone-aware UTC text")
    normalized = parsed.astimezone(UTC)
    if parsed.utcoffset().total_seconds() != 0 or normalized.isoformat() != value:
        raise IdentityPreviewPlanError("recorded_at must be canonical UTC text")
    if normalized > datetime.now(UTC):
        raise IdentityPreviewPlanError("recorded_at cannot be in the future")
    return normalized


def _selected_sessions(
    calendar: XNYSCalendarArtifact,
    *,
    start_session: date,
    end_session: date,
    expected_session_count: int,
) -> tuple[date, ...]:
    if (
        not isinstance(start_session, date)
        or isinstance(start_session, datetime)
        or not isinstance(end_session, date)
        or isinstance(end_session, datetime)
    ):
        raise IdentityPreviewPlanError("preview session bounds must be native dates")
    if start_session > end_session:
        raise IdentityPreviewPlanError("start_session cannot follow end_session")
    if type(expected_session_count) is not int or expected_session_count <= 0:
        raise IdentityPreviewPlanError("expected_session_count must be a positive native int")
    selected = tuple(
        item.session_date
        for item in calendar.sessions
        if start_session <= item.session_date <= end_session
    )
    if (
        not selected
        or selected[0] != start_session
        or selected[-1] != end_session
        or len(selected) != expected_session_count
    ):
        raise IdentityPreviewPlanError(
            "preview session bounds/count differ from the full frozen XNYS calendar"
        )
    return selected


def _require_exchange_calendars_version() -> None:
    try:
        installed = version("exchange-calendars")
    except PackageNotFoundError as exc:
        raise IdentityPreviewPlanError("exchange-calendars is not installed") from exc
    if installed != _REQUIRED_EXCHANGE_CALENDARS_VERSION:
        raise IdentityPreviewPlanError(
            "exchange-calendars must be exactly 4.13.2 for the frozen S7 calendar"
        )


def _preflight_source_manifests(
    root: Path,
    *,
    sessions: tuple[date, ...],
    resource_caps: S7DetectorPreviewResourceCaps,
) -> tuple[S7SelectedPreviewSource, ...]:
    """Read only two exact release manifests and reconcile their selected metadata."""

    store = SilverStore(root)
    selected: list[S7SelectedPreviewSource] = []
    for table in _PREVIEW_SOURCE_TABLES:
        pin = S7_SOURCE_PINS[table]
        try:
            release, stored = store.load_release(pin.release_id)
            expected_manifest_path = (
                f"manifests/silver/releases/release_id={pin.release_id}.json"
            )
            manifest_path = safe_relative_path(root, expected_manifest_path)
        except (ArtifactError, OSError, SilverStoreError) as exc:
            raise IdentityPreviewPlanError(
                f"cannot load exact pinned release manifest for {table}"
            ) from exc
        if (
            stored.path != expected_manifest_path
            or stored.sha256 != pin.release_manifest_sha256
            or not manifest_path.is_file()
            or manifest_path.is_symlink()
            or sha256_file(manifest_path) != pin.release_manifest_sha256
            or release.release_id != pin.release_id
            or release.table != table
            or release.build_id != pin.build_id
            or len(release.outputs) != pin.artifact_count
            or sum(item.row_count or 0 for item in release.outputs) != pin.row_count
        ):
            raise IdentityPreviewPlanError(
                f"exact pinned release manifest metadata differs for {table}"
            )
        by_session: dict[date, ArtifactRef] = {}
        for output in release.outputs:
            match = _SESSION_PARTITION.search(output.path)
            if (
                match is None
                or output.table != table
                or output.role is not ArtifactRole.DATA
                or output.media_type != "application/vnd.apache.parquet"
                or output.row_count is None
            ):
                raise IdentityPreviewPlanError(
                    f"pinned daily release has invalid manifest metadata for {table}"
                )
            session = date.fromisoformat(match.group(1))
            if session in by_session:
                raise IdentityPreviewPlanError(
                    f"pinned daily release has duplicate sessions for {table}"
                )
            by_session[session] = output
        for session in sessions:
            output = by_session.get(session)
            if output is None:
                raise IdentityPreviewPlanError(
                    f"pinned daily release is missing selected sessions for {table}"
                )
            selected.append(
                S7SelectedPreviewSource(
                    table=table,
                    session_date=session,
                    release_id=pin.release_id,
                    release_manifest_sha256=pin.release_manifest_sha256,
                    path=output.path,
                    sha256=output.sha256,
                    bytes=output.bytes,
                    row_count=output.row_count,
                )
            )
    result = tuple(sorted(selected))
    asset_rows = sum(
        item.row_count for item in result if item.table == "asset_observation_daily"
    )
    universe_rows = sum(
        item.row_count for item in result if item.table == "universe_source_daily"
    )
    if resource_caps.asset_parent_scanned_row_cap != asset_rows:
        raise IdentityPreviewPlanError(
            "asset_parent_scanned_row_cap differs from selected release metadata"
        )
    if resource_caps.universe_scanned_row_cap != universe_rows:
        raise IdentityPreviewPlanError(
            "universe_scanned_row_cap differs from selected release metadata"
        )
    if resource_caps.total_scanned_row_cap != asset_rows + universe_rows:
        raise IdentityPreviewPlanError(
            "total_scanned_row_cap differs from selected release metadata"
        )
    if resource_caps.source_artifact_cap != len(result):
        raise IdentityPreviewPlanError(
            "source_artifact_cap differs from selected release metadata"
        )
    if resource_caps.source_bytes_cap != sum(item.bytes for item in result):
        raise IdentityPreviewPlanError(
            "source_bytes_cap differs from selected release metadata"
        )
    return result


def _verify_git_checkout(repo_root: Path, git_commit: str) -> None:
    if not isinstance(repo_root, Path):
        raise IdentityPreviewPlanError("repo_root must be a Path")
    if not isinstance(git_commit, str) or not _GIT_COMMIT.fullmatch(git_commit):
        raise IdentityPreviewPlanError("git_commit must be an exact lowercase 40-hex commit")
    root = repo_root.expanduser().resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise IdentityPreviewPlanError(
            "S7 plan/request code is not executing from repo_root"
        ) from exc
    if module_relative != "backend/ame_stocks_api/silver/identity_preview_request.py":
        raise IdentityPreviewPlanError("S7 plan/request module path is not canonical")
    try:
        top = _git_output(root, "rev-parse", "--show-toplevel")
        head = _git_output(root, "rev-parse", "HEAD")
        status = _git_output(root, "status", "--porcelain=v1", "--untracked-files=all")
        tracked = tuple(
            _git_output(root, "ls-files", "--error-unmatch", "--", relative)
            for relative in _TRACKED_ENTRYPOINTS
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise IdentityPreviewPlanError("cannot verify the S7 plan/request Git checkout") from exc
    if Path(top).resolve() != root:
        raise IdentityPreviewPlanError("repo_root is not the exact Git top level")
    if head != git_commit:
        raise IdentityPreviewPlanError("Git HEAD differs from the exact requested commit")
    if status:
        raise IdentityPreviewPlanError("S7 plan/request Git checkout is not clean")
    if tracked != _TRACKED_ENTRYPOINTS:
        raise IdentityPreviewPlanError("S7 plan/request entrypoints are not exactly tracked")


def _git_output(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return completed.stdout.strip()


def _preflight_immutable_document(root: Path, relative: str, content: bytes) -> bool:
    try:
        target = safe_relative_path(root, relative)
    except ArtifactError as exc:
        raise IdentityPreviewPlanError(str(exc)) from exc
    current = root
    for part in target.parent.relative_to(root).parts:
        current /= part
        if current.exists() and not current.is_dir():
            raise IdentityPreviewPlanError(
                f"immutable artifact parent is not a directory: {current.relative_to(root)}"
            )
    if not target.exists():
        return False
    if target.is_symlink() or not target.is_file():
        raise IdentityPreviewPlanError(
            f"immutable artifact target is not a regular file: {relative}"
        )
    if target.read_bytes() != content:
        raise IdentityPreviewPlanError(
            f"immutable artifact target has conflicting bytes: {relative}"
        )
    return True


def _stored_document(
    value: dict[str, object],
    *,
    expected_path: str,
    expected_sha256: str,
    expected_bytes: int,
    label: str,
) -> StoredIdentityPreviewDocument:
    if not {"path", "sha256", "bytes"} <= set(value):
        raise IdentityPreviewPlanError(f"{label} write receipt is incomplete")
    stored = StoredIdentityPreviewDocument(
        path=str(value["path"]),
        sha256=str(value["sha256"]),
        bytes=int(value["bytes"]),
    )
    if (
        stored.path != expected_path
        or stored.sha256 != expected_sha256
        or stored.bytes != expected_bytes
    ):
        raise IdentityPreviewPlanError(f"{label} write receipt differs from preflight")
    return stored


__all__ = [
    "FULL_XNYS_CALENDAR_END",
    "FULL_XNYS_CALENDAR_START",
    "S7DetectorPreviewRequestRun",
    "S7SelectedPreviewSource",
    "create_s7_detector_preview_request",
]
