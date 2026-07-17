"""Freeze only the S7 schema/evidence receipt and Gate-A inventory request.

The orchestration in this module is deliberately control-plane only.  It
revalidates every pinned Git, schema, evidence, release, preview-lineage and
calendar input before constructing three canonical JSON documents in memory.
Only after every source and destination passes preflight are those documents
written immutably and read back.  No Parquet data, network client, detector,
inventory runner, adjudication path, table materializer or publisher is used.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import UTC, date, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from types import MappingProxyType
from typing import Final

from ame_stocks_api.artifacts import ArtifactError, safe_relative_path, sha256_file, stable_digest
from ame_stocks_api.silver.calendar_artifact import (
    XNYSCalendarArtifact,
    build_xnys_calendar_artifact,
    load_xnys_calendar_artifact,
)
from ame_stocks_api.silver.contracts import ArtifactRole, SilverContractError, TableContract
from ame_stocks_api.silver.identity_market_inventory_plan import (
    APPROVAL_TEXT,
    APPROVAL_TEXT_SHA256,
    APPROVED_CONTRACT_PINS,
    APPROVED_SUBJECT_GIT_COMMIT,
    APPROVED_SUBJECT_GIT_TREE,
    DAILY_SOURCE_ARTIFACT_COUNT,
    DAILY_SOURCE_BYTES,
    DAILY_SOURCE_ROW_COUNT,
    EXACT_SOURCE_PINS,
    EXTERNAL_EVIDENCE_MANIFEST_ID,
    EXTERNAL_EVIDENCE_MANIFEST_PATH,
    EXTERNAL_EVIDENCE_MANIFEST_SHA256,
    INVENTORY_CALENDAR_ARTIFACT_ID,
    INVENTORY_CALENDAR_ARTIFACT_SHA256,
    INVENTORY_END_SESSION,
    INVENTORY_SESSION_COUNT,
    INVENTORY_START_SESSION,
    PREVIEW_APPROVAL_ID,
    PREVIEW_ARTIFACT_ID,
    PREVIEW_ARTIFACT_SHA256,
    PREVIEW_CASE_COUNT,
    PREVIEW_CASE_EVIDENCE_SET_DIGEST,
    PREVIEW_COMPLETION_ID,
    PREVIEW_COMPLETION_SHA256,
    PREVIEW_PLAN_ID,
    PREVIEW_SUSPECTED_ROW_COUNT,
    IdentityMarketInventoryPlanError,
    IdentityMarketInventoryPlanStore,
    S7CompositeInventoryApprovalRequest,
    S7CompositeInventoryPlan,
    S7SchemaEvidenceApprovalBundle,
    StoredIdentityMarketInventoryDocument,
)
from ame_stocks_api.silver.identity_preview_runner import S7DetectorPreviewCompletion
from ame_stocks_api.silver.identity_provider_evidence import (
    S4BounceProviderEvidenceManifest,
)
from ame_stocks_api.silver.identity_resolution_contract import S7_CONTRACTS
from ame_stocks_api.silver.identity_source import (
    S7_S4_RELEASE_SET_ID,
    S7_S4_RELEASE_SET_MANIFEST_SHA256,
    S7_SIX_RELEASE_BINDING_ID,
)
from ame_stocks_api.silver.identity_streaming_preview import (
    BoundedIdentityPreviewArtifact,
)
from ame_stocks_api.silver.store import SilverStore, SilverStoreError

FULL_XNYS_CALENDAR_START: Final = date(2016, 7, 11)
FULL_XNYS_CALENDAR_END: Final = date(2026, 12, 31)
REQUIRED_EXCHANGE_CALENDARS_VERSION: Final = "4.13.2"

PRIOR_PREVIEW_PLAN_ID: Final = PREVIEW_PLAN_ID
PRIOR_PREVIEW_APPROVAL_ID: Final = PREVIEW_APPROVAL_ID
PRIOR_PREVIEW_COMPLETION_PATH: Final = (
    "manifests/silver/identity/detector-preview-completions/"
    f"plan_id={PRIOR_PREVIEW_PLAN_ID}/approval_id={PRIOR_PREVIEW_APPROVAL_ID}/manifest.json"
)
PRIOR_PREVIEW_PATH: Final = (
    "manifests/silver/identity-bounce-bounded-previews/"
    f"preview_artifact_id={PREVIEW_ARTIFACT_ID}/manifest.json"
)
S4_RELEASE_SET_PATH: Final = (
    "manifests/silver/release-sets/assets/"
    f"release_set_id={S7_S4_RELEASE_SET_ID}/manifest.json"
)

_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SESSION_PARTITION = re.compile(r"(?:^|/)session_date=(\d{4}-\d{2}-\d{2})(?:/|$)")
_DAILY_TABLES: Final = frozenset({"asset_observation_daily", "universe_source_daily"})
_RESOURCE_PATH_BY_TABLE: Final = MappingProxyType(
    {
        pin.table: (
            "backend/ame_stocks_api/silver/schema_resources/"
            f"{pin.table}.schema-v1.json"
        )
        for pin in APPROVED_CONTRACT_PINS
    }
)
_TRACKED_RUNTIME_PATHS: Final = (
    "pyproject.toml",
    "backend/ame_stocks_api/artifacts.py",
    "backend/ame_stocks_api/cli/silver_identity_market_inventory_request.py",
    "backend/ame_stocks_api/silver/calendar_artifact.py",
    "backend/ame_stocks_api/silver/contracts.py",
    "backend/ame_stocks_api/silver/identity_market_inventory_plan.py",
    "backend/ame_stocks_api/silver/identity_market_inventory_request.py",
    "backend/ame_stocks_api/silver/identity_preview_runner.py",
    "backend/ame_stocks_api/silver/identity_provider_evidence.py",
    "backend/ame_stocks_api/silver/identity_resolution_contract.py",
    "backend/ame_stocks_api/silver/identity_source.py",
    "backend/ame_stocks_api/silver/identity_streaming_preview.py",
    "backend/ame_stocks_api/silver/store.py",
)


@dataclass(frozen=True, slots=True)
class S7ReleaseManifestPreflight:
    """Manifest-only reconciliation of the six exact published parents."""

    release_manifest_count: int
    daily_artifact_count: int
    daily_row_count: int
    daily_source_bytes: int


@dataclass(frozen=True, slots=True)
class S7PreviewLineagePreflight:
    """Exact persisted preview/completion/provider-evidence lineage summary."""

    completion_path: str
    provider_evidence_manifest_count: int
    case_evidence_set_digest: str


@dataclass(frozen=True, slots=True)
class S7MarketInventoryRequestRun:
    """Read-back verified receipts from one control-only orchestration run."""

    calendar: XNYSCalendarArtifact
    schema_approval: S7SchemaEvidenceApprovalBundle
    schema_approval_document: StoredIdentityMarketInventoryDocument
    plan: S7CompositeInventoryPlan
    plan_document: StoredIdentityMarketInventoryDocument
    approval_request: S7CompositeInventoryApprovalRequest
    approval_request_document: StoredIdentityMarketInventoryDocument
    releases: S7ReleaseManifestPreflight
    preview_lineage: S7PreviewLineagePreflight
    all_documents_preexisting: bool


def create_s7_market_inventory_request(
    data_root: Path,
    *,
    repo_root: Path,
    git_commit: str,
    recorded_at: str,
    approval_recorded_by: str,
    plan_created_by: str,
    request_created_by: str,
    approval_text_sha256: str,
) -> S7MarketInventoryRequestRun:
    """Preflight all exact inputs, then write only bundle, plan and request JSON."""

    root = _data_root(data_root)
    repository = _verify_git_checkout(repo_root, git_commit)
    instant = _recorded_at(recorded_at)
    if approval_text_sha256 != APPROVAL_TEXT_SHA256:
        raise IdentityMarketInventoryPlanError(
            "approval text SHA-256 does not match the exact user authorization"
        )

    tracked_subject_paths = _preflight_schema_and_external_evidence(repository)
    _verify_approved_subject_paths(repository, tracked_subject_paths)
    calendar = _load_exact_existing_calendar(root)
    releases = _preflight_release_manifests(root, calendar)
    preview_lineage = _preflight_existing_preview_lineage(root, calendar)

    approval = S7SchemaEvidenceApprovalBundle.create(
        recorded_by=approval_recorded_by,
        recorded_at_utc=instant,
        exact_approval_text=APPROVAL_TEXT,
    )
    prospective_approval_receipt = StoredIdentityMarketInventoryDocument(
        approval.relative_path,
        approval.sha256,
        len(approval.content),
    )
    plan = S7CompositeInventoryPlan.create(
        created_by=plan_created_by,
        created_at_utc=instant,
        git_commit=git_commit,
        approval=approval,
        approval_receipt=prospective_approval_receipt,
        calendar_artifact_id=calendar.calendar_artifact_id,
        calendar_artifact_sha256=calendar.sha256,
    )
    prospective_plan_receipt = StoredIdentityMarketInventoryDocument(
        plan.relative_path,
        plan.sha256,
        len(plan.content),
    )
    request = S7CompositeInventoryApprovalRequest.create(
        plan,
        prospective_plan_receipt,
        created_by=request_created_by,
        created_at_utc=instant,
    )

    documents = (
        (approval.relative_path, approval.content),
        (plan.relative_path, plan.content),
        (request.relative_path, request.content),
    )
    preexisting = tuple(
        _preflight_immutable_document(root, relative_path, content)
        for relative_path, content in documents
    )

    store = IdentityMarketInventoryPlanStore(root)
    approval_stored = store.store_schema_evidence_bundle(approval)
    plan_stored = store.store_plan(plan)
    request_stored = store.store_approval_request(request)

    loaded_approval, loaded_approval_stored = store.load_schema_evidence_bundle(
        approval.approval_id,
        expected_sha256=approval.sha256,
    )
    loaded_plan, loaded_plan_stored = store.load_plan(
        plan.plan_id,
        expected_sha256=plan.sha256,
    )
    loaded_request, loaded_request_stored = store.load_approval_request(
        request.request_event_id,
        expected_sha256=request.sha256,
    )
    if loaded_approval != approval or loaded_approval_stored != approval_stored:
        raise IdentityMarketInventoryPlanError("schema/evidence approval readback differs")
    if loaded_plan != plan or loaded_plan_stored != plan_stored:
        raise IdentityMarketInventoryPlanError("Composite inventory plan readback differs")
    if loaded_request != request or loaded_request_stored != request_stored:
        raise IdentityMarketInventoryPlanError("inventory approval request readback differs")

    return S7MarketInventoryRequestRun(
        calendar=calendar,
        schema_approval=approval,
        schema_approval_document=approval_stored,
        plan=plan,
        plan_document=plan_stored,
        approval_request=request,
        approval_request_document=request_stored,
        releases=releases,
        preview_lineage=preview_lineage,
        all_documents_preexisting=all(preexisting),
    )


def _data_root(value: Path) -> Path:
    if not isinstance(value, Path):
        raise IdentityMarketInventoryPlanError("data_root must be a Path")
    expanded = value.expanduser()
    if expanded.is_symlink():
        raise IdentityMarketInventoryPlanError("data_root cannot be a symlink")
    root = expanded.resolve()
    if not root.is_dir():
        raise IdentityMarketInventoryPlanError("data_root must be an existing directory")
    return root


def _recorded_at(value: str) -> datetime:
    if not isinstance(value, str):
        raise IdentityMarketInventoryPlanError("recorded_at must be canonical UTC text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IdentityMarketInventoryPlanError(
            "recorded_at must be canonical UTC text"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IdentityMarketInventoryPlanError("recorded_at must be timezone-aware UTC text")
    normalized = parsed.astimezone(UTC)
    if parsed.utcoffset().total_seconds() != 0 or normalized.isoformat() != value:
        raise IdentityMarketInventoryPlanError("recorded_at must be canonical UTC text")
    if normalized > datetime.now(UTC):
        raise IdentityMarketInventoryPlanError("recorded_at cannot be in the future")
    return normalized


def _verify_git_checkout(repo_root: Path, git_commit: str) -> Path:
    if not isinstance(repo_root, Path):
        raise IdentityMarketInventoryPlanError("repo_root must be a Path")
    if not isinstance(git_commit, str) or not _GIT_COMMIT.fullmatch(git_commit):
        raise IdentityMarketInventoryPlanError(
            "git_commit must be an exact lowercase 40-hex commit"
        )
    expanded = repo_root.expanduser()
    if expanded.is_symlink():
        raise IdentityMarketInventoryPlanError("repo_root cannot be a symlink")
    root = expanded.resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise IdentityMarketInventoryPlanError(
            "inventory request code is not executing from repo_root"
        ) from exc
    if module_relative != _TRACKED_RUNTIME_PATHS[6]:
        raise IdentityMarketInventoryPlanError("inventory request module path is not canonical")
    if Path(_git_output(root, "rev-parse", "--show-toplevel")).resolve() != root:
        raise IdentityMarketInventoryPlanError("repo_root is not the exact Git top level")
    if _git_output(root, "rev-parse", "HEAD") != git_commit:
        raise IdentityMarketInventoryPlanError("Git HEAD differs from the exact requested commit")
    if _git_output(root, "status", "--porcelain=v1", "--untracked-files=all"):
        raise IdentityMarketInventoryPlanError("inventory request Git checkout is not clean")
    for relative in _TRACKED_RUNTIME_PATHS:
        _git_output(root, "ls-files", "--error-unmatch", "--", relative)
    if _git_output(root, "rev-parse", f"{APPROVED_SUBJECT_GIT_COMMIT}^{{tree}}") != (
        APPROVED_SUBJECT_GIT_TREE
    ):
        raise IdentityMarketInventoryPlanError("approved subject Git tree cannot be reproduced")
    return root


def _preflight_schema_and_external_evidence(repo: Path) -> tuple[str, ...]:
    tracked: list[str] = []
    for pin in APPROVED_CONTRACT_PINS:
        candidate = _read_regular_repo_file(repo, pin.candidate_path)
        resource_path = _RESOURCE_PATH_BY_TABLE[pin.table]
        resource = _read_regular_repo_file(repo, resource_path)
        if hashlib.sha256(candidate).hexdigest() != pin.candidate_sha256:
            raise IdentityMarketInventoryPlanError(
                f"{pin.table} candidate SHA-256 differs from the approved bytes"
            )
        if resource != candidate:
            raise IdentityMarketInventoryPlanError(
                f"packaged {pin.table} resource differs from the approved candidate"
            )
        try:
            raw_contract = json.loads(candidate)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise IdentityMarketInventoryPlanError(
                f"{pin.table} candidate is not valid JSON"
            ) from exc
        contract = TableContract.from_dict(raw_contract)
        if (
            contract != S7_CONTRACTS.get(pin.table)
            or contract.table != pin.table
            or contract.domain != pin.domain
            or contract.schema_version != 1
            or contract.contract_id != pin.contract_id
            or contract.schema_digest != pin.schema_digest
        ):
            raise IdentityMarketInventoryPlanError(
                f"{pin.table} candidate contract identity differs from approval"
            )
        tracked.extend((pin.candidate_path, resource_path))

    manifest_content = _read_regular_repo_file(repo, EXTERNAL_EVIDENCE_MANIFEST_PATH)
    if hashlib.sha256(manifest_content).hexdigest() != EXTERNAL_EVIDENCE_MANIFEST_SHA256:
        raise IdentityMarketInventoryPlanError("external evidence manifest SHA-256 differs")
    try:
        manifest = json.loads(manifest_content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityMarketInventoryPlanError(
            "external evidence manifest is not valid JSON"
        ) from exc
    if not isinstance(manifest, dict):
        raise IdentityMarketInventoryPlanError("external evidence manifest must be an object")
    logical = {key: value for key, value in manifest.items() if key != "manifest_id"}
    detector = manifest.get("detector_preview_binding")
    if (
        manifest.get("manifest_id") != EXTERNAL_EVIDENCE_MANIFEST_ID
        or stable_digest(logical) != EXTERNAL_EVIDENCE_MANIFEST_ID
        or manifest.get("manifest_status") != "candidate_not_approved"
    ):
        raise IdentityMarketInventoryPlanError(
            "external evidence manifest identity or status differs"
        )

    if manifest.get("source_six_release_binding_id") != S7_SIX_RELEASE_BINDING_ID:
        raise IdentityMarketInventoryPlanError("external evidence source binding differs")
    expected_detector = {
        "completion_id": PREVIEW_COMPLETION_ID,
        "identity_case_count": PREVIEW_CASE_COUNT,
        "preview_id": PREVIEW_ARTIFACT_ID,
        "preview_rewritten": False,
        "suspected_row_count": PREVIEW_SUSPECTED_ROW_COUNT,
    }
    if detector != expected_detector:
        raise IdentityMarketInventoryPlanError("external evidence preview binding differs")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 25:
        raise IdentityMarketInventoryPlanError("external evidence artifact set is incomplete")
    evidence_paths: list[str] = []
    for receipt in artifacts:
        if not isinstance(receipt, dict):
            raise IdentityMarketInventoryPlanError("external evidence receipt is invalid")
        relative = receipt.get("path")
        expected_sha = receipt.get("sha256")
        expected_bytes = receipt.get("bytes")
        if (
            not isinstance(relative, str)
            or not isinstance(expected_sha, str)
            or not isinstance(expected_bytes, int)
        ):
            raise IdentityMarketInventoryPlanError("external evidence receipt fields are invalid")
        content = _read_regular_repo_file(repo, relative)
        if len(content) != expected_bytes or hashlib.sha256(content).hexdigest() != expected_sha:
            raise IdentityMarketInventoryPlanError(
                f"external evidence bytes differ from manifest: {relative}"
            )
        evidence_paths.append(relative)
    if len(set(evidence_paths)) != 25:
        raise IdentityMarketInventoryPlanError("external evidence manifest repeats a path")
    tracked.extend((EXTERNAL_EVIDENCE_MANIFEST_PATH, *evidence_paths))
    return tuple(sorted(set(tracked)))


def _verify_approved_subject_paths(repo: Path, relative_paths: tuple[str, ...]) -> None:
    for relative in relative_paths:
        _git_output(repo, "ls-files", "--error-unmatch", "--", relative)
    completed = subprocess.run(
        (
            "git",
            "-C",
            str(repo),
            "diff",
            "--quiet",
            APPROVED_SUBJECT_GIT_COMMIT,
            "--",
            *relative_paths,
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        raise IdentityMarketInventoryPlanError(
            "approved schema/evidence bytes differ from their subject Git commit"
        )


def _load_exact_existing_calendar(root: Path) -> XNYSCalendarArtifact:
    try:
        installed = version("exchange-calendars")
    except PackageNotFoundError as exc:
        raise IdentityMarketInventoryPlanError("exchange-calendars is not installed") from exc
    if installed != REQUIRED_EXCHANGE_CALENDARS_VERSION:
        raise IdentityMarketInventoryPlanError(
            f"exchange-calendars must be exactly {REQUIRED_EXCHANGE_CALENDARS_VERSION}"
        )
    expected = build_xnys_calendar_artifact(
        FULL_XNYS_CALENDAR_START,
        FULL_XNYS_CALENDAR_END,
    )
    if (
        expected.calendar_artifact_id != INVENTORY_CALENDAR_ARTIFACT_ID
        or expected.sha256 != INVENTORY_CALENDAR_ARTIFACT_SHA256
        or len(expected.sessions) != 2_635
    ):
        raise IdentityMarketInventoryPlanError(
            "computed XNYS calendar differs from the exact reviewed binding"
        )
    loaded = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=expected.calendar_artifact_id,
        expected_sha256=expected.sha256,
    )
    if loaded != expected:
        raise IdentityMarketInventoryPlanError("existing XNYS calendar differs from exact bytes")
    sessions = tuple(
        item.session_date
        for item in loaded.sessions
        if INVENTORY_START_SESSION <= item.session_date <= INVENTORY_END_SESSION
    )
    if (
        len(sessions) != INVENTORY_SESSION_COUNT
        or sessions[0] != INVENTORY_START_SESSION
        or sessions[-1] != INVENTORY_END_SESSION
    ):
        raise IdentityMarketInventoryPlanError("inventory session spine differs from calendar")
    return loaded


def _preflight_release_manifests(
    root: Path,
    calendar: XNYSCalendarArtifact,
) -> S7ReleaseManifestPreflight:
    release_set = safe_relative_path(root, S4_RELEASE_SET_PATH)
    if (
        not release_set.is_file()
        or release_set.is_symlink()
        or sha256_file(release_set) != S7_S4_RELEASE_SET_MANIFEST_SHA256
    ):
        raise IdentityMarketInventoryPlanError("S4 release-set marker differs from exact pin")

    expected_sessions = {
        item.session_date
        for item in calendar.sessions
        if INVENTORY_START_SESSION <= item.session_date <= INVENTORY_END_SESSION
    }
    daily_artifacts = 0
    daily_rows = 0
    daily_bytes = 0
    store = SilverStore(root)
    for pin in EXACT_SOURCE_PINS:
        try:
            release, stored = store.load_release(pin.release_id)
        except (OSError, SilverContractError, SilverStoreError) as exc:
            raise IdentityMarketInventoryPlanError(
                f"cannot load exact release manifest for {pin.table}"
            ) from exc
        expected_path = f"manifests/silver/releases/release_id={pin.release_id}.json"
        physical_path = safe_relative_path(root, expected_path)
        outputs = tuple(release.outputs)
        if (
            stored.path != expected_path
            or stored.sha256 != pin.release_manifest_sha256
            or not physical_path.is_file()
            or physical_path.is_symlink()
            or sha256_file(physical_path) != pin.release_manifest_sha256
            or release.release_id != pin.release_id
            or release.table != pin.table
            or release.build_id != pin.build_id
            or len(outputs) != pin.artifact_count
            or sum(item.row_count or 0 for item in outputs) != pin.row_count
        ):
            raise IdentityMarketInventoryPlanError(
                f"exact release manifest metadata differs for {pin.table}"
            )
        if any(
            item.table != pin.table
            or item.role is not ArtifactRole.DATA
            or item.media_type != "application/vnd.apache.parquet"
            or item.row_count is None
            for item in outputs
        ):
            raise IdentityMarketInventoryPlanError(
                f"release manifest has non-DATA or uncounted output for {pin.table}"
            )
        if pin.table not in _DAILY_TABLES:
            continue
        sessions: set[date] = set()
        for output in outputs:
            match = _SESSION_PARTITION.search(output.path)
            if match is None:
                raise IdentityMarketInventoryPlanError(
                    f"daily release output lacks session partition for {pin.table}"
                )
            session = date.fromisoformat(match.group(1))
            if session in sessions:
                raise IdentityMarketInventoryPlanError(
                    f"daily release repeats a session for {pin.table}"
                )
            sessions.add(session)
        if sessions != expected_sessions:
            raise IdentityMarketInventoryPlanError(
                f"daily release session coverage differs for {pin.table}"
            )
        daily_artifacts += len(outputs)
        daily_rows += sum(item.row_count or 0 for item in outputs)
        daily_bytes += sum(item.bytes for item in outputs)

    if (
        daily_artifacts != DAILY_SOURCE_ARTIFACT_COUNT
        or daily_rows != DAILY_SOURCE_ROW_COUNT
        or daily_bytes != DAILY_SOURCE_BYTES
    ):
        raise IdentityMarketInventoryPlanError(
            "daily release totals differ from exact 5026/138757511/15910278169"
        )
    return S7ReleaseManifestPreflight(
        release_manifest_count=len(EXACT_SOURCE_PINS),
        daily_artifact_count=daily_artifacts,
        daily_row_count=daily_rows,
        daily_source_bytes=daily_bytes,
    )


def _preflight_existing_preview_lineage(
    root: Path,
    calendar: XNYSCalendarArtifact,
) -> S7PreviewLineagePreflight:
    completion_content = _read_exact_data_file(
        root,
        PRIOR_PREVIEW_COMPLETION_PATH,
        PREVIEW_COMPLETION_SHA256,
    )
    completion = S7DetectorPreviewCompletion.from_dict(
        _decode_json(completion_content, "completion")
    )
    if (
        completion.content != completion_content
        or completion.completion_id != PREVIEW_COMPLETION_ID
        or completion.sha256 != PREVIEW_COMPLETION_SHA256
        or completion.relative_path != PRIOR_PREVIEW_COMPLETION_PATH
        or completion.plan_id != PRIOR_PREVIEW_PLAN_ID
        or completion.approval_id != PRIOR_PREVIEW_APPROVAL_ID
        or completion.preview_artifact_id != PREVIEW_ARTIFACT_ID
        or completion.preview_artifact_path != PRIOR_PREVIEW_PATH
        or completion.preview_artifact_sha256 != PREVIEW_ARTIFACT_SHA256
        or completion.case_count != PREVIEW_CASE_COUNT
        or completion.suspected_provider_figi_bounce_rows != PREVIEW_SUSPECTED_ROW_COUNT
        or completion.case_evidence_set_digest != PREVIEW_CASE_EVIDENCE_SET_DIGEST
        or completion.calendar_artifact_id != calendar.calendar_artifact_id
        or completion.calendar_artifact_sha256 != calendar.sha256
    ):
        raise IdentityMarketInventoryPlanError("existing detector completion lineage differs")

    preview_content = _read_exact_data_file(root, PRIOR_PREVIEW_PATH, PREVIEW_ARTIFACT_SHA256)
    preview = BoundedIdentityPreviewArtifact(
        preview_artifact_id=PREVIEW_ARTIFACT_ID,
        sha256=PREVIEW_ARTIFACT_SHA256,
        content=preview_content,
        document=MappingProxyType(_decode_json(preview_content, "preview artifact")),
    )
    if preview.relative_path != PRIOR_PREVIEW_PATH:
        raise IdentityMarketInventoryPlanError("existing preview path differs")

    evidence_refs = tuple(completion.case_evidence)
    if len(evidence_refs) != PREVIEW_CASE_COUNT:
        raise IdentityMarketInventoryPlanError("completion provider evidence set is incomplete")
    for ref in evidence_refs:
        content = _read_exact_data_file(root, ref.path, ref.sha256)
        if len(content) != ref.bytes:
            raise IdentityMarketInventoryPlanError(
                f"provider evidence byte count differs: {ref.path}"
            )
        manifest = S4BounceProviderEvidenceManifest.from_dict(
            _decode_json(content, f"provider evidence {ref.identity_case_id}")
        )
        case_id = manifest.case_snapshot.get("identity_case_id")
        if (
            manifest.content != content
            or manifest.manifest_id != ref.manifest_id
            or manifest.relative_path != ref.path
            or case_id != ref.identity_case_id
            or manifest.plan_id != PRIOR_PREVIEW_PLAN_ID
            or manifest.approval_id != PRIOR_PREVIEW_APPROVAL_ID
            or manifest.preview_artifact_id != PREVIEW_ARTIFACT_ID
            or manifest.preview_artifact_sha256 != PREVIEW_ARTIFACT_SHA256
            or manifest.availability_calendar_id != calendar.calendar_artifact_id
            or manifest.availability_calendar_sha256 != calendar.sha256
        ):
            raise IdentityMarketInventoryPlanError(
                f"provider evidence trust chain differs: {ref.path}"
            )
    return S7PreviewLineagePreflight(
        completion_path=PRIOR_PREVIEW_COMPLETION_PATH,
        provider_evidence_manifest_count=len(evidence_refs),
        case_evidence_set_digest=completion.case_evidence_set_digest,
    )


def _preflight_immutable_document(root: Path, relative: str, content: bytes) -> bool:
    try:
        target = safe_relative_path(root, relative)
    except ArtifactError as exc:
        raise IdentityMarketInventoryPlanError(str(exc)) from exc
    current = root
    for part in target.parent.relative_to(root).parts:
        current /= part
        if current.exists() and not current.is_dir():
            raise IdentityMarketInventoryPlanError(
                f"immutable artifact parent is not a directory: {current.relative_to(root)}"
            )
        if current.is_symlink():
            raise IdentityMarketInventoryPlanError(
                f"immutable artifact parent cannot be a symlink: {current.relative_to(root)}"
            )
    if not target.exists():
        return False
    if target.is_symlink() or not target.is_file():
        raise IdentityMarketInventoryPlanError(
            f"immutable artifact target is not a regular file: {relative}"
        )
    if target.read_bytes() != content:
        raise IdentityMarketInventoryPlanError(
            f"immutable artifact target has conflicting bytes: {relative}"
        )
    return True


def _read_regular_repo_file(repo: Path, relative: str) -> bytes:
    try:
        path = safe_relative_path(repo, relative)
    except ArtifactError as exc:
        raise IdentityMarketInventoryPlanError(str(exc)) from exc
    if not path.is_file() or path.is_symlink():
        raise IdentityMarketInventoryPlanError(f"tracked repository file is unsafe: {relative}")
    return path.read_bytes()


def _read_exact_data_file(root: Path, relative: str, expected_sha256: str) -> bytes:
    try:
        path = safe_relative_path(root, relative)
    except ArtifactError as exc:
        raise IdentityMarketInventoryPlanError(str(exc)) from exc
    if not path.is_file() or path.is_symlink():
        raise IdentityMarketInventoryPlanError(f"exact control evidence is missing: {relative}")
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != expected_sha256:
        raise IdentityMarketInventoryPlanError(f"exact control evidence SHA differs: {relative}")
    return content


def _decode_json(content: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityMarketInventoryPlanError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise IdentityMarketInventoryPlanError(f"{label} must be a JSON object")
    return value


def _git_output(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown Git error"
        raise IdentityMarketInventoryPlanError(
            f"cannot verify inventory-request Git checkout: {detail}"
        )
    return completed.stdout.strip()


__all__ = [
    "FULL_XNYS_CALENDAR_END",
    "FULL_XNYS_CALENDAR_START",
    "PRIOR_PREVIEW_APPROVAL_ID",
    "PRIOR_PREVIEW_COMPLETION_PATH",
    "PRIOR_PREVIEW_PLAN_ID",
    "S7MarketInventoryRequestRun",
    "S7PreviewLineagePreflight",
    "S7ReleaseManifestPreflight",
    "create_s7_market_inventory_request",
]
