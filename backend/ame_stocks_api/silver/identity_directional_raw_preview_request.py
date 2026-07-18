"""Local-only preflight helpers for the S7 directional raw-preview request.

The orchestration in this module is intentionally restricted to a clean local
Git checkout and immutable JSON control documents.  It has no Parquet, network,
approval, runner, registry, adjudication, materialization, or publication path.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.contracts import SilverContractError, TableContract
from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
    DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_directional_raw_preview_plan import (
    AUTHORIZED_ACTION,
    REQUIRED_PREPARATION_RUNTIME_PATHS,
    REQUIRED_PREPARATION_VERIFICATION_PATHS,
    IdentityDirectionalRawPreviewPlanStore,
    S7DirectionalRawPreviewControlFilePin,
    S7DirectionalRawPreviewPreparationPlan,
    S7DirectionalRawPreviewScopeSet,
    StoredDirectionalRawPreviewControl,
    directional_raw_preview_plan_path,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


CONTRACT_CANDIDATE_PATH = (
    "docs/silver/contracts/identity/identity_directional_raw_preview_slot.schema-v1.candidate.json"
)
CONTRACT_RESOURCE_PATH = (
    "backend/ame_stocks_api/silver/schema_resources/"
    "identity_directional_raw_preview_slot.schema-v1.json"
)
REQUEST_SCHEMA_VERSION = 1
REQUEST_RULE_VERSION = "s7_directional_raw_preview_preparation_request_v1"
REQUEST_LITERAL_VERSION = "s7_directional_raw_preview_preparation_approval_literal_v1"
REQUEST_STATE = "awaiting_literal_human_approval"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class IdentityDirectionalRawPreviewRequestError(RuntimeError):
    """Raised when local request controls cannot be frozen safely."""


@dataclass(frozen=True, slots=True)
class RepositoryFilePin:
    """Exact tracked-file identity used to bind a future execution request."""

    path: str
    git_blob: str
    sha256: str
    bytes: int

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "git_blob": self.git_blob,
            "path": self.path,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class DirectionalRawPreviewContractPin:
    contract_id: str
    schema_digest: str
    candidate_sha256: str
    resource_sha256: str

    def __post_init__(self) -> None:
        if (
            self.contract_id != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID
            or self.schema_digest != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST
            or self.candidate_sha256 != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256
            or self.resource_sha256 != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256
        ):
            raise IdentityDirectionalRawPreviewRequestError(
                "directional raw-preview contract pin changed"
            )

    def to_dict(self) -> dict[str, str]:
        return {
            "candidate_sha256": self.candidate_sha256,
            "contract_id": self.contract_id,
            "resource_sha256": self.resource_sha256,
            "schema_digest": self.schema_digest,
        }


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewPreparationRequest:
    """Human review request for the non-executing preparation Plan only."""

    plan_id: str
    plan_path: str
    plan_sha256: str
    scope_set_id: str
    scope_set_sha256: str
    input_binding_digest: str
    resource_caps_digest: str
    runtime_file_set_digest: str
    verification_file_set_digest: str
    preparation_design_digest: str
    registry_semantics_digest: str
    contract_id: str
    contract_schema_digest: str
    contract_candidate_sha256: str
    created_by: str
    created_at_utc: datetime
    authorized_action: str = AUTHORIZED_ACTION
    request_state: str = REQUEST_STATE

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("scope-set ID", self.scope_set_id),
            ("scope-set SHA-256", self.scope_set_sha256),
            ("input binding digest", self.input_binding_digest),
            ("resource caps digest", self.resource_caps_digest),
            ("runtime file-set digest", self.runtime_file_set_digest),
            ("verification file-set digest", self.verification_file_set_digest),
            ("preparation design digest", self.preparation_design_digest),
            ("registry semantics digest", self.registry_semantics_digest),
            ("contract ID", self.contract_id),
            ("contract schema digest", self.contract_schema_digest),
            ("contract candidate SHA-256", self.contract_candidate_sha256),
        ):
            if not isinstance(value, str) or not _DIGEST.fullmatch(value):
                raise IdentityDirectionalRawPreviewRequestError(f"{label} must be lowercase 64-hex")
        if self.plan_path != directional_raw_preview_request_plan_path(self.plan_id):
            raise IdentityDirectionalRawPreviewRequestError("request plan path is not canonical")
        if (
            self.contract_id != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID
            or self.contract_schema_digest != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST
            or self.contract_candidate_sha256
            != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256
            or self.registry_semantics_digest
            != DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
        ):
            raise IdentityDirectionalRawPreviewRequestError(
                "request contract or registry binding changed"
            )
        if (
            not isinstance(self.created_by, str)
            or not self.created_by
            or self.created_by.strip() != self.created_by
            or len(self.created_by) > 200
        ):
            raise IdentityDirectionalRawPreviewRequestError("request created_by is unsafe")
        object.__setattr__(
            self,
            "created_at_utc",
            _validate_utc_datetime(self.created_at_utc, "request created_at_utc"),
        )
        if self.authorized_action != AUTHORIZED_ACTION or self.request_state != REQUEST_STATE:
            raise IdentityDirectionalRawPreviewRequestError(
                "non-executing request boundary changed"
            )

    @classmethod
    def create(
        cls,
        plan: S7DirectionalRawPreviewPreparationPlan,
        plan_receipt: StoredDirectionalRawPreviewControl,
        *,
        contract: DirectionalRawPreviewContractPin,
        created_by: str,
        created_at_utc: datetime,
    ) -> S7DirectionalRawPreviewPreparationRequest:
        if not isinstance(plan, S7DirectionalRawPreviewPreparationPlan):
            raise IdentityDirectionalRawPreviewRequestError("request plan has wrong type")
        if (
            plan_receipt.path != plan.relative_path
            or plan_receipt.sha256 != plan.sha256
            or plan_receipt.bytes != len(plan.content)
        ):
            raise IdentityDirectionalRawPreviewRequestError("request plan receipt differs")
        created = _validate_utc_datetime(created_at_utc, "request created_at_utc")
        if created < plan.created_at_utc:
            raise IdentityDirectionalRawPreviewRequestError("request predates plan")
        return cls(
            plan_id=plan.plan_id,
            plan_path=plan_receipt.path,
            plan_sha256=plan_receipt.sha256,
            scope_set_id=plan.scope_set_id,
            scope_set_sha256=plan.scope_set_sha256,
            input_binding_digest=plan.input_binding_digest,
            resource_caps_digest=plan.resource_caps.digest,
            runtime_file_set_digest=plan.runtime_file_set_digest,
            verification_file_set_digest=plan.verification_file_set_digest,
            preparation_design_digest=plan.preparation_design_digest,
            registry_semantics_digest=(
                DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
            ),
            contract_id=contract.contract_id,
            contract_schema_digest=contract.schema_digest,
            contract_candidate_sha256=contract.candidate_sha256,
            created_by=created_by,
            created_at_utc=created,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_directional_raw_preview_preparation_request",
            "authorization_flags": {
                "adjudication": False,
                "approval_receipt_creation": False,
                "canonical_identity_materialization": False,
                "data_read": False,
                "exact_group_history_read": False,
                "external_evidence_capture": False,
                "forced_liquidation": False,
                "full_run": False,
                "inventory_rerun": False,
                "materialization": False,
                "network_access": False,
                "parquet_read": False,
                "preview_execution": False,
                "publication": False,
                "registry_evaluation": False,
                "s5_s6_identity_confirmation": False,
            },
            "authorized_action": self.authorized_action,
            "contract_candidate_sha256": self.contract_candidate_sha256,
            "contract_id": self.contract_id,
            "contract_schema_digest": self.contract_schema_digest,
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "input_binding_digest": self.input_binding_digest,
            "plan_id": self.plan_id,
            "plan_path": self.plan_path,
            "plan_sha256": self.plan_sha256,
            "preparation_design_digest": self.preparation_design_digest,
            "request_rule_version": REQUEST_RULE_VERSION,
            "request_state": self.request_state,
            "resource_caps_digest": self.resource_caps_digest,
            "runtime_file_set_digest": self.runtime_file_set_digest,
            "registry_semantics_digest": self.registry_semantics_digest,
            "schema_version": REQUEST_SCHEMA_VERSION,
            "scope_set_id": self.scope_set_id,
            "scope_set_sha256": self.scope_set_sha256,
            "verification_file_set_digest": self.verification_file_set_digest,
        }

    @property
    def request_event_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> MappingProxyType[str, object]:
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
        return directional_raw_preview_request_path(self.request_event_id)

    @property
    def canonical_approval_literal(self) -> str:
        return json.dumps(
            {
                "authorized_action": self.authorized_action,
                "contract_candidate_sha256": self.contract_candidate_sha256,
                "contract_id": self.contract_id,
                "contract_schema_digest": self.contract_schema_digest,
                "input_binding_digest": self.input_binding_digest,
                "literal_version": REQUEST_LITERAL_VERSION,
                "plan_id": self.plan_id,
                "plan_sha256": self.plan_sha256,
                "preparation_design_digest": self.preparation_design_digest,
                "request_event_id": self.request_event_id,
                "request_event_sha256": self.sha256,
                "resource_caps_digest": self.resource_caps_digest,
                "runtime_file_set_digest": self.runtime_file_set_digest,
                "registry_semantics_digest": self.registry_semantics_digest,
                "scope_set_id": self.scope_set_id,
                "scope_set_sha256": self.scope_set_sha256,
                "verification_file_set_digest": self.verification_file_set_digest,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewPreparationRequest:
        if not isinstance(value, dict):
            raise IdentityDirectionalRawPreviewRequestError("request must be an object")
        request = cls(
            plan_id=_required_text(value, "plan_id"),
            plan_path=_required_text(value, "plan_path"),
            plan_sha256=_required_text(value, "plan_sha256"),
            scope_set_id=_required_text(value, "scope_set_id"),
            scope_set_sha256=_required_text(value, "scope_set_sha256"),
            input_binding_digest=_required_text(value, "input_binding_digest"),
            resource_caps_digest=_required_text(value, "resource_caps_digest"),
            runtime_file_set_digest=_required_text(value, "runtime_file_set_digest"),
            verification_file_set_digest=_required_text(value, "verification_file_set_digest"),
            preparation_design_digest=_required_text(value, "preparation_design_digest"),
            registry_semantics_digest=_required_text(value, "registry_semantics_digest"),
            contract_id=_required_text(value, "contract_id"),
            contract_schema_digest=_required_text(value, "contract_schema_digest"),
            contract_candidate_sha256=_required_text(value, "contract_candidate_sha256"),
            created_by=_required_text(value, "created_by"),
            created_at_utc=_parse_utc_text(value.get("created_at_utc")),
            authorized_action=_required_text(value, "authorized_action"),
            request_state=_required_text(value, "request_state"),
        )
        if _canonical_bytes(value) != request.content:
            raise IdentityDirectionalRawPreviewRequestError(
                "request does not reproduce canonical bytes"
            )
        return request


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewRequestRun:
    scope: S7DirectionalRawPreviewScopeSet
    scope_document: StoredDirectionalRawPreviewControl
    plan: S7DirectionalRawPreviewPreparationPlan
    plan_document: StoredDirectionalRawPreviewControl
    request: S7DirectionalRawPreviewPreparationRequest
    request_document: StoredDirectionalRawPreviewControl
    contract: DirectionalRawPreviewContractPin
    git_tree: str
    all_documents_preexisting: bool


def create_s7_directional_raw_preview_request(
    control_root: Path,
    *,
    repo_root: Path,
    git_commit: str,
    recorded_at: str,
    scope_created_by: str,
    plan_created_by: str,
    request_created_by: str,
) -> S7DirectionalRawPreviewRequestRun:
    """Preflight everything, then write exactly scope, Plan, and Request JSON."""

    instant = parse_nonfuture_utc(recorded_at)
    _require_separate_actors(
        scope_created_by,
        plan_created_by,
        request_created_by,
    )
    repository, git_tree = verify_exact_clean_checkout(repo_root, git_commit)
    contract = verify_directional_raw_preview_contract_bytes(repository)
    runtime_files = _adapt_file_pins(
        pin_tracked_files(repository, REQUIRED_PREPARATION_RUNTIME_PATHS),
        S7DirectionalRawPreviewControlFilePin,
    )
    verification_files = _adapt_file_pins(
        pin_tracked_files(repository, REQUIRED_PREPARATION_VERIFICATION_PATHS),
        S7DirectionalRawPreviewControlFilePin,
    )
    store = IdentityDirectionalRawPreviewPlanStore(control_root)
    scope = S7DirectionalRawPreviewScopeSet.create(
        created_by=scope_created_by,
        created_at_utc=instant,
    )
    prospective_scope = StoredDirectionalRawPreviewControl(
        scope.relative_path,
        scope.sha256,
        len(scope.content),
    )
    plan = S7DirectionalRawPreviewPreparationPlan.create(
        created_by=plan_created_by,
        created_at_utc=instant,
        git_commit=git_commit,
        git_tree=git_tree,
        runtime_files=runtime_files,
        verification_files=verification_files,
        scope=scope,
        stored_scope=prospective_scope,
    )
    prospective_plan = StoredDirectionalRawPreviewControl(
        plan.relative_path,
        plan.sha256,
        len(plan.content),
    )
    request = S7DirectionalRawPreviewPreparationRequest.create(
        plan,
        prospective_plan,
        contract=contract,
        created_by=request_created_by,
        created_at_utc=instant,
    )

    destinations = (
        (scope.relative_path, scope.sha256),
        (plan.relative_path, plan.sha256),
        (request.relative_path, request.sha256),
    )
    preexisting = tuple(
        preflight_destination(store.root, relative, checksum) for relative, checksum in destinations
    )
    scope_document = store.store_scope(scope)
    plan_document = store.store_plan(plan)
    request_document = _store_request(store.root, request)
    loaded_scope, loaded_scope_document = store.load_scope(
        scope.scope_set_id,
        expected_sha256=scope.sha256,
    )
    loaded_plan, loaded_plan_document = store.load_plan(
        plan.plan_id,
        expected_sha256=plan.sha256,
    )
    loaded_request, loaded_request_document = _load_request(
        store.root,
        request.request_event_id,
        expected_sha256=request.sha256,
    )
    if (
        loaded_scope != scope
        or loaded_scope_document != scope_document
        or loaded_plan != plan
        or loaded_plan_document != plan_document
        or loaded_request != request
        or loaded_request_document != request_document
    ):
        raise IdentityDirectionalRawPreviewRequestError(
            "directional raw-preview controls differ on immutable readback"
        )
    return S7DirectionalRawPreviewRequestRun(
        scope=scope,
        scope_document=scope_document,
        plan=plan,
        plan_document=plan_document,
        request=request,
        request_document=request_document,
        contract=contract,
        git_tree=git_tree,
        all_documents_preexisting=all(preexisting),
    )


def verify_directional_raw_preview_contract_bytes(
    repo_root: Path,
) -> DirectionalRawPreviewContractPin:
    """Replay candidate/resource bytes without opening any data artifact."""

    candidate_path = repo_root / CONTRACT_CANDIDATE_PATH
    resource_path = repo_root / CONTRACT_RESOURCE_PATH
    if any(
        not path.is_file() or path.is_symlink() or path.resolve() != path
        for path in (candidate_path, resource_path)
    ):
        raise IdentityDirectionalRawPreviewRequestError(
            "directional raw-preview contract bytes are unavailable or unsafe"
        )
    candidate_bytes = candidate_path.read_bytes()
    resource_bytes = resource_path.read_bytes()
    if candidate_bytes != resource_bytes:
        raise IdentityDirectionalRawPreviewRequestError(
            "directional raw-preview candidate and resource bytes differ"
        )
    checksum = hashlib.sha256(candidate_bytes).hexdigest()
    try:
        parsed = TableContract.from_dict(json.loads(candidate_bytes))
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        SilverContractError,
        TypeError,
        ValueError,
    ) as exc:
        raise IdentityDirectionalRawPreviewRequestError(
            "directional raw-preview contract cannot be parsed"
        ) from exc
    if (
        checksum != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256
        or parsed != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT
        or parsed.contract_id != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID
        or parsed.schema_digest != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST
    ):
        raise IdentityDirectionalRawPreviewRequestError(
            "directional raw-preview contract identity changed"
        )
    return DirectionalRawPreviewContractPin(
        contract_id=parsed.contract_id,
        schema_digest=parsed.schema_digest,
        candidate_sha256=checksum,
        resource_sha256=checksum,
    )


def verify_exact_clean_checkout(
    repo_root: Path,
    expected_commit: str,
) -> tuple[Path, str]:
    """Require one exact clean checkout and return its tree object ID."""

    expanded = repo_root.expanduser()
    if expanded.is_symlink():
        raise IdentityDirectionalRawPreviewRequestError("repo_root is unsafe")
    repository = expanded.resolve()
    if not repository.is_dir():
        raise IdentityDirectionalRawPreviewRequestError("repo_root is unsafe")
    try:
        top = Path(_git(repository, "rev-parse", "--show-toplevel")).resolve()
        head = _git(repository, "rev-parse", "HEAD")
        tree = _git(repository, "rev-parse", "HEAD^{tree}")
        status = _git(repository, "status", "--porcelain", "--untracked-files=all")
    except (OSError, subprocess.SubprocessError) as exc:
        raise IdentityDirectionalRawPreviewRequestError(
            "directional raw-preview Git checkout cannot be verified"
        ) from exc
    if top != repository or head != expected_commit or status:
        raise IdentityDirectionalRawPreviewRequestError(
            "directional raw-preview checkout is dirty, displaced, or at the wrong commit"
        )
    return repository, tree


def pin_tracked_files(
    repo_root: Path,
    relative_paths: Iterable[str],
) -> tuple[RepositoryFilePin, ...]:
    """Pin exact HEAD blobs and working bytes for a fixed path set."""

    selected = tuple(relative_paths)
    if any(not isinstance(relative, str) for relative in selected):
        raise IdentityDirectionalRawPreviewRequestError("required tracked path must be text")
    pins: list[RepositoryFilePin] = []
    for relative in sorted(set(selected)):
        relative_path = Path(relative)
        if (
            not relative
            or relative_path.is_absolute()
            or ".." in relative_path.parts
            or relative_path.as_posix() != relative
        ):
            raise IdentityDirectionalRawPreviewRequestError(
                f"required tracked path is unsafe: {relative!r}"
            )
        path = repo_root / relative
        if not path.is_file() or path.is_symlink() or path.resolve() != path:
            raise IdentityDirectionalRawPreviewRequestError(
                f"required tracked file is missing or unsafe: {relative}"
            )
        try:
            git_blob = _git(repo_root, "rev-parse", f"HEAD:{relative}")
            working_blob = _git(
                repo_root,
                "hash-object",
                "--no-filters",
                "--",
                relative,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise IdentityDirectionalRawPreviewRequestError(
                f"required file is not tracked at HEAD: {relative}"
            ) from exc
        if git_blob != working_blob:
            raise IdentityDirectionalRawPreviewRequestError(
                f"required file differs from HEAD: {relative}"
            )
        pins.append(
            RepositoryFilePin(
                path=relative,
                git_blob=git_blob,
                sha256=sha256_file(path),
                bytes=path.stat().st_size,
            )
        )
    return tuple(pins)


def preflight_destination(root: Path, relative: str, expected_sha256: str) -> bool:
    """Reject symlink, type, or byte conflicts before any control write."""

    try:
        path = safe_relative_path(root, relative)
    except ArtifactError as exc:
        raise IdentityDirectionalRawPreviewRequestError(
            f"immutable directional raw-preview control conflicts: {relative}"
        ) from exc
    if not path.exists() and not path.is_symlink():
        return False
    if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
        raise IdentityDirectionalRawPreviewRequestError(
            f"immutable directional raw-preview control conflicts: {relative}"
        )
    return True


def parse_nonfuture_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise IdentityDirectionalRawPreviewRequestError(
            "recorded_at must be canonical ISO-8601 UTC"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IdentityDirectionalRawPreviewRequestError("recorded_at must include UTC")
    normalized = parsed.astimezone(UTC)
    if parsed.utcoffset().total_seconds() != 0 or normalized.isoformat() != value:
        raise IdentityDirectionalRawPreviewRequestError(
            "recorded_at must be canonical ISO-8601 UTC"
        )
    if normalized > datetime.now(UTC):
        raise IdentityDirectionalRawPreviewRequestError("recorded_at cannot be in the future")
    return normalized


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    return completed.stdout.strip()


def _adapt_file_pins(
    pins: tuple[RepositoryFilePin, ...],
    factory: Callable[..., Any],
) -> tuple[Any, ...]:
    """Adapt repository pins to the Plan module's immutable value type."""

    return tuple(factory(**pin.to_dict()) for pin in pins)


def directional_raw_preview_request_plan_path(plan_id: str) -> str:
    return directional_raw_preview_plan_path(plan_id)


def directional_raw_preview_request_path(request_event_id: str) -> str:
    if not isinstance(request_event_id, str) or not _DIGEST.fullmatch(request_event_id):
        raise IdentityDirectionalRawPreviewRequestError("request event ID must be lowercase 64-hex")
    return (
        "manifests/silver/identity/directional-raw-preview-preparation-requests/"
        f"request_event_id={request_event_id}/manifest.json"
    )


def _store_request(
    root: Path,
    request: S7DirectionalRawPreviewPreparationRequest,
) -> StoredDirectionalRawPreviewControl:
    try:
        path = safe_relative_path(root, request.relative_path)
        receipt = write_bytes_immutable(root, path, request.content)
    except ArtifactError as exc:
        raise IdentityDirectionalRawPreviewRequestError(str(exc)) from exc
    return StoredDirectionalRawPreviewControl(
        path=str(receipt["path"]),
        sha256=str(receipt["sha256"]),
        bytes=int(receipt["bytes"]),
    )


def _load_request(
    root: Path,
    request_event_id: str,
    *,
    expected_sha256: str,
) -> tuple[
    S7DirectionalRawPreviewPreparationRequest,
    StoredDirectionalRawPreviewControl,
]:
    relative = directional_raw_preview_request_path(request_event_id)
    path = safe_relative_path(root, relative)
    if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
        raise IdentityDirectionalRawPreviewRequestError("request control is missing or altered")
    content = path.read_bytes()
    try:
        document = json.loads(content, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityDirectionalRawPreviewRequestError("request control is not JSON") from exc
    if not isinstance(document, dict) or _canonical_bytes(document) != content:
        raise IdentityDirectionalRawPreviewRequestError("request control is not canonical JSON")
    request = S7DirectionalRawPreviewPreparationRequest.from_dict(document)
    if request.request_event_id != request_event_id or request.sha256 != expected_sha256:
        raise IdentityDirectionalRawPreviewRequestError("request control path or bytes differ")
    return request, StoredDirectionalRawPreviewControl(
        relative,
        expected_sha256,
        len(content),
    )


def _canonical_bytes(value: object) -> bytes:
    if not isinstance(value, dict) and not isinstance(value, MappingProxyType):
        raise IdentityDirectionalRawPreviewRequestError("control document must be an object")
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


def _required_text(value: dict[str, object], key: str) -> str:
    selected = value.get(key)
    if not isinstance(selected, str):
        raise IdentityDirectionalRawPreviewRequestError(f"{key} must be text")
    return selected


def _parse_utc_text(value: object) -> datetime:
    if not isinstance(value, str):
        raise IdentityDirectionalRawPreviewRequestError("created_at_utc must be text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IdentityDirectionalRawPreviewRequestError(
            "created_at_utc must be canonical ISO-8601 UTC"
        ) from exc
    normalized = _validate_utc_datetime(parsed, "created_at_utc")
    if normalized.isoformat() != value:
        raise IdentityDirectionalRawPreviewRequestError(
            "created_at_utc must be canonical ISO-8601 UTC"
        )
    return normalized


def _validate_utc_datetime(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityDirectionalRawPreviewRequestError(f"{label} must include UTC")
    normalized = value.astimezone(UTC)
    if value.utcoffset().total_seconds() != 0:
        raise IdentityDirectionalRawPreviewRequestError(f"{label} must be UTC")
    return normalized


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IdentityDirectionalRawPreviewRequestError(
                "request control contains duplicate keys"
            )
        result[key] = value
    return result


def _require_separate_actors(*actors: str) -> None:
    if any(
        not isinstance(actor, str) or not actor or actor.strip() != actor or len(actor) > 200
        for actor in actors
    ):
        raise IdentityDirectionalRawPreviewRequestError("control actor is unsafe")
    if len(set(actors)) != len(actors):
        raise IdentityDirectionalRawPreviewRequestError(
            "scope, plan, and request actors must be distinct"
        )
