"""Publish the exact approved S4 Assets plan as one visibility-atomic release set.

The three ordinary workflow chains still end in standard ``published`` events and standard
``ApprovalReceipt`` / ``ReleaseManifest`` documents.  They are deliberately hidden from every
consumer until the final immutable release-set marker exists.  An immutable group approval and
intent are written before the first workflow mutation, so a crash can only leave a recoverable
prefix of the exact precomputed transaction.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType

from ame_stocks_api.artifacts import safe_relative_path, stable_digest, write_bytes_immutable
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.asset_publish_plan import (
    ASSET_PUBLICATION_SCOPE,
    AssetPublishPlan,
    AssetPublishTablePlan,
    _verify_build_outputs_without_sources,
)
from ame_stocks_api.silver.contracts import (
    ApprovalDecision,
    ApprovalReceipt,
    ApprovalStage,
    ArtifactRef,
    ArtifactRole,
    BuildKind,
    ReleaseManifest,
    SilverContractError,
    ensure_json_safe,
    thaw_json,
)
from ame_stocks_api.silver.store import (
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowEvent,
    WorkflowSnapshot,
    WorkflowState,
)

ASSET_RELEASE_SET_VERSION = 1
ASSET_RELEASE_SET_POLICY_VERSION = "s4-assets-release-set-v1"
ASSET_RELEASE_SET_APPROVAL_VERSION = 1
ASSET_RELEASE_SET_INTENT_VERSION = 1

CURRENT_ASSET_PUBLISH_PLAN_ID = (
    "908b0982f273149e2f5a4340edcf369f9b2463a09a85d92677c8bd401564ec01"
)
CURRENT_ASSET_PUBLISH_PLAN_SHA256 = (
    "cf6129c7149d2f38297d443e533f1d3e6f79eafe976b012d19d69830a4fa779d"
)
CURRENT_ASSET_PUBLISH_PLAN_BYTES = 14_291
CURRENT_ASSET_PUBLISH_PLAN_CREATOR_COMMIT = (
    "54f4af71d43cf5ba5c0d58b53b5d97836611ffee"
)
CURRENT_ASSET_MATERIALIZATION_COMMIT = "adc28b5dc05dccb0d4b963fe6be719367d9e7b97"
CURRENT_ASSET_PUBLISH_PLAN_PATH = (
    "manifests/silver/publish-plans/assets/"
    f"plan_id={CURRENT_ASSET_PUBLISH_PLAN_ID}/manifest.json"
)

APPROVAL_TEXT = (
    "批准 S4 PublishPlan ID 908b0982… / SHA cf6129c…，批准其中 7/2/8 个 "  # noqa: RUF001
    "warning waiver；三表 quarantine acceptance 为空；接受 RSS runtime review。"  # noqa: RUF001
)
APPROVAL_TEXT_SHA256 = (
    "d5f839d7ad5d6b37b11ca88556dff1f88c5cc707240d61e179b909f3a5e377c9"
)
if hashlib.sha256(APPROVAL_TEXT.encode("utf-8")).hexdigest() != APPROVAL_TEXT_SHA256:
    raise RuntimeError("the pinned S4 user-approval text no longer matches its SHA-256")
APPROVER = "user-approved-s4-assets-release-set"
COORDINATOR_ACTOR = "s4-assets-release-set-coordinator"

_TABLE_ORDER = tuple(ASSET_CONTRACTS)
_PROTECTED_TABLES = frozenset(_TABLE_ORDER)
_CURRENT_WARNING_RESULT_IDS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "asset_observation_daily": tuple(
            sorted(
                (
                    "26f3344d9d789ea1db9fb788340b6581060ea544277602f5bd0e2a78867b2d94",
                    "603c8a67ef7e957950ce22dbaecee7e528e5bb527fdb19ef1b9d37e9f6904279",
                    "55b5316c57b2fa4c303cfa271b95256f070accc9d7b368cea40a293d4239e02e",
                    "545b3c77a1c0c89111069cfdaf9a94b71451e0e8da0ec2b2eb06d6958fedec32",
                    "a648b4a710cf85843d4eb2c338e761dc336b3c83dde38b543b658f8bfd42a688",
                    "a0797564c49bd4e40c3f4ec5586bf5f4b407ef560668a059f347bc698adcbbf9",
                    "f61fb4bbd3852f10730ad4e7eabf353d79549884316fffd77494ed6ee62dae34",
                )
            )
        ),
        "asset_observation_version": tuple(
            sorted(
                (
                    "377432f81bc38e0ae1be5a9f2f4fe766eab072ee9c45cc8e41ed3ac5a1e60868",
                    "11f11939aca04f87a8b55ed33c1a6ac3fbf500c9d8f13db3a27667c4877a99ea",
                )
            )
        ),
        "universe_source_daily": tuple(
            sorted(
                (
                    "ae5fd10ed957b310108904e9da3fb77a417cad6eb4120194b2b43d162dffdda0",
                    "c6df008fa7fc0cd1dbc92852b46e686172eb598d0309feb1a4eb63673c2a7415",
                    "89fecde02637611f1c4fa04c4a61ca613aaa94d8eabdfa21fcba2585c4f233a6",
                    "60ca2e7bae2232fa1228408a3aeec422952752174d0d82b27f4bd95c77212ace",
                    "92834d808c45b00909ec0d2275981db84577c91b2039ef9f5eacad0e586784e7",
                    "a4439f82dedd2a077c0fcab7a2eebf66658a8ff76f1372f0a87ae81784d54426",
                    "3d83eb69ffc934e7be806eea7b93eeab81312cde26f16cb20ee802826b9b9750",
                    "7c4100fa549ee5be0c36818ce0242b88b87e031ca20d4a96d20639bdb93cdd34",
                )
            )
        ),
    }
)

_ALLOWED_RELEASE_COMMIT_DIFF = frozenset(
    {
        "backend/ame_stocks_api/cli/silver_assets_release_set.py",
        "backend/ame_stocks_api/silver/asset_release_set.py",
        "backend/ame_stocks_api/silver/contracts.py",
        "backend/ame_stocks_api/silver/reader.py",
        "backend/ame_stocks_api/silver/store.py",
        "pyproject.toml",
        "tests/test_silver_asset_release_set.py",
        "tests/test_silver_asset_release_visibility.py",
        "tests/test_silver_contracts.py",
    }
)


def _json_bytes(document: Mapping[str, object]) -> bytes:
    # Production S4 has 2,513 daily partitions per table. Keep every generic
    # JSON safety/secret check, but lift only the per-list bound for these exact,
    # strictly typed release and release-set documents.
    safe = ensure_json_safe(
        document,
        label="asset release-set document",
        max_list_items=5_000,
    )
    return (
        json.dumps(
            thaw_json(safe),
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise SilverContractError(f"{label} must be a lowercase SHA-256")
    return value


def _git_sha(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) != 40 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise SilverContractError(f"{label} must be a lowercase Git SHA")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise SilverContractError(f"{label} must be non-empty text")
    return value


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise SilverContractError(f"{label} must be a positive native int")
    return value


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise SilverContractError(f"{label} must be an object")
    return dict(value)


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise SilverContractError(f"{label} must be an array")
    return value


def _expect_keys(document: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(document) != expected:
        raise SilverContractError(
            f"{label} keys changed: missing={sorted(expected - set(document))}, "
            f"extra={sorted(set(document) - expected)}"
        )


def _utc_text(value: object, label: str) -> str:
    text = _text(value, label)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SilverContractError(f"{label} must be an ISO-8601 timestamp") from exc
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() is None
        or parsed.utcoffset().total_seconds() != 0
    ):
        raise SilverContractError(f"{label} must be UTC")
    return text


def _utc_datetime(value: object, label: str) -> datetime:
    text = _utc_text(value, label)
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _relative_path(value: object, label: str) -> str:
    text = _text(value, label)
    path = Path(text)
    if (
        path.is_absolute()
        or "\\" in text
        or path.as_posix() != text
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise SilverContractError(f"{label} must be normalized and relative")
    return text


def _exact_bool(value: object, expected: bool, label: str) -> bool:
    if type(value) is not bool or value is not expected:
        raise SilverContractError(f"{label} must be {str(expected).lower()}")
    return expected


def _normalized_warning_map(
    value: Mapping[str, tuple[str, ...]] | Mapping[str, list[str]],
) -> Mapping[str, tuple[str, ...]]:
    if set(value) != set(_TABLE_ORDER):
        raise SilverContractError("asset release-set warnings must cover the exact three tables")
    normalized: dict[str, tuple[str, ...]] = {}
    for table in _TABLE_ORDER:
        items = tuple(sorted(_digest(item, f"{table} warning result ID") for item in value[table]))
        if len(items) != len(set(items)):
            raise SilverContractError(f"asset release-set warning IDs repeat for {table}")
        normalized[table] = items
    return MappingProxyType(normalized)


def _normalized_empty_quarantine_map(
    value: Mapping[str, tuple[str, ...]] | Mapping[str, list[str]],
) -> Mapping[str, tuple[str, ...]]:
    if set(value) != set(_TABLE_ORDER):
        raise SilverContractError(
            "asset release-set quarantine acceptance must cover the exact three tables"
        )
    normalized: dict[str, tuple[str, ...]] = {}
    for table in _TABLE_ORDER:
        items = tuple(
            _digest(item, f"{table} quarantine issue ID") for item in value[table]
        )
        if items:
            raise SilverContractError(
                "S4 release-set quarantine acceptance must be empty for every table"
            )
        normalized[table] = ()
    return MappingProxyType(normalized)


def _empty_quarantine_map() -> Mapping[str, tuple[str, ...]]:
    return MappingProxyType({table: () for table in _TABLE_ORDER})


@dataclass(frozen=True, slots=True)
class AssetReleaseSetApproval:
    publish_plan_id: str
    publish_plan_path: str
    publish_plan_sha256: str
    publish_plan_bytes: int
    publish_plan_creator_commit: str
    materialization_git_commit: str
    release_orchestration_git_commit: str
    approval_text: str
    approval_text_sha256: str
    approver: str
    decided_at: str
    warning_result_ids_by_table: Mapping[str, tuple[str, ...]]
    accepted_quarantine_issue_ids_by_table: Mapping[str, tuple[str, ...]]
    runtime_review_digest: str
    runtime_review_accepted: bool = True
    publication_scope: str = ASSET_PUBLICATION_SCOPE
    backtest_identity_eligible: bool = False

    def __post_init__(self) -> None:
        _digest(self.publish_plan_id, "publish plan ID")
        _digest(self.publish_plan_sha256, "publish plan SHA")
        _positive_int(self.publish_plan_bytes, "publish plan bytes")
        _git_sha(self.publish_plan_creator_commit, "publish plan creator commit")
        _git_sha(self.materialization_git_commit, "materialization commit")
        _git_sha(self.release_orchestration_git_commit, "release orchestration commit")
        _relative_path(self.publish_plan_path, "publish plan path")
        expected_plan_path = (
            "manifests/silver/publish-plans/assets/"
            f"plan_id={self.publish_plan_id}/manifest.json"
        )
        if self.publish_plan_path != expected_plan_path:
            raise SilverContractError("asset release-set approval plan path changed")
        _text(self.approval_text, "approval text")
        _digest(self.approval_text_sha256, "approval text SHA")
        if hashlib.sha256(self.approval_text.encode()).hexdigest() != self.approval_text_sha256:
            raise SilverContractError("asset release-set approval text digest mismatch")
        _text(self.approver, "approver")
        _utc_text(self.decided_at, "decided_at")
        object.__setattr__(
            self,
            "warning_result_ids_by_table",
            _normalized_warning_map(self.warning_result_ids_by_table),
        )
        object.__setattr__(
            self,
            "accepted_quarantine_issue_ids_by_table",
            _normalized_empty_quarantine_map(
                self.accepted_quarantine_issue_ids_by_table
            ),
        )
        _digest(self.runtime_review_digest, "runtime review digest")
        if self.runtime_review_accepted is not True:
            raise SilverContractError("S4 runtime review must be explicitly accepted")
        if self.publication_scope != ASSET_PUBLICATION_SCOPE:
            raise SilverContractError("S4 release-set publication scope changed")
        if self.backtest_identity_eligible is not False:
            raise SilverContractError("S4 release set cannot be backtest identity eligible")

    @property
    def approval_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_text": self.approval_text,
            "approval_text_sha256": self.approval_text_sha256,
            "approver": self.approver,
            "accepted_quarantine_issue_ids_by_table": {
                table: [] for table in _TABLE_ORDER
            },
            "asset_release_set_approval_version": ASSET_RELEASE_SET_APPROVAL_VERSION,
            "backtest_identity_eligible": False,
            "decided_at": self.decided_at,
            "materialization_git_commit": self.materialization_git_commit,
            "publication_scope": self.publication_scope,
            "publish_plan_bytes": self.publish_plan_bytes,
            "publish_plan_creator_commit": self.publish_plan_creator_commit,
            "publish_plan_id": self.publish_plan_id,
            "publish_plan_path": self.publish_plan_path,
            "publish_plan_sha256": self.publish_plan_sha256,
            "release_orchestration_git_commit": self.release_orchestration_git_commit,
            "runtime_review_accepted": True,
            "runtime_review_digest": self.runtime_review_digest,
            "warning_result_ids_by_table": {
                table: list(items)
                for table, items in self.warning_result_ids_by_table.items()
            },
        }

    def to_dict(self) -> dict[str, object]:
        return {"approval_id": self.approval_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> AssetReleaseSetApproval:
        document = _object(value, "asset release-set approval")
        expected = {
            "approval_id",
            "approval_text",
            "approval_text_sha256",
            "approver",
            "accepted_quarantine_issue_ids_by_table",
            "asset_release_set_approval_version",
            "backtest_identity_eligible",
            "decided_at",
            "materialization_git_commit",
            "publication_scope",
            "publish_plan_bytes",
            "publish_plan_creator_commit",
            "publish_plan_id",
            "publish_plan_path",
            "publish_plan_sha256",
            "release_orchestration_git_commit",
            "runtime_review_accepted",
            "runtime_review_digest",
            "warning_result_ids_by_table",
        }
        _expect_keys(document, expected, "asset release-set approval")
        if (
            _positive_int(
                document["asset_release_set_approval_version"],
                "asset release-set approval version",
            )
            != ASSET_RELEASE_SET_APPROVAL_VERSION
        ):
            raise SilverContractError("unsupported asset release-set approval version")
        warnings = _object(document["warning_result_ids_by_table"], "warning result IDs")
        quarantines = _object(
            document["accepted_quarantine_issue_ids_by_table"],
            "accepted quarantine issue IDs",
        )
        approval = cls(
            publish_plan_id=_digest(document["publish_plan_id"], "publish plan ID"),
            publish_plan_path=_relative_path(
                document["publish_plan_path"], "publish plan path"
            ),
            publish_plan_sha256=_digest(document["publish_plan_sha256"], "publish plan SHA"),
            publish_plan_bytes=_positive_int(document["publish_plan_bytes"], "plan bytes"),
            publish_plan_creator_commit=_git_sha(
                document["publish_plan_creator_commit"], "plan creator commit"
            ),
            materialization_git_commit=_git_sha(
                document["materialization_git_commit"], "materialization commit"
            ),
            release_orchestration_git_commit=_git_sha(
                document["release_orchestration_git_commit"], "release commit"
            ),
            approval_text=_text(document["approval_text"], "approval text"),
            approval_text_sha256=_digest(
                document["approval_text_sha256"], "approval text SHA"
            ),
            approver=_text(document["approver"], "approver"),
            decided_at=_utc_text(document["decided_at"], "decided_at"),
            warning_result_ids_by_table={
                table: tuple(_digest(item, "warning result ID") for item in _array(items, table))
                for table, items in warnings.items()
            },
            accepted_quarantine_issue_ids_by_table={
                table: tuple(
                    _digest(item, "quarantine issue ID")
                    for item in _array(items, table)
                )
                for table, items in quarantines.items()
            },
            runtime_review_digest=_digest(
                document["runtime_review_digest"], "runtime review digest"
            ),
            runtime_review_accepted=_exact_bool(
                document["runtime_review_accepted"], True, "runtime_review_accepted"
            ),
            publication_scope=_text(document["publication_scope"], "publication scope"),
            backtest_identity_eligible=_exact_bool(
                document["backtest_identity_eligible"],
                False,
                "backtest_identity_eligible",
            ),
        )
        if document["approval_id"] != approval.approval_id:
            raise SilverContractError("asset release-set approval ID mismatch")
        return approval


@dataclass(frozen=True, slots=True)
class AssetReleaseSetMember:
    table: str
    domain: str
    workflow_id: str
    contract_id: str
    schema_version: int
    full_ready_event_sha256: str
    awaiting_publish_event_sha256: str
    published_event_sha256: str
    full_run_plan_id: str
    full_run_plan_sha256: str
    build_id: str
    build_manifest_sha256: str
    warning_result_ids: tuple[str, ...]
    accepted_quarantine_issue_ids: tuple[str, ...]
    approval_id: str
    approval_path: str
    approval_sha256: str
    release_id: str
    release_path: str
    release_sha256: str
    outputs: tuple[ArtifactRef, ...]

    def __post_init__(self) -> None:
        if self.table not in _PROTECTED_TABLES:
            raise SilverContractError("asset release-set member table is not protected S4 data")
        _text(self.domain, "member domain")
        _positive_int(self.schema_version, "member schema version")
        for label, item in (
            ("workflow ID", self.workflow_id),
            ("contract ID", self.contract_id),
            ("full-ready event SHA", self.full_ready_event_sha256),
            ("awaiting-publish event SHA", self.awaiting_publish_event_sha256),
            ("published event SHA", self.published_event_sha256),
            ("full-run plan ID", self.full_run_plan_id),
            ("full-run plan SHA", self.full_run_plan_sha256),
            ("build ID", self.build_id),
            ("build manifest SHA", self.build_manifest_sha256),
            ("approval ID", self.approval_id),
            ("approval SHA", self.approval_sha256),
            ("release ID", self.release_id),
            ("release SHA", self.release_sha256),
        ):
            _digest(item, label)
        warnings = tuple(
            sorted(
                _digest(item, "warning result ID")
                for item in self.warning_result_ids
            )
        )
        if len(warnings) != len(set(warnings)):
            raise SilverContractError("asset release-set member warning IDs repeat")
        object.__setattr__(self, "warning_result_ids", warnings)
        if tuple(self.accepted_quarantine_issue_ids):
            raise SilverContractError("S4 release-set quarantine acceptance must be empty")
        object.__setattr__(self, "accepted_quarantine_issue_ids", ())
        outputs = tuple(sorted(self.outputs, key=lambda item: item.path))
        if not outputs or any(
            output.role is not ArtifactRole.DATA or output.table != self.table
            for output in outputs
        ):
            raise SilverContractError("asset release-set member outputs must be table DATA")
        object.__setattr__(self, "outputs", outputs)
        for path in (self.approval_path, self.release_path):
            _relative_path(path, "asset release-set document path")
        if self.approval_path != f"manifests/silver/approvals/{self.approval_id}.json":
            raise SilverContractError("asset release-set member approval path changed")
        if self.release_path != (
            "manifests/silver/releases/"
            f"release_id={self.release_id}.json"
        ):
            raise SilverContractError("asset release-set member release path changed")

    def to_dict(self) -> dict[str, object]:
        return {
            "accepted_quarantine_issue_ids": [],
            "approval_id": self.approval_id,
            "approval_path": self.approval_path,
            "approval_sha256": self.approval_sha256,
            "awaiting_publish_event_sha256": self.awaiting_publish_event_sha256,
            "build_id": self.build_id,
            "build_manifest_sha256": self.build_manifest_sha256,
            "contract_id": self.contract_id,
            "domain": self.domain,
            "full_ready_event_sha256": self.full_ready_event_sha256,
            "full_run_plan_id": self.full_run_plan_id,
            "full_run_plan_sha256": self.full_run_plan_sha256,
            "outputs": [item.to_dict() for item in self.outputs],
            "published_event_sha256": self.published_event_sha256,
            "release_id": self.release_id,
            "release_path": self.release_path,
            "release_sha256": self.release_sha256,
            "schema_version": self.schema_version,
            "table": self.table,
            "warning_result_ids": list(self.warning_result_ids),
            "workflow_id": self.workflow_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> AssetReleaseSetMember:
        document = _object(value, "asset release-set member")
        expected = {
            "accepted_quarantine_issue_ids",
            "approval_id",
            "approval_path",
            "approval_sha256",
            "awaiting_publish_event_sha256",
            "build_id",
            "build_manifest_sha256",
            "contract_id",
            "domain",
            "full_ready_event_sha256",
            "full_run_plan_id",
            "full_run_plan_sha256",
            "outputs",
            "published_event_sha256",
            "release_id",
            "release_path",
            "release_sha256",
            "schema_version",
            "table",
            "warning_result_ids",
            "workflow_id",
        }
        _expect_keys(document, expected, "asset release-set member")
        return cls(
            table=_text(document["table"], "member table"),
            domain=_text(document["domain"], "member domain"),
            workflow_id=_digest(document["workflow_id"], "workflow ID"),
            contract_id=_digest(document["contract_id"], "contract ID"),
            schema_version=_positive_int(document["schema_version"], "schema version"),
            full_ready_event_sha256=_digest(
                document["full_ready_event_sha256"], "full-ready event SHA"
            ),
            awaiting_publish_event_sha256=_digest(
                document["awaiting_publish_event_sha256"], "awaiting event SHA"
            ),
            published_event_sha256=_digest(
                document["published_event_sha256"], "published event SHA"
            ),
            full_run_plan_id=_digest(document["full_run_plan_id"], "full-run plan ID"),
            full_run_plan_sha256=_digest(
                document["full_run_plan_sha256"], "full-run plan SHA"
            ),
            build_id=_digest(document["build_id"], "build ID"),
            build_manifest_sha256=_digest(
                document["build_manifest_sha256"], "build manifest SHA"
            ),
            warning_result_ids=tuple(
                _digest(item, "warning result ID")
                for item in _array(document["warning_result_ids"], "warning IDs")
            ),
            accepted_quarantine_issue_ids=tuple(
                _digest(item, "quarantine issue ID")
                for item in _array(
                    document["accepted_quarantine_issue_ids"], "quarantine IDs"
                )
            ),
            approval_id=_digest(document["approval_id"], "approval ID"),
            approval_path=_relative_path(document["approval_path"], "approval path"),
            approval_sha256=_digest(document["approval_sha256"], "approval SHA"),
            release_id=_digest(document["release_id"], "release ID"),
            release_path=_relative_path(document["release_path"], "release path"),
            release_sha256=_digest(document["release_sha256"], "release SHA"),
            outputs=tuple(
                ArtifactRef.from_dict(item)
                for item in _array(document["outputs"], "release outputs")
            ),
        )


def _normalize_members(
    members: tuple[AssetReleaseSetMember, ...],
) -> tuple[AssetReleaseSetMember, ...]:
    if len(members) != len(_TABLE_ORDER) or {item.table for item in members} != set(
        _TABLE_ORDER
    ):
        raise SilverContractError("asset release set must contain exactly the three S4 tables")
    return tuple(sorted(members, key=lambda item: _TABLE_ORDER.index(item.table)))


@dataclass(frozen=True, slots=True)
class AssetReleaseSetIntent:
    group_approval_id: str
    group_approval_path: str
    group_approval_sha256: str
    publish_plan_id: str
    publish_plan_path: str
    publish_plan_sha256: str
    release_orchestration_git_commit: str
    recorded_at: str
    runtime_review_digest: str
    members: tuple[AssetReleaseSetMember, ...]
    publication_scope: str = ASSET_PUBLICATION_SCOPE
    backtest_identity_eligible: bool = False

    def __post_init__(self) -> None:
        for label, item in (
            ("group approval ID", self.group_approval_id),
            ("group approval SHA", self.group_approval_sha256),
            ("publish plan ID", self.publish_plan_id),
            ("publish plan SHA", self.publish_plan_sha256),
            ("runtime review digest", self.runtime_review_digest),
        ):
            _digest(item, label)
        _git_sha(self.release_orchestration_git_commit, "release orchestration commit")
        _utc_text(self.recorded_at, "recorded_at")
        _relative_path(self.group_approval_path, "group approval path")
        _relative_path(self.publish_plan_path, "publish plan path")
        if self.group_approval_path != (
            "manifests/silver/release-set-approvals/assets/"
            f"approval_id={self.group_approval_id}/manifest.json"
        ):
            raise SilverContractError("asset release-set intent approval path changed")
        if self.publish_plan_path != (
            "manifests/silver/publish-plans/assets/"
            f"plan_id={self.publish_plan_id}/manifest.json"
        ):
            raise SilverContractError("asset release-set intent plan path changed")
        object.__setattr__(self, "members", _normalize_members(tuple(self.members)))
        if self.publication_scope != ASSET_PUBLICATION_SCOPE:
            raise SilverContractError("asset release-set intent scope changed")
        if self.backtest_identity_eligible is not False:
            raise SilverContractError("asset release-set intent cannot be backtest eligible")

    @property
    def intent_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "asset_release_set_intent_version": ASSET_RELEASE_SET_INTENT_VERSION,
            "backtest_identity_eligible": False,
            "group_approval_id": self.group_approval_id,
            "group_approval_path": self.group_approval_path,
            "group_approval_sha256": self.group_approval_sha256,
            "members": [item.to_dict() for item in self.members],
            "publication_scope": self.publication_scope,
            "publish_plan_id": self.publish_plan_id,
            "publish_plan_path": self.publish_plan_path,
            "publish_plan_sha256": self.publish_plan_sha256,
            "recorded_at": self.recorded_at,
            "release_orchestration_git_commit": self.release_orchestration_git_commit,
            "runtime_review_digest": self.runtime_review_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {"intent_id": self.intent_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> AssetReleaseSetIntent:
        document = _object(value, "asset release-set intent")
        expected = {
            "asset_release_set_intent_version",
            "backtest_identity_eligible",
            "group_approval_id",
            "group_approval_path",
            "group_approval_sha256",
            "intent_id",
            "members",
            "publication_scope",
            "publish_plan_id",
            "publish_plan_path",
            "publish_plan_sha256",
            "recorded_at",
            "release_orchestration_git_commit",
            "runtime_review_digest",
        }
        _expect_keys(document, expected, "asset release-set intent")
        if (
            _positive_int(
                document["asset_release_set_intent_version"],
                "asset release-set intent version",
            )
            != ASSET_RELEASE_SET_INTENT_VERSION
        ):
            raise SilverContractError("unsupported asset release-set intent version")
        intent = cls(
            group_approval_id=_digest(document["group_approval_id"], "group approval ID"),
            group_approval_path=_relative_path(
                document["group_approval_path"], "approval path"
            ),
            group_approval_sha256=_digest(
                document["group_approval_sha256"], "group approval SHA"
            ),
            publish_plan_id=_digest(document["publish_plan_id"], "publish plan ID"),
            publish_plan_path=_relative_path(
                document["publish_plan_path"], "publish plan path"
            ),
            publish_plan_sha256=_digest(document["publish_plan_sha256"], "plan SHA"),
            release_orchestration_git_commit=_git_sha(
                document["release_orchestration_git_commit"], "release commit"
            ),
            recorded_at=_utc_text(document["recorded_at"], "recorded_at"),
            runtime_review_digest=_digest(
                document["runtime_review_digest"], "runtime review digest"
            ),
            members=tuple(
                AssetReleaseSetMember.from_dict(item)
                for item in _array(document["members"], "members")
            ),
            publication_scope=_text(document["publication_scope"], "scope"),
            backtest_identity_eligible=_exact_bool(
                document["backtest_identity_eligible"],
                False,
                "backtest_identity_eligible",
            ),
        )
        if document["intent_id"] != intent.intent_id:
            raise SilverContractError("asset release-set intent ID mismatch")
        return intent


@dataclass(frozen=True, slots=True)
class AssetReleaseSet:
    intent_id: str
    intent_path: str
    intent_sha256: str
    group_approval_id: str
    group_approval_path: str
    group_approval_sha256: str
    publish_plan_id: str
    publish_plan_path: str
    publish_plan_sha256: str
    publish_plan_bytes: int
    publish_plan_creator_commit: str
    materialization_git_commit: str
    release_orchestration_git_commit: str
    committed_at: str
    runtime_review_digest: str
    runtime_review_accepted: bool
    members: tuple[AssetReleaseSetMember, ...]
    publication_scope: str = ASSET_PUBLICATION_SCOPE
    backtest_identity_eligible: bool = False

    def __post_init__(self) -> None:
        for label, item in (
            ("intent ID", self.intent_id),
            ("intent SHA", self.intent_sha256),
            ("group approval ID", self.group_approval_id),
            ("group approval SHA", self.group_approval_sha256),
            ("publish plan ID", self.publish_plan_id),
            ("publish plan SHA", self.publish_plan_sha256),
            ("runtime review digest", self.runtime_review_digest),
        ):
            _digest(item, label)
        _positive_int(self.publish_plan_bytes, "publish plan bytes")
        _git_sha(self.publish_plan_creator_commit, "publish plan creator commit")
        _git_sha(self.materialization_git_commit, "materialization commit")
        _git_sha(self.release_orchestration_git_commit, "release commit")
        _utc_text(self.committed_at, "committed_at")
        for label, path in (
            ("intent path", self.intent_path),
            ("group approval path", self.group_approval_path),
            ("publish plan path", self.publish_plan_path),
        ):
            _relative_path(path, label)
        if self.intent_path != (
            "manifests/silver/release-set-intents/assets/"
            f"intent_id={self.intent_id}/manifest.json"
        ):
            raise SilverContractError("asset release-set intent path changed")
        if self.group_approval_path != (
            "manifests/silver/release-set-approvals/assets/"
            f"approval_id={self.group_approval_id}/manifest.json"
        ):
            raise SilverContractError("asset release-set approval path changed")
        if self.publish_plan_path != (
            "manifests/silver/publish-plans/assets/"
            f"plan_id={self.publish_plan_id}/manifest.json"
        ):
            raise SilverContractError("asset release-set plan path changed")
        object.__setattr__(self, "members", _normalize_members(tuple(self.members)))
        if self.runtime_review_accepted is not True:
            raise SilverContractError("asset release set must accept runtime review")
        if self.publication_scope != ASSET_PUBLICATION_SCOPE:
            raise SilverContractError("asset release set scope changed")
        if self.backtest_identity_eligible is not False:
            raise SilverContractError("asset release set cannot be backtest identity eligible")

    @property
    def release_set_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "asset_release_set_policy_version": ASSET_RELEASE_SET_POLICY_VERSION,
            "asset_release_set_version": ASSET_RELEASE_SET_VERSION,
            "backtest_identity_eligible": False,
            "committed_at": self.committed_at,
            "group_approval_id": self.group_approval_id,
            "group_approval_path": self.group_approval_path,
            "group_approval_sha256": self.group_approval_sha256,
            "intent_id": self.intent_id,
            "intent_path": self.intent_path,
            "intent_sha256": self.intent_sha256,
            "materialization_git_commit": self.materialization_git_commit,
            "members": [item.to_dict() for item in self.members],
            "publication_scope": self.publication_scope,
            "publish_plan_bytes": self.publish_plan_bytes,
            "publish_plan_creator_commit": self.publish_plan_creator_commit,
            "publish_plan_id": self.publish_plan_id,
            "publish_plan_path": self.publish_plan_path,
            "publish_plan_sha256": self.publish_plan_sha256,
            "release_orchestration_git_commit": self.release_orchestration_git_commit,
            "runtime_review_accepted": True,
            "runtime_review_digest": self.runtime_review_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {"release_set_id": self.release_set_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> AssetReleaseSet:
        document = _object(value, "asset release set")
        expected = {
            "asset_release_set_policy_version",
            "asset_release_set_version",
            "backtest_identity_eligible",
            "committed_at",
            "group_approval_id",
            "group_approval_path",
            "group_approval_sha256",
            "intent_id",
            "intent_path",
            "intent_sha256",
            "materialization_git_commit",
            "members",
            "publication_scope",
            "publish_plan_bytes",
            "publish_plan_creator_commit",
            "publish_plan_id",
            "publish_plan_path",
            "publish_plan_sha256",
            "release_orchestration_git_commit",
            "release_set_id",
            "runtime_review_accepted",
            "runtime_review_digest",
        }
        _expect_keys(document, expected, "asset release set")
        if document["asset_release_set_policy_version"] != ASSET_RELEASE_SET_POLICY_VERSION:
            raise SilverContractError("unsupported asset release-set policy version")
        if (
            _positive_int(
                document["asset_release_set_version"],
                "asset release-set version",
            )
            != ASSET_RELEASE_SET_VERSION
        ):
            raise SilverContractError("unsupported asset release-set version")
        release_set = cls(
            intent_id=_digest(document["intent_id"], "intent ID"),
            intent_path=_relative_path(document["intent_path"], "intent path"),
            intent_sha256=_digest(document["intent_sha256"], "intent SHA"),
            group_approval_id=_digest(document["group_approval_id"], "approval ID"),
            group_approval_path=_relative_path(
                document["group_approval_path"], "approval path"
            ),
            group_approval_sha256=_digest(document["group_approval_sha256"], "approval SHA"),
            publish_plan_id=_digest(document["publish_plan_id"], "publish plan ID"),
            publish_plan_path=_relative_path(
                document["publish_plan_path"], "publish plan path"
            ),
            publish_plan_sha256=_digest(document["publish_plan_sha256"], "plan SHA"),
            publish_plan_bytes=_positive_int(document["publish_plan_bytes"], "plan bytes"),
            publish_plan_creator_commit=_git_sha(
                document["publish_plan_creator_commit"], "plan creator commit"
            ),
            materialization_git_commit=_git_sha(
                document["materialization_git_commit"], "materialization commit"
            ),
            release_orchestration_git_commit=_git_sha(
                document["release_orchestration_git_commit"], "release commit"
            ),
            committed_at=_utc_text(document["committed_at"], "committed_at"),
            runtime_review_digest=_digest(
                document["runtime_review_digest"], "runtime review digest"
            ),
            runtime_review_accepted=_exact_bool(
                document["runtime_review_accepted"], True, "runtime_review_accepted"
            ),
            members=tuple(
                AssetReleaseSetMember.from_dict(item)
                for item in _array(document["members"], "members")
            ),
            publication_scope=_text(document["publication_scope"], "scope"),
            backtest_identity_eligible=_exact_bool(
                document["backtest_identity_eligible"],
                False,
                "backtest_identity_eligible",
            ),
        )
        if document["release_set_id"] != release_set.release_set_id:
            raise SilverContractError("asset release-set ID mismatch")
        return release_set


@dataclass(frozen=True, slots=True)
class AssetReleaseSetRun:
    publish_plan: AssetPublishPlan
    release_set: AssetReleaseSet
    document: StoredDocument
    approval: AssetReleaseSetApproval
    approval_document: StoredDocument
    intent: AssetReleaseSetIntent
    intent_document: StoredDocument
    workflows_by_table: Mapping[str, WorkflowSnapshot]
    idempotent: bool


def asset_release_requires_set(table: str) -> bool:
    """Return whether a table is protected by the S4 all-or-nothing visibility marker."""

    return table in _PROTECTED_TABLES


@dataclass(frozen=True, slots=True)
class _MemberTransaction:
    member: AssetReleaseSetMember
    request_event: WorkflowEvent
    publish_receipt: ApprovalReceipt
    publish_receipt_document: StoredDocument
    release: ReleaseManifest
    release_document: StoredDocument
    publish_event: WorkflowEvent


@dataclass(frozen=True, slots=True)
class _Preflight:
    snapshots: Mapping[str, WorkflowSnapshot]
    builds: Mapping[str, object]
    build_documents: Mapping[str, StoredDocument]
    contracts: Mapping[str, object]


def release_asset_publish_plan(
    data_root: Path,
    *,
    expected_publish_plan_id: str,
    expected_publish_plan_sha256: str,
    repo_root: Path,
    release_orchestration_git_commit: str,
    recorded_at: str,
) -> AssetReleaseSetRun:
    """Consume only the exact production PublishPlan authorized by the user."""

    if (
        expected_publish_plan_id != CURRENT_ASSET_PUBLISH_PLAN_ID
        or expected_publish_plan_sha256 != CURRENT_ASSET_PUBLISH_PLAN_SHA256
    ):
        raise SilverStoreError("S4 release-set request is outside the authorized PublishPlan")
    run = _release_asset_publish_plan(
        data_root,
        expected_publish_plan_id=expected_publish_plan_id,
        expected_publish_plan_sha256=expected_publish_plan_sha256,
        expected_publish_plan_bytes=CURRENT_ASSET_PUBLISH_PLAN_BYTES,
        expected_publish_plan_creator_commit=CURRENT_ASSET_PUBLISH_PLAN_CREATOR_COMMIT,
        expected_materialization_commit=CURRENT_ASSET_MATERIALIZATION_COMMIT,
        expected_warning_result_ids_by_table=_CURRENT_WARNING_RESULT_IDS,
        repo_root=repo_root,
        release_orchestration_git_commit=release_orchestration_git_commit,
        recorded_at=recorded_at,
        git_verifier=_verify_release_checkout,
        runtime_evidence_verifier=_verify_runtime_review_file,
    )
    _require_production_release_authority(
        data_root.expanduser().resolve(),
        run.release_set,
    )
    return run


def _release_asset_publish_plan(
    data_root: Path,
    *,
    expected_publish_plan_id: str,
    expected_publish_plan_sha256: str,
    expected_publish_plan_bytes: int,
    expected_publish_plan_creator_commit: str,
    expected_materialization_commit: str,
    expected_warning_result_ids_by_table: Mapping[str, tuple[str, ...]] | None,
    repo_root: Path,
    release_orchestration_git_commit: str,
    recorded_at: str,
    git_verifier: Callable[[Path, str, AssetPublishPlan], None],
    runtime_evidence_verifier: Callable[[Path, AssetPublishPlan], None],
    before_final_lock: Callable[[], None] | None = None,
    transition_barrier: Callable[[str, str | None], None] | None = None,
) -> AssetReleaseSetRun:
    """Fixture-capable implementation with exact prefix recovery and one final marker."""

    root = data_root.expanduser().resolve()
    _git_sha(expected_publish_plan_creator_commit, "publish plan creator commit")
    _git_sha(expected_materialization_commit, "materialization commit")
    _git_sha(release_orchestration_git_commit, "release orchestration commit")
    _utc_text(recorded_at, "recorded_at")
    plan, plan_document = _load_publish_plan(
        root,
        expected_publish_plan_id,
        expected_publish_plan_sha256,
        expected_publish_plan_bytes,
    )
    if (
        plan.orchestration_git_commit != expected_publish_plan_creator_commit
        or plan.materialization_git_commit != expected_materialization_commit
        or plan.publication_scope != ASSET_PUBLICATION_SCOPE
        or plan.backtest_identity_eligible is not False
        or plan.requires_release_set is not True
        or plan.requires_runtime_review_acceptance is not True
    ):
        raise SilverStoreError("S4 PublishPlan release authority changed")
    observed_warning_map = {
        item.table: item.warning_result_ids for item in plan.tables
    }
    if expected_warning_result_ids_by_table is not None and dict(
        _normalized_warning_map(expected_warning_result_ids_by_table)
    ) != observed_warning_map:
        raise SilverStoreError("S4 PublishPlan warning result IDs changed")
    if any(item.accepted_quarantine_issue_ids for item in plan.tables):
        raise SilverStoreError("S4 release set requires empty quarantine acceptance")
    if set().union(*map(set, observed_warning_map.values())) and sum(
        len(items) for items in observed_warning_map.values()
    ) != len(set().union(*map(set, observed_warning_map.values()))):
        raise SilverStoreError("S4 warning result ID appears in more than one table")
    runtime_review_digest = stable_digest(plan.runtime_review.to_dict())
    runtime_evidence_verifier(root, plan)
    git_verifier(repo_root, release_orchestration_git_commit, plan)

    store = SilverStore(root)
    preflight = _preflight_release(
        store,
        root,
        plan,
        recorded_at=recorded_at,
        verify_artifacts=False,
    )
    group_approval = AssetReleaseSetApproval(
        publish_plan_id=plan.plan_id,
        publish_plan_path=plan_document.path,
        publish_plan_sha256=plan_document.sha256,
        publish_plan_bytes=plan_document.bytes,
        publish_plan_creator_commit=expected_publish_plan_creator_commit,
        materialization_git_commit=expected_materialization_commit,
        release_orchestration_git_commit=release_orchestration_git_commit,
        approval_text=APPROVAL_TEXT,
        approval_text_sha256=APPROVAL_TEXT_SHA256,
        approver=APPROVER,
        decided_at=recorded_at,
        warning_result_ids_by_table=observed_warning_map,
        accepted_quarantine_issue_ids_by_table=_empty_quarantine_map(),
        runtime_review_digest=runtime_review_digest,
    )
    group_approval_path = (
        "manifests/silver/release-set-approvals/assets/"
        f"approval_id={group_approval.approval_id}/manifest.json"
    )
    group_approval_document = _document_identity(
        group_approval_path, group_approval.to_dict()
    )
    transactions = _precompute_transactions(
        plan,
        preflight,
        group_approval,
        group_approval_document,
        recorded_at=recorded_at,
    )
    intent = AssetReleaseSetIntent(
        group_approval_id=group_approval.approval_id,
        group_approval_path=group_approval_document.path,
        group_approval_sha256=group_approval_document.sha256,
        publish_plan_id=plan.plan_id,
        publish_plan_path=plan_document.path,
        publish_plan_sha256=plan_document.sha256,
        release_orchestration_git_commit=release_orchestration_git_commit,
        recorded_at=recorded_at,
        runtime_review_digest=runtime_review_digest,
        members=tuple(transactions[table].member for table in _TABLE_ORDER),
    )
    intent_path = (
        "manifests/silver/release-set-intents/assets/"
        f"intent_id={intent.intent_id}/manifest.json"
    )
    intent_document = _document_identity(intent_path, intent.to_dict())
    release_set = AssetReleaseSet(
        intent_id=intent.intent_id,
        intent_path=intent_document.path,
        intent_sha256=intent_document.sha256,
        group_approval_id=group_approval.approval_id,
        group_approval_path=group_approval_document.path,
        group_approval_sha256=group_approval_document.sha256,
        publish_plan_id=plan.plan_id,
        publish_plan_path=plan_document.path,
        publish_plan_sha256=plan_document.sha256,
        publish_plan_bytes=plan_document.bytes,
        publish_plan_creator_commit=expected_publish_plan_creator_commit,
        materialization_git_commit=expected_materialization_commit,
        release_orchestration_git_commit=release_orchestration_git_commit,
        committed_at=recorded_at,
        runtime_review_digest=runtime_review_digest,
        runtime_review_accepted=True,
        members=intent.members,
    )
    release_set_path = (
        "manifests/silver/release-sets/assets/"
        f"release_set_id={release_set.release_set_id}/manifest.json"
    )
    release_set_document = _document_identity(
        release_set_path, release_set.to_dict()
    )

    if before_final_lock is not None:
        before_final_lock()
    release_root = root / "manifests" / "silver" / "release-sets" / "assets"
    with store._directory_lock(release_root, ".lock"), ExitStack() as locks:
        for workflow_id in sorted(item.workflow_id for item in plan.tables):
            locks.enter_context(store._workflow_lock(workflow_id))
        locked_plan, locked_plan_document = _load_publish_plan(
            root,
            expected_publish_plan_id,
            expected_publish_plan_sha256,
            expected_publish_plan_bytes,
        )
        if locked_plan != plan or locked_plan_document != plan_document:
            raise SilverStoreError("S4 PublishPlan changed before release-set commit")
        runtime_evidence_verifier(root, locked_plan)
        git_verifier(repo_root, release_orchestration_git_commit, locked_plan)
        _preflight_release(
            store,
            root,
            locked_plan,
            recorded_at=recorded_at,
            verify_artifacts=True,
        )
        runtime_evidence_verifier(root, locked_plan)
        for table in _TABLE_ORDER:
            _assert_exact_transaction_prefix(store, transactions[table])
        _assert_no_competing_authority(
            root, group_approval, intent, release_set
        )

        observed_group_document = _write_control_document(
            root, group_approval_document.path, group_approval.to_dict()
        )
        if observed_group_document != group_approval_document:
            raise SilverStoreError("S4 group approval document identity changed")
        _barrier(transition_barrier, "group_approval", None)
        observed_intent_document = _write_control_document(
            root, intent_document.path, intent.to_dict()
        )
        if observed_intent_document != intent_document:
            raise SilverStoreError("S4 release-set intent identity changed")
        _barrier(transition_barrier, "intent", None)

        # Phase one: every member reaches the exact hidden awaiting_publish prefix
        # before any member can receive its published event.
        for table in _TABLE_ORDER:
            transaction = transactions[table]
            events = store.workflow_events(transaction.member.workflow_id)
            if len(events) == 8:
                record = store._write_event(transaction.request_event)
                if record.event_sha256 != transaction.member.awaiting_publish_event_sha256:
                    raise SilverStoreError("S4 awaiting-publish event digest changed")
            _assert_exact_transaction_prefix(store, transaction, minimum_sequence=9)
            _barrier(transition_barrier, "awaiting_publish", table)

        # Phase two: deterministic orphan approval/release documents are safe; the
        # reader guard keeps them invisible until the final marker is committed.
        for table in _TABLE_ORDER:
            transaction = transactions[table]
            events = store.workflow_events(transaction.member.workflow_id)
            if len(events) == 9:
                approval_document = store._store_approval(transaction.publish_receipt)
                release_document = store._store_release(transaction.release)
                if (
                    approval_document != transaction.publish_receipt_document
                    or release_document != transaction.release_document
                ):
                    raise SilverStoreError("S4 publish document identity changed")
                _barrier(transition_barrier, "publish_documents", table)
                record = store._write_event(transaction.publish_event)
                if record.event_sha256 != transaction.member.published_event_sha256:
                    raise SilverStoreError("S4 published event digest changed")
            _assert_exact_transaction_prefix(store, transaction, minimum_sequence=10)
            _barrier(transition_barrier, "published", table)

        workflows: dict[str, WorkflowSnapshot] = {}
        for table in _TABLE_ORDER:
            transaction = transactions[table]
            snapshot = store.verify_workflow_trust_chain(
                transaction.member.workflow_id, verify_artifacts=False
            )
            if (
                snapshot.state is not WorkflowState.PUBLISHED
                or snapshot.sequence != 10
                or snapshot.event_sha256
                != transaction.member.published_event_sha256
            ):
                raise SilverStoreError(f"S4 release-set member did not publish: {table}")
            workflows[table] = snapshot

        path = safe_relative_path(root, release_set_path)
        existed = path.exists()
        runtime_evidence_verifier(root, locked_plan)
        _barrier(transition_barrier, "before_marker", None)
        document = _write_control_document(root, release_set_path, release_set.to_dict())
        if document != release_set_document:
            raise SilverStoreError("S4 release-set marker identity changed")
        observed = AssetReleaseSet.from_dict(json.loads(path.read_bytes()))
        if observed != release_set:
            raise SilverStoreError("stored S4 release-set marker changed")
        for table in _TABLE_ORDER:
            _assert_exact_transaction_prefix(
                store, transactions[table], minimum_sequence=10
            )
        _barrier(transition_barrier, "marker", None)

    verified = _verify_release_set(root, release_set, document)
    if verified != release_set:
        raise SilverStoreError("S4 release-set post-commit verification changed")
    return AssetReleaseSetRun(
        publish_plan=plan,
        release_set=release_set,
        document=document,
        approval=group_approval,
        approval_document=group_approval_document,
        intent=intent,
        intent_document=intent_document,
        workflows_by_table=MappingProxyType(workflows),
        idempotent=existed,
    )


def _preflight_release(
    store: SilverStore,
    root: Path,
    plan: AssetPublishPlan,
    *,
    recorded_at: str,
    verify_artifacts: bool,
) -> _Preflight:
    snapshots: dict[str, WorkflowSnapshot] = {}
    builds: dict[str, object] = {}
    build_documents: dict[str, StoredDocument] = {}
    contracts: dict[str, object] = {}
    shared_inputs: tuple[ArtifactRef, ...] | None = None
    for table_plan in plan.tables:
        table = table_plan.table
        snapshot = store.verify_workflow_trust_chain(
            table_plan.workflow_id, verify_artifacts=False
        )
        if snapshot.state not in {
            WorkflowState.FULL_READY,
            WorkflowState.AWAITING_PUBLISH,
            WorkflowState.PUBLISHED,
        } or snapshot.sequence not in {8, 9, 10}:
            raise SilverStoreError(
                f"S4 release-set cannot resume {table} from {snapshot.state.value}"
            )
        events = store.workflow_events(table_plan.workflow_id)
        if (
            len(events) < 8
            or events[7].event_sha256 != table_plan.full_ready_event_sha256
            or events[7].path != table_plan.full_ready_event_path
        ):
            raise SilverStoreError(f"S4 full-ready event changed for {table}")
        if _utc_datetime(recorded_at, "recorded_at") < _utc_datetime(
            events[7].event.created_at,
            f"{table} full-ready created_at",
        ):
            raise SilverStoreError(
                f"S4 release recorded_at predates full_ready for {table}"
            )
        contract, _ = store.load_workflow_contract(table_plan.workflow_id)
        if contract != ASSET_CONTRACTS[table]:
            raise SilverStoreError(f"S4 release-set contract changed for {table}")
        build, build_document = store.load_build(table, table_plan.build_id)
        if (
            build_document.sha256 != table_plan.build_manifest_sha256
            or build_document.path != table_plan.build_manifest_path
            or build.intent.kind is not BuildKind.FULL
            or build.intent.workflow_id != table_plan.workflow_id
        ):
            raise SilverStoreError(f"S4 release-set build changed for {table}")
        full_plan, full_plan_document = store.load_full_run_plan(
            table, table_plan.full_run_plan_id
        )
        if (
            full_plan_document.sha256 != table_plan.full_run_plan_sha256
            or full_plan_document.path != table_plan.full_run_plan_path
            or full_plan.source_digest != table_plan.source_digest
        ):
            raise SilverStoreError(f"S4 full-run plan changed for {table}")
        warnings = tuple(
            sorted(check.result_id for check in build.qa_checks if check.status.value == "warning")
        )
        if warnings != table_plan.warning_result_ids:
            raise SilverStoreError(f"S4 release warning set changed for {table}")
        store.validate_qa_gate(build, warnings, ())
        if verify_artifacts:
            _verify_build_outputs_without_sources(
                store,
                root,
                build,
                contract,
                workflow_id=table_plan.workflow_id,
                build_id=table_plan.build_id,
            )
        data_outputs = tuple(
            output for output in build.outputs if output.role is ArtifactRole.DATA
        )
        if (
            len(data_outputs) != table_plan.output_data_partition_count
            or sum(int(item.row_count or 0) for item in data_outputs)
            != table_plan.output_rows
            or sum(item.bytes for item in data_outputs) != table_plan.output_data_bytes
        ):
            raise SilverStoreError(f"S4 release output summary changed for {table}")
        if shared_inputs is None:
            shared_inputs = full_plan.inputs
        elif tuple(item.to_dict() for item in full_plan.inputs) != tuple(
            item.to_dict() for item in shared_inputs
        ):
            raise SilverStoreError("S4 release members no longer share one source inventory")
        snapshots[table] = snapshot
        builds[table] = build
        build_documents[table] = build_document
        contracts[table] = contract
    if shared_inputs is None:  # pragma: no cover - AssetPublishPlan enforces three tables
        raise SilverStoreError("S4 release set has no source inputs")
    if verify_artifacts:
        store.verify_source_artifacts(shared_inputs, ASSET_CONTRACTS[_TABLE_ORDER[0]])
    return _Preflight(
        snapshots=MappingProxyType(snapshots),
        builds=MappingProxyType(builds),
        build_documents=MappingProxyType(build_documents),
        contracts=MappingProxyType(contracts),
    )


def _precompute_transactions(
    plan: AssetPublishPlan,
    preflight: _Preflight,
    group_approval: AssetReleaseSetApproval,
    group_approval_document: StoredDocument,
    *,
    recorded_at: str,
) -> Mapping[str, _MemberTransaction]:
    transactions: dict[str, _MemberTransaction] = {}
    for table_plan in plan.tables:
        table = table_plan.table
        contract = preflight.contracts[table]
        build = preflight.builds[table]
        build_document = preflight.build_documents[table]
        request_note = _expected_request_note(
            plan.plan_id, group_approval.approval_id
        )
        request_event = WorkflowEvent(
            workflow_id=table_plan.workflow_id,
            sequence=9,
            previous_event_sha256=table_plan.full_ready_event_sha256,
            from_state=WorkflowState.FULL_READY,
            to_state=WorkflowState.AWAITING_PUBLISH,
            actor=COORDINATOR_ACTOR,
            created_at=recorded_at,
            evidence={},
            note=request_note,
        )
        request_sha = _sha256_bytes(_json_bytes(request_event.to_dict()))
        publish_note = _expected_publish_note(
            plan.plan_id,
            group_approval.publish_plan_sha256,
            group_approval.approval_id,
            group_approval_document.sha256,
        )
        receipt = ApprovalReceipt(
            workflow_id=table_plan.workflow_id,
            stage=ApprovalStage.PUBLISH,
            decision=ApprovalDecision.APPROVED,
            subject_id=table_plan.build_id,
            subject_manifest_sha256=table_plan.build_manifest_sha256,
            expected_event_sha256=request_sha,
            approver=APPROVER,
            decided_at=recorded_at,
            note=publish_note,
            waived_qa_result_ids=table_plan.warning_result_ids,
            accepted_quarantine_issue_ids=(),
        )
        receipt_path = f"manifests/silver/approvals/{receipt.approval_id}.json"
        receipt_document = _document_identity(receipt_path, receipt.to_dict())
        outputs = tuple(
            output for output in build.outputs if output.role is ArtifactRole.DATA
        )
        release = ReleaseManifest(
            workflow_id=table_plan.workflow_id,
            domain=contract.domain,
            table=table,
            schema_version=contract.schema_version,
            contract_id=contract.contract_id,
            build_id=table_plan.build_id,
            build_manifest_sha256=build_document.sha256,
            approval_id=receipt.approval_id,
            approval_sha256=receipt_document.sha256,
            released_at=recorded_at,
            outputs=outputs,
        )
        release_path = (
            "manifests/silver/releases/"
            f"release_id={release.release_id}.json"
        )
        release_document = _document_identity(release_path, release.to_dict())
        publish_event = WorkflowEvent(
            workflow_id=table_plan.workflow_id,
            sequence=10,
            previous_event_sha256=request_sha,
            from_state=WorkflowState.AWAITING_PUBLISH,
            to_state=WorkflowState.PUBLISHED,
            actor=APPROVER,
            created_at=recorded_at,
            evidence={
                "approval_id": receipt.approval_id,
                "approval_path": receipt_document.path,
                "approval_sha256": receipt_document.sha256,
                "build_id": table_plan.build_id,
                "build_manifest_sha256": build_document.sha256,
                "release_id": release.release_id,
                "release_path": release_document.path,
                "release_sha256": release_document.sha256,
            },
            note=publish_note,
        )
        published_sha = _sha256_bytes(_json_bytes(publish_event.to_dict()))
        member = AssetReleaseSetMember(
            table=table,
            domain=contract.domain,
            workflow_id=table_plan.workflow_id,
            contract_id=contract.contract_id,
            schema_version=contract.schema_version,
            full_ready_event_sha256=table_plan.full_ready_event_sha256,
            awaiting_publish_event_sha256=request_sha,
            published_event_sha256=published_sha,
            full_run_plan_id=table_plan.full_run_plan_id,
            full_run_plan_sha256=table_plan.full_run_plan_sha256,
            build_id=table_plan.build_id,
            build_manifest_sha256=build_document.sha256,
            warning_result_ids=table_plan.warning_result_ids,
            accepted_quarantine_issue_ids=(),
            approval_id=receipt.approval_id,
            approval_path=receipt_document.path,
            approval_sha256=receipt_document.sha256,
            release_id=release.release_id,
            release_path=release_document.path,
            release_sha256=release_document.sha256,
            outputs=outputs,
        )
        transactions[table] = _MemberTransaction(
            member=member,
            request_event=request_event,
            publish_receipt=receipt,
            publish_receipt_document=receipt_document,
            release=release,
            release_document=release_document,
            publish_event=publish_event,
        )
    return MappingProxyType(transactions)


def _expected_request_note(publish_plan_id: str, group_approval_id: str) -> str:
    return (
        f"Submitted exact S4 PublishPlan {publish_plan_id} as group approval "
        f"{group_approval_id}; member remains hidden until release-set marker."
    )


def _expected_publish_note(
    publish_plan_id: str,
    publish_plan_sha256: str,
    group_approval_id: str,
    group_approval_sha256: str,
) -> str:
    return (
        f"User authorization SHA-256={APPROVAL_TEXT_SHA256}; S4 PublishPlan "
        f"id={publish_plan_id}, sha256={publish_plan_sha256}; group approval "
        f"id={group_approval_id}, sha256={group_approval_sha256}; "
        f"runtime review accepted; publication_scope={ASSET_PUBLICATION_SCOPE}; "
        "backtest_identity_eligible=false."
    )


def _assert_exact_transaction_prefix(
    store: SilverStore,
    transaction: _MemberTransaction,
    *,
    minimum_sequence: int = 8,
) -> None:
    events = store.workflow_events(transaction.member.workflow_id)
    if len(events) not in {8, 9, 10} or len(events) < minimum_sequence:
        raise SilverStoreError("S4 release-set workflow is not an exact recoverable prefix")
    if events[7].event_sha256 != transaction.member.full_ready_event_sha256:
        raise SilverStoreError("S4 release-set full-ready prefix changed")
    _read_immutable_bytes(
        store.root,
        events[7].path,
        transaction.member.full_ready_event_sha256,
    )
    if len(events) >= 9 and (
        events[8].event != transaction.request_event
        or events[8].event_sha256 != transaction.member.awaiting_publish_event_sha256
    ):
        raise SilverStoreError("S4 release-set awaiting-publish prefix changed")
    if len(events) >= 9:
        _read_immutable_bytes(
            store.root,
            events[8].path,
            transaction.member.awaiting_publish_event_sha256,
        )
    if len(events) >= 10:
        approval, approval_document = store.load_approval(transaction.member.approval_id)
        release, release_document = store.load_release(transaction.member.release_id)
        if (
            approval != transaction.publish_receipt
            or approval_document != transaction.publish_receipt_document
            or release != transaction.release
            or release_document != transaction.release_document
            or events[9].event != transaction.publish_event
            or events[9].event_sha256 != transaction.member.published_event_sha256
        ):
            raise SilverStoreError("S4 release-set published prefix changed")
        _read_immutable_bytes(
            store.root,
            transaction.member.approval_path,
            transaction.member.approval_sha256,
        )
        _read_immutable_bytes(
            store.root,
            transaction.member.release_path,
            transaction.member.release_sha256,
        )
        _read_immutable_bytes(
            store.root,
            events[9].path,
            transaction.member.published_event_sha256,
        )


def _assert_no_competing_authority(
    root: Path,
    expected_approval: AssetReleaseSetApproval,
    expected_intent: AssetReleaseSetIntent,
    expected_release_set: AssetReleaseSet,
) -> None:
    """Reject a second immutable authority for the same exact PublishPlan."""

    approvals_root = (
        root / "manifests" / "silver" / "release-set-approvals" / "assets"
    )
    if approvals_root.exists():
        if approvals_root.is_symlink() or not approvals_root.is_dir():
            raise SilverStoreError("S4 group approval directory is unsafe")
        for path in sorted(approvals_root.glob("approval_id=*/manifest.json")):
            if path.is_symlink() or path.parent.is_symlink():
                raise SilverStoreError("S4 group approval path contains a symlink")
            content = _read_immutable_bytes(root, path.relative_to(root).as_posix())
            try:
                observed = AssetReleaseSetApproval.from_dict(json.loads(content))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise SilverStoreError("S4 group approval is invalid JSON") from exc
            if path.parent.name != f"approval_id={observed.approval_id}":
                raise SilverStoreError("S4 group approval path identity changed")
            if observed.publish_plan_id == expected_approval.publish_plan_id and (
                observed.approval_id != expected_approval.approval_id
                or observed != expected_approval
            ):
                raise SilverStoreError(
                    "S4 PublishPlan already has a different immutable group approval"
                )

    intents_root = root / "manifests" / "silver" / "release-set-intents" / "assets"
    if intents_root.exists():
        if intents_root.is_symlink() or not intents_root.is_dir():
            raise SilverStoreError("S4 release-set intent directory is unsafe")
        for path in sorted(intents_root.glob("intent_id=*/manifest.json")):
            if path.is_symlink() or path.parent.is_symlink():
                raise SilverStoreError("S4 release-set intent path contains a symlink")
            content = _read_immutable_bytes(root, path.relative_to(root).as_posix())
            try:
                observed = AssetReleaseSetIntent.from_dict(json.loads(content))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise SilverStoreError("S4 release-set intent is invalid JSON") from exc
            if path.parent.name != f"intent_id={observed.intent_id}":
                raise SilverStoreError("S4 release-set intent path identity changed")
            if observed.publish_plan_id == expected_intent.publish_plan_id and (
                observed.intent_id != expected_intent.intent_id
                or observed != expected_intent
            ):
                raise SilverStoreError(
                    "S4 PublishPlan already has a different immutable release-set intent"
                )

    release_sets_root = root / "manifests" / "silver" / "release-sets" / "assets"
    if release_sets_root.exists():
        if release_sets_root.is_symlink() or not release_sets_root.is_dir():
            raise SilverStoreError("S4 release-set marker directory is unsafe")
        for path in sorted(release_sets_root.glob("release_set_id=*/manifest.json")):
            if path.is_symlink() or path.parent.is_symlink():
                raise SilverStoreError("S4 release-set marker path contains a symlink")
            content = _read_immutable_bytes(root, path.relative_to(root).as_posix())
            try:
                observed = AssetReleaseSet.from_dict(json.loads(content))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise SilverStoreError("S4 release-set marker is invalid JSON") from exc
            if path.parent.name != f"release_set_id={observed.release_set_id}":
                raise SilverStoreError("S4 release-set marker path identity changed")
            if (
                observed.publish_plan_id == expected_intent.publish_plan_id
                and observed != expected_release_set
            ):
                raise SilverStoreError(
                    "S4 PublishPlan already has a different immutable release-set marker"
                )


def _barrier(
    callback: Callable[[str, str | None], None] | None,
    stage: str,
    table: str | None,
) -> None:
    if callback is not None:
        callback(stage, table)


def _document_identity(path: str, document: Mapping[str, object]) -> StoredDocument:
    content = _json_bytes(document)
    return StoredDocument(path=path, sha256=_sha256_bytes(content), bytes=len(content))


def _write_control_document(
    root: Path,
    path: str,
    document: Mapping[str, object],
) -> StoredDocument:
    content = _json_bytes(document)
    stored = write_bytes_immutable(root, safe_relative_path(root, path), content)
    observed = StoredDocument(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
    )
    _read_immutable_bytes(root, path, observed.sha256, observed.bytes)
    return observed


def _load_publish_plan(
    root: Path,
    plan_id: str,
    expected_sha256: str,
    expected_bytes: int,
) -> tuple[AssetPublishPlan, StoredDocument]:
    _digest(plan_id, "publish plan ID")
    _digest(expected_sha256, "publish plan SHA")
    _positive_int(expected_bytes, "publish plan bytes")
    path = (
        "manifests/silver/publish-plans/assets/"
        f"plan_id={plan_id}/manifest.json"
    )
    content = _read_immutable_bytes(root, path, expected_sha256, expected_bytes)
    try:
        plan = AssetPublishPlan.from_dict(json.loads(content))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SilverStoreError("S4 PublishPlan is invalid JSON") from exc
    if plan.plan_id != plan_id:
        raise SilverStoreError("S4 PublishPlan path identity changed")
    return plan, StoredDocument(path=path, sha256=expected_sha256, bytes=expected_bytes)


def _read_immutable_bytes(
    root: Path,
    relative_path: str,
    expected_sha256: str | None = None,
    expected_bytes: int | None = None,
) -> bytes:
    path = safe_relative_path(root, relative_path)
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise SilverStoreError(f"cannot read immutable S4 release document: {path}") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_mode & 0o222
        ):
            raise SilverStoreError(f"S4 release document is not immutable: {path}")
        if before.st_size > 128 * 1024 * 1024:
            raise SilverStoreError(f"S4 release document exceeds 128 MiB: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            content = handle.read()
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    def identity(item: os.stat_result) -> tuple[int, ...]:
        return (
            item.st_dev,
            item.st_ino,
            item.st_mode,
            item.st_nlink,
            item.st_size,
            item.st_mtime_ns,
            item.st_ctime_ns,
        )
    if identity(before) != identity(after) or len(content) != before.st_size:
        raise SilverStoreError(f"S4 release document changed while reading: {path}")
    observed_sha = _sha256_bytes(content)
    if expected_sha256 is not None and observed_sha != expected_sha256:
        raise SilverStoreError(f"S4 release document checksum changed: {path}")
    if expected_bytes is not None and len(content) != expected_bytes:
        raise SilverStoreError(f"S4 release document byte count changed: {path}")
    return content


def _verify_runtime_review_file(root: Path, plan: AssetPublishPlan) -> None:
    evidence = plan.runtime_review
    path = safe_relative_path(root, evidence.source_path)
    if path.is_symlink() or not path.is_file():
        raise SilverStoreError("S4 runtime review evidence file is missing")
    before = path.stat()
    content = path.read_bytes()
    after = path.stat()
    if (
        before.st_ino != after.st_ino
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_nlink != 1
        or after.st_nlink != 1
        or len(content) != evidence.source_bytes
        or _sha256_bytes(content) != evidence.source_sha256
    ):
        raise SilverStoreError("S4 runtime review evidence changed")


def require_asset_release_set_membership(
    data_root: Path,
    release_id: str,
) -> AssetReleaseSet:
    """Require one complete marker containing ``release_id`` and validate all members."""

    root, match = _asset_release_set_membership_marker(data_root, release_id)
    verified = _verify_release_set(root, *match)
    _require_production_release_authority(root, verified)
    return verified


def _require_asset_release_set_control_membership(
    data_root: Path,
    release_id: str,
) -> AssetReleaseSet:
    """Authenticate the complete S4 control plane without reading member DATA.

    This private boundary exists only for a caller that immediately verifies an exact,
    capability-bound artifact subset.  The public evidence reader continues to use
    :func:`require_asset_release_set_membership`, which physically verifies every member.
    """

    root, match = _asset_release_set_membership_marker(data_root, release_id)
    verified = _verify_release_set_control_plane(root, *match)
    _require_production_release_authority(root, verified)
    return verified


def _asset_release_set_membership_marker(
    data_root: Path,
    release_id: str,
) -> tuple[Path, tuple[AssetReleaseSet, StoredDocument]]:
    """Resolve exactly one immutable release-set marker containing ``release_id``."""

    _digest(release_id, "release ID")
    root = data_root.expanduser().resolve()
    base = root / "manifests" / "silver" / "release-sets" / "assets"
    if not base.is_dir() or base.is_symlink():
        raise SilverStoreError("S4 release-set marker is missing")
    matches: list[tuple[AssetReleaseSet, StoredDocument]] = []
    for path in sorted(base.glob("release_set_id=*/manifest.json")):
        if path.is_symlink() or path.parent.is_symlink():
            raise SilverStoreError("S4 release-set marker path contains a symlink")
        relative = str(path.relative_to(root))
        content = _read_immutable_bytes(root, relative)
        try:
            release_set = AssetReleaseSet.from_dict(json.loads(content))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SilverStoreError("S4 release-set marker is invalid JSON") from exc
        if path.parent.name != f"release_set_id={release_set.release_set_id}":
            raise SilverStoreError("S4 release-set marker path identity changed")
        if any(member.release_id == release_id for member in release_set.members):
            matches.append(
                (
                    release_set,
                    StoredDocument(
                        path=relative,
                        sha256=_sha256_bytes(content),
                        bytes=len(content),
                    ),
                )
            )
    if len(matches) != 1:
        raise SilverStoreError(
            "S4 release is not backed by exactly one complete release-set marker"
        )
    return root, matches[0]


def _require_production_release_authority(
    root: Path,
    release_set: AssetReleaseSet,
) -> None:
    """Pin the public reader boundary to the one user-authorized production plan."""

    if (
        release_set.publish_plan_id != CURRENT_ASSET_PUBLISH_PLAN_ID
        or release_set.publish_plan_path != CURRENT_ASSET_PUBLISH_PLAN_PATH
        or release_set.publish_plan_sha256 != CURRENT_ASSET_PUBLISH_PLAN_SHA256
        or release_set.publish_plan_bytes != CURRENT_ASSET_PUBLISH_PLAN_BYTES
        or release_set.publish_plan_creator_commit
        != CURRENT_ASSET_PUBLISH_PLAN_CREATOR_COMMIT
        or release_set.materialization_git_commit
        != CURRENT_ASSET_MATERIALIZATION_COMMIT
        or release_set.publication_scope != ASSET_PUBLICATION_SCOPE
        or release_set.backtest_identity_eligible is not False
        or release_set.runtime_review_accepted is not True
    ):
        raise SilverStoreError("S4 release-set marker is not the production authority")
    content = _read_immutable_bytes(
        root,
        release_set.group_approval_path,
        release_set.group_approval_sha256,
    )
    approval = AssetReleaseSetApproval.from_dict(json.loads(content))
    if (
        approval.approval_text != APPROVAL_TEXT
        or approval.approval_text_sha256 != APPROVAL_TEXT_SHA256
        or approval.approver != APPROVER
        or dict(approval.warning_result_ids_by_table)
        != dict(_CURRENT_WARNING_RESULT_IDS)
        or dict(approval.accepted_quarantine_issue_ids_by_table)
        != dict(_empty_quarantine_map())
    ):
        raise SilverStoreError("S4 release-set approval is not the production authority")
    plan, _ = _load_publish_plan(
        root,
        release_set.publish_plan_id,
        release_set.publish_plan_sha256,
        release_set.publish_plan_bytes,
    )
    _verify_runtime_review_file(root, plan)


def _verify_release_set(
    root: Path,
    release_set: AssetReleaseSet,
    document: StoredDocument,
) -> AssetReleaseSet:
    """Verify the complete control plane and every physical DATA member."""

    return _verify_release_set_impl(
        root,
        release_set,
        document,
        verify_member_artifacts=True,
    )


def _verify_release_set_control_plane(
    root: Path,
    release_set: AssetReleaseSet,
    document: StoredDocument,
) -> AssetReleaseSet:
    """Verify the complete immutable control plane, leaving DATA to a scoped verifier."""

    return _verify_release_set_impl(
        root,
        release_set,
        document,
        verify_member_artifacts=False,
    )


def _verify_release_set_impl(
    root: Path,
    release_set: AssetReleaseSet,
    document: StoredDocument,
    *,
    verify_member_artifacts: bool,
) -> AssetReleaseSet:
    if type(verify_member_artifacts) is not bool:
        raise SilverStoreError("S4 release-set DATA verification mode is invalid")
    expected_marker_path = (
        "manifests/silver/release-sets/assets/"
        f"release_set_id={release_set.release_set_id}/manifest.json"
    )
    if document.path != expected_marker_path:
        raise SilverStoreError("S4 release-set marker path identity changed")
    marker_content = _read_immutable_bytes(
        root, document.path, document.sha256, document.bytes
    )
    if AssetReleaseSet.from_dict(json.loads(marker_content)) != release_set:
        raise SilverStoreError("S4 release-set marker content changed")
    approval_content = _read_immutable_bytes(
        root,
        release_set.group_approval_path,
        release_set.group_approval_sha256,
    )
    approval = AssetReleaseSetApproval.from_dict(json.loads(approval_content))
    if (
        approval.approval_id != release_set.group_approval_id
        or approval.publish_plan_id != release_set.publish_plan_id
        or approval.publish_plan_path != release_set.publish_plan_path
        or approval.publish_plan_sha256 != release_set.publish_plan_sha256
        or approval.publish_plan_bytes != release_set.publish_plan_bytes
        or approval.publish_plan_creator_commit
        != release_set.publish_plan_creator_commit
        or approval.materialization_git_commit
        != release_set.materialization_git_commit
        or approval.release_orchestration_git_commit
        != release_set.release_orchestration_git_commit
        or approval.decided_at != release_set.committed_at
        or approval.runtime_review_digest != release_set.runtime_review_digest
        or approval.publication_scope != release_set.publication_scope
        or approval.backtest_identity_eligible
        != release_set.backtest_identity_eligible
        or approval.runtime_review_accepted is not True
        or approval.approval_text != APPROVAL_TEXT
        or approval.approval_text_sha256 != APPROVAL_TEXT_SHA256
        or approval.approver != APPROVER
        or dict(approval.accepted_quarantine_issue_ids_by_table)
        != dict(_empty_quarantine_map())
    ):
        raise SilverStoreError("S4 release-set group approval changed")
    intent_content = _read_immutable_bytes(
        root, release_set.intent_path, release_set.intent_sha256
    )
    intent = AssetReleaseSetIntent.from_dict(json.loads(intent_content))
    if (
        intent.intent_id != release_set.intent_id
        or intent.members != release_set.members
        or intent.group_approval_id != approval.approval_id
        or intent.group_approval_path != release_set.group_approval_path
        or intent.group_approval_sha256 != release_set.group_approval_sha256
        or intent.publish_plan_id != release_set.publish_plan_id
        or intent.publish_plan_path != release_set.publish_plan_path
        or intent.publish_plan_sha256 != release_set.publish_plan_sha256
        or intent.release_orchestration_git_commit
        != release_set.release_orchestration_git_commit
        or intent.recorded_at != release_set.committed_at
        or intent.runtime_review_digest != release_set.runtime_review_digest
        or intent.publication_scope != release_set.publication_scope
        or intent.backtest_identity_eligible
        != release_set.backtest_identity_eligible
    ):
        raise SilverStoreError("S4 release-set intent changed")
    plan, plan_document = _load_publish_plan(
        root,
        release_set.publish_plan_id,
        release_set.publish_plan_sha256,
        release_set.publish_plan_bytes,
    )
    if (
        plan_document.path != release_set.publish_plan_path
        or plan.orchestration_git_commit != release_set.publish_plan_creator_commit
        or plan.materialization_git_commit != release_set.materialization_git_commit
        or stable_digest(plan.runtime_review.to_dict())
        != release_set.runtime_review_digest
        or plan.publication_scope != ASSET_PUBLICATION_SCOPE
        or plan.backtest_identity_eligible is not False
        or plan.requires_release_set is not True
        or plan.requires_runtime_review_acceptance is not True
    ):
        raise SilverStoreError("S4 release-set PublishPlan binding changed")
    warning_map = {item.table: item.warning_result_ids for item in plan.tables}
    if dict(approval.warning_result_ids_by_table) != warning_map:
        raise SilverStoreError("S4 release-set approval warnings changed")

    plan_by_table = {item.table: item for item in plan.tables}
    if set(plan_by_table) != set(_TABLE_ORDER):
        raise SilverStoreError("S4 release-set PublishPlan table scope changed")

    store = SilverStore(root)
    for member in release_set.members:
        table_plan = plan_by_table[member.table]
        _assert_member_matches_publish_plan(member, table_plan)
        release, release_document = store.load_release(member.release_id)
        receipt, receipt_document = store.load_approval(member.approval_id)
        contract, _ = store.load_workflow_contract(member.workflow_id)
        build, build_document = store.load_build(member.table, member.build_id)
        full_plan, full_plan_document = store.load_full_run_plan(
            member.table, member.full_run_plan_id
        )
        snapshot = store.verify_workflow_trust_chain(
            member.workflow_id, verify_artifacts=False
        )
        events = store.workflow_events(member.workflow_id)
        if len(events) != 10:
            raise SilverStoreError(
                f"S4 release-set workflow length changed: {member.table}"
            )
        request_event = events[8].event
        publish_event = events[9].event
        expected_request_note = _expected_request_note(
            plan.plan_id, approval.approval_id
        )
        expected_publish_note = _expected_publish_note(
            plan.plan_id,
            release_set.publish_plan_sha256,
            approval.approval_id,
            release_set.group_approval_sha256,
        )
        expected_publish_evidence = {
            "approval_id": member.approval_id,
            "approval_path": member.approval_path,
            "approval_sha256": member.approval_sha256,
            "build_id": member.build_id,
            "build_manifest_sha256": member.build_manifest_sha256,
            "release_id": member.release_id,
            "release_path": member.release_path,
            "release_sha256": member.release_sha256,
        }
        data_outputs = tuple(
            output for output in build.outputs if output.role is ArtifactRole.DATA
        )
        bindings = {
            "workflow": (
                snapshot.state is WorkflowState.PUBLISHED
                and snapshot.sequence == 10
                and snapshot.event_sha256 == member.published_event_sha256
                and events[7].path == table_plan.full_ready_event_path
                and events[7].event_sha256 == member.full_ready_event_sha256
                and events[8].event_sha256
                == member.awaiting_publish_event_sha256
                and request_event.sequence == 9
                and request_event.previous_event_sha256
                == member.full_ready_event_sha256
                and request_event.from_state is WorkflowState.FULL_READY
                and request_event.to_state is WorkflowState.AWAITING_PUBLISH
                and request_event.actor == COORDINATOR_ACTOR
                and request_event.created_at == release_set.committed_at
                and dict(request_event.evidence) == {}
                and request_event.note == expected_request_note
                and publish_event.sequence == 10
                and publish_event.previous_event_sha256
                == member.awaiting_publish_event_sha256
                and publish_event.from_state is WorkflowState.AWAITING_PUBLISH
                and publish_event.to_state is WorkflowState.PUBLISHED
                and publish_event.actor == approval.approver
                and publish_event.created_at == release_set.committed_at
                and dict(publish_event.evidence) == expected_publish_evidence
                and publish_event.note == expected_publish_note
            ),
            "contract_build_plan": (
                contract == ASSET_CONTRACTS[member.table]
                and contract.contract_id == table_plan.contract_id
                and contract.schema_version == table_plan.schema_version
                and contract.domain == member.domain
                and build_document.sha256 == member.build_manifest_sha256
                and build_document.path == table_plan.build_manifest_path
                and build.intent.kind is BuildKind.FULL
                and build.intent.workflow_id == member.workflow_id
                and full_plan_document.sha256
                == member.full_run_plan_sha256
                and full_plan_document.path == table_plan.full_run_plan_path
                and full_plan.source_digest == table_plan.source_digest
                and data_outputs == member.outputs
                and len(data_outputs) == table_plan.output_data_partition_count
                and sum(int(item.row_count or 0) for item in data_outputs)
                == table_plan.output_rows
                and sum(item.bytes for item in data_outputs)
                == table_plan.output_data_bytes
            ),
            "release": (
                release_document.sha256 == member.release_sha256
                and release_document.path == member.release_path
                and release.workflow_id == member.workflow_id
                and release.domain == member.domain
                and release.table == member.table
                and release.schema_version == member.schema_version
                and release.contract_id == member.contract_id
                and release.build_id == member.build_id
                and release.build_manifest_sha256
                == member.build_manifest_sha256
                and release.approval_id == member.approval_id
                and release.approval_sha256 == member.approval_sha256
                and release.released_at == release_set.committed_at
                and tuple(release.outputs) == member.outputs
            ),
            "receipt": (
                receipt_document.sha256 == member.approval_sha256
                and receipt_document.path == member.approval_path
                and receipt.workflow_id == member.workflow_id
                and receipt.stage is ApprovalStage.PUBLISH
                and receipt.decision is ApprovalDecision.APPROVED
                and receipt.subject_id == member.build_id
                and receipt.subject_manifest_sha256
                == member.build_manifest_sha256
                and receipt.expected_event_sha256
                == member.awaiting_publish_event_sha256
                and receipt.waived_qa_result_ids == member.warning_result_ids
                and not receipt.accepted_quarantine_issue_ids
                and receipt.approver == approval.approver
                and receipt.decided_at == approval.decided_at
                and receipt.note == expected_publish_note
            ),
        }
        failed_bindings = sorted(
            label for label, matches in bindings.items() if not matches
        )
        if failed_bindings:
            raise SilverStoreError(
                f"S4 release-set member changed: {member.table}: "
                f"{','.join(failed_bindings)}"
            )
        _read_immutable_bytes(root, events[7].path, member.full_ready_event_sha256)
        _read_immutable_bytes(
            root, events[8].path, member.awaiting_publish_event_sha256
        )
        _read_immutable_bytes(root, events[9].path, member.published_event_sha256)
        _read_immutable_bytes(root, member.approval_path, member.approval_sha256)
        _read_immutable_bytes(root, member.release_path, member.release_sha256)
        store.validate_qa_gate(build, member.warning_result_ids, ())
        if verify_member_artifacts:
            for output in member.outputs:
                store.verify_artifact(output, contract=contract)
    return release_set


def _assert_member_matches_publish_plan(
    member: AssetReleaseSetMember,
    table_plan: AssetPublishTablePlan,
) -> None:
    if (
        member.table != table_plan.table
        or member.workflow_id != table_plan.workflow_id
        or member.contract_id != table_plan.contract_id
        or member.schema_version != table_plan.schema_version
        or member.full_ready_event_sha256 != table_plan.full_ready_event_sha256
        or member.full_run_plan_id != table_plan.full_run_plan_id
        or member.full_run_plan_sha256 != table_plan.full_run_plan_sha256
        or member.build_id != table_plan.build_id
        or member.build_manifest_sha256 != table_plan.build_manifest_sha256
        or member.warning_result_ids != table_plan.warning_result_ids
        or member.accepted_quarantine_issue_ids
        != table_plan.accepted_quarantine_issue_ids
    ):
        raise SilverStoreError(
            f"S4 release-set member does not match PublishPlan: {member.table}"
        )


def _verify_release_checkout(
    repo_root: Path,
    release_orchestration_git_commit: str,
    plan: AssetPublishPlan,
) -> None:
    root = repo_root.expanduser().resolve()
    top = _git(root, "rev-parse", "--show-toplevel")
    if Path(top).resolve() != root:
        raise SilverStoreError("S4 release-set repo_root is not the Git top level")
    if _git(root, "rev-parse", "HEAD") != release_orchestration_git_commit:
        raise SilverStoreError("S4 release-set Git HEAD differs from pinned commit")
    if _git(root, "status", "--porcelain", "--untracked-files=all"):
        raise SilverStoreError("S4 release-set Git checkout is not clean")
    module_path = Path(__file__).resolve()
    try:
        module_relative = str(module_path.relative_to(root))
    except ValueError as exc:
        raise SilverStoreError("S4 release-set code is outside the verified checkout") from exc
    if not _git(root, "ls-files", "--error-unmatch", module_relative):
        raise SilverStoreError("S4 release-set module is not tracked")
    for ancestor in (plan.orchestration_git_commit, plan.materialization_git_commit):
        result = subprocess.run(
            ["git", "merge-base", "--is-ancestor", ancestor, release_orchestration_git_commit],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise SilverStoreError("S4 release-set commit ancestry changed")
    changed = set(
        filter(
            None,
            _git(
                root,
                "diff",
                "--name-only",
                plan.orchestration_git_commit,
                release_orchestration_git_commit,
            ).splitlines(),
        )
    )
    if not changed or not changed <= _ALLOWED_RELEASE_COMMIT_DIFF:
        raise SilverStoreError(
            f"S4 release-set commit contains unauthorized changes: {sorted(changed)}"
        )
    required = {
        "backend/ame_stocks_api/cli/silver_assets_release_set.py",
        "backend/ame_stocks_api/silver/asset_release_set.py",
        "backend/ame_stocks_api/silver/contracts.py",
        "backend/ame_stocks_api/silver/reader.py",
        "backend/ame_stocks_api/silver/store.py",
        "pyproject.toml",
    }
    if not required <= changed:
        raise SilverStoreError("S4 release-set commit is missing reviewed runtime files")


def _git(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise SilverStoreError(
            f"S4 release-set Git command failed: {' '.join(arguments)}: {detail}"
        )
    return result.stdout.strip()


__all__ = [
    "APPROVAL_TEXT",
    "APPROVAL_TEXT_SHA256",
    "ASSET_RELEASE_SET_POLICY_VERSION",
    "CURRENT_ASSET_PUBLISH_PLAN_ID",
    "CURRENT_ASSET_PUBLISH_PLAN_SHA256",
    "AssetReleaseSet",
    "AssetReleaseSetApproval",
    "AssetReleaseSetIntent",
    "AssetReleaseSetMember",
    "AssetReleaseSetRun",
    "asset_release_requires_set",
    "release_asset_publish_plan",
    "require_asset_release_set_membership",
]
