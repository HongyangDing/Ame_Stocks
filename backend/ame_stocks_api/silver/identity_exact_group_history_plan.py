"""Non-executing preparation controls for the S7 exact-group history review.

The package frozen here is deliberately narrower than an identity decision.  It
authorizes a later control-plane build for three exact provider/market groups;
it cannot inspect a manifest, stat or open a Parquet file, infer an effective
interval, adjudicate an identity, or publish a Silver table.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
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
from ame_stocks_api.silver.identity_exact_group_history_contract import (
    EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST,
    EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST,
    EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID,
    EXACT_GROUP_HISTORY_S4_RELEASE_SET_MANIFEST_SHA256,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST,
)

SCHEMA_VERSION: Final = 1
SCOPE_RULE_VERSION: Final = "s7_exact_group_history_scope_v1"
PREPARATION_RULE_VERSION: Final = "s7_exact_group_history_preparation_v1"
PREPARATION_REQUEST_RULE_VERSION: Final = "s7_exact_group_history_preparation_request_v1"
PREPARATION_AUTHORIZED_ACTION: Final = (
    "prepare_and_freeze_exact_s7_three_group_full_history_review_control_package_"
    "without_manifest_or_parquet_read_or_execution"
)
PREPARATION_LITERAL_VERSION: Final = "s7_exact_group_history_preparation_literal_v1"
START_SESSION: Final = date(2016, 7, 11)
END_SESSION: Final = date(2026, 7, 9)
SESSION_COUNT: Final = 2_513
SOURCE_ARTIFACT_COUNT: Final = 5_026
SOURCE_ROW_COUNT: Final = 138_757_511
SOURCE_BYTES: Final = 15_910_278_169

INVENTORY_CANDIDATE_ID: Final = "b35dc51b5798db2f8cf7783a1f2953990898bc5dde539107beabe53d85a57044"
INVENTORY_CANDIDATE_SHA256: Final = (
    "11fa38df8aaa07a781e80e80d0844213bf7d859cba3826ef26c693d735697970"
)
INVENTORY_COMPLETION_ID: Final = "4472b730bbf5e77b19253c0f6bfc4b78df3135bc2f46424262fff7f735cdce15"
INVENTORY_COMPLETION_SHA256: Final = (
    "255197634284c23c0b42f17b59398c07d5ab1d9d8c9f82493a363924a240a282"
)
INVENTORY_SOURCE_ARTIFACT_SET_DIGEST: Final = (
    "cb4a0e7cb73a59edcc74d2a8601c26d167dd1f9eed7b9821010040ddb0abcaaf"
)
DIRECTIONAL_CANDIDATE_ID: Final = "470d217b5eabb68949b14acc40c928ae919a7a3e1c14f60d992ce2bda838a5dd"
DIRECTIONAL_CANDIDATE_SHA256: Final = (
    "e3b4ee3f0369a1fc6a9327bbe82e937636e71d1232245a306497a956f83a5074"
)
DIRECTIONAL_COMPLETION_ID: Final = (
    "768505241e743da7a28569b9ba09bcef61742053e4d602f80d5aac7984cb5d1b"
)
DIRECTIONAL_COMPLETION_SHA256: Final = (
    "61a6e0259834a12a5569951a6ff83f33bed049576642c33883e78cd5e05b254a"
)
CALENDAR_ARTIFACT_ID: Final = "31cc575ae55542a580ee17e09aa242159bbcaedd0a001fd2184021a541b734bd"
CALENDAR_ARTIFACT_SHA256: Final = "3f026761a9f752d1e00c89c9f72383e7d8c0a7f7dcb2cdf8ef82e5831dfc0da7"

ASSET_RELEASE_ID: Final = "26819530e50cb92cbe0ec833d4b731b959c8bd2463ee2197255c02994241d44c"
ASSET_RELEASE_SHA256: Final = "f5fb26e75f44382caddf980e8fdf88a77903465b55bfd367f8d9029852848084"
UNIVERSE_RELEASE_ID: Final = "c7e0d9a75857cbca130ba8873a737411ccb2f11d3e711ee0c0b0d9d0e2f5c614"
UNIVERSE_RELEASE_SHA256: Final = "6b2c6ca1b612c4c38ddc8e359c1402c177a4f19b0295604d42b78bcd5804596d"

FIXED_GROUPS: Final = (
    MappingProxyType(
        {
            "provider": "massive",
            "market": "stocks",
            "locale": "us",
            "ticker": "SOR",
            "observed_composite_figi": "BBG000KMY6N2",
        }
    ),
    MappingProxyType(
        {
            "provider": "massive",
            "market": "stocks",
            "locale": "us",
            "ticker": "XZO",
            "observed_composite_figi": "BBG01XL8FHT0",
        }
    ),
    MappingProxyType(
        {
            "provider": "massive",
            "market": "stocks",
            "locale": "us",
            "ticker": "ANABV",
            "observed_composite_figi": "BBG021DMXXT2",
        }
    ),
)

DEFAULT_RUNTIME_PATHS: Final = frozenset(
    {
        "pyproject.toml",
        "backend/ame_stocks_api/__init__.py",
        "backend/ame_stocks_api/artifacts.py",
        "backend/ame_stocks_api/providers/__init__.py",
        "backend/ame_stocks_api/providers/massive.py",
        "backend/ame_stocks_api/providers/mock.py",
        "backend/ame_stocks_api/cli/__init__.py",
        "backend/ame_stocks_api/silver/__init__.py",
        "backend/ame_stocks_api/silver/contracts.py",
        "backend/ame_stocks_api/silver/asset_contract.py",
        "backend/ame_stocks_api/silver/asset_full_run_plan.py",
        "backend/ame_stocks_api/silver/asset_publish_plan.py",
        "backend/ame_stocks_api/silver/asset_release_set.py",
        "backend/ame_stocks_api/silver/asset_source.py",
        "backend/ame_stocks_api/silver/assets.py",
        "backend/ame_stocks_api/silver/calendar_artifact.py",
        "backend/ame_stocks_api/silver/availability.py",
        "backend/ame_stocks_api/silver/exchange_contract.py",
        "backend/ame_stocks_api/silver/fixed_cases.py",
        "backend/ame_stocks_api/silver/store.py",
        "backend/ame_stocks_api/silver/identity_exact_group_history_contract.py",
        "backend/ame_stocks_api/silver/identity_exact_group_history_plan.py",
        "backend/ame_stocks_api/silver/identity_exact_group_history_manifest.py",
        "backend/ame_stocks_api/silver/identity_exact_group_history_approval.py",
        "backend/ame_stocks_api/silver/identity_exact_group_history_runner.py",
        "backend/ame_stocks_api/silver/identity_market_inventory_engine.py",
        "backend/ame_stocks_api/silver/identity_bounce.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_contract.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_request.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_request.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_approval.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_runner.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_execution_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_approval.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_runner.py",
        "backend/ame_stocks_api/silver/identity_preview_plan.py",
        "backend/ame_stocks_api/silver/identity_preview_runner.py",
        "backend/ame_stocks_api/silver/identity_provider_evidence.py",
        "backend/ame_stocks_api/silver/identity_source.py",
        "backend/ame_stocks_api/silver/identity_streaming_preview.py",
        "backend/ame_stocks_api/silver/reader.py",
        "backend/ame_stocks_api/silver/ticker_event_contract.py",
        "backend/ame_stocks_api/silver/ticker_type_contract.py",
        "backend/ame_stocks_api/silver/ticker_overview_contract.py",
        "packages/ame_stocks_core/__init__.py",
        "packages/ame_stocks_core/contracts/__init__.py",
        "packages/ame_stocks_core/contracts/data_provider.py",
        "packages/ame_stocks_core/contracts/factor.py",
        "backend/ame_stocks_api/cli/silver_identity_exact_group_history_prepare.py",
        "backend/ame_stocks_api/cli/silver_identity_exact_group_history_manifest_approval.py",
        "backend/ame_stocks_api/cli/silver_identity_exact_group_history_manifest_run.py",
        "backend/ame_stocks_api/cli/prepare_s7_exact_group_history_execution_approval.py",
        "backend/ame_stocks_api/cli/run_s7_exact_group_history_review.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_manifest_request.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_manifest_approval.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_manifest_run.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_approval.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_run.py",
        "backend/ame_stocks_api/silver/schema_resources/"
        "identity_directional_raw_preview_slot.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/asset_observation_daily.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/asset_observation_version.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/universe_source_daily.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/exchange_dim.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/"
        "identity_exact_group_history_review_slot.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/ticker_event_request_status.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/ticker_change_event.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/ticker_type_dim.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/ticker_overview_safe.schema-v1.json",
        "docs/silver-s7-directional-raw-preview-design.md",
        "docs/silver/contracts/identity/"
        "identity_directional_raw_preview_slot.schema-v1.candidate.json",
        "docs/silver-s7-exact-group-history-review-design.md",
        "docs/silver/contracts/identity/"
        "identity_exact_group_history_review_slot.schema-v1.candidate.json",
    }
)
DEFAULT_VERIFICATION_PATHS: Final = frozenset(
    {
        "tests/test_silver_identity_exact_group_history_plan.py",
        "tests/test_silver_identity_exact_group_history_manifest.py",
        "tests/test_silver_identity_exact_group_history_contract.py",
        "tests/test_silver_identity_exact_group_history_approval.py",
        "tests/test_silver_identity_exact_group_history_runner.py",
        "tests/test_silver_identity_exact_group_history_run_cli.py",
        "tests/test_silver_identity_directional_raw_preview_contract.py",
        "tests/test_silver_identity_directional_raw_preview_plan.py",
        "tests/test_silver_identity_directional_raw_preview_request.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_plan.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_request.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_approval.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_runner.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_run.py",
        "tests/test_silver_identity_directional_raw_preview_execution_plan.py",
        "tests/test_silver_identity_directional_raw_preview_approval.py",
        "tests/test_silver_identity_directional_raw_preview_runner.py",
        "tests/test_silver_identity_directional_raw_preview_run.py",
        "tests/test_silver_identity_provider_evidence.py",
        "tests/test_silver_identity_source.py",
        "tests/test_silver_identity_market_inventory_engine.py",
        "tests/test_silver_identity_bounce.py",
        "tests/test_silver_identity_preview_plan.py",
        "tests/test_silver_identity_preview_runner.py",
        "tests/test_silver_identity_streaming_preview.py",
        "tests/test_silver_asset_contracts.py",
        "tests/test_silver_asset_full_run_plan.py",
        "tests/test_silver_asset_publish_plan.py",
        "tests/test_silver_asset_release_set.py",
        "tests/test_silver_asset_source.py",
        "tests/test_silver_assets.py",
        "tests/test_silver_calendar_artifact.py",
        "tests/test_silver_exchange_contract.py",
        "tests/test_silver_ticker_event_contracts.py",
        "tests/test_silver_ticker_type_contract.py",
        "tests/test_silver_ticker_overview_contract.py",
        "tests/test_massive_provider.py",
        "tests/test_data_provider.py",
        "tests/test_factor_contract.py",
        "tests/test_silver_lazy_imports.py",
    }
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")


class IdentityExactGroupHistoryPlanError(RuntimeError):
    """Raised when an exact-group preparation control is not exact."""


def canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise IdentityExactGroupHistoryPlanError(f"{label} must be an object")
    return dict(value)


def _expect_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise IdentityExactGroupHistoryPlanError(
            f"{label} schema differs: missing={sorted(expected - set(value))}, "
            f"extra={sorted(set(value) - expected)}"
        )


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise IdentityExactGroupHistoryPlanError(f"{label} must be non-empty text")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise IdentityExactGroupHistoryPlanError(f"{label} contains controls")
    return value


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if not _DIGEST.fullmatch(text):
        raise IdentityExactGroupHistoryPlanError(f"{label} must be lowercase 64-hex")
    return text


def _git_object(value: object, label: str) -> str:
    text = _text(value, label)
    if not _GIT_OBJECT.fullmatch(text):
        raise IdentityExactGroupHistoryPlanError(f"{label} must be lowercase 40-hex")
    return text


def _positive(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IdentityExactGroupHistoryPlanError(f"{label} must be a positive native int")
    return value


def _relative(value: object, label: str) -> str:
    text = _text(value, label)
    path = Path(text)
    if path.is_absolute() or path.as_posix() != text or ".." in path.parts:
        raise IdentityExactGroupHistoryPlanError(f"{label} is not a safe relative path")
    return text


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityExactGroupHistoryPlanError(f"{label} must be timezone-aware")
    if value.utcoffset().total_seconds() != 0:
        raise IdentityExactGroupHistoryPlanError(f"{label} must be UTC")
    return value.astimezone(UTC)


def _parse_utc(value: object, label: str) -> datetime:
    try:
        result = datetime.fromisoformat(_text(value, label))
    except ValueError as exc:
        raise IdentityExactGroupHistoryPlanError(f"{label} must be ISO-8601") from exc
    normalized = _utc(result, label)
    if normalized.isoformat() != value:
        raise IdentityExactGroupHistoryPlanError(f"{label} must be canonical UTC")
    return normalized


def _false_capabilities() -> dict[str, bool]:
    return {
        "adjudication": False,
        "canonical_identity_output": False,
        "exact_group_history_read": False,
        "full_run": False,
        "manifest_read": False,
        "network_access": False,
        "parquet_content_read": False,
        "publication": False,
        "registry_evaluation": False,
        "source_lstat": False,
    }


def _require_default_file_pin_paths(
    runtime_files: Sequence[ExactGroupHistoryFilePin],
    verification_files: Sequence[ExactGroupHistoryFilePin],
) -> None:
    runtime_paths = {item.path for item in runtime_files}
    verification_paths = {item.path for item in verification_files}
    missing_runtime = sorted(DEFAULT_RUNTIME_PATHS - runtime_paths)
    missing_verification = sorted(DEFAULT_VERIFICATION_PATHS - verification_paths)
    if missing_runtime or missing_verification:
        raise IdentityExactGroupHistoryPlanError(
            "required file pins are missing: "
            f"runtime={missing_runtime}, verification={missing_verification}"
        )


@dataclass(frozen=True, slots=True, order=True)
class ExactGroupHistoryFilePin:
    path: str
    git_blob: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative(self.path, "file path")
        _git_object(self.git_blob, "Git blob")
        _digest(self.sha256, "file SHA-256")
        _positive(self.bytes, "file bytes")

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "git_blob": self.git_blob,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> ExactGroupHistoryFilePin:
        item = _mapping(value, "file pin")
        _expect_keys(item, {"bytes", "git_blob", "path", "sha256"}, "file pin")
        return cls(
            path=_text(item["path"], "path"),
            git_blob=_text(item["git_blob"], "Git blob"),
            sha256=_text(item["sha256"], "SHA-256"),
            bytes=_positive(item["bytes"], "bytes"),
        )


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryExecutionCaps:
    physical_source_artifact_count: int = SOURCE_ARTIFACT_COUNT
    physical_source_row_count: int = SOURCE_ROW_COUNT
    physical_source_bytes: int = SOURCE_BYTES
    xnys_session_count: int = SESSION_COUNT
    review_group_count: int = 3
    rss_bytes_hard_cap: int = 2 * 1024**3
    tmp_bytes_hard_cap: int = 2 * 1024**3
    output_bytes_hard_cap: int = 512 * 1024**2
    wall_clock_seconds_hard_cap: int = 4 * 60 * 60
    disk_free_bytes_hard_floor: int = 40 * 1024**3
    selected_row_hard_cap: int = 1_000_000
    batch_row_count: int = 65_536

    def __post_init__(self) -> None:
        for label, value in self.to_dict().items():
            _positive(value, label)
        if self.to_dict() != {
            "batch_row_count": 65_536,
            "disk_free_bytes_hard_floor": 40 * 1024**3,
            "output_bytes_hard_cap": 512 * 1024**2,
            "physical_source_artifact_count": SOURCE_ARTIFACT_COUNT,
            "physical_source_bytes": SOURCE_BYTES,
            "physical_source_row_count": SOURCE_ROW_COUNT,
            "review_group_count": 3,
            "rss_bytes_hard_cap": 2 * 1024**3,
            "selected_row_hard_cap": 1_000_000,
            "tmp_bytes_hard_cap": 2 * 1024**3,
            "wall_clock_seconds_hard_cap": 4 * 60 * 60,
            "xnys_session_count": SESSION_COUNT,
        }:
            raise IdentityExactGroupHistoryPlanError("execution resource caps are not exact")

    def to_dict(self) -> dict[str, int]:
        return {
            "batch_row_count": self.batch_row_count,
            "disk_free_bytes_hard_floor": self.disk_free_bytes_hard_floor,
            "output_bytes_hard_cap": self.output_bytes_hard_cap,
            "physical_source_artifact_count": self.physical_source_artifact_count,
            "physical_source_bytes": self.physical_source_bytes,
            "physical_source_row_count": self.physical_source_row_count,
            "review_group_count": self.review_group_count,
            "rss_bytes_hard_cap": self.rss_bytes_hard_cap,
            "selected_row_hard_cap": self.selected_row_hard_cap,
            "tmp_bytes_hard_cap": self.tmp_bytes_hard_cap,
            "wall_clock_seconds_hard_cap": self.wall_clock_seconds_hard_cap,
            "xnys_session_count": self.xnys_session_count,
        }

    @property
    def digest(self) -> str:
        return stable_digest(self.to_dict())

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryExecutionCaps:
        item = _mapping(value, "execution resource caps")
        return cls(**{key: _positive(raw, key) for key, raw in item.items()})


@dataclass(frozen=True, slots=True, order=True)
class S7ExactGroupHistoryScope:
    provider: str
    market: str
    locale: str
    ticker: str
    observed_composite_figi: str

    def __post_init__(self) -> None:
        expected = next(
            (
                item
                for item in FIXED_GROUPS
                if item["ticker"] == self.ticker
                and item["observed_composite_figi"] == self.observed_composite_figi
            ),
            None,
        )
        if expected is None or self.to_dict() != dict(expected):
            raise IdentityExactGroupHistoryPlanError("review group is outside fixed scope")
        if not _FIGI.fullmatch(self.observed_composite_figi):
            raise IdentityExactGroupHistoryPlanError("Composite FIGI is malformed")

    @property
    def review_group_id(self) -> str:
        return stable_digest(self.to_dict())

    def to_dict(self) -> dict[str, str]:
        return {
            "locale": self.locale,
            "market": self.market,
            "observed_composite_figi": self.observed_composite_figi,
            "provider": self.provider,
            "ticker": self.ticker,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryScope:
        item = _mapping(value, "review group")
        _expect_keys(
            item,
            {"locale", "market", "observed_composite_figi", "provider", "ticker"},
            "review group",
        )
        return cls(**{key: _text(item[key], key) for key in item})


def fixed_scopes() -> tuple[S7ExactGroupHistoryScope, ...]:
    return tuple(sorted(S7ExactGroupHistoryScope.from_dict(item) for item in FIXED_GROUPS))


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryScopeSet:
    created_by: str
    created_at_utc: datetime
    groups: tuple[S7ExactGroupHistoryScope, ...] = fixed_scopes()

    def __post_init__(self) -> None:
        _text(self.created_by, "scope actor")
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        groups = tuple(sorted(self.groups))
        if groups != fixed_scopes():
            raise IdentityExactGroupHistoryPlanError("scope set is not the exact three groups")
        object.__setattr__(self, "groups", groups)

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_exact_group_history_scope_set",
            "capabilities": _false_capabilities(),
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "group_count": 3,
            "groups": [
                {**item.to_dict(), "review_group_id": item.review_group_id} for item in self.groups
            ],
            "history_coverage": {
                "end_session": END_SESSION.isoformat(),
                "session_count": SESSION_COUNT,
                "start_session": START_SESSION.isoformat(),
            },
            "scope_rule_version": SCOPE_RULE_VERSION,
            "fixed_scope_digest": EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST,
            "schema_version": SCHEMA_VERSION,
            "share_class_is_not_a_filter": True,
        }

    @property
    def scope_set_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "scope_set_id": self.scope_set_id})

    @property
    def content(self) -> bytes:
        return canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return exact_group_history_scope_path(self.scope_set_id)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryScopeSet:
        document = _mapping(value, "scope set")
        groups = []
        for raw in document.get("groups", []):
            item = _mapping(raw, "review group")
            logical = dict(item)
            review_group_id = logical.pop("review_group_id", None)
            group = S7ExactGroupHistoryScope.from_dict(logical)
            if review_group_id != group.review_group_id:
                raise IdentityExactGroupHistoryPlanError("review group ID differs")
            groups.append(group)
        result = cls(
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            groups=tuple(groups),
        )
        if result.content != canonical_bytes(document):
            raise IdentityExactGroupHistoryPlanError("scope set is not canonical and exact")
        return result


def exact_group_lineage() -> dict[str, object]:
    return {
        "calendar": {
            "artifact_id": CALENDAR_ARTIFACT_ID,
            "sha256": CALENDAR_ARTIFACT_SHA256,
        },
        "directional_preview": {
            "candidate_id": DIRECTIONAL_CANDIDATE_ID,
            "candidate_sha256": DIRECTIONAL_CANDIDATE_SHA256,
            "completion_id": DIRECTIONAL_COMPLETION_ID,
            "completion_sha256": DIRECTIONAL_COMPLETION_SHA256,
        },
        "inventory": {
            "candidate_id": INVENTORY_CANDIDATE_ID,
            "candidate_sha256": INVENTORY_CANDIDATE_SHA256,
            "completion_id": INVENTORY_COMPLETION_ID,
            "completion_sha256": INVENTORY_COMPLETION_SHA256,
            "source_artifact_count": SOURCE_ARTIFACT_COUNT,
            "source_artifact_set_digest": INVENTORY_SOURCE_ARTIFACT_SET_DIGEST,
            "source_bytes": SOURCE_BYTES,
            "source_row_count": SOURCE_ROW_COUNT,
        },
        "s4_releases": [
            {
                "artifact_count": SESSION_COUNT,
                "release_id": ASSET_RELEASE_ID,
                "release_manifest_sha256": ASSET_RELEASE_SHA256,
                "table": "asset_observation_daily",
            },
            {
                "artifact_count": SESSION_COUNT,
                "release_id": UNIVERSE_RELEASE_ID,
                "release_manifest_sha256": UNIVERSE_RELEASE_SHA256,
                "table": "universe_source_daily",
            },
        ],
        "s4_release_set": {
            "release_set_id": EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID,
            "release_set_manifest_sha256": (EXACT_GROUP_HISTORY_S4_RELEASE_SET_MANIFEST_SHA256),
        },
    }


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryPreparationPlan:
    created_by: str
    created_at_utc: datetime
    git_commit: str
    git_tree: str
    runtime_files: tuple[ExactGroupHistoryFilePin, ...]
    verification_files: tuple[ExactGroupHistoryFilePin, ...]
    scope_set_id: str
    scope_set_sha256: str
    contract_id: str
    contract_schema_digest: str
    contract_candidate_sha256: str
    execution_resource_caps: S7ExactGroupHistoryExecutionCaps = S7ExactGroupHistoryExecutionCaps()

    def __post_init__(self) -> None:
        _text(self.created_by, "plan actor")
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        _git_object(self.git_commit, "Git commit")
        _git_object(self.git_tree, "Git tree")
        for label, value in (
            ("scope set ID", self.scope_set_id),
            ("scope set SHA-256", self.scope_set_sha256),
            ("contract ID", self.contract_id),
            ("contract schema digest", self.contract_schema_digest),
            ("contract candidate SHA-256", self.contract_candidate_sha256),
        ):
            _digest(value, label)
        runtime = tuple(sorted(self.runtime_files))
        verification = tuple(sorted(self.verification_files))
        if not runtime or not verification:
            raise IdentityExactGroupHistoryPlanError("file pins cannot be empty")
        paths = [item.path for item in (*runtime, *verification)]
        if len(paths) != len(set(paths)):
            raise IdentityExactGroupHistoryPlanError("file pin paths overlap")
        _require_default_file_pin_paths(runtime, verification)
        object.__setattr__(self, "runtime_files", runtime)
        object.__setattr__(self, "verification_files", verification)
        if not isinstance(self.execution_resource_caps, S7ExactGroupHistoryExecutionCaps):
            raise IdentityExactGroupHistoryPlanError("execution resource caps type differs")
        if (
            self.contract_id != IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID
            or self.contract_schema_digest != IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST
            or self.contract_candidate_sha256
            != IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256
        ):
            raise IdentityExactGroupHistoryPlanError("exact-group contract binding differs")

    @property
    def runtime_file_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.runtime_files])

    @property
    def verification_file_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.verification_files])

    @property
    def input_binding_digest(self) -> str:
        return stable_digest(
            {
                "contract": {
                    "candidate_sha256": self.contract_candidate_sha256,
                    "contract_id": self.contract_id,
                    "fixed_scope_digest": EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST,
                    "observed_run_semantics_digest": (
                        EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST
                    ),
                    "qa_semantics_digest": (
                        IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST
                    ),
                    "schema_digest": self.contract_schema_digest,
                },
                "lineage": exact_group_lineage(),
                "resource_caps_digest": self.execution_resource_caps.digest,
                "scope_set_id": self.scope_set_id,
                "scope_set_sha256": self.scope_set_sha256,
            }
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_exact_group_history_preparation_plan",
            "authorized_action": PREPARATION_AUTHORIZED_ACTION,
            "capabilities": _false_capabilities(),
            "contract": {
                "candidate_sha256": self.contract_candidate_sha256,
                "contract_id": self.contract_id,
                "fixed_scope_digest": EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST,
                "observed_run_semantics_digest": (
                    EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST
                ),
                "qa_semantics_digest": (
                    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST
                ),
                "schema_digest": self.contract_schema_digest,
            },
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "git_binding": {
                "git_commit": self.git_commit,
                "git_tree": self.git_tree,
                "runtime_file_set_digest": self.runtime_file_set_digest,
                "runtime_files": [item.to_dict() for item in self.runtime_files],
            },
            "input_binding_digest": self.input_binding_digest,
            "lineage": exact_group_lineage(),
            "plan_rule_version": PREPARATION_RULE_VERSION,
            "plan_state": "awaiting_exact_preparation_approval",
            "resource_caps": self.execution_resource_caps.to_dict(),
            "resource_caps_digest": self.execution_resource_caps.digest,
            "schema_version": SCHEMA_VERSION,
            "scope_binding": {
                "scope_set_id": self.scope_set_id,
                "scope_set_sha256": self.scope_set_sha256,
            },
            "verification_binding": {
                "verification_file_set_digest": self.verification_file_set_digest,
                "verification_files": [item.to_dict() for item in self.verification_files],
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
        return canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return exact_group_history_preparation_plan_path(self.plan_id)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryPreparationPlan:
        document = _mapping(value, "preparation plan")
        git = _mapping(document.get("git_binding"), "Git binding")
        verification = _mapping(document.get("verification_binding"), "verification binding")
        scope = _mapping(document.get("scope_binding"), "scope binding")
        contract = _mapping(document.get("contract"), "contract")
        result = cls(
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            git_commit=_text(git.get("git_commit"), "Git commit"),
            git_tree=_text(git.get("git_tree"), "Git tree"),
            runtime_files=tuple(
                ExactGroupHistoryFilePin.from_dict(item) for item in git.get("runtime_files", [])
            ),
            verification_files=tuple(
                ExactGroupHistoryFilePin.from_dict(item)
                for item in verification.get("verification_files", [])
            ),
            scope_set_id=_text(scope.get("scope_set_id"), "scope set ID"),
            scope_set_sha256=_text(scope.get("scope_set_sha256"), "scope set SHA"),
            contract_id=_text(contract.get("contract_id"), "contract ID"),
            contract_schema_digest=_text(contract.get("schema_digest"), "schema digest"),
            contract_candidate_sha256=_text(contract.get("candidate_sha256"), "candidate SHA-256"),
            execution_resource_caps=S7ExactGroupHistoryExecutionCaps.from_dict(
                document.get("resource_caps")
            ),
        )
        _require_default_file_pin_paths(result.runtime_files, result.verification_files)
        if result.content != canonical_bytes(document):
            raise IdentityExactGroupHistoryPlanError("preparation plan is not canonical and exact")
        return result


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryPreparationRequest:
    created_by: str
    created_at_utc: datetime
    plan_id: str
    plan_sha256: str
    input_binding_digest: str
    scope_set_id: str
    scope_set_sha256: str
    contract_id: str
    contract_schema_digest: str
    contract_candidate_sha256: str
    resource_caps_digest: str

    def __post_init__(self) -> None:
        _text(self.created_by, "request actor")
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("input binding digest", self.input_binding_digest),
            ("scope set ID", self.scope_set_id),
            ("scope set SHA-256", self.scope_set_sha256),
            ("contract ID", self.contract_id),
            ("contract schema digest", self.contract_schema_digest),
            ("contract candidate SHA-256", self.contract_candidate_sha256),
            ("resource caps digest", self.resource_caps_digest),
        ):
            _digest(value, label)

    @classmethod
    def create(
        cls,
        plan: S7ExactGroupHistoryPreparationPlan,
        *,
        created_by: str,
        created_at_utc: datetime,
    ) -> S7ExactGroupHistoryPreparationRequest:
        if created_by == plan.created_by or created_at_utc <= plan.created_at_utc:
            raise IdentityExactGroupHistoryPlanError("request actor/time must follow plan")
        result = cls(
            created_by=created_by,
            created_at_utc=created_at_utc,
            plan_id=plan.plan_id,
            plan_sha256=plan.sha256,
            input_binding_digest=plan.input_binding_digest,
            scope_set_id=plan.scope_set_id,
            scope_set_sha256=plan.scope_set_sha256,
            contract_id=plan.contract_id,
            contract_schema_digest=plan.contract_schema_digest,
            contract_candidate_sha256=plan.contract_candidate_sha256,
            resource_caps_digest=plan.execution_resource_caps.digest,
        )
        verify_preparation_request_binding(plan, result)
        return result

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_exact_group_history_preparation_request",
            "authorized_action": PREPARATION_AUTHORIZED_ACTION,
            "capabilities": _false_capabilities(),
            "contract_candidate_sha256": self.contract_candidate_sha256,
            "contract_id": self.contract_id,
            "contract_schema_digest": self.contract_schema_digest,
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "input_binding_digest": self.input_binding_digest,
            "literal_version": PREPARATION_LITERAL_VERSION,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "request_rule_version": PREPARATION_REQUEST_RULE_VERSION,
            "request_state": "awaiting_literal_human_approval",
            "resource_caps_digest": self.resource_caps_digest,
            "schema_version": SCHEMA_VERSION,
            "scope_set_id": self.scope_set_id,
            "scope_set_sha256": self.scope_set_sha256,
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
        return canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return exact_group_history_preparation_request_path(self.request_event_id)

    @property
    def canonical_approval_literal(self) -> str:
        payload = {
            key: value
            for key, value in self.document.items()
            if key
            not in {
                "artifact_type",
                "capabilities",
                "created_at_utc",
                "created_by",
                "request_rule_version",
                "request_state",
                "schema_version",
            }
        }
        payload["request_event_sha256"] = self.sha256
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryPreparationRequest:
        document = _mapping(value, "preparation request")
        result = cls(
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            plan_id=_text(document.get("plan_id"), "plan ID"),
            plan_sha256=_text(document.get("plan_sha256"), "plan SHA"),
            input_binding_digest=_text(
                document.get("input_binding_digest"), "input binding digest"
            ),
            scope_set_id=_text(document.get("scope_set_id"), "scope set ID"),
            scope_set_sha256=_text(document.get("scope_set_sha256"), "scope set SHA"),
            contract_id=_text(document.get("contract_id"), "contract ID"),
            contract_schema_digest=_text(
                document.get("contract_schema_digest"), "contract schema digest"
            ),
            contract_candidate_sha256=_text(
                document.get("contract_candidate_sha256"), "contract candidate SHA"
            ),
            resource_caps_digest=_text(
                document.get("resource_caps_digest"), "resource caps digest"
            ),
        )
        if result.content != canonical_bytes(document):
            raise IdentityExactGroupHistoryPlanError("preparation request is not canonical")
        return result


def verify_preparation_request_binding(
    plan: S7ExactGroupHistoryPreparationPlan,
    request: S7ExactGroupHistoryPreparationRequest,
) -> None:
    """Require a byte-domain exact Request projection of its Preparation Plan."""

    expected = (
        plan.plan_id,
        plan.sha256,
        plan.input_binding_digest,
        plan.scope_set_id,
        plan.scope_set_sha256,
        plan.contract_id,
        plan.contract_schema_digest,
        plan.contract_candidate_sha256,
        plan.execution_resource_caps.digest,
    )
    actual = (
        request.plan_id,
        request.plan_sha256,
        request.input_binding_digest,
        request.scope_set_id,
        request.scope_set_sha256,
        request.contract_id,
        request.contract_schema_digest,
        request.contract_candidate_sha256,
        request.resource_caps_digest,
    )
    if (
        actual != expected
        or request.created_by == plan.created_by
        or request.created_at_utc <= plan.created_at_utc
    ):
        raise IdentityExactGroupHistoryPlanError("preparation request cross-binding differs")


@dataclass(frozen=True, slots=True)
class StoredExactGroupHistoryControl:
    logical_id: str
    path: str
    sha256: str
    bytes: int


class ExactGroupHistoryPlanStore:
    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()

    def store_scope(self, value: S7ExactGroupHistoryScopeSet) -> StoredExactGroupHistoryControl:
        return self._write(value.scope_set_id, value.relative_path, value.content)

    def store_plan(
        self, value: S7ExactGroupHistoryPreparationPlan
    ) -> StoredExactGroupHistoryControl:
        return self._write(value.plan_id, value.relative_path, value.content)

    def store_request(
        self, value: S7ExactGroupHistoryPreparationRequest
    ) -> StoredExactGroupHistoryControl:
        return self._write(value.request_event_id, value.relative_path, value.content)

    def load_scope(self, scope_set_id: str, sha256: str) -> S7ExactGroupHistoryScopeSet:
        return self._read(
            exact_group_history_scope_path(scope_set_id),
            sha256,
            S7ExactGroupHistoryScopeSet.from_dict,
        )

    def load_plan(self, plan_id: str, sha256: str) -> S7ExactGroupHistoryPreparationPlan:
        return self._read(
            exact_group_history_preparation_plan_path(plan_id),
            sha256,
            S7ExactGroupHistoryPreparationPlan.from_dict,
        )

    def load_request(
        self, request_event_id: str, sha256: str
    ) -> S7ExactGroupHistoryPreparationRequest:
        return self._read(
            exact_group_history_preparation_request_path(request_event_id),
            sha256,
            S7ExactGroupHistoryPreparationRequest.from_dict,
        )

    def _write(
        self, logical_id: str, relative: str, content: bytes
    ) -> StoredExactGroupHistoryControl:
        try:
            receipt = write_bytes_immutable(
                self.root, safe_relative_path(self.root, relative), content
            )
        except ArtifactError as exc:
            raise IdentityExactGroupHistoryPlanError(str(exc)) from exc
        return StoredExactGroupHistoryControl(
            logical_id=logical_id,
            path=str(receipt["path"]),
            sha256=str(receipt["sha256"]),
            bytes=int(receipt["bytes"]),
        )

    def _read(self, relative: str, expected_sha: str, parser: Any) -> Any:
        _digest(expected_sha, "expected SHA-256")
        try:
            path = safe_relative_path(self.root, relative)
        except ArtifactError as exc:
            raise IdentityExactGroupHistoryPlanError(str(exc)) from exc
        if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha:
            raise IdentityExactGroupHistoryPlanError(f"control differs: {relative}")
        try:
            document = json.loads(path.read_bytes(), object_pairs_hook=_reject_duplicates)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise IdentityExactGroupHistoryPlanError(f"control is invalid: {relative}") from exc
        value = parser(document)
        if value.content != path.read_bytes():
            raise IdentityExactGroupHistoryPlanError(f"control bytes differ: {relative}")
        return value


def exact_group_history_scope_path(scope_set_id: str) -> str:
    _digest(scope_set_id, "scope set ID")
    return (
        "manifests/silver/identity/exact-group-history-scope-sets/"
        f"scope_set_id={scope_set_id}/manifest.json"
    )


def exact_group_history_preparation_plan_path(plan_id: str) -> str:
    _digest(plan_id, "plan ID")
    return (
        "manifests/silver/identity/exact-group-history-preparation-plans/"
        f"plan_id={plan_id}/manifest.json"
    )


def exact_group_history_preparation_request_path(request_id: str) -> str:
    _digest(request_id, "request event ID")
    return (
        "manifests/silver/identity/exact-group-history-preparation-requests/"
        f"request_event_id={request_id}.json"
    )


def pin_tracked_files(
    repo_root: Path, paths: Sequence[str]
) -> tuple[ExactGroupHistoryFilePin, ...]:
    root = repo_root.expanduser().resolve()
    result = []
    for relative in sorted(set(paths)):
        _relative(relative, "tracked path")
        path = root / relative
        if not path.is_file() or path.is_symlink():
            raise IdentityExactGroupHistoryPlanError(f"tracked file missing: {relative}")
        result.append(
            ExactGroupHistoryFilePin(
                path=relative,
                git_blob=_git(root, "rev-parse", f"HEAD:{relative}"),
                sha256=sha256_file(path),
                bytes=path.stat().st_size,
            )
        )
    return tuple(result)


def verify_exact_clean_checkout(repo_root: Path, expected_commit: str) -> tuple[str, str]:
    root = repo_root.expanduser().resolve()
    _git_object(expected_commit, "expected commit")
    if Path(_git(root, "rev-parse", "--show-toplevel")).resolve() != root:
        raise IdentityExactGroupHistoryPlanError("repository root differs")
    commit = _git(root, "rev-parse", "HEAD")
    tree = _git(root, "rev-parse", "HEAD^{tree}")
    if commit != expected_commit or _git(root, "status", "--porcelain", "--untracked-files=all"):
        raise IdentityExactGroupHistoryPlanError("checkout is not exact and clean")
    return commit, tree


def prepare_exact_group_history_controls(
    *,
    repo_root: Path,
    control_root: Path,
    expected_git_commit: str,
    scope_created_by: str,
    plan_created_by: str,
    request_created_by: str,
    scope_created_at_utc: datetime,
    plan_created_at_utc: datetime,
    request_created_at_utc: datetime,
    contract_id: str,
    contract_schema_digest: str,
    contract_candidate_sha256: str,
    extra_runtime_paths: Sequence[str] = (),
    extra_verification_paths: Sequence[str] = (),
) -> tuple[
    StoredExactGroupHistoryControl,
    StoredExactGroupHistoryControl,
    StoredExactGroupHistoryControl,
    S7ExactGroupHistoryPreparationRequest,
]:
    actors = (scope_created_by, plan_created_by, request_created_by)
    if len(set(actors)) != 3:
        raise IdentityExactGroupHistoryPlanError("preparation actors must be distinct")
    if not (scope_created_at_utc < plan_created_at_utc < request_created_at_utc):
        raise IdentityExactGroupHistoryPlanError("preparation timestamps must strictly increase")
    if _utc(request_created_at_utc, "request_created_at_utc") > datetime.now(UTC):
        raise IdentityExactGroupHistoryPlanError("preparation timestamp cannot be in the future")
    commit, tree = verify_exact_clean_checkout(repo_root, expected_git_commit)
    runtime = pin_tracked_files(
        repo_root, tuple(DEFAULT_RUNTIME_PATHS) + tuple(extra_runtime_paths)
    )
    verification = pin_tracked_files(
        repo_root, tuple(DEFAULT_VERIFICATION_PATHS) + tuple(extra_verification_paths)
    )
    scope = S7ExactGroupHistoryScopeSet(scope_created_by, scope_created_at_utc)
    plan = S7ExactGroupHistoryPreparationPlan(
        created_by=plan_created_by,
        created_at_utc=plan_created_at_utc,
        git_commit=commit,
        git_tree=tree,
        runtime_files=runtime,
        verification_files=verification,
        scope_set_id=scope.scope_set_id,
        scope_set_sha256=scope.sha256,
        contract_id=contract_id,
        contract_schema_digest=contract_schema_digest,
        contract_candidate_sha256=contract_candidate_sha256,
    )
    request = S7ExactGroupHistoryPreparationRequest.create(
        plan, created_by=request_created_by, created_at_utc=request_created_at_utc
    )
    store = ExactGroupHistoryPlanStore(control_root)
    return (
        store.store_scope(scope),
        store.store_plan(plan),
        store.store_request(request),
        request,
    )


def _git(root: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args], cwd=root, check=True, capture_output=True, text=True
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise IdentityExactGroupHistoryPlanError(f"Git command failed: {' '.join(args)}") from exc
    return result.stdout.strip()


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IdentityExactGroupHistoryPlanError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


__all__ = [
    "ASSET_RELEASE_ID",
    "ASSET_RELEASE_SHA256",
    "CALENDAR_ARTIFACT_ID",
    "CALENDAR_ARTIFACT_SHA256",
    "DEFAULT_RUNTIME_PATHS",
    "DEFAULT_VERIFICATION_PATHS",
    "DIRECTIONAL_CANDIDATE_ID",
    "DIRECTIONAL_CANDIDATE_SHA256",
    "DIRECTIONAL_COMPLETION_ID",
    "DIRECTIONAL_COMPLETION_SHA256",
    "END_SESSION",
    "FIXED_GROUPS",
    "INVENTORY_CANDIDATE_ID",
    "INVENTORY_CANDIDATE_SHA256",
    "INVENTORY_COMPLETION_ID",
    "INVENTORY_COMPLETION_SHA256",
    "INVENTORY_SOURCE_ARTIFACT_SET_DIGEST",
    "PREPARATION_AUTHORIZED_ACTION",
    "PREPARATION_LITERAL_VERSION",
    "SESSION_COUNT",
    "SOURCE_ARTIFACT_COUNT",
    "SOURCE_BYTES",
    "SOURCE_ROW_COUNT",
    "START_SESSION",
    "UNIVERSE_RELEASE_ID",
    "UNIVERSE_RELEASE_SHA256",
    "ExactGroupHistoryFilePin",
    "ExactGroupHistoryPlanStore",
    "IdentityExactGroupHistoryPlanError",
    "S7ExactGroupHistoryExecutionCaps",
    "S7ExactGroupHistoryPreparationPlan",
    "S7ExactGroupHistoryPreparationRequest",
    "S7ExactGroupHistoryScope",
    "S7ExactGroupHistoryScopeSet",
    "StoredExactGroupHistoryControl",
    "canonical_bytes",
    "exact_group_history_preparation_plan_path",
    "exact_group_history_preparation_request_path",
    "exact_group_history_scope_path",
    "exact_group_lineage",
    "fixed_scopes",
    "pin_tracked_files",
    "prepare_exact_group_history_controls",
    "verify_exact_clean_checkout",
    "verify_preparation_request_binding",
]
