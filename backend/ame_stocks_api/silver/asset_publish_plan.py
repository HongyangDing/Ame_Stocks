"""Create the immutable, review-only S4 Assets publication plan.

This module deliberately has no approval, ``request_publish`` or ``publish`` capability.  It
re-verifies the three exact ``full_ready`` workflows, freezes their full-history QA evidence in
one immutable plan, and stops for explicit user review.  A later release-set gate must consume
the exact plan ID and checksum before any workflow can advance.
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
from pathlib import Path
from types import MappingProxyType

import exchange_calendars as xcals
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import (
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    ASSET_OBSERVATION_VERSION_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.asset_full_run_plan import (
    CURRENT_ASSET_FULL_RUN_PLAN_AUTHORIZATION,
    AssetFullRunPlanAuthorization,
    _validate_plan_authorization,
    _verify_plan_inventory_binding,
    _verify_plan_preflight,
)
from ame_stocks_api.silver.contracts import (
    ArtifactRef,
    ArtifactRole,
    BuildKind,
    BuildManifest,
    FullRunPlan,
    QACheckResult,
    QASeverity,
    QAStatus,
    QuarantineRecord,
    QuarantineReviewStatus,
    SilverContractError,
    TableContract,
)
from ame_stocks_api.silver.store import (
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowEventRecord,
    WorkflowState,
)

ASSET_PUBLISH_PLAN_VERSION = 1
ASSET_PUBLISH_PLAN_POLICY_VERSION = "s4-assets-publish-plan-v1"
ASSET_PUBLICATION_SCOPE = "identity_evidence_pending_s7"
CURRENT_ASSET_MATERIALIZATION_GIT_COMMIT = "adc28b5dc05dccb0d4b963fe6be719367d9e7b97"
CURRENT_ASSET_RUNTIME_LOG_PATH = "tmp/s4-assets-full-adc28b5/full-run.log"
CURRENT_ASSET_RUNTIME_LOG_SHA256 = (
    "6da92dee6ceae2457f933e0290b7e0ede4fa19312890496da1a027f39bad9c10"
)
CURRENT_ASSET_RUNTIME_LOG_BYTES = 5_649
ASSET_RUNTIME_RSS_WARNING = "process RSS exceeded the reviewed 0.75 GiB estimate"
ASSET_RUNTIME_RSS_REVIEW_STATUS = "estimate_exceeded_exact_peak_unavailable"
ASSET_RUNTIME_EVIDENCE_LIMITATION = (
    "exact_process_max_rss_bytes_not_persisted_by_asset-full-v1"
)

_OBSERVATION_TABLE = ASSET_OBSERVATION_DAILY_CONTRACT.table
_VERSION_TABLE = ASSET_OBSERVATION_VERSION_CONTRACT.table
_UNIVERSE_TABLE = UNIVERSE_SOURCE_DAILY_CONTRACT.table
_TABLE_ORDER = (_OBSERVATION_TABLE, _VERSION_TABLE, _UNIVERSE_TABLE)
_CONTRACTS_BY_TABLE: Mapping[str, TableContract] = {
    _OBSERVATION_TABLE: ASSET_OBSERVATION_DAILY_CONTRACT,
    _VERSION_TABLE: ASSET_OBSERVATION_VERSION_CONTRACT,
    _UNIVERSE_TABLE: UNIVERSE_SOURCE_DAILY_CONTRACT,
}
_EMPTY_QUARANTINE_IDS = {severity.value: () for severity in QASeverity}
_REVIEWED_LOGIC_CLOSURE = (
    "backend/ame_stocks_api/artifacts.py",
    "backend/ame_stocks_api/cli/silver_assets_full.py",
    "backend/ame_stocks_api/silver/asset_contract.py",
    "backend/ame_stocks_api/silver/asset_full.py",
    "backend/ame_stocks_api/silver/asset_full_run_plan.py",
    "backend/ame_stocks_api/silver/asset_preview.py",
    "backend/ame_stocks_api/silver/asset_source.py",
    "backend/ame_stocks_api/silver/assets.py",
    "backend/ame_stocks_api/silver/contracts.py",
    "backend/ame_stocks_api/silver/exchange_contract.py",
    "backend/ame_stocks_api/silver/fixed_cases.py",
    "backend/ame_stocks_api/silver/reader.py",
    "backend/ame_stocks_api/silver/schema_resources/asset_observation_daily.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/asset_observation_version.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/exchange_dim.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/ticker_type_dim.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/universe_source_daily.schema-v1.json",
    "backend/ame_stocks_api/silver/store.py",
    "backend/ame_stocks_api/silver/ticker_type_contract.py",
    "backend/ame_stocks_api/providers/massive.py",
    "docs/silver/source-profiles/assets-full-2026-07-13.json",
)


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise SilverContractError(f"{label} must be a lowercase SHA-256")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise SilverContractError(f"{label} must be a non-negative native int")
    return value


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise SilverContractError(f"{label} must be a positive native int")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise SilverContractError(f"{label} must be a non-empty string")
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


@dataclass(frozen=True, slots=True)
class AssetPublishWarning:
    """One exact full-build warning proposed for the later publish waiver."""

    result_id: str
    check: QACheckResult

    def __post_init__(self) -> None:
        _sha256(self.result_id, "publish warning result_id")
        if self.result_id != self.check.result_id:
            raise SilverContractError("publish warning result_id does not match its QA check")
        if self.check.status is not QAStatus.WARNING:
            raise SilverContractError("publish plan may freeze warning QA results only")

    def to_dict(self) -> dict[str, object]:
        return {"result_id": self.result_id, "qa_check": self.check.to_dict()}

    @classmethod
    def from_dict(cls, value: object) -> AssetPublishWarning:
        document = _object(value, "asset publish warning")
        _expect_keys(document, {"result_id", "qa_check"}, "asset publish warning")
        return cls(
            result_id=_sha256(document["result_id"], "publish warning result_id"),
            check=QACheckResult.from_dict(document["qa_check"]),
        )


@dataclass(frozen=True, slots=True)
class AssetRuntimeReviewEvidence:
    """Exact full-run log evidence for the separately reviewed RSS warning."""

    source_path: str
    source_sha256: str
    source_bytes: int
    completed_sessions: int
    qa_warning_counts_by_table: Mapping[str, int]
    warning_messages: tuple[str, ...]
    expected_rss_ceiling_bytes: int
    hard_rss_limit_bytes: int
    rss_review_status: str = ASSET_RUNTIME_RSS_REVIEW_STATUS
    observed_max_rss_bytes: None = None
    evidence_limitation: str = ASSET_RUNTIME_EVIDENCE_LIMITATION

    def __post_init__(self) -> None:
        path = Path(_text(self.source_path, "runtime evidence source_path"))
        if path.is_absolute() or path.as_posix() != self.source_path:
            raise SilverContractError(
                "runtime evidence source_path must be normalized and relative"
            )
        _sha256(self.source_sha256, "runtime evidence source_sha256")
        _positive_int(self.source_bytes, "runtime evidence source_bytes")
        _positive_int(self.completed_sessions, "runtime evidence completed_sessions")
        counts = dict(self.qa_warning_counts_by_table)
        if set(counts) != set(_TABLE_ORDER) or any(
            type(value) is not int or value < 0 for value in counts.values()
        ):
            raise SilverContractError("runtime QA warning counts must cover the three S4 tables")
        object.__setattr__(
            self,
            "qa_warning_counts_by_table",
            MappingProxyType(dict(sorted(counts.items()))),
        )
        warnings = tuple(self.warning_messages)
        if warnings != (ASSET_RUNTIME_RSS_WARNING,):
            raise SilverContractError("runtime review must freeze the exact S4 RSS warning")
        object.__setattr__(self, "warning_messages", warnings)
        _positive_int(
            self.expected_rss_ceiling_bytes,
            "runtime expected_rss_ceiling_bytes",
        )
        _positive_int(self.hard_rss_limit_bytes, "runtime hard_rss_limit_bytes")
        if self.hard_rss_limit_bytes <= self.expected_rss_ceiling_bytes:
            raise SilverContractError("runtime hard RSS limit must exceed the reviewed estimate")
        if self.rss_review_status != ASSET_RUNTIME_RSS_REVIEW_STATUS:
            raise SilverContractError("runtime RSS review status changed")
        if self.observed_max_rss_bytes is not None:
            raise SilverContractError("full-run-v1 did not persist exact max RSS bytes")
        if self.evidence_limitation != ASSET_RUNTIME_EVIDENCE_LIMITATION:
            raise SilverContractError("runtime evidence limitation changed")

    def to_dict(self) -> dict[str, object]:
        return {
            "completed_sessions": self.completed_sessions,
            "evidence_limitation": self.evidence_limitation,
            "expected_rss_ceiling_bytes": self.expected_rss_ceiling_bytes,
            "hard_rss_limit_bytes": self.hard_rss_limit_bytes,
            "observed_max_rss_bytes": None,
            "qa_warning_counts_by_table": dict(self.qa_warning_counts_by_table),
            "rss_review_status": self.rss_review_status,
            "source_bytes": self.source_bytes,
            "source_path": self.source_path,
            "source_sha256": self.source_sha256,
            "warning_messages": list(self.warning_messages),
        }

    @classmethod
    def from_dict(cls, value: object) -> AssetRuntimeReviewEvidence:
        document = _object(value, "asset runtime review evidence")
        expected = {
            "completed_sessions",
            "evidence_limitation",
            "expected_rss_ceiling_bytes",
            "hard_rss_limit_bytes",
            "observed_max_rss_bytes",
            "qa_warning_counts_by_table",
            "rss_review_status",
            "source_bytes",
            "source_path",
            "source_sha256",
            "warning_messages",
        }
        _expect_keys(document, expected, "asset runtime review evidence")
        if document["observed_max_rss_bytes"] is not None:
            raise SilverContractError("runtime observed max RSS must remain null")
        counts = _object(
            document["qa_warning_counts_by_table"],
            "runtime QA warning counts",
        )
        return cls(
            source_path=_text(document["source_path"], "runtime source_path"),
            source_sha256=_sha256(document["source_sha256"], "runtime source_sha256"),
            source_bytes=_positive_int(document["source_bytes"], "runtime source_bytes"),
            completed_sessions=_positive_int(
                document["completed_sessions"], "runtime completed_sessions"
            ),
            qa_warning_counts_by_table={
                table: _nonnegative_int(count, f"runtime warning count:{table}")
                for table, count in counts.items()
            },
            warning_messages=tuple(
                _text(item, "runtime warning")
                for item in _array(document["warning_messages"], "runtime warnings")
            ),
            expected_rss_ceiling_bytes=_positive_int(
                document["expected_rss_ceiling_bytes"],
                "runtime expected_rss_ceiling_bytes",
            ),
            hard_rss_limit_bytes=_positive_int(
                document["hard_rss_limit_bytes"],
                "runtime hard_rss_limit_bytes",
            ),
            rss_review_status=_text(
                document["rss_review_status"], "runtime rss_review_status"
            ),
            observed_max_rss_bytes=None,
            evidence_limitation=_text(
                document["evidence_limitation"], "runtime evidence_limitation"
            ),
        )


@dataclass(frozen=True, slots=True)
class AssetPublishTablePlan:
    """Exact review evidence for one of the three S4 full builds."""

    table: str
    workflow_id: str
    contract_id: str
    schema_version: int
    full_ready_event_path: str
    full_ready_event_sha256: str
    full_run_plan_id: str
    full_run_plan_path: str
    full_run_plan_sha256: str
    build_id: str
    build_manifest_path: str
    build_manifest_sha256: str
    materialization_git_commit: str
    transform_version: str
    exchange_calendar_version: str
    source_digest: str
    date_start: str
    date_end: str
    input_session_count: int
    input_manifest_count: int
    input_page_count: int
    input_rows: int
    input_manifest_bytes: int
    input_compressed_bytes: int
    input_raw_bytes: int
    manifest_inventory_sha256: str
    artifact_inventory_sha256: str
    output_data_partition_count: int
    output_rows: int
    output_data_bytes: int
    qa_check_count: int
    warnings: tuple[AssetPublishWarning, ...]
    quarantine_issue_rows: int
    quarantine_unique_source_rows: int
    quarantine_issue_ids_by_severity: Mapping[str, tuple[str, ...]]
    accepted_quarantine_issue_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.table not in _TABLE_ORDER:
            raise SilverContractError("asset publish table is not one of the three S4 tables")
        for label, value in (
            ("workflow_id", self.workflow_id),
            ("contract_id", self.contract_id),
            ("full_ready_event_sha256", self.full_ready_event_sha256),
            ("full_run_plan_id", self.full_run_plan_id),
            ("full_run_plan_sha256", self.full_run_plan_sha256),
            ("build_id", self.build_id),
            ("build_manifest_sha256", self.build_manifest_sha256),
            ("source_digest", self.source_digest),
            ("manifest_inventory_sha256", self.manifest_inventory_sha256),
            ("artifact_inventory_sha256", self.artifact_inventory_sha256),
        ):
            _sha256(value, f"asset publish {label}")
        if len(self.materialization_git_commit) != 40 or any(
            character not in "0123456789abcdef" for character in self.materialization_git_commit
        ):
            raise SilverContractError("materialization_git_commit must be a lowercase Git SHA")
        _positive_int(self.schema_version, "schema_version")
        _text(self.full_ready_event_path, "full_ready_event_path")
        _text(self.full_run_plan_path, "full_run_plan_path")
        _text(self.build_manifest_path, "build_manifest_path")
        _text(self.transform_version, "transform_version")
        _text(self.exchange_calendar_version, "exchange_calendar_version")
        _text(self.date_start, "date_start")
        _text(self.date_end, "date_end")
        if self.date_start > self.date_end:
            raise SilverContractError("asset publish date range is reversed")
        _positive_int(self.input_session_count, "input_session_count")
        _positive_int(self.input_manifest_count, "input_manifest_count")
        _positive_int(self.input_page_count, "input_page_count")
        _nonnegative_int(self.input_rows, "input_rows")
        _positive_int(self.input_manifest_bytes, "input_manifest_bytes")
        _positive_int(self.input_compressed_bytes, "input_compressed_bytes")
        _positive_int(self.input_raw_bytes, "input_raw_bytes")
        _positive_int(self.output_data_partition_count, "output_data_partition_count")
        _nonnegative_int(self.output_rows, "output_rows")
        _nonnegative_int(self.output_data_bytes, "output_data_bytes")
        _positive_int(self.qa_check_count, "qa_check_count")
        warnings = tuple(sorted(self.warnings, key=lambda item: item.result_id))
        if len({item.result_id for item in warnings}) != len(warnings):
            raise SilverContractError("asset publish warning result IDs are duplicated")
        if any(item.check.table != self.table for item in warnings):
            raise SilverContractError("asset publish warning belongs to another table")
        object.__setattr__(self, "warnings", warnings)
        _nonnegative_int(self.quarantine_issue_rows, "quarantine_issue_rows")
        _nonnegative_int(
            self.quarantine_unique_source_rows,
            "quarantine_unique_source_rows",
        )
        normalized = {
            key: tuple(items) for key, items in self.quarantine_issue_ids_by_severity.items()
        }
        if normalized != _EMPTY_QUARANTINE_IDS:
            raise SilverContractError("S4 publish plan requires every quarantine set to be empty")
        object.__setattr__(
            self,
            "quarantine_issue_ids_by_severity",
            MappingProxyType(normalized),
        )
        if self.quarantine_issue_rows or self.quarantine_unique_source_rows:
            raise SilverContractError("S4 publish plan requires zero quarantine rows")
        if tuple(self.accepted_quarantine_issue_ids):
            raise SilverContractError("S4 publish plan quarantine acceptance must be empty")
        object.__setattr__(self, "accepted_quarantine_issue_ids", ())

    @property
    def warning_result_ids(self) -> tuple[str, ...]:
        return tuple(item.result_id for item in self.warnings)

    def to_dict(self) -> dict[str, object]:
        return {
            "accepted_quarantine_issue_ids": [],
            "build_id": self.build_id,
            "build_manifest_path": self.build_manifest_path,
            "build_manifest_sha256": self.build_manifest_sha256,
            "contract_id": self.contract_id,
            "date_end": self.date_end,
            "date_start": self.date_start,
            "exchange_calendar_version": self.exchange_calendar_version,
            "full_ready_event_path": self.full_ready_event_path,
            "full_ready_event_sha256": self.full_ready_event_sha256,
            "full_run_plan_id": self.full_run_plan_id,
            "full_run_plan_path": self.full_run_plan_path,
            "full_run_plan_sha256": self.full_run_plan_sha256,
            "input_compressed_bytes": self.input_compressed_bytes,
            "input_manifest_bytes": self.input_manifest_bytes,
            "input_manifest_count": self.input_manifest_count,
            "input_page_count": self.input_page_count,
            "input_raw_bytes": self.input_raw_bytes,
            "input_rows": self.input_rows,
            "input_session_count": self.input_session_count,
            "manifest_inventory_sha256": self.manifest_inventory_sha256,
            "materialization_git_commit": self.materialization_git_commit,
            "output_data_bytes": self.output_data_bytes,
            "output_data_partition_count": self.output_data_partition_count,
            "output_rows": self.output_rows,
            "qa_check_count": self.qa_check_count,
            "quarantine_issue_ids_by_severity": {
                key: list(items)
                for key, items in sorted(self.quarantine_issue_ids_by_severity.items())
            },
            "quarantine_issue_rows": self.quarantine_issue_rows,
            "quarantine_unique_source_rows": self.quarantine_unique_source_rows,
            "schema_version": self.schema_version,
            "source_digest": self.source_digest,
            "artifact_inventory_sha256": self.artifact_inventory_sha256,
            "table": self.table,
            "transform_version": self.transform_version,
            "warnings": [item.to_dict() for item in self.warnings],
            "workflow_id": self.workflow_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> AssetPublishTablePlan:
        document = _object(value, "asset publish table plan")
        expected = {
            "accepted_quarantine_issue_ids",
            "build_id",
            "build_manifest_path",
            "build_manifest_sha256",
            "contract_id",
            "date_end",
            "date_start",
            "exchange_calendar_version",
            "full_ready_event_path",
            "full_ready_event_sha256",
            "full_run_plan_id",
            "full_run_plan_path",
            "full_run_plan_sha256",
            "input_compressed_bytes",
            "input_manifest_bytes",
            "input_manifest_count",
            "input_page_count",
            "input_raw_bytes",
            "input_rows",
            "input_session_count",
            "manifest_inventory_sha256",
            "materialization_git_commit",
            "output_data_bytes",
            "output_data_partition_count",
            "output_rows",
            "qa_check_count",
            "quarantine_issue_ids_by_severity",
            "quarantine_issue_rows",
            "quarantine_unique_source_rows",
            "schema_version",
            "source_digest",
            "artifact_inventory_sha256",
            "table",
            "transform_version",
            "warnings",
            "workflow_id",
        }
        _expect_keys(document, expected, "asset publish table plan")
        quarantine = _object(
            document["quarantine_issue_ids_by_severity"],
            "quarantine issue IDs",
        )
        return cls(
            table=_text(document["table"], "table"),
            workflow_id=_sha256(document["workflow_id"], "workflow_id"),
            contract_id=_sha256(document["contract_id"], "contract_id"),
            schema_version=_positive_int(document["schema_version"], "schema_version"),
            full_ready_event_path=_text(
                document["full_ready_event_path"], "full_ready_event_path"
            ),
            full_ready_event_sha256=_sha256(
                document["full_ready_event_sha256"], "full_ready_event_sha256"
            ),
            full_run_plan_id=_sha256(document["full_run_plan_id"], "full_run_plan_id"),
            full_run_plan_path=_text(document["full_run_plan_path"], "full_run_plan_path"),
            full_run_plan_sha256=_sha256(
                document["full_run_plan_sha256"], "full_run_plan_sha256"
            ),
            build_id=_sha256(document["build_id"], "build_id"),
            build_manifest_path=_text(
                document["build_manifest_path"], "build_manifest_path"
            ),
            build_manifest_sha256=_sha256(
                document["build_manifest_sha256"], "build_manifest_sha256"
            ),
            materialization_git_commit=_text(
                document["materialization_git_commit"], "materialization_git_commit"
            ),
            transform_version=_text(document["transform_version"], "transform_version"),
            exchange_calendar_version=_text(
                document["exchange_calendar_version"], "exchange_calendar_version"
            ),
            source_digest=_sha256(document["source_digest"], "source_digest"),
            date_start=_text(document["date_start"], "date_start"),
            date_end=_text(document["date_end"], "date_end"),
            input_session_count=_positive_int(
                document["input_session_count"], "input_session_count"
            ),
            input_manifest_count=_positive_int(
                document["input_manifest_count"], "input_manifest_count"
            ),
            input_page_count=_positive_int(
                document["input_page_count"], "input_page_count"
            ),
            input_rows=_nonnegative_int(document["input_rows"], "input_rows"),
            input_manifest_bytes=_positive_int(
                document["input_manifest_bytes"], "input_manifest_bytes"
            ),
            input_compressed_bytes=_positive_int(
                document["input_compressed_bytes"], "input_compressed_bytes"
            ),
            input_raw_bytes=_positive_int(
                document["input_raw_bytes"], "input_raw_bytes"
            ),
            manifest_inventory_sha256=_sha256(
                document["manifest_inventory_sha256"], "manifest_inventory_sha256"
            ),
            artifact_inventory_sha256=_sha256(
                document["artifact_inventory_sha256"], "artifact_inventory_sha256"
            ),
            output_data_partition_count=_positive_int(
                document["output_data_partition_count"],
                "output_data_partition_count",
            ),
            output_rows=_nonnegative_int(document["output_rows"], "output_rows"),
            output_data_bytes=_nonnegative_int(
                document["output_data_bytes"], "output_data_bytes"
            ),
            qa_check_count=_positive_int(document["qa_check_count"], "qa_check_count"),
            warnings=tuple(
                AssetPublishWarning.from_dict(item)
                for item in _array(document["warnings"], "warnings")
            ),
            quarantine_issue_rows=_nonnegative_int(
                document["quarantine_issue_rows"], "quarantine_issue_rows"
            ),
            quarantine_unique_source_rows=_nonnegative_int(
                document["quarantine_unique_source_rows"],
                "quarantine_unique_source_rows",
            ),
            quarantine_issue_ids_by_severity={
                key: tuple(_text(item, "quarantine issue ID") for item in _array(items, key))
                for key, items in quarantine.items()
            },
            accepted_quarantine_issue_ids=tuple(
                _text(item, "accepted quarantine issue ID")
                for item in _array(
                    document["accepted_quarantine_issue_ids"],
                    "accepted quarantine issue IDs",
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class AssetPublishPlan:
    """One immutable review object for the three-table S4 publication unit."""

    orchestration_git_commit: str
    materialization_git_commit: str
    tables: tuple[AssetPublishTablePlan, ...]
    runtime_review: AssetRuntimeReviewEvidence
    publication_scope: str = ASSET_PUBLICATION_SCOPE
    backtest_identity_eligible: bool = False
    requires_release_set: bool = True
    requires_runtime_review_acceptance: bool = True

    def __post_init__(self) -> None:
        for label, value in (
            ("orchestration_git_commit", self.orchestration_git_commit),
            ("materialization_git_commit", self.materialization_git_commit),
        ):
            if len(value) != 40 or any(
                character not in "0123456789abcdef" for character in value
            ):
                raise SilverContractError(f"{label} must be a lowercase Git SHA")
        if self.publication_scope != ASSET_PUBLICATION_SCOPE:
            raise SilverContractError("asset publish plan scope changed")
        if self.backtest_identity_eligible is not False:
            raise SilverContractError("S4 identity evidence cannot be backtest identity eligible")
        if self.requires_release_set is not True:
            raise SilverContractError("S4 publication must require a three-table release set")
        if self.requires_runtime_review_acceptance is not True:
            raise SilverContractError("S4 publication must require explicit runtime review")
        if len(self.tables) != len(_TABLE_ORDER) or {
            item.table for item in self.tables
        } != set(_TABLE_ORDER):
            raise SilverContractError("asset publish plan must contain the exact three S4 tables")
        normalized = tuple(sorted(self.tables, key=lambda item: _TABLE_ORDER.index(item.table)))
        if tuple(item.table for item in normalized) != _TABLE_ORDER:
            raise SilverContractError("asset publish plan must contain the exact three S4 tables")
        if any(
            item.materialization_git_commit != self.materialization_git_commit
            for item in normalized
        ):
            raise SilverContractError(
                "asset publish tables do not share one materialization commit"
            )
        if self.runtime_review.completed_sessions != normalized[0].input_session_count:
            raise SilverContractError("runtime review session count differs from S4 scope")
        expected_warning_counts = {
            item.table: len(item.warnings) for item in normalized
        }
        if dict(self.runtime_review.qa_warning_counts_by_table) != expected_warning_counts:
            raise SilverContractError("runtime log QA warning counts differ from full builds")
        object.__setattr__(self, "tables", normalized)

    @property
    def plan_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def warning_counts_by_table(self) -> dict[str, int]:
        return {item.table: len(item.warnings) for item in self.tables}

    def logical_payload(self) -> dict[str, object]:
        return {
            "asset_publish_plan_policy_version": ASSET_PUBLISH_PLAN_POLICY_VERSION,
            "asset_publish_plan_version": ASSET_PUBLISH_PLAN_VERSION,
            "backtest_identity_eligible": False,
            "materialization_git_commit": self.materialization_git_commit,
            "orchestration_git_commit": self.orchestration_git_commit,
            "publication_scope": self.publication_scope,
            "requires_release_set": True,
            "requires_runtime_review_acceptance": True,
            "runtime_review": self.runtime_review.to_dict(),
            "tables": [item.to_dict() for item in self.tables],
        }

    def to_dict(self) -> dict[str, object]:
        return {"plan_id": self.plan_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> AssetPublishPlan:
        document = _object(value, "asset publish plan")
        expected = {
            "asset_publish_plan_policy_version",
            "asset_publish_plan_version",
            "backtest_identity_eligible",
            "materialization_git_commit",
            "orchestration_git_commit",
            "plan_id",
            "publication_scope",
            "requires_release_set",
            "requires_runtime_review_acceptance",
            "runtime_review",
            "tables",
        }
        _expect_keys(document, expected, "asset publish plan")
        if document["asset_publish_plan_policy_version"] != ASSET_PUBLISH_PLAN_POLICY_VERSION:
            raise SilverContractError("unsupported asset publish plan policy version")
        if document["asset_publish_plan_version"] != ASSET_PUBLISH_PLAN_VERSION:
            raise SilverContractError("unsupported asset publish plan version")
        if type(document["backtest_identity_eligible"]) is not bool:
            raise SilverContractError("backtest_identity_eligible must be boolean")
        if type(document["requires_release_set"]) is not bool:
            raise SilverContractError("requires_release_set must be boolean")
        if type(document["requires_runtime_review_acceptance"]) is not bool:
            raise SilverContractError("requires_runtime_review_acceptance must be boolean")
        plan = cls(
            orchestration_git_commit=_text(
                document["orchestration_git_commit"], "orchestration_git_commit"
            ),
            materialization_git_commit=_text(
                document["materialization_git_commit"], "materialization_git_commit"
            ),
            tables=tuple(
                AssetPublishTablePlan.from_dict(item)
                for item in _array(document["tables"], "tables")
            ),
            runtime_review=AssetRuntimeReviewEvidence.from_dict(
                document["runtime_review"]
            ),
            publication_scope=_text(document["publication_scope"], "publication_scope"),
            backtest_identity_eligible=document["backtest_identity_eligible"],
            requires_release_set=document["requires_release_set"],
            requires_runtime_review_acceptance=document[
                "requires_runtime_review_acceptance"
            ],
        )
        if document["plan_id"] != plan.plan_id:
            raise SilverContractError("asset publish plan digest mismatch")
        return plan


@dataclass(frozen=True, slots=True)
class AssetPublishPlanRun:
    plan: AssetPublishPlan
    document: StoredDocument
    idempotent: bool


def create_asset_publish_plan(
    data_root: Path,
    *,
    workflow_ids_by_table: Mapping[str, str],
    full_ready_event_sha256_by_table: Mapping[str, str],
    build_ids_by_table: Mapping[str, str],
    build_manifest_sha256_by_table: Mapping[str, str],
    full_run_plan_ids_by_table: Mapping[str, str],
    full_run_plan_sha256_by_table: Mapping[str, str],
    repo_root: Path,
    orchestration_git_commit: str,
) -> AssetPublishPlanRun:
    """Re-verify and freeze the exact S4 full builds without changing workflow state."""

    return _create_asset_publish_plan(
        data_root,
        workflow_ids_by_table=workflow_ids_by_table,
        full_ready_event_sha256_by_table=full_ready_event_sha256_by_table,
        build_ids_by_table=build_ids_by_table,
        build_manifest_sha256_by_table=build_manifest_sha256_by_table,
        full_run_plan_ids_by_table=full_run_plan_ids_by_table,
        full_run_plan_sha256_by_table=full_run_plan_sha256_by_table,
        repo_root=repo_root,
        orchestration_git_commit=orchestration_git_commit,
        authorization=CURRENT_ASSET_FULL_RUN_PLAN_AUTHORIZATION,
        expected_materialization_git_commit=CURRENT_ASSET_MATERIALIZATION_GIT_COMMIT,
        git_verifier=_verify_git_checkout,
        runtime_review_loader=_load_runtime_review_evidence,
    )


def _create_asset_publish_plan(
    data_root: Path,
    *,
    workflow_ids_by_table: Mapping[str, str],
    full_ready_event_sha256_by_table: Mapping[str, str],
    build_ids_by_table: Mapping[str, str],
    build_manifest_sha256_by_table: Mapping[str, str],
    full_run_plan_ids_by_table: Mapping[str, str],
    full_run_plan_sha256_by_table: Mapping[str, str],
    repo_root: Path,
    orchestration_git_commit: str,
    authorization: AssetFullRunPlanAuthorization,
    expected_materialization_git_commit: str,
    git_verifier: Callable[[Path, str, str], None],
    runtime_review_loader: Callable[
        [Path, Mapping[str, Mapping[str, str]], AssetFullRunPlanAuthorization],
        AssetRuntimeReviewEvidence,
    ],
    before_final_lock: Callable[[], None] | None = None,
) -> AssetPublishPlanRun:
    root = data_root.expanduser().resolve()
    maps = {
        "workflow_ids": _exact_digest_map(workflow_ids_by_table, "workflow IDs"),
        "full_ready_events": _exact_digest_map(
            full_ready_event_sha256_by_table, "full-ready event SHAs"
        ),
        "build_ids": _exact_digest_map(build_ids_by_table, "build IDs"),
        "build_shas": _exact_digest_map(
            build_manifest_sha256_by_table, "build manifest SHAs"
        ),
        "plan_ids": _exact_digest_map(full_run_plan_ids_by_table, "full-run plan IDs"),
        "plan_shas": _exact_digest_map(
            full_run_plan_sha256_by_table, "full-run plan SHAs"
        ),
    }
    if len(orchestration_git_commit) != 40 or any(
        character not in "0123456789abcdef" for character in orchestration_git_commit
    ):
        raise SilverStoreError("orchestration Git commit must be a lowercase SHA")
    if len(expected_materialization_git_commit) != 40 or any(
        character not in "0123456789abcdef"
        for character in expected_materialization_git_commit
    ):
        raise SilverStoreError("materialization Git commit must be a lowercase SHA")
    if maps["workflow_ids"] != dict(authorization.workflow_ids_by_table):
        raise SilverStoreError("S4 publish workflow IDs differ from the authorized scope")

    expected_session_dates = _authorized_session_dates(authorization)
    git_verifier(
        repo_root,
        orchestration_git_commit,
        expected_materialization_git_commit,
    )

    store = SilverStore(root)
    table_plans: list[AssetPublishTablePlan] = []
    shared_scope: tuple[object, ...] | None = None
    shared_source_inputs: tuple[ArtifactRef, ...] | None = None

    # Every expensive source/output check completes before the one allowed write.  Trust-chain
    # verification remains metadata-only here so the shared 72,038-page source inventory is
    # hashed once, rather than once for both the plan and build in each of three workflows.
    for table in _TABLE_ORDER:
        workflow_id = maps["workflow_ids"][table]
        snapshot = store.verify_workflow_trust_chain(workflow_id, verify_artifacts=False)
        if snapshot.state is not WorkflowState.FULL_READY:
            raise SilverStoreError(f"S4 publish review requires full_ready for {table}")
        if snapshot.event_sha256 != maps["full_ready_events"][table]:
            raise SilverStoreError(f"S4 full-ready event changed for {table}")
        workflow_events = store.workflow_events(workflow_id)
        for event_record in workflow_events:
            _require_immutable_control_file(
                root,
                event_record.path,
                event_record.event_sha256,
            )
        full_event = _single_event(store, workflow_id, WorkflowState.FULL_READY)
        if full_event.event_sha256 != maps["full_ready_events"][table]:
            raise SilverStoreError(f"S4 full-ready event identity changed for {table}")

        contract, contract_document = store.load_workflow_contract(workflow_id)
        if contract != _CONTRACTS_BY_TABLE[table]:
            raise SilverStoreError(f"S4 registered contract changed for {table}")
        _require_immutable_control_file(
            root,
            contract_document.path,
            contract_document.sha256,
        )

        preview_event = _single_event(store, workflow_id, WorkflowState.PREVIEW_READY)
        preview_build_id = preview_event.event.evidence.get("build_id")
        preview_build_sha = preview_event.event.evidence.get("build_manifest_sha256")
        if not isinstance(preview_build_id, str) or not isinstance(preview_build_sha, str):
            raise SilverStoreError(f"S4 preview build evidence is incomplete for {table}")
        preview, preview_document = store.load_build(table, preview_build_id)
        if preview_document.sha256 != preview_build_sha:
            raise SilverStoreError(f"S4 preview build manifest changed for {table}")
        _require_immutable_control_file(root, preview_document.path, preview_document.sha256)

        build, build_document = store.load_build(table, maps["build_ids"][table])
        if build_document.sha256 != maps["build_shas"][table]:
            raise SilverStoreError(f"S4 full build manifest changed for {table}")
        if (
            build.intent.kind is not BuildKind.FULL
            or build.intent.workflow_id != workflow_id
            or full_event.event.evidence.get("build_id") != maps["build_ids"][table]
            or full_event.event.evidence.get("build_manifest_sha256") != build_document.sha256
        ):
            raise SilverStoreError(f"S4 full build binding changed for {table}")

        full_plan, full_plan_document = store.load_full_run_plan(
            table, maps["plan_ids"][table]
        )
        if full_plan_document.sha256 != maps["plan_shas"][table]:
            raise SilverStoreError(f"S4 full-run plan manifest changed for {table}")
        plan_event = _single_event(store, workflow_id, WorkflowState.FULL_RUN_PLAN_REVIEW)
        if (
            plan_event.event.evidence.get("full_run_plan_id") != maps["plan_ids"][table]
            or plan_event.event.evidence.get("full_run_plan_sha256")
            != full_plan_document.sha256
            or build.intent.parameters.get("approved_full_run_plan_id")
            != maps["plan_ids"][table]
        ):
            raise SilverStoreError(f"S4 full-run plan lineage changed for {table}")

        _require_immutable_control_file(root, full_event.path, full_event.event_sha256)
        _require_immutable_control_file(root, build_document.path, build_document.sha256)
        _require_immutable_control_file(
            root, full_plan_document.path, full_plan_document.sha256
        )
        _validate_full_plan_scope(
            full_plan,
            table=table,
            workflow_id=workflow_id,
            authorization=authorization,
            expected_materialization_git_commit=expected_materialization_git_commit,
        )
        _validate_plan_authorization(full_plan, authorization=authorization)
        _verify_plan_preflight(root, full_plan, authorization=authorization)
        _verify_plan_inventory_binding(root, full_plan, authorization=authorization)
        full_plan_source_digest = full_plan.source_digest
        data_outputs = _validate_s4_full_build_scope(
            build,
            contract=contract,
            workflow_id=workflow_id,
            full_plan=full_plan,
            full_plan_source_digest=full_plan_source_digest,
            authorization=authorization,
            expected_session_dates=expected_session_dates,
        )
        _verify_build_outputs_without_sources(
            store,
            root,
            preview,
            contract,
            workflow_id=workflow_id,
            build_id=preview_build_id,
        )
        _verify_build_outputs_without_sources(
            store,
            root,
            build,
            contract,
            workflow_id=workflow_id,
            build_id=maps["build_ids"][table],
        )

        warnings = tuple(
            AssetPublishWarning(check.result_id, check)
            for check in build.qa_checks
            if check.status is QAStatus.WARNING
        )
        if not warnings:
            raise SilverStoreError(f"S4 full build unexpectedly has no review warnings for {table}")

        current_scope = (
            authorization.date_start,
            authorization.date_end,
            authorization.expected_session_count,
            authorization.expected_input_rows,
            full_plan_source_digest,
            full_plan.input_artifact_count,
            full_plan.input_bytes,
            full_plan.transform_version,
            full_plan.exchange_calendar_version,
            full_plan.git_commit,
        )
        if shared_scope is None:
            shared_scope = current_scope
        elif current_scope != shared_scope:
            raise SilverStoreError(f"S4 full-run scope diverged for {table}")
        if shared_source_inputs is None:
            shared_source_inputs = full_plan.inputs

        table_plans.append(
            AssetPublishTablePlan(
                table=table,
                workflow_id=workflow_id,
                contract_id=contract.contract_id,
                schema_version=contract.schema_version,
                full_ready_event_path=full_event.path,
                full_ready_event_sha256=full_event.event_sha256,
                full_run_plan_id=maps["plan_ids"][table],
                full_run_plan_path=full_plan_document.path,
                full_run_plan_sha256=full_plan_document.sha256,
                build_id=maps["build_ids"][table],
                build_manifest_path=build_document.path,
                build_manifest_sha256=build_document.sha256,
                materialization_git_commit=expected_materialization_git_commit,
                transform_version=full_plan.transform_version,
                exchange_calendar_version=full_plan.exchange_calendar_version,
                source_digest=full_plan_source_digest,
                date_start=current_scope[0],
                date_end=current_scope[1],
                input_session_count=authorization.expected_session_count,
                input_manifest_count=authorization.expected_manifest_count,
                input_page_count=authorization.expected_page_count,
                input_rows=authorization.expected_input_rows,
                input_manifest_bytes=authorization.expected_manifest_bytes,
                input_compressed_bytes=authorization.expected_compressed_bytes,
                input_raw_bytes=authorization.expected_raw_bytes,
                manifest_inventory_sha256=authorization.manifest_inventory_sha256,
                artifact_inventory_sha256=authorization.artifact_inventory_sha256,
                output_data_partition_count=len(data_outputs),
                output_rows=authorization.expected_output_rows_by_table[table],
                output_data_bytes=sum(item.bytes for item in data_outputs),
                qa_check_count=len(build.qa_checks),
                warnings=warnings,
                quarantine_issue_rows=build.quarantine_issue_rows,
                quarantine_unique_source_rows=build.quarantine_unique_source_rows,
                quarantine_issue_ids_by_severity=build.quarantine_issue_ids_by_severity,
                accepted_quarantine_issue_ids=(),
            )
        )

    if shared_source_inputs is None:  # pragma: no cover - exact table map prevents this
        raise SilverStoreError("S4 source inventory is missing")
    store.verify_source_artifacts(
        shared_source_inputs,
        _CONTRACTS_BY_TABLE[_OBSERVATION_TABLE],
    )
    runtime_review = runtime_review_loader(root, maps, authorization)
    plan = AssetPublishPlan(
        orchestration_git_commit=orchestration_git_commit,
        materialization_git_commit=expected_materialization_git_commit,
        tables=tuple(table_plans),
        runtime_review=runtime_review,
    )
    content = _json_bytes(plan.to_dict())
    path = (
        root
        / "manifests"
        / "silver"
        / "publish-plans"
        / "assets"
        / f"plan_id={plan.plan_id}"
        / "manifest.json"
    )
    if before_final_lock is not None:
        before_final_lock()
    with ExitStack() as locks:
        for workflow_id in sorted(maps["workflow_ids"].values()):
            locks.enter_context(store._workflow_lock(workflow_id))
        _assert_exact_full_ready(store, maps)
        existed = path.exists()
        stored = write_bytes_immutable(root, path, content)
        document = StoredDocument(
            path=str(stored["path"]),
            sha256=str(stored["sha256"]),
            bytes=int(stored["bytes"]),
        )
        _require_immutable_control_file(root, document.path, document.sha256)
        observed = AssetPublishPlan.from_dict(json.loads(path.read_bytes()))
        if observed != plan:
            raise SilverStoreError("stored S4 asset publish plan changed")
        _assert_exact_full_ready(store, maps)
    return AssetPublishPlanRun(plan=plan, document=document, idempotent=existed)


def _load_runtime_review_evidence(
    root: Path,
    maps: Mapping[str, Mapping[str, str]],
    authorization: AssetFullRunPlanAuthorization,
    *,
    expected_path: str = CURRENT_ASSET_RUNTIME_LOG_PATH,
    expected_sha256: str = CURRENT_ASSET_RUNTIME_LOG_SHA256,
    expected_bytes: int = CURRENT_ASSET_RUNTIME_LOG_BYTES,
) -> AssetRuntimeReviewEvidence:
    path = safe_relative_path(root, expected_path)
    if path.is_symlink() or not path.is_file():
        raise SilverStoreError("S4 full-run runtime log is not a regular file")
    before = path.stat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise SilverStoreError("S4 full-run runtime log is not a single-link regular file")
    content = path.read_bytes()
    after = path.stat()
    def identity(value: os.stat_result) -> tuple[int, ...]:
        return (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_nlink,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )
    if identity(before) != identity(after):
        raise SilverStoreError("S4 full-run runtime log changed while reading")
    observed_sha256 = hashlib.sha256(content).hexdigest()
    if len(content) != expected_bytes or observed_sha256 != expected_sha256:
        raise SilverStoreError("S4 full-run runtime log checksum changed")
    try:
        document = _object(json.loads(content), "S4 full-run runtime log")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SilverStoreError("S4 full-run runtime log is invalid JSON") from exc
    _expect_keys(
        document,
        {"completed_sessions", "idempotent", "mode", "tables", "warnings"},
        "S4 full-run runtime log",
    )
    warnings = tuple(
        _text(item, "S4 runtime warning")
        for item in _array(document["warnings"], "S4 runtime warnings")
    )
    if (
        document["completed_sessions"] != authorization.expected_session_count
        or document["idempotent"] is not False
        or document["mode"] != "full_ready_only"
        or warnings != (ASSET_RUNTIME_RSS_WARNING,)
    ):
        raise SilverStoreError("S4 full-run runtime review summary changed")
    tables = _object(document["tables"], "S4 runtime tables")
    if set(tables) != set(_TABLE_ORDER):
        raise SilverStoreError("S4 full-run runtime tables changed")
    qa_warning_counts: dict[str, int] = {}
    for table in _TABLE_ORDER:
        summary = _object(tables[table], f"S4 runtime table:{table}")
        qa_status_counts = _object(
            summary.get("qa_status_counts"),
            f"S4 runtime QA counts:{table}",
        )
        warning_count = _nonnegative_int(
            qa_status_counts.get("warning"),
            f"S4 runtime warning count:{table}",
        )
        if (
            summary.get("workflow_id") != maps["workflow_ids"][table]
            or summary.get("workflow_event_sha256")
            != maps["full_ready_events"][table]
            or summary.get("build_id") != maps["build_ids"][table]
            or summary.get("build_manifest_sha256") != maps["build_shas"][table]
            or summary.get("full_run_plan_id") != maps["plan_ids"][table]
            or summary.get("state") != WorkflowState.FULL_READY.value
            or summary.get("sequence") != 8
            or summary.get("date_start") != authorization.date_start
            or summary.get("date_end") != authorization.date_end
            or summary.get("input_session_count")
            != authorization.expected_session_count
            or summary.get("input_manifest_count")
            != authorization.expected_manifest_count
            or summary.get("input_page_count") != authorization.expected_page_count
            or summary.get("input_rows") != authorization.expected_input_rows
            or summary.get("input_compressed_bytes")
            != authorization.expected_compressed_bytes
            or summary.get("input_raw_bytes") != authorization.expected_raw_bytes
            or summary.get("output_data_partition_count")
            != authorization.expected_session_count
            or summary.get("output_artifact_count")
            != authorization.expected_session_count + 2
            or summary.get("output_rows")
            != authorization.expected_output_rows_by_table[table]
        ):
            raise SilverStoreError(f"S4 full-run runtime table summary changed for {table}")
        qa_warning_counts[table] = warning_count
    return AssetRuntimeReviewEvidence(
        source_path=expected_path,
        source_sha256=observed_sha256,
        source_bytes=len(content),
        completed_sessions=authorization.expected_session_count,
        qa_warning_counts_by_table=qa_warning_counts,
        warning_messages=warnings,
        expected_rss_ceiling_bytes=authorization.expected_rss_ceiling_bytes,
        hard_rss_limit_bytes=authorization.hard_rss_limit_bytes,
    )


def _authorized_session_dates(
    authorization: AssetFullRunPlanAuthorization,
) -> tuple[str, ...]:
    sessions = tuple(
        item.date().isoformat()
        for item in xcals.get_calendar("XNYS").sessions_in_range(
            authorization.date_start,
            authorization.date_end,
        )
    )
    if (
        len(sessions) != authorization.expected_session_count
        or not sessions
        or sessions[0] != authorization.date_start
        or sessions[-1] != authorization.date_end
    ):
        raise SilverStoreError("S4 authorized XNYS session scope changed")
    return sessions


def _validate_full_plan_scope(
    plan: FullRunPlan,
    *,
    table: str,
    workflow_id: str,
    authorization: AssetFullRunPlanAuthorization,
    expected_materialization_git_commit: str,
) -> None:
    contract = _CONTRACTS_BY_TABLE[table]
    if (
        plan.workflow_id != workflow_id
        or plan.table != table
        or plan.domain != contract.domain
        or plan.contract_id != contract.contract_id
        or plan.schema_version != contract.schema_version
        or plan.git_commit != expected_materialization_git_commit
        or plan.input_artifact_count != authorization.expected_page_count
        or plan.input_rows != authorization.expected_input_rows
        or plan.input_bytes != authorization.expected_compressed_bytes
    ):
        raise SilverStoreError(f"S4 authorized full-run plan scope changed for {table}")
    expected_parameters: Mapping[str, object] = {
        "calendar_name": "XNYS",
        "date_start": authorization.date_start,
        "date_end": authorization.date_end,
        "expected_input_rows": authorization.expected_input_rows,
        "expected_output_rows": authorization.expected_output_rows_by_table[table],
        "exchange_release_id": authorization.exchange_release_id,
        "exchange_release_sha256": authorization.exchange_release_sha256,
        "input_artifact_inventory_sha256": authorization.artifact_inventory_sha256,
        "input_compressed_bytes": authorization.expected_compressed_bytes,
        "input_manifest_bytes": authorization.expected_manifest_bytes,
        "input_manifest_count": authorization.expected_manifest_count,
        "input_manifest_inventory_sha256": authorization.manifest_inventory_sha256,
        "input_page_count": authorization.expected_page_count,
        "input_raw_bytes": authorization.expected_raw_bytes,
        "input_session_count": authorization.expected_session_count,
        "max_in_flight_sessions": authorization.max_in_flight_sessions,
        "parquet_writer_policy": dict(authorization.parquet_writer_policy),
        "pyarrow_version": authorization.pyarrow_version,
        "source_profile_path": authorization.source_profile_path,
        "source_profile_sha256": authorization.source_profile_sha256,
        "ticker_type_release_id": authorization.ticker_type_release_id,
        "ticker_type_release_sha256": authorization.ticker_type_release_sha256,
        "workers": authorization.workers,
    }
    changed = {
        key: (plan.parameters.get(key), expected)
        for key, expected in expected_parameters.items()
        if plan.parameters.get(key) != expected
    }
    if changed:
        raise SilverStoreError(
            f"S4 authorized full-run parameters changed for {table}: {sorted(changed)}"
        )


def _validate_s4_full_build_scope(
    build: BuildManifest,
    *,
    contract: TableContract,
    workflow_id: str,
    full_plan: FullRunPlan,
    full_plan_source_digest: str,
    authorization: AssetFullRunPlanAuthorization,
    expected_session_dates: tuple[str, ...],
) -> tuple[ArtifactRef, ...]:
    table = contract.table
    expected_output_rows = authorization.expected_output_rows_by_table[table]
    expected_version_rows = authorization.expected_output_rows_by_table[_VERSION_TABLE]
    if (
        build.intent.kind is not BuildKind.FULL
        or build.intent.workflow_id != workflow_id
        or build.intent.contract_id != contract.contract_id
        or build.intent.source_digest != full_plan_source_digest
        or build.intent.git_commit != full_plan.git_commit
        or build.intent.transform_version != full_plan.transform_version
        or build.intent.exchange_calendar_version != full_plan.exchange_calendar_version
    ):
        raise SilverStoreError(f"S4 full build identity changed for {table}")
    funnel = build.row_funnel
    if (
        funnel.input_rows != authorization.expected_input_rows
        or funnel.accepted_source_rows != authorization.expected_input_rows
        or funnel.exact_duplicate_excess != 0
        or funnel.quarantined_source_rows != 0
        or funnel.unmapped_source_rows
        != authorization.expected_input_rows - expected_output_rows
        or funnel.version_preserved_rows != expected_version_rows
        or dict(funnel.output_rows_by_table) != {table: expected_output_rows}
    ):
        raise SilverStoreError(f"S4 full row funnel changed for {table}")
    _require_zero_quarantine(build, table)
    failed = tuple(
        check.result_id for check in build.qa_checks if check.status is QAStatus.FAILED
    )
    expected_partition_key = (
        f"full_history:{authorization.date_start}:{authorization.date_end}"
    )
    if (
        failed
        or {check.check_id for check in build.qa_checks}
        != set(contract.required_qa_checks)
        or any(check.partition_key != expected_partition_key for check in build.qa_checks)
        or any(check.blocks_publish for check in build.qa_checks)
    ):
        raise SilverStoreError(f"S4 full QA scope changed for {table}: failed={failed}")

    data = tuple(item for item in build.outputs if item.role is ArtifactRole.DATA)
    qa = tuple(item for item in build.outputs if item.role is ArtifactRole.QA)
    quarantine = tuple(
        item for item in build.outputs if item.role is ArtifactRole.QUARANTINE
    )
    prefix = SilverStore.build_output_prefix(build.intent)
    expected_data_paths = {
        (
            f"{prefix}/data/session_year={session_date[:4]}/"
            f"session_date={session_date}/part-00000.parquet"
        )
        for session_date in expected_session_dates
    }
    if (
        len(data) != authorization.expected_session_count
        or {item.path for item in data} != expected_data_paths
        or sum(int(item.row_count or 0) for item in data) != expected_output_rows
        or len(qa) != 1
        or qa[0].path != f"{prefix}/qa/qa-check-result.parquet"
        or qa[0].row_count != len(contract.required_qa_checks)
        or len(quarantine) != 1
        or quarantine[0].path
        != f"{prefix}/quarantine/quarantine-record.parquet"
        or quarantine[0].row_count != 0
        or len(build.outputs) != authorization.expected_session_count + 2
    ):
        raise SilverStoreError(f"S4 exact output partitions changed for {table}")
    return data


def _verify_build_outputs_without_sources(
    store: SilverStore,
    root: Path,
    build: BuildManifest,
    contract: TableContract,
    *,
    workflow_id: str,
    build_id: str,
) -> None:
    """Perform ``verify_build``'s physical output checks without rehashing shared sources."""

    store.validate_build_manifest(build, contract, workflow_id=workflow_id)
    prefix = SilverStore.build_output_prefix(build.intent)
    prefix_path = safe_relative_path(root, prefix)
    if not prefix_path.is_dir():
        raise SilverStoreError(f"S4 build output directory is missing: {prefix}")
    data_count = 0
    qa_rows: list[dict[str, object]] = []
    quarantine_rows = 0
    quarantine_source_ids: set[str] = set()
    quarantine_issue_ids = {severity.value: set() for severity in QASeverity}
    for output in build.outputs:
        output_path = store.verify_artifact(
            output,
            contract=contract if output.role is ArtifactRole.DATA else None,
        )
        if output.role is ArtifactRole.DATA:
            data_count += 1
            if output.table != contract.table:
                raise SilverStoreError("S4 data artifact belongs to another table")
        elif output.role is ArtifactRole.QA:
            qa_rows.extend(pq.read_table(output_path).to_pylist())
        elif output.role is ArtifactRole.QUARANTINE:
            rows = pq.read_table(output_path).to_pylist()
            quarantine_rows += len(rows)
            for row in rows:
                record = QuarantineRecord.from_dict(row)
                if (
                    record.detected_build_id != build_id
                    or record.table_name != contract.table
                    or record.review_status is not QuarantineReviewStatus.PENDING
                ):
                    raise SilverStoreError("S4 quarantine artifact lineage changed")
                quarantine_source_ids.add(record.source_record_id)
                quarantine_issue_ids[record.severity.value].add(record.issue_id)
    expected_qa_rows = [check.to_output_dict(build_id) for check in build.qa_checks]
    def row_key(row: Mapping[str, object]) -> tuple[str, str, str]:
        return (
            str(row["table_name"]),
            str(row["partition_key"]),
            str(row["check_id"]),
        )
    if data_count == 0 or sorted(qa_rows, key=row_key) != sorted(
        expected_qa_rows,
        key=row_key,
    ):
        raise SilverStoreError("S4 physical QA outputs changed")
    if (
        quarantine_rows != build.quarantine_issue_rows
        or len(quarantine_source_ids) != build.quarantine_unique_source_rows
        or quarantine_issue_ids
        != {
            severity: set(issue_ids)
            for severity, issue_ids in build.quarantine_issue_ids_by_severity.items()
        }
    ):
        raise SilverStoreError("S4 physical quarantine outputs changed")
    declared = {item.path for item in build.outputs}
    actual: set[str] = set()
    for path in prefix_path.rglob("*"):
        if path.is_symlink():
            raise SilverStoreError(f"S4 build output tree contains a symlink: {path}")
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    if actual != declared:
        raise SilverStoreError(
            "S4 build output file set changed: "
            f"missing={sorted(declared - actual)}, extra={sorted(actual - declared)}"
        )


def _assert_exact_full_ready(
    store: SilverStore,
    maps: Mapping[str, Mapping[str, str]],
) -> None:
    for table in _TABLE_ORDER:
        snapshot = store.status(maps["workflow_ids"][table])
        if (
            snapshot.state is not WorkflowState.FULL_READY
            or snapshot.event_sha256 != maps["full_ready_events"][table]
        ):
            raise SilverStoreError(
                f"S4 workflow changed before publish-plan commit: {table}"
            )


def _exact_digest_map(value: Mapping[str, str], label: str) -> dict[str, str]:
    normalized = dict(value)
    if set(normalized) != set(_TABLE_ORDER):
        raise SilverStoreError(f"S4 publish {label} table keys are incomplete")
    for table, digest in normalized.items():
        try:
            _sha256(digest, f"{label}:{table}")
        except SilverContractError as exc:
            raise SilverStoreError(str(exc)) from exc
    return normalized


def _single_event(
    store: SilverStore,
    workflow_id: str,
    state: WorkflowState,
) -> WorkflowEventRecord:
    matches = [
        item for item in store.workflow_events(workflow_id) if item.event.to_state is state
    ]
    if len(matches) != 1:
        raise SilverStoreError(f"S4 workflow must have one {state.value} event")
    return matches[0]


def _require_zero_quarantine(build: BuildManifest, table: str) -> None:
    if (
        build.quarantine_issue_rows != 0
        or build.quarantine_unique_source_rows != 0
        or any(build.quarantine_issue_ids_by_severity.values())
        or build.row_funnel.quarantined_source_rows != 0
    ):
        raise SilverStoreError(f"S4 publish review requires empty quarantine for {table}")


def _require_immutable_control_file(root: Path, relative_path: str, checksum: str) -> None:
    path = safe_relative_path(root, relative_path)
    if path.is_symlink() or not path.is_file():
        raise SilverStoreError(f"S4 control document is not a regular file: {relative_path}")
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode):
        raise SilverStoreError(f"S4 control document is not regular: {relative_path}")
    if stat.S_IMODE(metadata.st_mode) != 0o444 or metadata.st_nlink != 1:
        raise SilverStoreError(f"S4 control document is not immutable: {relative_path}")
    if sha256_file(path) != checksum:
        raise SilverStoreError(f"S4 control document checksum changed: {relative_path}")


def _json_bytes(document: Mapping[str, object]) -> bytes:
    return (
        json.dumps(document, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


def _verify_git_checkout(
    repo_root: Path,
    orchestration_git_commit: str,
    materialization_git_commit: str,
) -> None:
    root = repo_root.expanduser().resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise SilverStoreError(
            "S4 publish-plan code is not executing from the verified Git checkout"
        ) from exc
    top_level = _git_output(root, "rev-parse", "--show-toplevel")
    head = _git_output(root, "rev-parse", "HEAD")
    tracked_module = _git_output(root, "ls-files", "--error-unmatch", "--", module_relative)
    status_output = _git_output(root, "status", "--porcelain=v1", "--untracked-files=all")
    if Path(top_level).resolve() != root:
        raise SilverStoreError("S4 publish-plan repo_root is not the Git top level")
    if head != orchestration_git_commit:
        raise SilverStoreError("S4 publish-plan Git HEAD differs from the pinned commit")
    if tracked_module != module_relative:
        raise SilverStoreError("S4 publish-plan module is not tracked")
    if status_output:
        raise SilverStoreError("S4 publish-plan Git checkout is not clean")
    ancestor = subprocess.run(
        (
            "git",
            "-C",
            str(root),
            "merge-base",
            "--is-ancestor",
            materialization_git_commit,
            orchestration_git_commit,
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if ancestor.returncode != 0:
        raise SilverStoreError("S4 materialization commit is not an ancestor of orchestration")
    _verify_reviewed_logic_closure(
        root,
        materialization_git_commit,
        orchestration_git_commit,
    )


def _verify_reviewed_logic_closure(
    root: Path,
    materialization_git_commit: str,
    orchestration_git_commit: str,
) -> None:
    drift = subprocess.run(
        (
            "git",
            "-C",
            str(root),
            "diff",
            "--quiet",
            materialization_git_commit,
            orchestration_git_commit,
            "--",
            *_REVIEWED_LOGIC_CLOSURE,
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if drift.returncode == 1:
        raise SilverStoreError("S4 reviewed materialization logic changed after the full build")
    if drift.returncode != 0:
        raise SilverStoreError("cannot verify S4 reviewed logic closure")


def _git_output(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        raise SilverStoreError(
            f"S4 publish-plan Git command failed: {' '.join(arguments)}: "
            f"{completed.stderr.strip()}"
        )
    return completed.stdout.strip()


__all__ = [
    "ASSET_PUBLICATION_SCOPE",
    "ASSET_PUBLISH_PLAN_POLICY_VERSION",
    "ASSET_PUBLISH_PLAN_VERSION",
    "ASSET_RUNTIME_EVIDENCE_LIMITATION",
    "ASSET_RUNTIME_RSS_REVIEW_STATUS",
    "ASSET_RUNTIME_RSS_WARNING",
    "CURRENT_ASSET_MATERIALIZATION_GIT_COMMIT",
    "CURRENT_ASSET_RUNTIME_LOG_BYTES",
    "CURRENT_ASSET_RUNTIME_LOG_PATH",
    "CURRENT_ASSET_RUNTIME_LOG_SHA256",
    "AssetPublishPlan",
    "AssetPublishPlanRun",
    "AssetPublishTablePlan",
    "AssetPublishWarning",
    "AssetRuntimeReviewEvidence",
    "create_asset_publish_plan",
]
