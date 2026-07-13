"""S4 Assets full-run plan control plane without a full transform.

This module has two deliberately separate operations:

* :func:`create_asset_full_run_plans` verifies the complete ten-year Bronze scope,
  records one shared immutable resource preflight, and advances the three S4 workflows only
  from ``awaiting_review`` to ``full_run_plan_review``.
* :func:`approve_asset_full_run_plans` approves only the three exact plan ID / manifest SHA /
  plan-review event triples supplied by the caller.  It cannot build or publish data.

The implementation is restart-safe across the three independent workflow journals.  A partial
run reuses the same source inventory and the same preflight observation, so plan IDs cannot drift
because disk free space changed after the first transition.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from importlib.metadata import version
from pathlib import Path
from types import MappingProxyType

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    ASSET_OBSERVATION_VERSION_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.asset_source import build_asset_source_inventory
from ame_stocks_api.silver.assets import (
    ASSET_METADATA_TIME_SCOPE,
    ASSET_REFERENCE_TIME_SCOPE,
    ASSET_SOURCE_AVAILABILITY_QUALITY,
    ASSET_SOURCE_AVAILABILITY_RULE,
    ASSET_TRANSFORM_VERSION,
    ASSET_VERSION_SELECTION_RULE,
    UNIVERSE_SOURCE_AVAILABILITY_RULE,
)
from ame_stocks_api.silver.contracts import (
    SEPARATE_FULL_RUN_PLAN_POLICY,
    ArtifactRef,
    ArtifactRole,
    BuildKind,
    BuildManifest,
    FullRunPlan,
    QASeverity,
    QAStatus,
    SourceInventory,
    SourceLayer,
    TableContract,
    UpstreamManifestRef,
)
from ame_stocks_api.silver.exchange_contract import EXCHANGE_DIM_CONTRACT
from ame_stocks_api.silver.reader import PublishedSilverReader
from ame_stocks_api.silver.store import (
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowEventRecord,
    WorkflowSnapshot,
    WorkflowState,
)
from ame_stocks_api.silver.ticker_type_contract import TICKER_TYPE_DIM_CONTRACT

ASSET_FULL_RUN_PLAN_POLICY_VERSION = "s4-assets-full-run-plan-v1"
ASSET_FULL_RUN_PREFLIGHT_VERSION = 1
ASSET_FULL_PARQUET_WRITER_POLICY: Mapping[str, object] = MappingProxyType(
    {
        "compression": "zstd",
        "version": "2.6",
        "write_statistics": True,
    }
)
GIB = 1024**3

_OBSERVATION_TABLE = ASSET_OBSERVATION_DAILY_CONTRACT.table
_VERSION_TABLE = ASSET_OBSERVATION_VERSION_CONTRACT.table
_UNIVERSE_TABLE = UNIVERSE_SOURCE_DAILY_CONTRACT.table
_TABLE_ORDER = (_OBSERVATION_TABLE, _VERSION_TABLE, _UNIVERSE_TABLE)
_CONTRACTS_BY_TABLE: Mapping[str, TableContract] = MappingProxyType(
    {
        _OBSERVATION_TABLE: ASSET_OBSERVATION_DAILY_CONTRACT,
        _VERSION_TABLE: ASSET_OBSERVATION_VERSION_CONTRACT,
        _UNIVERSE_TABLE: UNIVERSE_SOURCE_DAILY_CONTRACT,
    }
)

_PRODUCTION_WORKFLOW_IDS = MappingProxyType(
    {
        _OBSERVATION_TABLE: (
            "c1bae241ed90e49aed1ae8a98b6801f511d6abaac2cef93c66ccba59d33775ec"
        ),
        _VERSION_TABLE: "989c8c513905e2710714c0b6f94352119e8fb1128147d8c2db9486c1e03df6da",
        _UNIVERSE_TABLE: "918ebc04d2eded87243387804d58fa9f24e4282ee27a8a26ac6ac22f4390b755",
    }
)
_PRODUCTION_PREVIEW_BUILD_IDS = MappingProxyType(
    {
        _OBSERVATION_TABLE: (
            "baaf04a909973984f51eaaeccfd3e2408763acd6aa76403cdf62017edd0422ba"
        ),
        _VERSION_TABLE: "1c560bbaffbb7a838fbcbccf90d0da83e4c69f2866515bf860f0c05eb1406e8f",
        _UNIVERSE_TABLE: "442ac3894e68e14332621b73de6b4eb83e362c549328223c57b63f80828dc755",
    }
)
_PRODUCTION_PREVIEW_MANIFEST_SHA256 = MappingProxyType(
    {
        _OBSERVATION_TABLE: (
            "5ce4d35c06cfd1ed87e0f847baa2f6d7a95258ddee7b8c913c0a3f5791a11a58"
        ),
        _VERSION_TABLE: "fced8a5bb82ed0ab6e0850ed7680397709e78d5d47c58b097309977adf547f65",
        _UNIVERSE_TABLE: "ef502a1d759b58017411a6686b23d0376a741566950a54aa7a7da5a7272d8b65",
    }
)
_PRODUCTION_AWAITING_REVIEW_EVENT_SHA256 = MappingProxyType(
    {
        _OBSERVATION_TABLE: (
            "4d172aa12ff368e0dd42f77df83eeeadcba6c51a800baac10ab4fdda11e7e53c"
        ),
        _VERSION_TABLE: "b0fe4549477f079fb92f75cc05732baa5a7de04820c40bfca659c37a7b195c47",
        _UNIVERSE_TABLE: "d9d993eafa729de1f88b785ee1752f0144e7a3a5ebb6f9fc082a0e611c564b76",
    }
)

_EXCHANGE_RELEASE_ID = "feab0e1f32a5685d1115a6e4e87aab8ff50c18b99c6336a8790ecba44464d838"
_EXCHANGE_RELEASE_SHA256 = "d8789e6cf760ffb6274077736c18e37bd69330139ea1c6ecf2f420bb56f93f07"
_TICKER_TYPE_RELEASE_ID = (
    "11a62f9c06ea5c609c159a7d619ba94cabbe39d3b07518fec279fa4758c882f6"
)
_TICKER_TYPE_RELEASE_SHA256 = (
    "5568a905bb1cdfe791a300f5b12fdd1e2041e3e1c1aacfbf6cc78f4890b95f47"
)


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label} must be a positive native int")
    return value


@dataclass(frozen=True, slots=True)
class AssetFullRunPlanAuthorization:
    """Frozen production evidence and resource envelope for S4 plan creation."""

    workflow_ids_by_table: Mapping[str, str]
    preview_build_ids_by_table: Mapping[str, str]
    preview_manifest_sha256_by_table: Mapping[str, str]
    awaiting_review_event_sha256_by_table: Mapping[str, str]
    source_profile_path: str
    source_profile_sha256: str
    date_start: str
    date_end: str
    expected_session_count: int
    expected_manifest_count: int
    expected_page_count: int
    expected_input_rows: int
    expected_manifest_bytes: int
    expected_compressed_bytes: int
    expected_raw_bytes: int
    manifest_inventory_sha256: str
    artifact_inventory_sha256: str
    expected_output_rows_by_table: Mapping[str, int]
    estimated_data_bytes_by_table: Mapping[str, int]
    estimated_data_bytes_total_point: int
    exchange_release_id: str
    exchange_release_sha256: str
    ticker_type_release_id: str
    ticker_type_release_sha256: str
    pyarrow_version: str
    parquet_writer_policy: Mapping[str, object]
    workers: int = 1
    max_in_flight_sessions: int = 1
    stable_output_cap_bytes: int = 20 * GIB
    peak_incremental_cap_bytes: int = 24 * GIB
    stable_project_cap_bytes: int = 120 * GIB
    peak_project_cap_bytes: int = 140 * GIB
    free_space_floor_bytes: int = 40 * GIB
    free_space_warning_bytes: int = 60 * GIB
    runtime_estimate_seconds: int = 22 * 60 * 60
    runtime_review_ceiling_seconds: int = 30 * 60 * 60
    expected_rss_ceiling_bytes: int = 3 * GIB // 4
    hard_rss_limit_bytes: int = 2 * GIB
    dependency_lineage_required: bool = True

    def __post_init__(self) -> None:
        mappings = {
            "workflow_ids_by_table": self.workflow_ids_by_table,
            "preview_build_ids_by_table": self.preview_build_ids_by_table,
            "preview_manifest_sha256_by_table": self.preview_manifest_sha256_by_table,
            "awaiting_review_event_sha256_by_table": (
                self.awaiting_review_event_sha256_by_table
            ),
            "expected_output_rows_by_table": self.expected_output_rows_by_table,
            "estimated_data_bytes_by_table": self.estimated_data_bytes_by_table,
        }
        for label, value in mappings.items():
            normalized = dict(value)
            if set(normalized) != set(_TABLE_ORDER):
                raise ValueError(f"asset full-run {label} table keys are incomplete")
            if label not in {
                "expected_output_rows_by_table",
                "estimated_data_bytes_by_table",
            }:
                for table, digest in normalized.items():
                    _sha256(digest, f"{label}:{table}")
            else:
                for table, rows in normalized.items():
                    _positive_int(rows, f"{label}:{table}")
            object.__setattr__(self, label, MappingProxyType(dict(sorted(normalized.items()))))
        for label, digest in (
            ("source_profile_sha256", self.source_profile_sha256),
            ("manifest_inventory_sha256", self.manifest_inventory_sha256),
            ("artifact_inventory_sha256", self.artifact_inventory_sha256),
            ("exchange_release_id", self.exchange_release_id),
            ("exchange_release_sha256", self.exchange_release_sha256),
            ("ticker_type_release_id", self.ticker_type_release_id),
            ("ticker_type_release_sha256", self.ticker_type_release_sha256),
        ):
            _sha256(digest, label)
        profile_path = Path(self.source_profile_path)
        if profile_path.is_absolute() or profile_path.as_posix() != self.source_profile_path:
            raise ValueError("asset full-run source profile path must be normalized and relative")
        if not self.date_start or not self.date_end or self.date_start > self.date_end:
            raise ValueError("asset full-run date scope is invalid")
        for label in (
            "expected_session_count",
            "expected_manifest_count",
            "expected_page_count",
            "expected_input_rows",
            "expected_manifest_bytes",
            "expected_compressed_bytes",
            "expected_raw_bytes",
            "estimated_data_bytes_total_point",
            "workers",
            "max_in_flight_sessions",
            "stable_output_cap_bytes",
            "peak_incremental_cap_bytes",
            "stable_project_cap_bytes",
            "peak_project_cap_bytes",
            "free_space_floor_bytes",
            "free_space_warning_bytes",
            "runtime_estimate_seconds",
            "runtime_review_ceiling_seconds",
            "expected_rss_ceiling_bytes",
            "hard_rss_limit_bytes",
        ):
            _positive_int(getattr(self, label), f"asset full-run {label}")
        if self.free_space_warning_bytes < self.free_space_floor_bytes:
            raise ValueError("asset full-run free-space warning must not be below the floor")
        if self.stable_project_cap_bytes < self.stable_output_cap_bytes:
            raise ValueError("asset full-run stable project cap is below stable output cap")
        if self.peak_project_cap_bytes < self.peak_incremental_cap_bytes:
            raise ValueError("asset full-run peak project cap is below incremental peak cap")
        if self.runtime_review_ceiling_seconds < self.runtime_estimate_seconds:
            raise ValueError("asset full-run runtime ceiling must not be below the estimate")
        if self.hard_rss_limit_bytes < self.expected_rss_ceiling_bytes:
            raise ValueError("asset full-run hard RSS limit must not be below the estimate")
        if sum(self.estimated_data_bytes_by_table.values()) != (
            self.estimated_data_bytes_total_point
        ):
            raise ValueError("asset full-run per-table data estimates do not reconcile")
        if self.estimated_data_bytes_total_point > self.stable_output_cap_bytes:
            raise ValueError("asset full-run point output estimate exceeds stable cap")
        if type(self.dependency_lineage_required) is not bool:
            raise ValueError("asset full-run dependency_lineage_required must be boolean")
        if not isinstance(self.pyarrow_version, str) or not self.pyarrow_version:
            raise ValueError("asset full-run pyarrow_version must be nonempty text")
        writer_policy = dict(self.parquet_writer_policy)
        if writer_policy != dict(ASSET_FULL_PARQUET_WRITER_POLICY):
            raise ValueError("asset full-run Parquet writer policy is not authorized")
        object.__setattr__(
            self,
            "parquet_writer_policy",
            MappingProxyType(writer_policy),
        )


CURRENT_ASSET_FULL_RUN_PLAN_AUTHORIZATION = AssetFullRunPlanAuthorization(
    workflow_ids_by_table=_PRODUCTION_WORKFLOW_IDS,
    preview_build_ids_by_table=_PRODUCTION_PREVIEW_BUILD_IDS,
    preview_manifest_sha256_by_table=_PRODUCTION_PREVIEW_MANIFEST_SHA256,
    awaiting_review_event_sha256_by_table=_PRODUCTION_AWAITING_REVIEW_EVENT_SHA256,
    source_profile_path="docs/silver/source-profiles/assets-full-2026-07-13.json",
    source_profile_sha256=(
        "5d813c13d6e79c8da43d230b223b19e3d6aebb9846f865be1236e4299e6e48a6"
    ),
    date_start="2016-07-11",
    date_end="2026-07-09",
    expected_session_count=2_513,
    expected_manifest_count=5_026,
    expected_page_count=72_038,
    expected_input_rows=69_381_182,
    expected_manifest_bytes=51_279_874,
    expected_compressed_bytes=2_531_325_892,
    expected_raw_bytes=19_187_199_648,
    manifest_inventory_sha256=(
        "43da9c7cd2adc2a69e1badffb947807e5db04b45a627619765986b7d85bc1853"
    ),
    artifact_inventory_sha256=(
        "3a019c3a1568d16dc873bff79010b5afcbeff490779215abddb75599e7c0f11b"
    ),
    expected_output_rows_by_table={
        _OBSERVATION_TABLE: 69_381_182,
        _VERSION_TABLE: 9_706,
        _UNIVERSE_TABLE: 69_376_329,
    },
    estimated_data_bytes_by_table={
        _OBSERVATION_TABLE: 8_412_451_760,
        _VERSION_TABLE: 13_858_448,
        _UNIVERSE_TABLE: 7_729_265_031,
    },
    estimated_data_bytes_total_point=16_155_575_239,
    exchange_release_id=_EXCHANGE_RELEASE_ID,
    exchange_release_sha256=_EXCHANGE_RELEASE_SHA256,
    ticker_type_release_id=_TICKER_TYPE_RELEASE_ID,
    ticker_type_release_sha256=_TICKER_TYPE_RELEASE_SHA256,
    pyarrow_version="25.0.0",
    parquet_writer_policy=ASSET_FULL_PARQUET_WRITER_POLICY,
)


@dataclass(frozen=True, slots=True)
class AssetManifestScope:
    date_start: str
    date_end: str
    session_count: int
    manifest_count: int
    page_count: int
    input_rows: int
    manifest_bytes: int
    compressed_bytes: int
    raw_bytes: int
    manifest_inventory_sha256: str
    artifact_inventory_sha256: str
    active_pages: int
    active_rows: int
    inactive_pages: int
    inactive_rows: int
    max_session_pages: int
    max_session_page_dates: tuple[str, ...]
    max_session_rows: int
    max_session_row_dates: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "active_pages": self.active_pages,
            "active_rows": self.active_rows,
            "artifact_inventory_sha256": self.artifact_inventory_sha256,
            "compressed_bytes": self.compressed_bytes,
            "date_end": self.date_end,
            "date_start": self.date_start,
            "inactive_pages": self.inactive_pages,
            "inactive_rows": self.inactive_rows,
            "input_rows": self.input_rows,
            "manifest_bytes": self.manifest_bytes,
            "manifest_count": self.manifest_count,
            "manifest_inventory_sha256": self.manifest_inventory_sha256,
            "max_session_page_dates": list(self.max_session_page_dates),
            "max_session_pages": self.max_session_pages,
            "max_session_row_dates": list(self.max_session_row_dates),
            "max_session_rows": self.max_session_rows,
            "page_count": self.page_count,
            "raw_bytes": self.raw_bytes,
            "session_count": self.session_count,
        }


@dataclass(frozen=True, slots=True)
class AssetFullRunPreflight:
    preflight_id: str
    document: StoredDocument
    observed_free_bytes: int
    observed_project_bytes: int
    disk_status: str
    logical_scope: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class AssetTableFullRunPlanRun:
    workflow: WorkflowSnapshot
    plan: FullRunPlan
    plan_document: StoredDocument
    required_waived_qa_result_ids: tuple[str, ...]
    required_accepted_quarantine_issue_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AssetFullRunPlanRun:
    observation: AssetTableFullRunPlanRun
    version: AssetTableFullRunPlanRun
    universe: AssetTableFullRunPlanRun
    inventory: SourceInventory
    inventory_document: StoredDocument
    source_profile_path: str
    source_profile_sha256: str
    scope: AssetManifestScope
    preflight: AssetFullRunPreflight

    @property
    def table_runs(self) -> tuple[AssetTableFullRunPlanRun, ...]:
        return (self.observation, self.version, self.universe)


@dataclass(frozen=True, slots=True)
class AssetFullRunPlanApprovalRun:
    workflows_by_table: Mapping[str, WorkflowSnapshot]
    approved_plan_ids_by_table: Mapping[str, str]


def create_asset_full_run_plans(
    data_root: Path,
    *,
    repo_root: Path,
    workflow_ids: Mapping[str, str],
    expected_event_sha256_by_table: Mapping[str, str],
    source_profile_path: str,
    expected_source_profile_sha256: str,
    expected_manifest_inventory_sha256: str,
    expected_artifact_inventory_sha256: str,
    expected_input_rows: int,
    git_commit: str,
    recorded_at: str,
    actor: str = "s4-assets-full-run-plan-author",
    note: str = "",
) -> AssetFullRunPlanRun:
    """Create the exact production S4 plans and stop before approval."""

    return _create_asset_full_run_plans_authorized(
        data_root,
        repo_root=repo_root,
        workflow_ids=workflow_ids,
        expected_event_sha256_by_table=expected_event_sha256_by_table,
        source_profile_path=source_profile_path,
        expected_source_profile_sha256=expected_source_profile_sha256,
        expected_manifest_inventory_sha256=expected_manifest_inventory_sha256,
        expected_artifact_inventory_sha256=expected_artifact_inventory_sha256,
        expected_input_rows=expected_input_rows,
        git_commit=git_commit,
        recorded_at=recorded_at,
        actor=actor,
        note=note,
        authorization=CURRENT_ASSET_FULL_RUN_PLAN_AUTHORIZATION,
    )


def _create_asset_full_run_plans_authorized(
    data_root: Path,
    *,
    repo_root: Path,
    workflow_ids: Mapping[str, str],
    expected_event_sha256_by_table: Mapping[str, str],
    source_profile_path: str,
    expected_source_profile_sha256: str,
    expected_manifest_inventory_sha256: str,
    expected_artifact_inventory_sha256: str,
    expected_input_rows: int,
    git_commit: str,
    recorded_at: str,
    actor: str,
    note: str,
    authorization: AssetFullRunPlanAuthorization,
    transition_barrier: Callable[[str], None] | None = None,
    git_verifier: Callable[[Path, str], None] | None = None,
) -> AssetFullRunPlanRun:
    workflows = dict(workflow_ids)
    expected_events = dict(expected_event_sha256_by_table)
    _validate_create_request(
        workflows=workflows,
        expected_events=expected_events,
        source_profile_path=source_profile_path,
        expected_source_profile_sha256=expected_source_profile_sha256,
        expected_manifest_inventory_sha256=expected_manifest_inventory_sha256,
        expected_artifact_inventory_sha256=expected_artifact_inventory_sha256,
        expected_input_rows=expected_input_rows,
        authorization=authorization,
    )
    verifier = _verify_git_checkout if git_verifier is None else git_verifier
    verifier(repo_root, git_commit)
    if version("pyarrow") != authorization.pyarrow_version:
        raise SilverStoreError("asset full-run PyArrow version is not authorized")
    root = data_root.expanduser().resolve()
    store = SilverStore(root)
    snapshots: dict[str, WorkflowSnapshot] = {}
    previews: dict[str, tuple[BuildManifest, StoredDocument]] = {}
    for table in _TABLE_ORDER:
        # Validate the bounded preview outputs themselves before any of the three
        # workflow journals can be mutated.  On recovery this also validates the
        # already-recorded full-run plan inputs through the trust-chain walker.
        snapshot = store.verify_workflow_trust_chain(workflows[table], verify_artifacts=True)
        if snapshot.event_sha256 != expected_events[table]:
            raise SilverStoreError(f"stale asset full-run plan expected event for {table}")
        if snapshot.state not in {
            WorkflowState.AWAITING_REVIEW,
            WorkflowState.FULL_RUN_PLAN_REVIEW,
        }:
            raise SilverStoreError(
                f"asset full-run plan cannot run {table} from {snapshot.state.value}"
            )
        preview = _validate_preview_ancestry(
            store,
            table=table,
            snapshot=snapshot,
            authorization=authorization,
        )
        snapshots[table] = snapshot
        previews[table] = preview

    profile, profile_document = _load_source_profile(
        repo_root,
        source_profile_path,
        expected_source_profile_sha256,
    )
    manifest_paths = tuple(
        sorted(
            path.relative_to(root).as_posix()
            for path in (root / "manifests/massive/assets").glob("*.json")
        )
    )
    bronze_inventory = build_asset_source_inventory(
        root,
        manifest_paths=manifest_paths,
        git_commit=git_commit,
    )
    scope = _measure_manifest_scope(root, bronze_inventory)
    _validate_profile_and_scope(profile, scope, authorization=authorization)
    inventory = _bind_dependency_lineage(
        root,
        store,
        bronze_inventory,
        authorization=authorization,
    )
    preflight = _load_or_create_preflight(
        root,
        inventory=inventory,
        scope=scope,
        source_profile_path=source_profile_path,
        source_profile_sha256=profile_document.sha256,
        authorization=authorization,
    )
    inventory_document = store.register_source_inventory(inventory)
    inputs = _inventory_inputs(inventory, inventory_document)
    calendar_version = f"exchange-calendars=={version('exchange-calendars')}"
    plans = {
        table: _build_plan(
            table=table,
            workflow_id=workflows[table],
            preview=previews[table][0],
            preview_document=previews[table][1],
            reviewed_preview_event_sha256=(
                authorization.awaiting_review_event_sha256_by_table[table]
            ),
            inputs=inputs,
            inventory=inventory,
            inventory_document=inventory_document,
            git_commit=git_commit,
            calendar_version=calendar_version,
            scope=scope,
            source_profile_path=source_profile_path,
            source_profile_sha256=profile_document.sha256,
            preflight=preflight,
            authorization=authorization,
        )
        for table in _TABLE_ORDER
    }

    table_runs: dict[str, AssetTableFullRunPlanRun] = {}
    for table in _TABLE_ORDER:
        current = snapshots[table]
        expected_plan = plans[table]
        if current.state is WorkflowState.FULL_RUN_PLAN_REVIEW:
            plan, plan_document = _load_current_plan(store, table, current)
            if plan.to_dict() != expected_plan.to_dict():
                raise SilverStoreError(f"existing asset full-run plan changed for {table}")
            if current.evidence.get("full_run_plan_sha256") != plan_document.sha256:
                raise SilverStoreError(f"existing asset full-run plan SHA changed for {table}")
            _verify_plan_preflight(root, plan, authorization=authorization)
        else:
            _enforce_resource_gate(root, authorization)
            verifier(repo_root, git_commit)
            current = store.record_full_run_plan(
                expected_plan,
                expected_event_sha256=current.event_sha256,
                actor=actor,
                recorded_at=recorded_at,
                note=note,
            )
            plan, plan_document = _load_current_plan(store, table, current)
            if plan.to_dict() != expected_plan.to_dict():  # pragma: no cover - store identity
                raise SilverStoreError(f"recorded asset full-run plan changed for {table}")
            if transition_barrier is not None:
                transition_barrier(table)
        waivers, accepted = _required_preview_exceptions(previews[table][0])
        table_runs[table] = AssetTableFullRunPlanRun(
            workflow=current,
            plan=plan,
            plan_document=plan_document,
            required_waived_qa_result_ids=waivers,
            required_accepted_quarantine_issue_ids=accepted,
        )
    return AssetFullRunPlanRun(
        observation=table_runs[_OBSERVATION_TABLE],
        version=table_runs[_VERSION_TABLE],
        universe=table_runs[_UNIVERSE_TABLE],
        inventory=inventory,
        inventory_document=inventory_document,
        source_profile_path=source_profile_path,
        source_profile_sha256=profile_document.sha256,
        scope=scope,
        preflight=preflight,
    )


def approve_asset_full_run_plans(
    data_root: Path,
    *,
    workflow_ids: Mapping[str, str],
    expected_plan_ids_by_table: Mapping[str, str],
    expected_plan_sha256_by_table: Mapping[str, str],
    expected_plan_event_sha256_by_table: Mapping[str, str],
    waived_qa_result_ids_by_table: Mapping[str, tuple[str, ...]],
    accepted_quarantine_issue_ids_by_table: Mapping[str, tuple[str, ...]],
    approver: str,
    decided_at: str,
    note: str = "",
) -> AssetFullRunPlanApprovalRun:
    """Approve exact plan triples only; never start a full build or publication."""

    return _approve_asset_full_run_plans_authorized(
        data_root,
        workflow_ids=workflow_ids,
        expected_plan_ids_by_table=expected_plan_ids_by_table,
        expected_plan_sha256_by_table=expected_plan_sha256_by_table,
        expected_plan_event_sha256_by_table=expected_plan_event_sha256_by_table,
        waived_qa_result_ids_by_table=waived_qa_result_ids_by_table,
        accepted_quarantine_issue_ids_by_table=accepted_quarantine_issue_ids_by_table,
        approver=approver,
        decided_at=decided_at,
        note=note,
        authorization=CURRENT_ASSET_FULL_RUN_PLAN_AUTHORIZATION,
    )


def _approve_asset_full_run_plans_authorized(
    data_root: Path,
    *,
    workflow_ids: Mapping[str, str],
    expected_plan_ids_by_table: Mapping[str, str],
    expected_plan_sha256_by_table: Mapping[str, str],
    expected_plan_event_sha256_by_table: Mapping[str, str],
    waived_qa_result_ids_by_table: Mapping[str, tuple[str, ...]],
    accepted_quarantine_issue_ids_by_table: Mapping[str, tuple[str, ...]],
    approver: str,
    decided_at: str,
    note: str,
    authorization: AssetFullRunPlanAuthorization,
    transition_barrier: Callable[[str], None] | None = None,
) -> AssetFullRunPlanApprovalRun:
    workflows = dict(workflow_ids)
    if workflows != dict(authorization.workflow_ids_by_table):
        raise SilverStoreError("asset full-run approval workflow IDs are not authorized")
    plan_ids = _exact_digest_map(expected_plan_ids_by_table, "expected plan ID")
    plan_shas = _exact_digest_map(expected_plan_sha256_by_table, "expected plan SHA")
    event_shas = _exact_digest_map(
        expected_plan_event_sha256_by_table,
        "expected plan-review event SHA",
    )
    waivers = _exact_tuple_map(waived_qa_result_ids_by_table, "waived QA result IDs")
    accepted = _exact_tuple_map(
        accepted_quarantine_issue_ids_by_table,
        "accepted quarantine issue IDs",
    )
    root = data_root.expanduser().resolve()
    store = SilverStore(root)

    # Validate every table before writing the first approval event.
    snapshots: dict[str, WorkflowSnapshot] = {}
    plan_events: dict[str, WorkflowEventRecord] = {}
    for table in _TABLE_ORDER:
        # Approval must not trust only the embedded preview QA or plan refs.  Walk
        # and hash every referenced artifact for all three tables before writing
        # the first approval event.
        snapshot = store.verify_workflow_trust_chain(workflows[table], verify_artifacts=True)
        if snapshot.state not in {
            WorkflowState.FULL_RUN_PLAN_REVIEW,
            WorkflowState.APPROVED_FULL_RUN,
        }:
            raise SilverStoreError(
                f"asset full-run approval cannot run {table} from {snapshot.state.value}"
            )
        event = _single_event(store, snapshot.workflow_id, WorkflowState.FULL_RUN_PLAN_REVIEW)
        if event.event_sha256 != event_shas[table]:
            raise SilverStoreError(f"asset full-run plan-review event mismatch for {table}")
        if (
            event.event.evidence.get("full_run_plan_id") != plan_ids[table]
            or event.event.evidence.get("full_run_plan_sha256") != plan_shas[table]
        ):
            raise SilverStoreError(f"asset full-run expected plan triple mismatch for {table}")
        plan, document = store.load_full_run_plan(table, plan_ids[table])
        if document.sha256 != plan_shas[table]:
            raise SilverStoreError(f"asset full-run plan manifest SHA mismatch for {table}")
        _verify_plan_preflight(root, plan, authorization=authorization)
        preview, _ = store.load_build(table, plan.reviewed_preview_build_id)
        required_waivers, required_accepted = _required_preview_exceptions(preview)
        if waivers[table] != required_waivers:
            raise SilverStoreError(
                f"asset full-run approval waiver set mismatch for {table}"
            )
        if accepted[table] != required_accepted:
            raise SilverStoreError(
                f"asset full-run approval quarantine acceptance mismatch for {table}"
            )
        _validate_plan_authorization(plan, authorization=authorization)
        _verify_plan_inventory_binding(root, plan, authorization=authorization)
        if snapshot.state is WorkflowState.APPROVED_FULL_RUN:
            _validate_existing_approval(
                store,
                table=table,
                workflow_id=snapshot.workflow_id,
                plan_id=plan_ids[table],
                plan_sha256=plan_shas[table],
                plan_event_sha256=event_shas[table],
                approver=approver,
                decided_at=decided_at,
                note=note,
                waived_qa_result_ids=waivers[table],
                accepted_quarantine_issue_ids=accepted[table],
            )
        snapshots[table] = snapshot
        plan_events[table] = event

    for table in _TABLE_ORDER:
        if snapshots[table].state is WorkflowState.APPROVED_FULL_RUN:
            continue
        snapshots[table] = store.approve_full_run_plan(
            workflows[table],
            expected_event_sha256=plan_events[table].event_sha256,
            expected_plan_id=plan_ids[table],
            expected_plan_sha256=plan_shas[table],
            approver=approver,
            decided_at=decided_at,
            note=note,
            waived_qa_result_ids=waivers[table],
            accepted_quarantine_issue_ids=accepted[table],
        )
        if transition_barrier is not None:
            transition_barrier(table)
    return AssetFullRunPlanApprovalRun(
        workflows_by_table=MappingProxyType(dict(snapshots)),
        approved_plan_ids_by_table=MappingProxyType(dict(plan_ids)),
    )


def _validate_create_request(
    *,
    workflows: Mapping[str, str],
    expected_events: Mapping[str, str],
    source_profile_path: str,
    expected_source_profile_sha256: str,
    expected_manifest_inventory_sha256: str,
    expected_artifact_inventory_sha256: str,
    expected_input_rows: int,
    authorization: AssetFullRunPlanAuthorization,
) -> None:
    if workflows != dict(authorization.workflow_ids_by_table):
        raise SilverStoreError("asset full-run plan workflow IDs are not authorized")
    _exact_digest_map(expected_events, "expected current workflow event")
    if source_profile_path != authorization.source_profile_path:
        raise SilverStoreError("asset full-run source profile path is not authorized")
    if expected_source_profile_sha256 != authorization.source_profile_sha256:
        raise SilverStoreError("asset full-run source profile SHA is not authorized")
    if expected_manifest_inventory_sha256 != authorization.manifest_inventory_sha256:
        raise SilverStoreError("asset full-run manifest inventory digest is not authorized")
    if expected_artifact_inventory_sha256 != authorization.artifact_inventory_sha256:
        raise SilverStoreError("asset full-run artifact inventory digest is not authorized")
    if expected_input_rows != authorization.expected_input_rows:
        raise SilverStoreError("asset full-run expected input rows are not authorized")


def _validate_preview_ancestry(
    store: SilverStore,
    *,
    table: str,
    snapshot: WorkflowSnapshot,
    authorization: AssetFullRunPlanAuthorization,
) -> tuple[BuildManifest, StoredDocument]:
    if snapshot.workflow_id != authorization.workflow_ids_by_table[table]:
        raise SilverStoreError(f"asset full-run workflow identity changed for {table}")
    contract, _ = store.load_workflow_contract(snapshot.workflow_id)
    if contract != _CONTRACTS_BY_TABLE[table]:
        raise SilverStoreError(f"asset full-run contract changed for {table}")
    preview_event = _single_event(store, snapshot.workflow_id, WorkflowState.PREVIEW_READY)
    review_event = _single_event(store, snapshot.workflow_id, WorkflowState.AWAITING_REVIEW)
    if review_event.event_sha256 != authorization.awaiting_review_event_sha256_by_table[table]:
        raise SilverStoreError(f"asset full-run awaiting-review ancestry changed for {table}")
    build_id = authorization.preview_build_ids_by_table[table]
    if preview_event.event.evidence.get("build_id") != build_id:
        raise SilverStoreError(f"asset full-run preview build ID changed for {table}")
    build, document = store.load_build(table, build_id)
    if (
        document.sha256 != authorization.preview_manifest_sha256_by_table[table]
        or preview_event.event.evidence.get("build_manifest_sha256") != document.sha256
        or build.intent.kind is not BuildKind.PREVIEW
    ):
        raise SilverStoreError(f"asset full-run preview manifest changed for {table}")
    if (
        build.intent.parameters.get("full_run_scope_policy")
        != SEPARATE_FULL_RUN_PLAN_POLICY
        or build.preview is None
        or build.preview.full_run_projection.get("scope_binding_mode")
        != SEPARATE_FULL_RUN_PLAN_POLICY
    ):
        raise SilverStoreError(f"asset full-run preview lacks separate plan policy for {table}")
    return build, document


def _load_source_profile(
    repo_root: Path,
    relative_path: str,
    expected_sha256: str,
) -> tuple[dict[str, object], StoredDocument]:
    root = repo_root.expanduser().resolve()
    try:
        path = safe_relative_path(root, relative_path)
    except ArtifactError as exc:
        raise SilverStoreError("asset full-run source profile path is unsafe") from exc
    if not path.is_file():
        raise SilverStoreError("asset full-run source profile is not a regular file")
    try:
        tracked = _git_output(
            root,
            "ls-files",
            "--error-unmatch",
            "--",
            relative_path,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SilverStoreError("cannot verify versioned asset source profile") from exc
    if tracked != relative_path:
        raise SilverStoreError("asset full-run source profile is not versioned")
    content = path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if digest != expected_sha256:
        raise SilverStoreError("asset full-run source profile checksum mismatch")
    try:
        document = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SilverStoreError("asset full-run source profile is invalid JSON") from exc
    if not isinstance(document, dict):
        raise SilverStoreError("asset full-run source profile must be an object")
    return document, StoredDocument(relative_path, digest, len(content))


def _measure_manifest_scope(root: Path, inventory: SourceInventory) -> AssetManifestScope:
    inventory_items = {item.path: item for item in inventory.artifacts}
    observed_artifacts: set[str] = set()
    manifest_lines: list[str] = []
    artifact_lines: list[str] = []
    by_session_pages: defaultdict[str, int] = defaultdict(int)
    by_session_rows: defaultdict[str, int] = defaultdict(int)
    active_pages = active_rows = inactive_pages = inactive_rows = 0
    manifest_bytes = raw_bytes = 0
    session_scopes: defaultdict[str, set[bool]] = defaultdict(set)
    for upstream in sorted(inventory.upstream_manifests, key=lambda item: item.path):
        path = safe_relative_path(root, upstream.path)
        content = path.read_bytes()
        digest = hashlib.sha256(content).hexdigest()
        if digest != upstream.sha256:
            raise SilverStoreError("asset full-run manifest checksum changed")
        try:
            document = json.loads(content)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SilverStoreError("asset full-run manifest is invalid JSON") from exc
        if not isinstance(document, dict):
            raise SilverStoreError("asset full-run manifest must be an object")
        request_id = document.get("request_id")
        request = document.get("request")
        parameters = request.get("parameters") if isinstance(request, dict) else None
        active_text = parameters.get("active") if isinstance(parameters, dict) else None
        session_date = request.get("start") if isinstance(request, dict) else None
        if (
            not isinstance(request_id, str)
            or path.name != f"{request_id}.json"
            or active_text not in {"true", "false"}
            or not isinstance(session_date, str)
            or request.get("end") != session_date
        ):
            raise SilverStoreError("asset full-run manifest scope metadata changed")
        requested_active = active_text == "true"
        if requested_active in session_scopes[session_date]:
            raise SilverStoreError("asset full-run session has duplicate active scope")
        session_scopes[session_date].add(requested_active)
        manifest_bytes += len(content)
        manifest_lines.append(
            f"{request_id}\t{upstream.path}\t{len(content)}\t{digest}\n"
        )
        artifacts = document.get("artifacts")
        if not isinstance(artifacts, list):
            raise SilverStoreError("asset full-run manifest artifacts changed")
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                raise SilverStoreError("asset full-run manifest artifact is not an object")
            artifact_path = artifact.get("path")
            item = inventory_items.get(str(artifact_path))
            if item is None or item.path in observed_artifacts:
                raise SilverStoreError("asset full-run inventory/artifact membership changed")
            observed_artifacts.add(item.path)
            raw_sha = artifact.get("raw_sha256")
            raw_size = artifact.get("raw_bytes")
            if not isinstance(raw_sha, str) or type(raw_size) is not int or raw_size < 0:
                raise SilverStoreError("asset full-run raw page metadata changed")
            if (
                artifact.get("stored_sha256") != item.sha256
                or artifact.get("compressed_bytes") != item.bytes
                or artifact.get("record_count") != item.row_count
            ):
                raise SilverStoreError("asset full-run page metadata differs from inventory")
            raw_bytes += raw_size
            by_session_pages[session_date] += 1
            by_session_rows[session_date] += item.row_count
            artifact_lines.append(
                f"{item.path}\t{item.sha256}\t{raw_sha}\t{item.bytes}\t"
                f"{raw_size}\t{item.row_count}\n"
            )
            if requested_active:
                active_pages += 1
                active_rows += item.row_count
            else:
                inactive_pages += 1
                inactive_rows += item.row_count
    if observed_artifacts != set(inventory_items):
        raise SilverStoreError("asset full-run inventory contains undeclared artifacts")
    if not session_scopes or any(scopes != {False, True} for scopes in session_scopes.values()):
        raise SilverStoreError("asset full-run sessions are not exact active/inactive pairs")
    max_pages = max(by_session_pages.values())
    max_rows = max(by_session_rows.values())
    return AssetManifestScope(
        date_start=min(session_scopes),
        date_end=max(session_scopes),
        session_count=len(session_scopes),
        manifest_count=len(inventory.upstream_manifests),
        page_count=len(inventory.artifacts),
        input_rows=sum(item.row_count for item in inventory.artifacts),
        manifest_bytes=manifest_bytes,
        compressed_bytes=sum(item.bytes for item in inventory.artifacts),
        raw_bytes=raw_bytes,
        manifest_inventory_sha256=_digest_lines(manifest_lines),
        artifact_inventory_sha256=_digest_lines(artifact_lines),
        active_pages=active_pages,
        active_rows=active_rows,
        inactive_pages=inactive_pages,
        inactive_rows=inactive_rows,
        max_session_pages=max_pages,
        max_session_page_dates=tuple(
            sorted(date for date, count in by_session_pages.items() if count == max_pages)
        ),
        max_session_rows=max_rows,
        max_session_row_dates=tuple(
            sorted(date for date, count in by_session_rows.items() if count == max_rows)
        ),
    )


def _validate_profile_and_scope(
    profile: Mapping[str, object],
    scope: AssetManifestScope,
    *,
    authorization: AssetFullRunPlanAuthorization,
) -> None:
    authoritative = _mapping(profile.get("authoritative_inputs"), "authoritative_inputs")
    manifest_profile = _mapping(profile.get("manifest_profile"), "manifest_profile")
    row_funnel = _mapping(profile.get("row_funnel"), "row_funnel")
    hard_gates = _mapping(profile.get("hard_gate_numerators"), "hard_gate_numerators")
    write_boundary = _mapping(profile.get("write_boundary"), "write_boundary")
    expected_authoritative = {
        "artifact_count": scope.page_count,
        "artifact_inventory_digest": scope.artifact_inventory_sha256,
        "date_end": scope.date_end,
        "date_start": scope.date_start,
        "manifest_count": scope.manifest_count,
        "manifest_inventory_digest": scope.manifest_inventory_sha256,
        "session_count": scope.session_count,
    }
    for key, expected in expected_authoritative.items():
        if authoritative.get(key) != expected:
            raise SilverStoreError(f"asset full-run source profile {key} mismatch")
    if (
        profile.get("profile_summary_schema_version") != 1
        or profile.get("total_pages") != scope.page_count
        or profile.get("total_rows") != scope.input_rows
        or manifest_profile.get("complete_manifests") != scope.manifest_count
        or manifest_profile.get("failed_or_in_progress_manifests") != 0
        or manifest_profile.get("missing_active_inactive_session_pairs") != 0
        or manifest_profile.get("active_pages") != scope.active_pages
        or manifest_profile.get("active_rows") != scope.active_rows
        or manifest_profile.get("inactive_pages") != scope.inactive_pages
        or manifest_profile.get("inactive_rows") != scope.inactive_rows
        or row_funnel.get("source_rows") != scope.input_rows
        or row_funnel.get("accepted_observation_rows") != scope.input_rows
        or row_funnel.get("version_member_rows")
        != authorization.expected_output_rows_by_table[_VERSION_TABLE]
        or row_funnel.get("expected_universe_rows")
        != authorization.expected_output_rows_by_table[_UNIVERSE_TABLE]
    ):
        raise SilverStoreError("asset full-run source profile totals changed")
    if any(type(value) is not int or value != 0 for value in hard_gates.values()):
        raise SilverStoreError("asset full-run source profile hard gates are not all zero")
    audit_status = _mapping(authoritative.get("bronze_audit_status"), "bronze_audit_status")
    if audit_status != {"authoritative_plan": "passed", "physical_integrity": "passed"}:
        raise SilverStoreError("asset full-run Bronze audit status is not passed")
    if (
        write_boundary.get("bronze_or_manifest_mtime_changes_after_profile_start") != 0
        or write_boundary.get("profile_artifact_written_to_data_root") is not False
    ):
        raise SilverStoreError("asset full-run source profile write boundary changed")
    expected_scope = {
        "date_start": authorization.date_start,
        "date_end": authorization.date_end,
        "session_count": authorization.expected_session_count,
        "manifest_count": authorization.expected_manifest_count,
        "page_count": authorization.expected_page_count,
        "input_rows": authorization.expected_input_rows,
        "manifest_bytes": authorization.expected_manifest_bytes,
        "compressed_bytes": authorization.expected_compressed_bytes,
        "raw_bytes": authorization.expected_raw_bytes,
        "manifest_inventory_sha256": authorization.manifest_inventory_sha256,
        "artifact_inventory_sha256": authorization.artifact_inventory_sha256,
    }
    actual = scope.to_dict()
    for key, expected in expected_scope.items():
        if actual[key] != expected:
            raise SilverStoreError(f"asset full-run measured scope {key} mismatch")


def _bind_dependency_lineage(
    root: Path,
    store: SilverStore,
    inventory: SourceInventory,
    *,
    authorization: AssetFullRunPlanAuthorization,
) -> SourceInventory:
    if not authorization.dependency_lineage_required:
        return inventory
    dependencies = (
        (
            authorization.exchange_release_id,
            authorization.exchange_release_sha256,
            EXCHANGE_DIM_CONTRACT,
        ),
        (
            authorization.ticker_type_release_id,
            authorization.ticker_type_release_sha256,
            TICKER_TYPE_DIM_CONTRACT,
        ),
    )
    refs: list[UpstreamManifestRef] = []
    reader = PublishedSilverReader(root)
    for release_id, expected_sha, contract in dependencies:
        release, document = store.load_release(release_id)
        if document.sha256 != expected_sha:
            raise SilverStoreError("asset full-run dependency release SHA changed")
        if reader.inspect(release.release_id).contract != contract:
            raise SilverStoreError("asset full-run dependency contract changed")
        refs.append(UpstreamManifestRef(path=document.path, sha256=document.sha256))
    existing_paths = {item.path for item in inventory.upstream_manifests}
    if any(item.path in existing_paths for item in refs):
        raise SilverStoreError("asset full-run dependency path collides with Bronze")
    return replace(
        inventory,
        upstream_manifests=tuple(
            sorted((*inventory.upstream_manifests, *refs), key=lambda item: item.path)
        ),
    )


def _load_or_create_preflight(
    root: Path,
    *,
    inventory: SourceInventory,
    scope: AssetManifestScope,
    source_profile_path: str,
    source_profile_sha256: str,
    authorization: AssetFullRunPlanAuthorization,
) -> AssetFullRunPreflight:
    resource_policy = _resource_policy(authorization)
    logical_scope = {
        "git_commit": inventory.git_commit,
        "inventory_id": inventory.inventory_id,
        "resource_policy": resource_policy,
        "scope": scope.to_dict(),
        "source_profile_path": source_profile_path,
        "source_profile_sha256": source_profile_sha256,
    }
    preflight_id = stable_digest(
        {
            "asset_full_run_preflight_version": ASSET_FULL_RUN_PREFLIGHT_VERSION,
            "logical_scope": logical_scope,
        }
    )
    relative_path = (
        "manifests/silver/full-run-plan-preflights/assets/"
        f"preflight_id={preflight_id}/manifest.json"
    )
    path = safe_relative_path(root, relative_path)
    if path.exists():
        document, stored = _read_preflight(root, relative_path)
        if document.get("preflight_id") != preflight_id:
            raise SilverStoreError("asset full-run preflight ID changed")
        if document.get("logical_scope") != logical_scope:
            raise SilverStoreError("asset full-run preflight logical scope changed")
        observed_free = document.get("observed_free_bytes")
        observed_project = document.get("observed_project_bytes")
        disk_status = document.get("disk_status")
        if type(observed_free) is not int or type(observed_project) is not int:
            raise SilverStoreError("asset full-run preflight free-space evidence is invalid")
        _enforce_resource_values(observed_free, observed_project, authorization)
        if disk_status not in {"ok", "warning_below_60_gib"}:
            raise SilverStoreError("asset full-run preflight disk status is invalid")
        return AssetFullRunPreflight(
            preflight_id=preflight_id,
            document=stored,
            observed_free_bytes=observed_free,
            observed_project_bytes=observed_project,
            disk_status=str(disk_status),
            logical_scope=MappingProxyType(logical_scope),
        )
    observed_free, observed_project = _enforce_resource_gate(root, authorization)
    disk_status = (
        "warning_below_60_gib"
        if observed_free < authorization.free_space_warning_bytes
        else "ok"
    )
    document = {
        "asset_full_run_preflight_version": ASSET_FULL_RUN_PREFLIGHT_VERSION,
        "disk_status": disk_status,
        "logical_scope": logical_scope,
        "observed_free_bytes": observed_free,
        "observed_project_bytes": observed_project,
        "preflight_id": preflight_id,
    }
    content = _json_bytes(document)
    stored_raw = write_bytes_immutable(root, path, content)
    stored = StoredDocument(
        path=str(stored_raw["path"]),
        sha256=str(stored_raw["sha256"]),
        bytes=int(stored_raw["bytes"]),
    )
    return AssetFullRunPreflight(
        preflight_id=preflight_id,
        document=stored,
        observed_free_bytes=observed_free,
        observed_project_bytes=observed_project,
        disk_status=disk_status,
        logical_scope=MappingProxyType(logical_scope),
    )


def _build_plan(
    *,
    table: str,
    workflow_id: str,
    preview: BuildManifest,
    preview_document: StoredDocument,
    reviewed_preview_event_sha256: str,
    inputs: tuple[ArtifactRef, ...],
    inventory: SourceInventory,
    inventory_document: StoredDocument,
    git_commit: str,
    calendar_version: str,
    scope: AssetManifestScope,
    source_profile_path: str,
    source_profile_sha256: str,
    preflight: AssetFullRunPreflight,
    authorization: AssetFullRunPlanAuthorization,
) -> FullRunPlan:
    contract = _CONTRACTS_BY_TABLE[table]
    expected_output_rows = authorization.expected_output_rows_by_table[table]
    parameters = {
        "asset_full_run_plan_policy_version": ASSET_FULL_RUN_PLAN_POLICY_VERSION,
        "asset_metadata_time_scope": ASSET_METADATA_TIME_SCOPE,
        "asset_reference_time_scope": ASSET_REFERENCE_TIME_SCOPE,
        "asset_source_availability_quality": ASSET_SOURCE_AVAILABILITY_QUALITY,
        "asset_source_availability_rule": ASSET_SOURCE_AVAILABILITY_RULE,
        "asset_version_selection_rule": ASSET_VERSION_SELECTION_RULE,
        "calendar_name": "XNYS",
        "date_end": scope.date_end,
        "date_start": scope.date_start,
        "expected_input_rows": scope.input_rows,
        "expected_output_rows": expected_output_rows,
        "exchange_release_id": authorization.exchange_release_id,
        "exchange_release_sha256": authorization.exchange_release_sha256,
        "full_run_scope_policy": SEPARATE_FULL_RUN_PLAN_POLICY,
        "input_artifact_inventory_sha256": scope.artifact_inventory_sha256,
        "input_compressed_bytes": scope.compressed_bytes,
        "input_manifest_bytes": scope.manifest_bytes,
        "input_manifest_count": scope.manifest_count,
        "input_manifest_inventory_sha256": scope.manifest_inventory_sha256,
        "input_page_count": scope.page_count,
        "input_raw_bytes": scope.raw_bytes,
        "input_session_count": scope.session_count,
        "max_in_flight_sessions": authorization.max_in_flight_sessions,
        "parquet_writer_policy": dict(authorization.parquet_writer_policy),
        "preflight_id": preflight.preflight_id,
        "preflight_manifest_path": preflight.document.path,
        "preflight_manifest_sha256": preflight.document.sha256,
        "source_inventory_id": inventory.inventory_id,
        "source_inventory_manifest_path": inventory_document.path,
        "source_inventory_manifest_sha256": inventory_document.sha256,
        "source_profile_path": source_profile_path,
        "source_profile_sha256": source_profile_sha256,
        "ticker_type_release_id": authorization.ticker_type_release_id,
        "ticker_type_release_sha256": authorization.ticker_type_release_sha256,
        "pyarrow_version": authorization.pyarrow_version,
        "universe_source_availability_rule": UNIVERSE_SOURCE_AVAILABILITY_RULE,
        "workers": authorization.workers,
    }
    resource_projection = {
        **_resource_policy(authorization),
        "disk_free_bytes_at_plan_time": preflight.observed_free_bytes,
        "project_bytes_at_plan_time": preflight.observed_project_bytes,
        "disk_status_at_plan_time": preflight.disk_status,
        "expected_input_rows": scope.input_rows,
        "expected_output_rows": expected_output_rows,
        "input_compressed_bytes": scope.compressed_bytes,
        "input_manifest_bytes": scope.manifest_bytes,
        "input_raw_bytes": scope.raw_bytes,
        "max_session_page_dates": list(scope.max_session_page_dates),
        "max_session_pages": scope.max_session_pages,
        "max_session_row_dates": list(scope.max_session_row_dates),
        "max_session_rows": scope.max_session_rows,
        "preflight_id": preflight.preflight_id,
        "preflight_manifest_path": preflight.document.path,
        "preflight_manifest_sha256": preflight.document.sha256,
        "projection_basis": "2026-05-11 bounded preview plus full manifest metadata",
        "source_profile_path": source_profile_path,
        "source_profile_sha256": source_profile_sha256,
    }
    return FullRunPlan(
        workflow_id=workflow_id,
        domain=contract.domain,
        table=table,
        schema_version=contract.schema_version,
        contract_id=contract.contract_id,
        reviewed_preview_build_id=preview.build_id,
        reviewed_preview_manifest_sha256=preview_document.sha256,
        reviewed_preview_event_sha256=reviewed_preview_event_sha256,
        transform_version=ASSET_TRANSFORM_VERSION,
        git_commit=git_commit,
        exchange_calendar_version=calendar_version,
        inputs=inputs,
        parameters=parameters,
        resource_projection=resource_projection,
    )


def _resource_policy(authorization: AssetFullRunPlanAuthorization) -> dict[str, object]:
    conservative_stable = authorization.stable_output_cap_bytes
    conservative_peak = authorization.peak_incremental_cap_bytes
    if (
        authorization.estimated_data_bytes_total_point > conservative_stable
        or conservative_stable > authorization.stable_output_cap_bytes
        or conservative_peak > authorization.peak_incremental_cap_bytes
    ):
        raise SilverStoreError("asset full-run resource estimate exceeds its approved cap")
    return {
        "conservative_peak_incremental_bytes": conservative_peak,
        "conservative_stable_output_bytes": conservative_stable,
        "estimated_data_bytes_by_table": dict(
            authorization.estimated_data_bytes_by_table
        ),
        "estimated_data_bytes_total_point": (
            authorization.estimated_data_bytes_total_point
        ),
        "estimate_basis": (
            "2026-05-11 nonempty Parquet payload bytes scaled to full output rows, "
            "plus 2,513 measured empty-partition footer floors and the reviewed "
            "QA/quarantine artifacts"
        ),
        "expected_rss_ceiling_bytes": authorization.expected_rss_ceiling_bytes,
        "free_space_floor_bytes": authorization.free_space_floor_bytes,
        "free_space_warning_bytes": authorization.free_space_warning_bytes,
        "hard_rss_limit_bytes": authorization.hard_rss_limit_bytes,
        "host_memory_basis": "7.57_GiB_RAM_no_swap",
        "max_in_flight_sessions": authorization.max_in_flight_sessions,
        "peak_incremental_cap_bytes": authorization.peak_incremental_cap_bytes,
        "peak_project_cap_bytes": authorization.peak_project_cap_bytes,
        "runtime_estimate_seconds": authorization.runtime_estimate_seconds,
        "runtime_review_ceiling_seconds": authorization.runtime_review_ceiling_seconds,
        "stable_output_cap_bytes": authorization.stable_output_cap_bytes,
        "stable_project_cap_bytes": authorization.stable_project_cap_bytes,
        "workers": authorization.workers,
    }


def _enforce_resource_gate(
    root: Path,
    authorization: AssetFullRunPlanAuthorization,
) -> tuple[int, int]:
    observed_free = shutil.disk_usage(root).free
    observed_project = _project_file_bytes(root)
    _enforce_resource_values(observed_free, observed_project, authorization)
    return observed_free, observed_project


def _enforce_resource_values(
    observed_free: int,
    observed_project: int,
    authorization: AssetFullRunPlanAuthorization,
) -> None:
    if observed_free - authorization.peak_incremental_cap_bytes < (
        authorization.free_space_floor_bytes
    ):
        raise SilverStoreError(
            "asset full-run projected peak would breach the free-space floor"
        )
    if observed_project + authorization.stable_output_cap_bytes > (
        authorization.stable_project_cap_bytes
    ):
        raise SilverStoreError(
            "asset full-run projected stable project size exceeds its cap"
        )
    if observed_project + authorization.peak_incremental_cap_bytes > (
        authorization.peak_project_cap_bytes
    ):
        raise SilverStoreError(
            "asset full-run projected peak project size exceeds its cap"
        )


def _project_file_bytes(root: Path) -> int:
    total = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_symlink():
                    raise SilverStoreError(
                        f"asset full-run project contains a symlink: {entry.path}"
                    )
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
                else:
                    raise SilverStoreError(
                        f"asset full-run project contains a non-regular entry: {entry.path}"
                    )
    return total


def _inventory_inputs(
    inventory: SourceInventory,
    document: StoredDocument,
) -> tuple[ArtifactRef, ...]:
    return tuple(
        ArtifactRef(
            path=item.path,
            sha256=item.sha256,
            bytes=item.bytes,
            row_count=item.row_count,
            media_type=item.media_type,
            role=ArtifactRole.SOURCE,
            source_dataset=inventory.source_dataset,
            source_layer=inventory.source_layer,
            lineage_manifest_path=document.path,
            lineage_manifest_sha256=document.sha256,
        )
        for item in sorted(inventory.artifacts, key=lambda artifact: artifact.path)
    )


def _load_current_plan(
    store: SilverStore,
    table: str,
    snapshot: WorkflowSnapshot,
) -> tuple[FullRunPlan, StoredDocument]:
    plan_id = snapshot.evidence.get("full_run_plan_id")
    if not isinstance(plan_id, str):
        raise SilverStoreError(f"asset full-run plan event lacks plan ID for {table}")
    plan, document = store.load_full_run_plan(table, plan_id)
    if (
        snapshot.evidence.get("full_run_plan_path") != document.path
        or snapshot.evidence.get("full_run_plan_sha256") != document.sha256
    ):
        raise SilverStoreError(f"asset full-run plan event evidence changed for {table}")
    return plan, document


def _required_preview_exceptions(
    preview: BuildManifest,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    waivers = tuple(
        sorted(
            check.result_id
            for check in preview.qa_checks
            if check.status is QAStatus.WARNING
            or (
                check.status is QAStatus.FAILED
                and check.severity in {QASeverity.MEDIUM, QASeverity.LOW}
            )
        )
    )
    accepted = tuple(sorted(preview.quarantine_issue_ids_by_severity[QASeverity.HIGH.value]))
    return waivers, accepted


def _verify_plan_preflight(
    root: Path,
    plan: FullRunPlan,
    *,
    authorization: AssetFullRunPlanAuthorization,
) -> None:
    path = plan.parameters.get("preflight_manifest_path")
    expected_sha = plan.parameters.get("preflight_manifest_sha256")
    expected_id = plan.parameters.get("preflight_id")
    if not all(isinstance(item, str) for item in (path, expected_sha, expected_id)):
        raise SilverStoreError("asset full-run plan preflight binding is missing")
    document, stored = _read_preflight(root, str(path))
    logical_scope = document.get("logical_scope")
    if not isinstance(logical_scope, Mapping):
        raise SilverStoreError("asset full-run preflight logical scope is invalid")
    derived_id = stable_digest(
        {
            "asset_full_run_preflight_version": ASSET_FULL_RUN_PREFLIGHT_VERSION,
            "logical_scope": dict(logical_scope),
        }
    )
    canonical_path = (
        "manifests/silver/full-run-plan-preflights/assets/"
        f"preflight_id={derived_id}/manifest.json"
    )
    if (
        stored.path != canonical_path
        or stored.sha256 != expected_sha
        or document.get("preflight_id") != expected_id
        or expected_id != derived_id
        or document.get("asset_full_run_preflight_version")
        != ASSET_FULL_RUN_PREFLIGHT_VERSION
    ):
        raise SilverStoreError("asset full-run plan preflight binding changed")
    observed_free = document.get("observed_free_bytes")
    observed_project = document.get("observed_project_bytes")
    disk_status = document.get("disk_status")
    if (
        type(observed_free) is not int
        or observed_free < 0
        or type(observed_project) is not int
        or observed_project < 0
        or disk_status not in {"ok", "warning_below_60_gib"}
    ):
        raise SilverStoreError("asset full-run preflight resource evidence is invalid")
    _enforce_resource_values(observed_free, observed_project, authorization)
    expected_disk_status = (
        "warning_below_60_gib"
        if observed_free < authorization.free_space_warning_bytes
        else "ok"
    )
    if disk_status != expected_disk_status:
        raise SilverStoreError("asset full-run preflight disk status changed")
    scope = logical_scope.get("scope")
    if not isinstance(scope, Mapping):
        raise SilverStoreError("asset full-run preflight measured scope is invalid")
    expected_scope = {
        "artifact_inventory_sha256": authorization.artifact_inventory_sha256,
        "compressed_bytes": authorization.expected_compressed_bytes,
        "date_end": authorization.date_end,
        "date_start": authorization.date_start,
        "input_rows": authorization.expected_input_rows,
        "manifest_bytes": authorization.expected_manifest_bytes,
        "manifest_count": authorization.expected_manifest_count,
        "manifest_inventory_sha256": authorization.manifest_inventory_sha256,
        "page_count": authorization.expected_page_count,
        "raw_bytes": authorization.expected_raw_bytes,
        "session_count": authorization.expected_session_count,
    }
    parameters = plan.parameters
    if (
        logical_scope.get("git_commit") != plan.git_commit
        or logical_scope.get("inventory_id") != parameters.get("source_inventory_id")
        or logical_scope.get("resource_policy") != _resource_policy(authorization)
        or logical_scope.get("source_profile_path")
        != authorization.source_profile_path
        or logical_scope.get("source_profile_sha256")
        != authorization.source_profile_sha256
        or any(scope.get(key) != value for key, value in expected_scope.items())
    ):
        raise SilverStoreError("asset full-run preflight authorized scope changed")
    max_page_dates = scope.get("max_session_page_dates")
    max_row_dates = scope.get("max_session_row_dates")
    if (
        not isinstance(max_page_dates, list)
        or not max_page_dates
        or any(not isinstance(item, str) for item in max_page_dates)
        or not isinstance(max_row_dates, list)
        or not max_row_dates
        or any(not isinstance(item, str) for item in max_row_dates)
    ):
        raise SilverStoreError("asset full-run preflight maximum-session dates are invalid")
    projection = plan.resource_projection
    if (
        projection.get("preflight_id") != expected_id
        or projection.get("preflight_manifest_path") != path
        or projection.get("preflight_manifest_sha256") != expected_sha
        or projection.get("disk_free_bytes_at_plan_time")
        != document.get("observed_free_bytes")
        or projection.get("project_bytes_at_plan_time")
        != observed_project
        or projection.get("max_session_pages") != scope.get("max_session_pages")
        or projection.get("max_session_page_dates")
        != tuple(max_page_dates)
        or projection.get("max_session_rows") != scope.get("max_session_rows")
        or projection.get("max_session_row_dates") != tuple(max_row_dates)
    ):
        raise SilverStoreError("asset full-run plan/preflight projection changed")


def _validate_plan_authorization(
    plan: FullRunPlan,
    *,
    authorization: AssetFullRunPlanAuthorization,
) -> None:
    table = plan.table
    contract = _CONTRACTS_BY_TABLE.get(table)
    parameters = plan.parameters
    projection = plan.resource_projection
    expected_projection = _resource_policy(authorization)
    expected_calendar_version = f"exchange-calendars=={version('exchange-calendars')}"
    if (
        contract is None
        or plan.workflow_id != authorization.workflow_ids_by_table[table]
        or plan.domain != contract.domain
        or plan.schema_version != contract.schema_version
        or plan.contract_id != contract.contract_id
        or plan.transform_version != ASSET_TRANSFORM_VERSION
        or plan.exchange_calendar_version != expected_calendar_version
        or plan.reviewed_preview_build_id
        != authorization.preview_build_ids_by_table[table]
        or plan.reviewed_preview_manifest_sha256
        != authorization.preview_manifest_sha256_by_table[table]
        or plan.reviewed_preview_event_sha256
        != authorization.awaiting_review_event_sha256_by_table[table]
        or parameters.get("source_profile_path") != authorization.source_profile_path
        or parameters.get("source_profile_sha256") != authorization.source_profile_sha256
        or parameters.get("asset_full_run_plan_policy_version")
        != ASSET_FULL_RUN_PLAN_POLICY_VERSION
        or parameters.get("asset_metadata_time_scope") != ASSET_METADATA_TIME_SCOPE
        or parameters.get("asset_reference_time_scope") != ASSET_REFERENCE_TIME_SCOPE
        or parameters.get("asset_source_availability_quality")
        != ASSET_SOURCE_AVAILABILITY_QUALITY
        or parameters.get("asset_source_availability_rule")
        != ASSET_SOURCE_AVAILABILITY_RULE
        or parameters.get("asset_version_selection_rule")
        != ASSET_VERSION_SELECTION_RULE
        or parameters.get("universe_source_availability_rule")
        != UNIVERSE_SOURCE_AVAILABILITY_RULE
        or parameters.get("calendar_name") != "XNYS"
        or parameters.get("full_run_scope_policy")
        != SEPARATE_FULL_RUN_PLAN_POLICY
        or parameters.get("input_manifest_inventory_sha256")
        != authorization.manifest_inventory_sha256
        or parameters.get("input_artifact_inventory_sha256")
        != authorization.artifact_inventory_sha256
        or parameters.get("expected_input_rows") != authorization.expected_input_rows
        or parameters.get("expected_output_rows")
        != authorization.expected_output_rows_by_table[table]
        or parameters.get("exchange_release_id") != authorization.exchange_release_id
        or parameters.get("exchange_release_sha256")
        != authorization.exchange_release_sha256
        or parameters.get("ticker_type_release_id")
        != authorization.ticker_type_release_id
        or parameters.get("ticker_type_release_sha256")
        != authorization.ticker_type_release_sha256
        or parameters.get("date_start") != authorization.date_start
        or parameters.get("date_end") != authorization.date_end
        or parameters.get("input_session_count")
        != authorization.expected_session_count
        or parameters.get("input_manifest_count")
        != authorization.expected_manifest_count
        or parameters.get("input_page_count") != authorization.expected_page_count
        or parameters.get("input_manifest_bytes")
        != authorization.expected_manifest_bytes
        or parameters.get("input_compressed_bytes")
        != authorization.expected_compressed_bytes
        or parameters.get("input_raw_bytes") != authorization.expected_raw_bytes
        or parameters.get("workers") != authorization.workers
        or parameters.get("max_in_flight_sessions")
        != authorization.max_in_flight_sessions
        or parameters.get("pyarrow_version") != authorization.pyarrow_version
        or parameters.get("parquet_writer_policy")
        != authorization.parquet_writer_policy
        or version("pyarrow") != authorization.pyarrow_version
        or any(projection.get(key) != value for key, value in expected_projection.items())
    ):
        raise SilverStoreError(f"asset full-run approved plan scope changed for {table}")


def _verify_plan_inventory_binding(
    root: Path,
    plan: FullRunPlan,
    *,
    authorization: AssetFullRunPlanAuthorization,
) -> None:
    parameters = plan.parameters
    inventory_id = parameters.get("source_inventory_id")
    relative_path = parameters.get("source_inventory_manifest_path")
    expected_sha = parameters.get("source_inventory_manifest_sha256")
    if not all(
        isinstance(value, str)
        for value in (inventory_id, relative_path, expected_sha)
    ):
        raise SilverStoreError("asset full-run plan source inventory binding is missing")
    canonical_path = (
        "manifests/silver/source-inventories/assets/"
        f"inventory-{inventory_id}.json"
    )
    if relative_path != canonical_path:
        raise SilverStoreError("asset full-run source inventory path is not canonical")
    path = safe_relative_path(root, str(relative_path))
    if path.is_symlink() or not path.is_file():
        raise SilverStoreError("asset full-run source inventory is not a regular file")
    if path.stat().st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
        raise SilverStoreError("asset full-run source inventory remains writable")
    content = path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if digest != expected_sha:
        raise SilverStoreError("asset full-run source inventory SHA changed")
    try:
        document = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SilverStoreError("asset full-run source inventory is invalid JSON") from exc
    inventory = SourceInventory.from_dict(document)
    if (
        inventory.inventory_id != inventory_id
        or inventory.source_dataset != "assets"
        or inventory.source_layer is not SourceLayer.BRONZE
        or inventory.git_commit != plan.git_commit
    ):
        raise SilverStoreError("asset full-run source inventory identity changed")
    stored = StoredDocument(str(relative_path), digest, len(content))
    expected_inputs = _inventory_inputs(inventory, stored)
    if plan.inputs != expected_inputs:
        raise SilverStoreError(
            "asset full-run plan inputs are not the complete canonical source inventory"
        )
    if (
        plan.input_artifact_count != authorization.expected_page_count
        or plan.input_rows != authorization.expected_input_rows
        or plan.input_bytes != authorization.expected_compressed_bytes
    ):
        raise SilverStoreError("asset full-run plan input totals changed")
    bronze_manifests = tuple(
        item
        for item in inventory.upstream_manifests
        if item.path.startswith("manifests/massive/assets/")
    )
    dependency_manifests = tuple(
        item for item in inventory.upstream_manifests if item not in bronze_manifests
    )
    if len(bronze_manifests) != authorization.expected_manifest_count:
        raise SilverStoreError("asset full-run source inventory manifest count changed")
    expected_dependency_shas = (
        {
            authorization.exchange_release_sha256,
            authorization.ticker_type_release_sha256,
        }
        if authorization.dependency_lineage_required
        else set()
    )
    if (
        len(dependency_manifests) != len(expected_dependency_shas)
        or {item.sha256 for item in dependency_manifests} != expected_dependency_shas
    ):
        raise SilverStoreError("asset full-run source inventory dependency lineage changed")
    measured_scope = _measure_manifest_scope(
        root,
        replace(inventory, upstream_manifests=bronze_manifests),
    )
    expected_scope = {
        "artifact_inventory_sha256": authorization.artifact_inventory_sha256,
        "compressed_bytes": authorization.expected_compressed_bytes,
        "date_end": authorization.date_end,
        "date_start": authorization.date_start,
        "input_rows": authorization.expected_input_rows,
        "manifest_bytes": authorization.expected_manifest_bytes,
        "manifest_count": authorization.expected_manifest_count,
        "manifest_inventory_sha256": authorization.manifest_inventory_sha256,
        "page_count": authorization.expected_page_count,
        "raw_bytes": authorization.expected_raw_bytes,
        "session_count": authorization.expected_session_count,
    }
    measured = measured_scope.to_dict()
    if any(measured[key] != value for key, value in expected_scope.items()):
        raise SilverStoreError("asset full-run source inventory measured scope changed")


def _read_preflight(
    root: Path,
    relative_path: str,
) -> tuple[dict[str, object], StoredDocument]:
    path = safe_relative_path(root, relative_path)
    if path.is_symlink() or not path.is_file():
        raise SilverStoreError("asset full-run preflight is not a regular file")
    mode = path.stat().st_mode
    if mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
        raise SilverStoreError("asset full-run preflight remains writable")
    content = path.read_bytes()
    try:
        document = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SilverStoreError("asset full-run preflight is invalid JSON") from exc
    if not isinstance(document, dict):
        raise SilverStoreError("asset full-run preflight must be an object")
    expected_keys = {
        "asset_full_run_preflight_version",
        "disk_status",
        "logical_scope",
        "observed_free_bytes",
        "observed_project_bytes",
        "preflight_id",
    }
    if set(document) != expected_keys:
        raise SilverStoreError("asset full-run preflight fields changed")
    digest = hashlib.sha256(content).hexdigest()
    return document, StoredDocument(relative_path, digest, len(content))


def _validate_existing_approval(
    store: SilverStore,
    *,
    table: str,
    workflow_id: str,
    plan_id: str,
    plan_sha256: str,
    plan_event_sha256: str,
    approver: str,
    decided_at: str,
    note: str,
    waived_qa_result_ids: tuple[str, ...],
    accepted_quarantine_issue_ids: tuple[str, ...],
) -> None:
    event = _single_event(store, workflow_id, WorkflowState.APPROVED_FULL_RUN)
    if (
        event.event.evidence.get("approved_full_run_plan_id") != plan_id
        or event.event.evidence.get("approved_full_run_plan_sha256") != plan_sha256
        or event.event.previous_event_sha256 != plan_event_sha256
        or event.event.actor != approver
        or event.event.created_at != decided_at
        or event.event.note != note
    ):
        raise SilverStoreError(f"existing asset full-run approval changed for {table}")
    approval_id = event.event.evidence.get("approval_id")
    if not isinstance(approval_id, str):
        raise SilverStoreError(f"existing asset full-run approval ID is missing for {table}")
    receipt, _ = store.load_approval(approval_id)
    if (
        receipt.subject_id != plan_id
        or receipt.subject_manifest_sha256 != plan_sha256
        or tuple(receipt.waived_qa_result_ids) != tuple(sorted(waived_qa_result_ids))
        or tuple(receipt.accepted_quarantine_issue_ids)
        != tuple(sorted(accepted_quarantine_issue_ids))
    ):
        raise SilverStoreError(f"existing asset full-run approval receipt changed for {table}")


def _single_event(
    store: SilverStore,
    workflow_id: str,
    state: WorkflowState,
) -> WorkflowEventRecord:
    matches = [
        record for record in store.workflow_events(workflow_id) if record.event.to_state is state
    ]
    if len(matches) != 1:
        raise SilverStoreError(f"asset full-run workflow must have one {state.value} event")
    return matches[0]


def _exact_digest_map(value: Mapping[str, str], label: str) -> dict[str, str]:
    normalized = dict(value)
    if set(normalized) != set(_TABLE_ORDER):
        raise SilverStoreError(f"asset full-run {label} table keys are incomplete")
    for table, digest in normalized.items():
        _sha256(digest, f"{label}:{table}")
    return normalized


def _exact_tuple_map(
    value: Mapping[str, tuple[str, ...]],
    label: str,
) -> dict[str, tuple[str, ...]]:
    normalized = {table: tuple(items) for table, items in value.items()}
    if set(normalized) != set(_TABLE_ORDER):
        raise SilverStoreError(f"asset full-run {label} table keys are incomplete")
    for table, items in normalized.items():
        if len(set(items)) != len(items):
            raise SilverStoreError(f"asset full-run {label} are duplicated for {table}")
        for item in items:
            _sha256(item, f"{label}:{table}")
        normalized[table] = tuple(sorted(items))
    return normalized


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise SilverStoreError(f"asset full-run profile {label} must be an object")
    return dict(value)


def _digest_lines(lines: list[str]) -> str:
    return hashlib.sha256("".join(sorted(lines)).encode()).hexdigest()


def _json_bytes(document: Mapping[str, object]) -> bytes:
    return (
        json.dumps(document, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


def _verify_git_checkout(repo_root: Path, git_commit: str) -> None:
    root = repo_root.expanduser().resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise SilverStoreError(
            "asset full-run plan code is not executing from the verified Git checkout"
        ) from exc
    try:
        top_level = _git_output(root, "rev-parse", "--show-toplevel")
        head = _git_output(root, "rev-parse", "HEAD")
        tracked_module = _git_output(
            root,
            "ls-files",
            "--error-unmatch",
            "--",
            module_relative,
        )
        status = _git_output(root, "status", "--porcelain=v1", "--untracked-files=all")
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SilverStoreError("cannot verify asset full-run plan Git checkout") from exc
    if Path(top_level).resolve() != root:
        raise SilverStoreError("asset full-run repo_root is not the Git top level")
    if head != git_commit:
        raise SilverStoreError("asset full-run Git HEAD differs from --git-commit")
    if tracked_module != module_relative:
        raise SilverStoreError("asset full-run plan module is not tracked")
    if status:
        raise SilverStoreError("asset full-run Git checkout is not clean")


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
            f"asset full-run Git command failed: {' '.join(arguments)}: "
            f"{completed.stderr.strip()}"
        )
    return completed.stdout.strip()


__all__ = [
    "ASSET_FULL_RUN_PLAN_POLICY_VERSION",
    "CURRENT_ASSET_FULL_RUN_PLAN_AUTHORIZATION",
    "AssetFullRunPlanApprovalRun",
    "AssetFullRunPlanAuthorization",
    "AssetFullRunPlanRun",
    "AssetFullRunPreflight",
    "AssetManifestScope",
    "AssetTableFullRunPlanRun",
    "approve_asset_full_run_plans",
    "create_asset_full_run_plans",
]
