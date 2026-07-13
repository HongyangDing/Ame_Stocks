"""Recovery-safe, plan-bound S4 ten-year asset materialization.

The public runner consumes three already-approved :class:`FullRunPlan` documents.  It never
creates or approves a plan and never advances a workflow beyond ``full_ready``.  The expensive
source stream is bounded to one XNYS session at a time and the three table partitions are
materialized from one shared pure transform invocation.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import resource
import shutil
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from importlib.metadata import version
from pathlib import Path
from types import MappingProxyType
from typing import Any

import exchange_calendars as xcals
import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import (
    safe_relative_path,
    stable_digest,
    write_bytes_immutable,
    write_json_atomic,
)
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    ASSET_OBSERVATION_VERSION_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.asset_full_run_plan import (
    CURRENT_ASSET_FULL_RUN_PLAN_AUTHORIZATION,
    AssetFullRunPlanAuthorization,
    _project_file_bytes,
    _resource_policy,
    _validate_plan_authorization,
    _verify_plan_preflight,
)
from ame_stocks_api.silver.asset_preview import (
    CURRENT_ASSET_PREVIEW_AUTHORIZATION,
    _load_reference_dictionaries,
)
from ame_stocks_api.silver.asset_source import (
    AssetSourceReader,
    read_asset_source_inventory,
)
from ame_stocks_api.silver.assets import (
    ASSET_TRANSFORM_VERSION,
    AssetTableTransformResult,
    AssetTransformResult,
    transform_asset_session,
)
from ame_stocks_api.silver.contracts import (
    QA_RESULT_ARROW_SCHEMA,
    QUARANTINE_ARROW_SCHEMA,
    SEPARATE_FULL_RUN_PLAN_POLICY,
    ArtifactRef,
    ArtifactRole,
    BuildIntent,
    BuildKind,
    BuildManifest,
    FullRunPlan,
    QACheckResult,
    QASeverity,
    RowFunnel,
    SourceInventory,
    SourceLayer,
    TableContract,
    arrow_schema_digest,
)
from ame_stocks_api.silver.store import (
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowSnapshot,
    WorkflowState,
)

ASSET_FULL_POLICY_VERSION = "asset-full-v1"
FULL_HISTORY_PARTITION_KEY = "full_history:2016-07-11:2026-07-09"

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


AssetFullAuthorization = AssetFullRunPlanAuthorization
CURRENT_ASSET_FULL_AUTHORIZATION = CURRENT_ASSET_FULL_RUN_PLAN_AUTHORIZATION


@dataclass(frozen=True, slots=True)
class AssetFullTableRun:
    workflow: WorkflowSnapshot
    plan: FullRunPlan
    build: BuildManifest
    build_document: StoredDocument


@dataclass(frozen=True, slots=True)
class AssetFullRun:
    table_runs: Mapping[str, AssetFullTableRun]
    completed_sessions: int
    warnings: tuple[str, ...]
    idempotent: bool


@dataclass(frozen=True, slots=True)
class _FullScopeAudit:
    session_dates: tuple[date, ...]
    source_plan_denominator: int
    calendar_denominator: int


class _FullQAReducer:
    """Merge per-session QA without corrupting distinct or cross-session metrics."""

    _DISTINCT_CHECKS = frozenset(
        {
            "current_type_dictionary_unmatched_values",
            "current_exchange_dictionary_unmatched_values",
        }
    )
    _GLOBAL_CHECKS = frozenset(
        {
            "cross_session_ticker_identity_churn_groups",
            "casefold_collision_groups",
            "source_plan_invalid",
            "source_calendar_coverage_invalid",
        }
    )
    _IDENTITY_FIELDS = ("composite_figi", "share_class_figi", "cik")

    def __init__(self) -> None:
        self.session_count = 0
        self.metrics: dict[str, dict[str, list[int]]] = {table: {} for table in _TABLE_ORDER}
        self.funnels: dict[str, dict[str, Any]] = {
            table: {
                "input_rows": 0,
                "accepted_source_rows": 0,
                "exact_duplicate_excess": 0,
                "quarantined_source_rows": 0,
                "unmapped_source_rows": 0,
                "version_preserved_rows": 0,
                "output_rows": 0,
            }
            for table in _TABLE_ORDER
        }
        self.type_values: dict[str, set[str]] = {
            _OBSERVATION_TABLE: set(),
            _UNIVERSE_TABLE: set(),
        }
        self.exchange_values: dict[str, set[str]] = {
            _OBSERVATION_TABLE: set(),
            _UNIVERSE_TABLE: set(),
        }
        self.identities: dict[str, dict[str, dict[str, set[str]]]] = {
            _OBSERVATION_TABLE: {},
            _UNIVERSE_TABLE: {},
        }

    def add_session(
        self,
        results: Mapping[str, AssetTableTransformResult],
    ) -> None:
        if set(results) != set(_TABLE_ORDER):
            raise SilverStoreError("asset full transform table set is incomplete")
        for table in _TABLE_ORDER:
            result = results[table]
            if result.contract != _CONTRACTS_BY_TABLE[table]:
                raise SilverStoreError(f"asset full transform contract changed for {table}")
            if result.quarantine_records:
                raise SilverStoreError(f"asset full expected zero quarantine rows for {table}")
            blocking = tuple(check.check_id for check in result.qa_checks if check.blocks_publish)
            if blocking:
                raise SilverStoreError(f"asset full blocking session QA for {table}: {blocking}")
            if {item.check_id for item in result.qa_checks} != set(
                result.contract.required_qa_checks
            ):
                raise SilverStoreError(f"asset full per-session QA is incomplete for {table}")
            for check in result.qa_checks:
                if check.check_id in self._DISTINCT_CHECKS | self._GLOBAL_CHECKS:
                    continue
                aggregate = self.metrics[table].setdefault(check.check_id, [0, 0])
                aggregate[0] += check.numerator
                aggregate[1] += check.denominator
            self._add_funnel(table, result.row_funnel)
        for table in (_OBSERVATION_TABLE, _UNIVERSE_TABLE):
            output = results[table].table
            type_values = output.column("type_code").to_pylist()
            exchange_values = output.column("primary_exchange_mic").to_pylist()
            tickers = output.column("ticker").to_pylist()
            identity_columns = {
                field: output.column(field).to_pylist() for field in self._IDENTITY_FIELDS
            }
            self.type_values[table].update(item for item in type_values if isinstance(item, str))
            self.exchange_values[table].update(
                item for item in exchange_values if isinstance(item, str)
            )
            for index, ticker in enumerate(tickers):
                if not isinstance(ticker, str):  # contract makes this unreachable
                    continue
                fields = self.identities[table].setdefault(
                    ticker,
                    {field: set() for field in self._IDENTITY_FIELDS},
                )
                for field in self._IDENTITY_FIELDS:
                    value = identity_columns[field][index]
                    if isinstance(value, str) and value:
                        fields[field].add(value)
        self.session_count += 1

    def _add_funnel(self, table: str, funnel: RowFunnel) -> None:
        expected_outputs = dict(funnel.output_rows_by_table)
        if set(expected_outputs) != {table}:
            raise SilverStoreError(f"asset full row funnel table changed for {table}")
        target = self.funnels[table]
        for key in (
            "input_rows",
            "accepted_source_rows",
            "exact_duplicate_excess",
            "quarantined_source_rows",
            "unmapped_source_rows",
            "version_preserved_rows",
        ):
            target[key] += getattr(funnel, key)
        target["output_rows"] += expected_outputs[table]

    def final_funnel(self, table: str) -> RowFunnel:
        values = self.funnels[table]
        return RowFunnel(
            input_rows=values["input_rows"],
            accepted_source_rows=values["accepted_source_rows"],
            exact_duplicate_excess=values["exact_duplicate_excess"],
            quarantined_source_rows=values["quarantined_source_rows"],
            unmapped_source_rows=values["unmapped_source_rows"],
            version_preserved_rows=values["version_preserved_rows"],
            output_rows_by_table={table: values["output_rows"]},
        )

    def finalize(
        self,
        *,
        audit: _FullScopeAudit,
        current_ticker_types: frozenset[str],
        current_exchange_mics: frozenset[str],
    ) -> Mapping[str, tuple[QACheckResult, ...]]:
        if self.session_count != len(audit.session_dates) or not audit.session_dates:
            raise SilverStoreError("asset full QA reducer session coverage is incomplete")
        finalized: dict[str, tuple[QACheckResult, ...]] = {}
        partition_key = (
            f"full_history:{audit.session_dates[0].isoformat()}:"
            f"{audit.session_dates[-1].isoformat()}"
        )
        for table in _TABLE_ORDER:
            contract = _CONTRACTS_BY_TABLE[table]
            checks: list[QACheckResult] = []
            for rule in contract.qa_rules:
                if rule.check_id == "current_type_dictionary_unmatched_values":
                    values = self.type_values[table]
                    numerator, denominator = (
                        len(values - current_ticker_types),
                        len(values),
                    )
                elif rule.check_id == "current_exchange_dictionary_unmatched_values":
                    values = self.exchange_values[table]
                    numerator, denominator = (
                        len(values - current_exchange_mics),
                        len(values),
                    )
                elif rule.check_id == "cross_session_ticker_identity_churn_groups":
                    identities = self.identities[table]
                    denominator = len(identities)
                    if denominator == 0:
                        raise SilverStoreError(
                            f"asset full identity QA cannot use a 0/0 population for {table}"
                        )
                    numerator = sum(
                        any(len(fields[field]) > 1 for field in self._IDENTITY_FIELDS)
                        for fields in identities.values()
                    )
                elif rule.check_id == "casefold_collision_groups":
                    casefold_groups: dict[str, set[str]] = {}
                    for ticker in self.identities[table]:
                        casefold_groups.setdefault(ticker.casefold(), set()).add(ticker)
                    denominator = len(casefold_groups)
                    if denominator == 0:
                        raise SilverStoreError(
                            f"asset full casefold QA cannot use a 0/0 population for {table}"
                        )
                    numerator = sum(
                        len(exact_tickers) > 1 for exact_tickers in casefold_groups.values()
                    )
                elif rule.check_id == "source_plan_invalid":
                    numerator, denominator = (0, audit.source_plan_denominator)
                elif rule.check_id == "source_calendar_coverage_invalid":
                    numerator, denominator = (0, audit.calendar_denominator)
                else:
                    try:
                        numerator, denominator = self.metrics[table][rule.check_id]
                    except KeyError as exc:
                        raise SilverStoreError(
                            f"asset full global QA is missing {table}:{rule.check_id}"
                        ) from exc
                if numerator > denominator:
                    raise SilverStoreError(
                        f"asset full global QA numerator exceeds denominator: "
                        f"{table}:{rule.check_id}"
                    )
                rate = None if denominator == 0 else float(numerator / denominator)
                checks.append(
                    QACheckResult(
                        table=table,
                        partition_key=partition_key,
                        check_id=rule.check_id,
                        severity=rule.severity,
                        status=rule.expected_status(numerator=numerator, rate=rate),
                        numerator=numerator,
                        denominator=denominator,
                        rate=rate,
                        threshold=rule.threshold_expression,
                    )
                )
            if any(item.blocks_publish for item in checks):
                failed = tuple(item.check_id for item in checks if item.blocks_publish)
                raise SilverStoreError(f"asset full blocking aggregate QA for {table}: {failed}")
            finalized[table] = tuple(checks)
        return MappingProxyType(finalized)

    def to_dict(self) -> dict[str, object]:
        return {
            "exchange_values": {
                table: sorted(values) for table, values in sorted(self.exchange_values.items())
            },
            "funnels": self.funnels,
            "identities": {
                table: {
                    ticker: {field: sorted(values) for field, values in sorted(fields.items())}
                    for ticker, fields in sorted(identities.items())
                }
                for table, identities in sorted(self.identities.items())
            },
            "metrics": self.metrics,
            "session_count": self.session_count,
            "type_values": {
                table: sorted(values) for table, values in sorted(self.type_values.items())
            },
        }

    @classmethod
    def from_dict(cls, value: object) -> _FullQAReducer:
        if not isinstance(value, Mapping):
            raise SilverStoreError("asset full checkpoint reducer must be an object")
        reducer = cls()
        try:
            reducer.session_count = int(value["session_count"])
            reducer.type_values = {
                str(table): set(items) for table, items in value["type_values"].items()
            }
            reducer.exchange_values = {
                str(table): set(items) for table, items in value["exchange_values"].items()
            }
            reducer.metrics = {
                str(table): {
                    str(check_id): [int(pair[0]), int(pair[1])] for check_id, pair in checks.items()
                }
                for table, checks in value["metrics"].items()
            }
            reducer.funnels = {
                str(table): {str(key): int(item) for key, item in funnel.items()}
                for table, funnel in value["funnels"].items()
            }
            reducer.identities = {
                str(table): {
                    str(ticker): {str(field): set(items) for field, items in fields.items()}
                    for ticker, fields in identities.items()
                }
                for table, identities in value["identities"].items()
            }
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise SilverStoreError("asset full checkpoint reducer is invalid") from exc
        if set(reducer.metrics) != set(_TABLE_ORDER) or set(reducer.funnels) != set(_TABLE_ORDER):
            raise SilverStoreError("asset full checkpoint reducer table set changed")
        expected_distinct_tables = {_OBSERVATION_TABLE, _UNIVERSE_TABLE}
        if (
            set(reducer.type_values) != expected_distinct_tables
            or set(reducer.exchange_values) != expected_distinct_tables
            or set(reducer.identities) != expected_distinct_tables
        ):
            raise SilverStoreError("asset full checkpoint distinct QA table set changed")
        if reducer.session_count < 0:
            raise SilverStoreError("asset full checkpoint session count is invalid")
        return reducer


def run_asset_full(
    data_root: Path,
    *,
    workflow_ids: Mapping[str, str],
    approved_event_sha256_by_table: Mapping[str, str],
    approved_plan_id_by_table: Mapping[str, str],
    approved_plan_sha256_by_table: Mapping[str, str],
    git_commit: str,
    repo_root: Path,
    workers: int = 1,
    max_in_flight_sessions: int = 1,
    actor: str = "s4-assets-full-runner",
    calendar_name: str = "XNYS",
) -> AssetFullRun:
    """Materialize the exact approved S4 scope and stop at ``full_ready``."""

    return _run_asset_full_authorized(
        data_root,
        workflow_ids=workflow_ids,
        approved_event_sha256_by_table=approved_event_sha256_by_table,
        approved_plan_id_by_table=approved_plan_id_by_table,
        approved_plan_sha256_by_table=approved_plan_sha256_by_table,
        git_commit=git_commit,
        repo_root=repo_root,
        workers=workers,
        max_in_flight_sessions=max_in_flight_sessions,
        actor=actor,
        calendar_name=calendar_name,
        authorization=CURRENT_ASSET_FULL_AUTHORIZATION,
    )


def _run_asset_full_authorized(
    data_root: Path,
    *,
    workflow_ids: Mapping[str, str],
    approved_event_sha256_by_table: Mapping[str, str],
    approved_plan_id_by_table: Mapping[str, str],
    approved_plan_sha256_by_table: Mapping[str, str],
    git_commit: str,
    repo_root: Path,
    workers: int,
    max_in_flight_sessions: int,
    actor: str,
    calendar_name: str,
    authorization: AssetFullRunPlanAuthorization,
    transition_barrier: Callable[[str], None] | None = None,
    git_verifier: Callable[[Path, str], None] | None = None,
    transform_fn: Callable[..., AssetTransformResult] = transform_asset_session,
    monotonic: Callable[[], float] = time.monotonic,
    now_utc: Callable[[], str] | None = None,
) -> AssetFullRun:
    """Fixture-capable implementation with a shared S4 lock and durable checkpoints."""

    workflows = _exact_table_digest_map(workflow_ids, "workflow ID")
    approved_events = _exact_table_digest_map(
        approved_event_sha256_by_table,
        "approved event SHA",
    )
    approved_plan_ids = _exact_table_digest_map(
        approved_plan_id_by_table,
        "approved plan ID",
    )
    approved_plan_shas = _exact_table_digest_map(
        approved_plan_sha256_by_table,
        "approved plan SHA",
    )
    if workflows != dict(authorization.workflow_ids_by_table):
        raise SilverStoreError("asset full workflow IDs are not production-authorized")
    if workers != authorization.workers or workers != 1:
        raise SilverStoreError("asset full workers are frozen to 1")
    if (
        max_in_flight_sessions != authorization.max_in_flight_sessions
        or max_in_flight_sessions != 1
    ):
        raise SilverStoreError("asset full max_in_flight_sessions is frozen to 1")
    if calendar_name != "XNYS":
        raise SilverStoreError("asset full calendar is frozen to XNYS")
    if pa.__version__ != authorization.pyarrow_version:
        raise SilverStoreError(
            "asset full installed PyArrow differs from the approved serialization version"
        )
    verifier = _verify_git_checkout if git_verifier is None else git_verifier
    now = _now_utc if now_utc is None else now_utc
    verifier(repo_root, git_commit)

    root = data_root.expanduser().resolve()
    store = SilverStore(root)
    snapshots, plans, _plan_documents = _load_approved_plans(
        store,
        workflows=workflows,
        approved_events=approved_events,
        approved_plan_ids=approved_plan_ids,
        approved_plan_shas=approved_plan_shas,
        git_commit=git_commit,
        authorization=authorization,
    )
    intents = {
        table: _full_intent(plans[table], approved_plan_ids[table]) for table in _TABLE_ORDER
    }
    build_ids = {table: intents[table].build_id for table in _TABLE_ORDER}
    output_prefixes = {
        table: _full_output_prefix(intents[table], build_ids[table]) for table in _TABLE_ORDER
    }
    _validate_shared_plan_scope(plans, intents=intents, authorization=authorization)

    if all(snapshot.state is WorkflowState.FULL_READY for snapshot in snapshots.values()):
        table_runs = _load_existing_full_runs(
            store,
            snapshots=snapshots,
            plans=plans,
            intents=intents,
            authorization=authorization,
        )
        produced_bytes = sum(
            artifact.bytes for run in table_runs.values() for artifact in run.build.outputs
        )
        project_bytes = _project_file_bytes(root)
        baseline_bytes = max(0, project_bytes - produced_bytes)
        warnings = _enforce_live_resources(
            root,
            authorization=authorization,
            baseline_project_bytes=baseline_bytes,
            produced_bytes=produced_bytes,
            elapsed_seconds=0.0,
            final_project_bytes=project_bytes,
        )
        return AssetFullRun(
            table_runs=MappingProxyType(table_runs),
            completed_sessions=authorization.expected_session_count,
            warnings=warnings,
            idempotent=True,
        )

    inventory, _inventory_document = _load_plan_inventory(root, plans)
    reader, audit = _authorized_reader(
        root,
        inventory,
        authorization=authorization,
        calendar_name=calendar_name,
    )
    ticker_types, exchange_mics, dependency_refs = _load_reference_dictionaries(
        root,
        store,
        CURRENT_ASSET_PREVIEW_AUTHORIZATION,
    )
    _validate_dependency_refs(
        inventory,
        dependency_refs,
        authorization=authorization,
    )
    run_id = stable_digest(
        {
            "asset_full_policy_version": ASSET_FULL_POLICY_VERSION,
            "build_ids": build_ids,
            "plan_ids": dict(sorted(approved_plan_ids.items())),
            "source_inventory_id": inventory.inventory_id,
        }
    )
    lock_path = root / "manifests" / "silver" / "locks" / f"s4-assets-full-{run_id}.lock"
    warnings: list[str] = []
    invocation_started = monotonic()
    with _exclusive_run_lock(lock_path):
        observed_project_bytes = _project_file_bytes(root)
        warnings.extend(
            _enforce_live_resources(
                root,
                authorization=authorization,
                baseline_project_bytes=observed_project_bytes,
                produced_bytes=0,
                elapsed_seconds=0.0,
            )
        )
        checkpoint_path = _checkpoint_path(root, run_id)
        checkpoint = _load_or_initialize_checkpoint(
            root,
            checkpoint_path=checkpoint_path,
            run_id=run_id,
            plan_ids=approved_plan_ids,
            plan_shas=approved_plan_shas,
            build_ids=build_ids,
            session_dates=tuple(item.session_date for item in reader.sessions),
            baseline_project_bytes=observed_project_bytes,
            started_at=now(),
        )
        reducer = _FullQAReducer.from_dict(checkpoint["reducer"])
        _verify_checkpoint_state(
            root,
            checkpoint,
            reader=reader,
            intents=intents,
            output_prefixes=output_prefixes,
            contracts=_CONTRACTS_BY_TABLE,
        )
        invocation_base_elapsed = float(checkpoint["elapsed_active_seconds"])
        sessions = reader.sessions
        for index in range(int(checkpoint["next_session_index"]), len(sessions)):
            elapsed = invocation_base_elapsed + (monotonic() - invocation_started)
            produced = _checkpoint_output_bytes(checkpoint)
            warnings.extend(
                _enforce_live_resources(
                    root,
                    authorization=authorization,
                    baseline_project_bytes=int(checkpoint["baseline_project_bytes"]),
                    produced_bytes=produced,
                    elapsed_seconds=elapsed,
                )
            )
            session = sessions[index]
            records = tuple(reader.iter_session_records(session.session_date))
            if len(records) != session.declared_row_count:
                raise SilverStoreError(
                    f"asset full streamed rows changed for {session.session_date}"
                )
            transform_run_id = stable_digest(
                {
                    "asset_full_policy_version": ASSET_FULL_POLICY_VERSION,
                    "build_ids": [build_ids[table] for table in _TABLE_ORDER],
                    "session_date": session.session_date.isoformat(),
                }
            )
            transformed = transform_fn(
                session,
                records,
                build_id=transform_run_id,
                calendar_name=calendar_name,
                current_ticker_types=ticker_types,
                current_exchange_mics=exchange_mics,
            )
            results = _transform_results_by_table(transformed)
            reducer.add_session(results)
            session_outputs = {
                table: _write_data_partition(
                    root,
                    intent=intents[table],
                    output_prefix=output_prefixes[table],
                    session_date=session.session_date,
                    result=results[table],
                )
                for table in _TABLE_ORDER
            }
            session_checkpoint = _write_session_checkpoint(
                root,
                run_id=run_id,
                session_date=session.session_date,
                session_index=index,
                results=results,
                outputs=session_outputs,
                reducer_state=reducer.to_dict(),
            )
            for table in _TABLE_ORDER:
                checkpoint["outputs_by_table"][table].append(session_outputs[table].to_dict())
            checkpoint["session_checkpoints"].append(session_checkpoint)
            checkpoint["next_session_index"] = index + 1
            checkpoint["elapsed_active_seconds"] = invocation_base_elapsed + (
                monotonic() - invocation_started
            )
            checkpoint["reducer"] = reducer.to_dict()
            _write_checkpoint(checkpoint_path, checkpoint)
            warnings.extend(
                _enforce_live_resources(
                    root,
                    authorization=authorization,
                    baseline_project_bytes=int(checkpoint["baseline_project_bytes"]),
                    produced_bytes=_checkpoint_output_bytes(checkpoint),
                    elapsed_seconds=float(checkpoint["elapsed_active_seconds"]),
                )
            )
            _call_barrier(transition_barrier, f"after_session:{session.session_date}")

        warnings.extend(
            _enforce_live_resources(
                root,
                authorization=authorization,
                baseline_project_bytes=int(checkpoint["baseline_project_bytes"]),
                produced_bytes=_checkpoint_output_bytes(checkpoint),
                elapsed_seconds=invocation_base_elapsed + (monotonic() - invocation_started),
            )
        )
        _validate_final_cardinality(
            reducer,
            plans=plans,
            authorization=authorization,
        )
        aggregate_qa = reducer.finalize(
            audit=audit,
            current_ticker_types=ticker_types,
            current_exchange_mics=exchange_mics,
        )
        final_outputs = _write_final_system_outputs(
            root,
            intents=intents,
            output_prefixes=output_prefixes,
            aggregate_qa=aggregate_qa,
        )
        for table in _TABLE_ORDER:
            existing = {item["path"]: item for item in checkpoint["outputs_by_table"][table]}
            for artifact in final_outputs[table]:
                existing[artifact.path] = artifact.to_dict()
            checkpoint["outputs_by_table"][table] = [existing[path] for path in sorted(existing)]
        if checkpoint.get("completed_at") is None:
            checkpoint["completed_at"] = now()
        checkpoint["elapsed_active_seconds"] = invocation_base_elapsed + (
            monotonic() - invocation_started
        )
        checkpoint["reducer"] = reducer.to_dict()
        _write_checkpoint(checkpoint_path, checkpoint)
        final_output_bytes = _checkpoint_output_bytes(checkpoint)
        current_project_bytes = _project_file_bytes(root)
        warnings.extend(
            _enforce_live_resources(
                root,
                authorization=authorization,
                baseline_project_bytes=int(checkpoint["baseline_project_bytes"]),
                produced_bytes=final_output_bytes,
                elapsed_seconds=float(checkpoint["elapsed_active_seconds"]),
                final_project_bytes=current_project_bytes,
            )
        )
        manifests = {
            table: _full_manifest(
                intent=intents[table],
                outputs=tuple(
                    ArtifactRef.from_dict(item) for item in checkpoint["outputs_by_table"][table]
                ),
                row_funnel=reducer.final_funnel(table),
                qa_checks=aggregate_qa[table],
                started_at=str(checkpoint["started_at"]),
                completed_at=str(checkpoint["completed_at"]),
            )
            for table in _TABLE_ORDER
        }
        manifest_documents = {
            table: _write_build_manifest_immutable(root, manifests[table]) for table in _TABLE_ORDER
        }
        _call_barrier(transition_barrier, "after_manifests")
        current_project_bytes = _project_file_bytes(root)
        warnings.extend(
            _enforce_live_resources(
                root,
                authorization=authorization,
                baseline_project_bytes=int(checkpoint["baseline_project_bytes"]),
                produced_bytes=final_output_bytes,
                elapsed_seconds=invocation_base_elapsed + (monotonic() - invocation_started),
                final_project_bytes=current_project_bytes,
            )
        )

        table_runs: dict[str, AssetFullTableRun] = {}
        for table in _TABLE_ORDER:
            current = store.verify_workflow_trust_chain(
                workflows[table],
                verify_artifacts=True,
            )
            if current.state is WorkflowState.FULL_READY:
                existing = _load_event_full(store, table, current)
                if existing[0].to_dict() != manifests[table].to_dict():
                    raise SilverStoreError(
                        f"asset full existing full_ready build changed for {table}"
                    )
                table_runs[table] = AssetFullTableRun(
                    workflow=current,
                    plan=plans[table],
                    build=existing[0],
                    build_document=existing[1],
                )
                continue
            if (
                current.state is not WorkflowState.APPROVED_FULL_RUN
                or current.event_sha256 != approved_events[table]
            ):
                raise SilverStoreError(
                    f"asset full workflow moved unexpectedly before record: {table}"
                )
            current_project_bytes = _project_file_bytes(root)
            warnings.extend(
                _enforce_live_resources(
                    root,
                    authorization=authorization,
                    baseline_project_bytes=int(checkpoint["baseline_project_bytes"]),
                    produced_bytes=final_output_bytes,
                    elapsed_seconds=invocation_base_elapsed + (monotonic() - invocation_started),
                    final_project_bytes=current_project_bytes,
                )
            )
            verifier(repo_root, git_commit)
            recorded_at = now()
            current = store.record_full_build(
                manifests[table],
                expected_event_sha256=current.event_sha256,
                actor=actor,
                recorded_at=recorded_at,
                note=(
                    "Registered exact approved S4 full-history assets build; stopped at "
                    "full_ready without requesting publication."
                ),
            )
            _call_barrier(transition_barrier, f"after_record:{table}")
            loaded, document = _load_event_full(store, table, current)
            if document.sha256 != manifest_documents[table].sha256:
                raise SilverStoreError(f"asset full manifest SHA changed for {table}")
            table_runs[table] = AssetFullTableRun(
                workflow=current,
                plan=plans[table],
                build=loaded,
                build_document=document,
            )

        final_project_bytes = _project_file_bytes(root)
        produced_bytes = sum(
            artifact.bytes for table in _TABLE_ORDER for artifact in manifests[table].outputs
        )
        warnings.extend(
            _enforce_live_resources(
                root,
                authorization=authorization,
                baseline_project_bytes=int(checkpoint["baseline_project_bytes"]),
                produced_bytes=produced_bytes,
                elapsed_seconds=float(checkpoint["elapsed_active_seconds"]),
                final_project_bytes=final_project_bytes,
            )
        )
        return AssetFullRun(
            table_runs=MappingProxyType(table_runs),
            completed_sessions=len(reader.sessions),
            warnings=tuple(dict.fromkeys(warnings)),
            idempotent=False,
        )


def _exact_table_digest_map(value: Mapping[str, str], label: str) -> dict[str, str]:
    normalized = dict(value)
    if set(normalized) != set(_TABLE_ORDER):
        raise SilverStoreError(f"asset full {label} table keys are incomplete")
    for table, digest in normalized.items():
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise SilverStoreError(f"asset full {label} is invalid for {table}")
    return normalized


def _load_approved_plans(
    store: SilverStore,
    *,
    workflows: Mapping[str, str],
    approved_events: Mapping[str, str],
    approved_plan_ids: Mapping[str, str],
    approved_plan_shas: Mapping[str, str],
    git_commit: str,
    authorization: AssetFullRunPlanAuthorization,
) -> tuple[
    dict[str, WorkflowSnapshot],
    dict[str, FullRunPlan],
    dict[str, StoredDocument],
]:
    snapshots: dict[str, WorkflowSnapshot] = {}
    plans: dict[str, FullRunPlan] = {}
    documents: dict[str, StoredDocument] = {}
    canonical_inputs: tuple[ArtifactRef, ...] | None = None
    canonical_source_digest: str | None = None
    for table in _TABLE_ORDER:
        snapshot = store.verify_workflow_trust_chain(
            workflows[table],
            verify_artifacts=True,
        )
        if snapshot.state not in {
            WorkflowState.APPROVED_FULL_RUN,
            WorkflowState.FULL_READY,
        }:
            raise SilverStoreError(f"asset full cannot run {table} from {snapshot.state.value}")
        contract, _ = store.load_workflow_contract(snapshot.workflow_id)
        if contract != _CONTRACTS_BY_TABLE[table]:
            raise SilverStoreError(f"asset full workflow contract changed for {table}")
        approvals = [
            item
            for item in store.workflow_events(snapshot.workflow_id)
            if item.event.to_state is WorkflowState.APPROVED_FULL_RUN
        ]
        if len(approvals) != 1 or approvals[0].event_sha256 != approved_events[table]:
            raise SilverStoreError(f"asset full approved event changed for {table}")
        if (
            approvals[0].event.evidence.get("approved_full_run_plan_id") != approved_plan_ids[table]
            or approvals[0].event.evidence.get("approved_full_run_plan_sha256")
            != approved_plan_shas[table]
        ):
            raise SilverStoreError(f"asset full approved plan evidence changed for {table}")
        if (
            snapshot.state is WorkflowState.APPROVED_FULL_RUN
            and snapshot.event_sha256 != approved_events[table]
        ):
            raise SilverStoreError(f"asset full current approval changed for {table}")
        plan, document = store.load_full_run_plan(table, approved_plan_ids[table])
        if document.sha256 != approved_plan_shas[table]:
            raise SilverStoreError(f"asset full plan manifest SHA changed for {table}")
        if plan.git_commit != git_commit:
            raise SilverStoreError(f"asset full plan Git commit changed for {table}")
        _validate_plan_authorization(plan, authorization=authorization)
        _verify_plan_preflight(store.root, plan, authorization=authorization)
        if canonical_inputs is None:
            canonical_inputs = plan.inputs
            canonical_source_digest = plan.source_digest
        else:
            if plan.source_digest != canonical_source_digest or plan.inputs != canonical_inputs:
                raise SilverStoreError("asset full approved plans do not share exact inputs")
            # FullRunPlan.from_dict creates 72,038 ArtifactRef objects per table.  Retain one
            # canonical object graph so the three approved plans do not triple source-lineage
            # memory before the first bounded session is streamed.
            plan = replace(plan, inputs=canonical_inputs)
        snapshots[table] = snapshot
        plans[table] = plan
        documents[table] = document
    return snapshots, plans, documents


def _full_intent(plan: FullRunPlan, approved_plan_id: str) -> BuildIntent:
    return BuildIntent(
        workflow_id=plan.workflow_id,
        domain=plan.domain,
        table=plan.table,
        schema_version=plan.schema_version,
        contract_id=plan.contract_id,
        kind=BuildKind.FULL,
        attempt=1,
        retry_of_build_id=None,
        transform_version=plan.transform_version,
        git_commit=plan.git_commit,
        exchange_calendar_version=plan.exchange_calendar_version,
        inputs=plan.inputs,
        parameters={
            **dict(plan.parameters),
            "approved_preview_build_id": plan.reviewed_preview_build_id,
            "approved_full_run_plan_id": approved_plan_id,
        },
    )


def _validate_shared_plan_scope(
    plans: Mapping[str, FullRunPlan],
    *,
    intents: Mapping[str, BuildIntent],
    authorization: AssetFullRunPlanAuthorization,
) -> None:
    first = plans[_TABLE_ORDER[0]]
    expected_calendar = f"exchange-calendars=={version('exchange-calendars')}"
    for table in _TABLE_ORDER:
        plan = plans[table]
        contract = _CONTRACTS_BY_TABLE[table]
        if (
            plan.domain != contract.domain
            or plan.schema_version != contract.schema_version
            or plan.contract_id != contract.contract_id
            or plan.transform_version != ASSET_TRANSFORM_VERSION
            or plan.exchange_calendar_version != expected_calendar
            or plan.parameters.get("calendar_name") != "XNYS"
            or plan.parameters.get("full_run_scope_policy") != SEPARATE_FULL_RUN_PLAN_POLICY
            or plan.parameters.get("workers") != 1
            or plan.parameters.get("max_in_flight_sessions") != 1
            or plan.parameters.get("expected_input_rows") != authorization.expected_input_rows
            or plan.parameters.get("expected_output_rows")
            != authorization.expected_output_rows_by_table[table]
            or plan.inputs != first.inputs
            or plan.source_digest != first.source_digest
        ):
            raise SilverStoreError(f"asset full approved plan scope diverged for {table}")
        if intents[table].source_digest != plan.source_digest:
            raise SilverStoreError(f"asset full intent source changed for {table}")
        policy = _resource_policy(authorization)
        if any(plan.resource_projection.get(key) != value for key, value in policy.items()):
            raise SilverStoreError(f"asset full resource projection changed for {table}")


def _load_plan_inventory(
    root: Path,
    plans: Mapping[str, FullRunPlan],
) -> tuple[SourceInventory, StoredDocument]:
    first_inputs = plans[_TABLE_ORDER[0]].inputs
    first = first_inputs[0]
    lineage_path = first.lineage_manifest_path
    lineage_sha = first.lineage_manifest_sha256
    if lineage_path is None or lineage_sha is None:
        raise SilverStoreError("asset full plan source inventory lineage is missing")
    for plan in plans.values():
        if plan.inputs != first_inputs:
            raise SilverStoreError("asset full plans do not share exact inputs")
        if any(
            item.lineage_manifest_path != lineage_path
            or item.lineage_manifest_sha256 != lineage_sha
            for item in plan.inputs
        ):
            raise SilverStoreError("asset full inputs do not share one source inventory")
    path = safe_relative_path(root, lineage_path)
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise SilverStoreError("cannot read asset full source inventory") from exc
    if hashlib.sha256(content).hexdigest() != lineage_sha:
        raise SilverStoreError("asset full source inventory checksum changed")
    try:
        inventory = SourceInventory.from_dict(json.loads(content))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SilverStoreError("asset full source inventory is invalid JSON") from exc
    inventory_items = {item.path: item for item in inventory.artifacts}
    if set(inventory_items) != {item.path for item in first_inputs}:
        raise SilverStoreError("asset full plan inputs differ from source inventory")
    for item in first_inputs:
        source = inventory_items[item.path]
        if (
            item.role is not ArtifactRole.SOURCE
            or item.source_dataset != inventory.source_dataset
            or item.source_layer is not inventory.source_layer
            or item.sha256 != source.sha256
            or item.bytes != source.bytes
            or item.row_count != source.row_count
            or item.media_type != source.media_type
        ):
            raise SilverStoreError("asset full plan input metadata changed")
    return inventory, StoredDocument(lineage_path, lineage_sha, len(content))


def _authorized_reader(
    root: Path,
    inventory: SourceInventory,
    *,
    authorization: AssetFullRunPlanAuthorization,
    calendar_name: str,
) -> tuple[AssetSourceReader, _FullScopeAudit]:
    if inventory.source_dataset != "assets" or inventory.source_layer is not SourceLayer.BRONZE:
        raise SilverStoreError("asset full source inventory is not Bronze assets")
    bronze_refs = tuple(
        item
        for item in inventory.upstream_manifests
        if item.path.startswith("manifests/massive/assets/")
    )
    dependency_refs = tuple(
        item for item in inventory.upstream_manifests if item not in bronze_refs
    )
    expected_dependencies = (
        {
            (
                f"manifests/silver/releases/release_id={authorization.exchange_release_id}.json"
            ): authorization.exchange_release_sha256,
            (
                f"manifests/silver/releases/release_id={authorization.ticker_type_release_id}.json"
            ): authorization.ticker_type_release_sha256,
        }
        if authorization.dependency_lineage_required
        else {}
    )
    if {item.path: item.sha256 for item in dependency_refs} != expected_dependencies:
        raise SilverStoreError("asset full dependency release lineage changed")
    if len(bronze_refs) != authorization.expected_manifest_count:
        raise SilverStoreError("asset full Bronze manifest count changed")
    bronze_inventory = replace(inventory, upstream_manifests=bronze_refs)
    reader = read_asset_source_inventory(root, bronze_inventory)
    session_dates = tuple(item.session_date for item in reader.sessions)
    if not session_dates:
        raise SilverStoreError("asset full source has no sessions")
    expected_dates = tuple(
        item.date()
        for item in xcals.get_calendar(calendar_name).sessions_in_range(
            authorization.date_start,
            authorization.date_end,
        )
    )
    manifest_lines: list[str] = []
    artifact_lines: list[str] = []
    manifest_bytes = 0
    raw_bytes = 0
    for session in reader.sessions:
        for request in session.requests:
            manifest_path = safe_relative_path(root, request.source_manifest_path)
            size = manifest_path.stat().st_size
            manifest_bytes += size
            manifest_lines.append(
                f"{request.source_request_id}\t{request.source_manifest_path}\t{size}\t"
                f"{request.source_manifest_sha256}\n"
            )
            for page in request.pages:
                raw_bytes += page.raw_bytes
                artifact_lines.append(
                    f"{page.source_path}\t{page.source_artifact_sha256}\t{page.raw_sha256}\t"
                    f"{page.compressed_bytes}\t{page.raw_bytes}\t{page.record_count}\n"
                )
    manifest_digest = _digest_lines(manifest_lines)
    artifact_digest = _digest_lines(artifact_lines)
    checks = {
        "session dates": session_dates == expected_dates,
        "date start": session_dates[0].isoformat() == authorization.date_start,
        "date end": session_dates[-1].isoformat() == authorization.date_end,
        "sessions": reader.session_count == authorization.expected_session_count,
        "manifests": reader.request_count == authorization.expected_manifest_count,
        "pages": reader.page_count == authorization.expected_page_count,
        "rows": reader.declared_row_count == authorization.expected_input_rows,
        "manifest bytes": manifest_bytes == authorization.expected_manifest_bytes,
        "compressed bytes": sum(item.bytes for item in inventory.artifacts)
        == authorization.expected_compressed_bytes,
        "raw bytes": raw_bytes == authorization.expected_raw_bytes,
        "manifest digest": manifest_digest == authorization.manifest_inventory_sha256,
        "artifact digest": artifact_digest == authorization.artifact_inventory_sha256,
    }
    failed = tuple(label for label, passed in checks.items() if not passed)
    if failed:
        raise SilverStoreError(f"asset full source scope preflight changed: {failed}")
    return reader, _FullScopeAudit(
        session_dates=session_dates,
        source_plan_denominator=reader.request_count,
        calendar_denominator=len(expected_dates),
    )


def _validate_dependency_refs(
    inventory: SourceInventory,
    observed: Iterable[Any],
    *,
    authorization: AssetFullRunPlanAuthorization,
) -> None:
    expected = (
        {
            (
                f"manifests/silver/releases/release_id={authorization.exchange_release_id}.json"
            ): authorization.exchange_release_sha256,
            (
                f"manifests/silver/releases/release_id={authorization.ticker_type_release_id}.json"
            ): authorization.ticker_type_release_sha256,
        }
        if authorization.dependency_lineage_required
        else {}
    )
    inventory_dependencies = {
        item.path: item.sha256
        for item in inventory.upstream_manifests
        if not item.path.startswith("manifests/massive/assets/")
    }
    observed_dependencies = {item.path: item.sha256 for item in observed}
    if inventory_dependencies != expected or observed_dependencies != expected:
        raise SilverStoreError("asset full loaded dependency release lineage changed")


def _digest_lines(lines: Iterable[str]) -> str:
    return hashlib.sha256("".join(sorted(lines)).encode()).hexdigest()


def _process_max_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _enforce_live_resources(
    root: Path,
    *,
    authorization: AssetFullRunPlanAuthorization,
    baseline_project_bytes: int,
    produced_bytes: int,
    elapsed_seconds: float,
    final_project_bytes: int | None = None,
) -> tuple[str, ...]:
    if (
        any(type(item) is not int or item < 0 for item in (baseline_project_bytes, produced_bytes))
        or elapsed_seconds < 0
    ):
        raise SilverStoreError("asset full resource counters are invalid")
    if authorization.runtime_estimate_seconds > authorization.runtime_review_ceiling_seconds:
        raise SilverStoreError("asset full approved runtime estimate exceeds its ceiling")
    if elapsed_seconds > authorization.runtime_review_ceiling_seconds:
        raise SilverStoreError("asset full runtime exceeded its approved 30-hour ceiling")
    rss = _process_max_rss_bytes()
    if rss > authorization.hard_rss_limit_bytes:
        raise SilverStoreError("asset full process exceeded its 2 GiB RSS hard cap")
    if produced_bytes > authorization.peak_incremental_cap_bytes:
        raise SilverStoreError("asset full produced bytes exceed the incremental peak cap")
    current_project_estimate = baseline_project_bytes + produced_bytes
    if current_project_estimate > authorization.peak_project_cap_bytes:
        raise SilverStoreError("asset full project estimate exceeds the 140 GiB peak cap")
    if baseline_project_bytes + authorization.stable_output_cap_bytes > (
        authorization.stable_project_cap_bytes
    ):
        raise SilverStoreError("asset full projected stable project exceeds 120 GiB")
    if baseline_project_bytes + authorization.peak_incremental_cap_bytes > (
        authorization.peak_project_cap_bytes
    ):
        raise SilverStoreError("asset full projected peak project exceeds 140 GiB")
    free_bytes = shutil.disk_usage(root).free
    remaining_peak = max(0, authorization.peak_incremental_cap_bytes - produced_bytes)
    projected_free = free_bytes - remaining_peak
    if projected_free < authorization.free_space_floor_bytes:
        raise SilverStoreError("asset full projected remaining disk breaches the 40 GiB hard floor")
    if free_bytes < authorization.free_space_floor_bytes:
        raise SilverStoreError("asset full current disk breaches the 40 GiB hard floor")
    if final_project_bytes is not None:
        if final_project_bytes > authorization.stable_project_cap_bytes:
            raise SilverStoreError("asset full final project exceeds the 120 GiB stable cap")
        if produced_bytes > authorization.stable_output_cap_bytes:
            raise SilverStoreError("asset full final outputs exceed the 20 GiB stable cap")
    warnings: list[str] = []
    if projected_free < authorization.free_space_warning_bytes:
        warnings.append("projected remaining disk is below the 60 GiB warning threshold")
    if rss > authorization.expected_rss_ceiling_bytes:
        warnings.append("process RSS exceeded the reviewed 0.75 GiB estimate")
    return tuple(warnings)


def _transform_results_by_table(
    transformed: AssetTransformResult,
) -> Mapping[str, AssetTableTransformResult]:
    return MappingProxyType(
        {
            _OBSERVATION_TABLE: transformed.observation,
            _VERSION_TABLE: transformed.version,
            _UNIVERSE_TABLE: transformed.universe,
        }
    )


def _full_output_prefix(intent: BuildIntent, build_id: str) -> str:
    return (
        f"silver/schema=v{intent.schema_version}/{intent.domain}/{intent.table}/build_id={build_id}"
    )


def _partition_relative_path(output_prefix: str, session_date: date) -> str:
    return (
        f"{output_prefix}/data/session_year={session_date.year}/"
        f"session_date={session_date.isoformat()}/part-00000.parquet"
    )


def _write_data_partition(
    root: Path,
    *,
    intent: BuildIntent,
    output_prefix: str,
    session_date: date,
    result: AssetTableTransformResult,
) -> ArtifactRef:
    if result.contract.table != intent.table:
        raise SilverStoreError("asset full data result differs from its intent")
    return _write_parquet_artifact(
        root,
        relative_path=_partition_relative_path(output_prefix, session_date),
        table=result.table,
        role=ArtifactRole.DATA,
        table_name=intent.table,
    )


def _write_parquet_artifact(
    root: Path,
    *,
    relative_path: str,
    table: pa.Table,
    role: ArtifactRole,
    table_name: str,
) -> ArtifactRef:
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        version="2.6",
        write_statistics=True,
    )
    content = sink.getvalue().to_pybytes()
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, relative_path),
        content,
        temporary_directory=root / "tmp" / "silver-asset-full-immutable-writes",
    )
    return ArtifactRef(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
        row_count=table.num_rows,
        media_type="application/vnd.apache.parquet",
        role=role,
        table=table_name,
        schema_digest=arrow_schema_digest(table.schema),
    )


def _write_final_system_outputs(
    root: Path,
    *,
    intents: Mapping[str, BuildIntent],
    output_prefixes: Mapping[str, str],
    aggregate_qa: Mapping[str, tuple[QACheckResult, ...]],
) -> Mapping[str, tuple[ArtifactRef, ...]]:
    result: dict[str, tuple[ArtifactRef, ...]] = {}
    for table in _TABLE_ORDER:
        intent = intents[table]
        prefix = output_prefixes[table]
        qa_table = pa.Table.from_pylist(
            [item.to_output_dict(intent.build_id) for item in aggregate_qa[table]],
            schema=QA_RESULT_ARROW_SCHEMA,
        )
        quarantine_table = pa.Table.from_pylist([], schema=QUARANTINE_ARROW_SCHEMA)
        result[table] = (
            _write_parquet_artifact(
                root,
                relative_path=f"{prefix}/qa/qa-check-result.parquet",
                table=qa_table,
                role=ArtifactRole.QA,
                table_name="qa_check_result",
            ),
            _write_parquet_artifact(
                root,
                relative_path=f"{prefix}/quarantine/quarantine-record.parquet",
                table=quarantine_table,
                role=ArtifactRole.QUARANTINE,
                table_name="quarantine_record",
            ),
        )
    return MappingProxyType(result)


def _validate_final_cardinality(
    reducer: _FullQAReducer,
    *,
    plans: Mapping[str, FullRunPlan],
    authorization: AssetFullRunPlanAuthorization,
) -> None:
    if reducer.session_count != authorization.expected_session_count:
        raise SilverStoreError("asset full completed session count changed")
    for table in _TABLE_ORDER:
        funnel = reducer.final_funnel(table)
        expected_output = authorization.expected_output_rows_by_table[table]
        expected_unmapped = authorization.expected_input_rows - expected_output
        if (
            funnel.input_rows != authorization.expected_input_rows
            or funnel.accepted_source_rows != authorization.expected_input_rows
            or funnel.exact_duplicate_excess != 0
            or funnel.quarantined_source_rows != 0
            or funnel.unmapped_source_rows != expected_unmapped
            or funnel.version_preserved_rows
            != authorization.expected_output_rows_by_table[_VERSION_TABLE]
            or funnel.output_rows_by_table != {table: expected_output}
            or plans[table].parameters.get("expected_output_rows") != expected_output
        ):
            raise SilverStoreError(f"asset full final row funnel changed for {table}")


def _full_manifest(
    *,
    intent: BuildIntent,
    outputs: tuple[ArtifactRef, ...],
    row_funnel: RowFunnel,
    qa_checks: tuple[QACheckResult, ...],
    started_at: str,
    completed_at: str,
) -> BuildManifest:
    return BuildManifest(
        intent=intent,
        outputs=outputs,
        row_funnel=row_funnel,
        qa_checks=qa_checks,
        quarantine_issue_rows=0,
        quarantine_unique_source_rows=0,
        quarantine_issue_ids_by_severity={item.value: () for item in QASeverity},
        started_at=started_at,
        completed_at=completed_at,
        preview=None,
    )


@contextmanager
def _exclusive_run_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise SilverStoreError(f"cannot open asset full run lock: {path}") from exc
    try:
        file_stat = os.fstat(descriptor)
        if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_nlink != 1:
            raise SilverStoreError("asset full run lock is not a single-link regular file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SilverStoreError(
                "another asset full materializer holds the shared run lock"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _checkpoint_path(root: Path, run_id: str) -> Path:
    return safe_relative_path(
        root,
        (f"manifests/silver/checkpoints/assets-full/run_id={run_id}/checkpoint.json"),
    )


def _load_or_initialize_checkpoint(
    root: Path,
    *,
    checkpoint_path: Path,
    run_id: str,
    plan_ids: Mapping[str, str],
    plan_shas: Mapping[str, str],
    build_ids: Mapping[str, str],
    session_dates: tuple[date, ...],
    baseline_project_bytes: int,
    started_at: str,
) -> dict[str, Any]:
    expected_identity = {
        "run_id": run_id,
        "plan_ids": dict(sorted(plan_ids.items())),
        "plan_shas": dict(sorted(plan_shas.items())),
        "build_ids": dict(build_ids),
        "session_dates": [item.isoformat() for item in session_dates],
    }
    if checkpoint_path.exists():
        checkpoint = _read_checkpoint(checkpoint_path)
        for key, expected in expected_identity.items():
            if checkpoint.get(key) != expected:
                raise SilverStoreError(f"asset full checkpoint {key} changed")
        return checkpoint
    checkpoint: dict[str, Any] = {
        "asset_full_checkpoint_version": 1,
        **expected_identity,
        "baseline_project_bytes": baseline_project_bytes,
        "completed_at": None,
        "elapsed_active_seconds": 0.0,
        "next_session_index": 0,
        "outputs_by_table": {table: [] for table in _TABLE_ORDER},
        "reducer": _FullQAReducer().to_dict(),
        "session_checkpoints": [],
        "started_at": started_at,
    }
    _write_checkpoint(checkpoint_path, checkpoint)
    return checkpoint


def _write_checkpoint(path: Path, payload: Mapping[str, Any]) -> None:
    document = {
        "payload": dict(payload),
        "payload_sha256": stable_digest(payload),
    }
    write_json_atomic(path, document)


def _read_checkpoint(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise SilverStoreError("asset full checkpoint is not a regular file")
    try:
        document = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SilverStoreError("asset full checkpoint is invalid JSON") from exc
    if not isinstance(document, dict) or set(document) != {"payload", "payload_sha256"}:
        raise SilverStoreError("asset full checkpoint envelope changed")
    payload = document["payload"]
    if not isinstance(payload, dict) or document["payload_sha256"] != stable_digest(payload):
        raise SilverStoreError("asset full checkpoint checksum changed")
    required = {
        "asset_full_checkpoint_version",
        "baseline_project_bytes",
        "build_ids",
        "completed_at",
        "elapsed_active_seconds",
        "next_session_index",
        "outputs_by_table",
        "plan_ids",
        "plan_shas",
        "reducer",
        "run_id",
        "session_checkpoints",
        "session_dates",
        "started_at",
    }
    if set(payload) != required or payload["asset_full_checkpoint_version"] != 1:
        raise SilverStoreError("asset full checkpoint fields changed")
    return payload


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )


def _write_session_checkpoint(
    root: Path,
    *,
    run_id: str,
    session_date: date,
    session_index: int,
    results: Mapping[str, AssetTableTransformResult],
    outputs: Mapping[str, ArtifactRef],
    reducer_state: Mapping[str, object],
) -> dict[str, object]:
    reducer_state_sha256 = stable_digest(reducer_state)
    document = {
        "asset_full_session_checkpoint_version": 1,
        "outputs_by_table": {table: outputs[table].to_dict() for table in _TABLE_ORDER},
        "qa_by_table": {
            table: [item.to_dict() for item in results[table].qa_checks] for table in _TABLE_ORDER
        },
        "row_funnels_by_table": {
            table: results[table].row_funnel.to_dict() for table in _TABLE_ORDER
        },
        "run_id": run_id,
        "reducer_state_sha256": reducer_state_sha256,
        "session_date": session_date.isoformat(),
        "session_index": session_index,
    }
    relative = (
        "manifests/silver/checkpoints/assets-full/"
        f"run_id={run_id}/sessions/session_date={session_date.isoformat()}.json"
    )
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, relative),
        _json_bytes(document),
        temporary_directory=root / "tmp" / "silver-asset-full-checkpoints",
    )
    return {
        "bytes": int(stored["bytes"]),
        "path": str(stored["path"]),
        "reducer_state_sha256": reducer_state_sha256,
        "session_date": session_date.isoformat(),
        "session_index": session_index,
        "sha256": str(stored["sha256"]),
    }


def _verify_checkpoint_state(
    root: Path,
    checkpoint: Mapping[str, Any],
    *,
    reader: AssetSourceReader,
    intents: Mapping[str, BuildIntent],
    output_prefixes: Mapping[str, str],
    contracts: Mapping[str, TableContract],
) -> None:
    next_index = checkpoint.get("next_session_index")
    session_checkpoints = checkpoint.get("session_checkpoints")
    outputs = checkpoint.get("outputs_by_table")
    if (
        type(next_index) is not int
        or not 0 <= next_index <= len(reader.sessions)
        or not isinstance(session_checkpoints, list)
        or len(session_checkpoints) != next_index
        or not isinstance(outputs, Mapping)
        or set(outputs) != set(_TABLE_ORDER)
    ):
        raise SilverStoreError("asset full checkpoint progress counters changed")
    reducer = _FullQAReducer.from_dict(checkpoint.get("reducer"))
    if reducer.session_count != next_index:
        raise SilverStoreError("asset full checkpoint reducer progress changed")
    reducer_sha256 = stable_digest(reducer.to_dict())
    if next_index == 0 and reducer_sha256 != stable_digest(_FullQAReducer().to_dict()):
        raise SilverStoreError("asset full empty checkpoint reducer changed")
    expected_dates = [item.session_date.isoformat() for item in reader.sessions]
    if checkpoint.get("session_dates") != expected_dates:
        raise SilverStoreError("asset full checkpoint session inventory changed")
    for index, reference in enumerate(session_checkpoints):
        if not isinstance(reference, Mapping):
            raise SilverStoreError("asset full session checkpoint reference is invalid")
        expected_date = expected_dates[index]
        if (
            reference.get("session_index") != index
            or reference.get("session_date") != expected_date
        ):
            raise SilverStoreError("asset full session checkpoint order changed")
        path = safe_relative_path(root, reference.get("path"))
        try:
            content = path.read_bytes()
        except OSError as exc:
            raise SilverStoreError("asset full session checkpoint is missing") from exc
        if (
            path.is_symlink()
            or len(content) != reference.get("bytes")
            or hashlib.sha256(content).hexdigest() != reference.get("sha256")
        ):
            raise SilverStoreError("asset full session checkpoint checksum changed")
        try:
            document = json.loads(content)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SilverStoreError("asset full session checkpoint is invalid JSON") from exc
        if (
            document.get("asset_full_session_checkpoint_version") != 1
            or document.get("run_id") != checkpoint.get("run_id")
            or document.get("session_index") != index
            or document.get("session_date") != expected_date
            or document.get("reducer_state_sha256") != reference.get("reducer_state_sha256")
            or set(document.get("qa_by_table", {})) != set(_TABLE_ORDER)
            or set(document.get("row_funnels_by_table", {})) != set(_TABLE_ORDER)
            or set(document.get("outputs_by_table", {})) != set(_TABLE_ORDER)
        ):
            raise SilverStoreError("asset full session checkpoint content changed")
        for table in _TABLE_ORDER:
            tuple(QACheckResult.from_dict(item) for item in document["qa_by_table"][table])
            RowFunnel.from_dict(document["row_funnels_by_table"][table])
            ArtifactRef.from_dict(document["outputs_by_table"][table])
        if index == next_index - 1 and document.get("reducer_state_sha256") != reducer_sha256:
            raise SilverStoreError("asset full checkpoint reducer is not session-bound")

    store = SilverStore(root)
    for table in _TABLE_ORDER:
        table_outputs = outputs[table]
        if not isinstance(table_outputs, list):
            raise SilverStoreError("asset full checkpoint outputs are invalid")
        artifacts = tuple(ArtifactRef.from_dict(item) for item in table_outputs)
        data = tuple(item for item in artifacts if item.role is ArtifactRole.DATA)
        if len(data) != next_index:
            raise SilverStoreError(f"asset full checkpoint partition count changed for {table}")
        expected_paths = {
            _partition_relative_path(output_prefixes[table], reader.sessions[index].session_date)
            for index in range(next_index)
        }
        if {item.path for item in data} != expected_paths:
            raise SilverStoreError(f"asset full checkpoint partition paths changed for {table}")
        for index, reference in enumerate(session_checkpoints):
            session_path = _partition_relative_path(
                output_prefixes[table], reader.sessions[index].session_date
            )
            fragment_path = safe_relative_path(root, reference["path"])
            fragment = json.loads(fragment_path.read_bytes())
            fragment_artifact = ArtifactRef.from_dict(fragment["outputs_by_table"][table])
            matching = next((item for item in data if item.path == session_path), None)
            if matching is None or fragment_artifact.to_dict() != matching.to_dict():
                raise SilverStoreError(
                    f"asset full checkpoint output is not session-bound for {table}"
                )
        for artifact in artifacts:
            store.verify_artifact(
                artifact,
                contract=contracts[table] if artifact.role is ArtifactRole.DATA else None,
            )
        prefix = safe_relative_path(root, SilverStore.build_output_prefix(intents[table]))
        actual: set[str] = set()
        if prefix.exists():
            for path in prefix.rglob("*"):
                if path.is_symlink():
                    raise SilverStoreError("asset full output tree contains a symlink")
                if path.is_file():
                    actual.add(path.relative_to(root).as_posix())
        declared = {item.path for item in artifacts}
        orphan_candidates: set[str] = set()
        if next_index < len(reader.sessions):
            orphan_candidates.add(
                _partition_relative_path(
                    output_prefixes[table], reader.sessions[next_index].session_date
                )
            )
        else:
            output_prefix = output_prefixes[table]
            orphan_candidates.update(
                {
                    f"{output_prefix}/qa/qa-check-result.parquet",
                    f"{output_prefix}/quarantine/quarantine-record.parquet",
                }
            )
        extras = actual - declared
        if not extras.issubset(orphan_candidates):
            raise SilverStoreError(
                f"asset full output tree contains untracked artifacts for {table}: {sorted(extras)}"
            )


def _checkpoint_output_bytes(checkpoint: Mapping[str, Any]) -> int:
    try:
        return sum(
            int(item["bytes"])
            for table in _TABLE_ORDER
            for item in checkpoint["outputs_by_table"][table]
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SilverStoreError("asset full checkpoint output bytes are invalid") from exc


def _write_build_manifest_immutable(
    root: Path,
    manifest: BuildManifest,
) -> StoredDocument:
    relative = (
        "manifests/silver/builds/"
        f"{manifest.intent.table}/build_id={manifest.build_id}/manifest.json"
    )
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, relative),
        _json_bytes(manifest.to_dict()),
        temporary_directory=root / "tmp" / "silver-asset-full-manifests",
    )
    return StoredDocument(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
    )


def _load_event_full(
    store: SilverStore,
    table: str,
    snapshot: WorkflowSnapshot,
) -> tuple[BuildManifest, StoredDocument]:
    if snapshot.state is not WorkflowState.FULL_READY:
        raise SilverStoreError(f"asset full workflow is not full_ready for {table}")
    build_id = snapshot.evidence.get("build_id")
    expected_sha = snapshot.evidence.get("build_manifest_sha256")
    expected_path = snapshot.evidence.get("build_manifest_path")
    if not all(isinstance(item, str) for item in (build_id, expected_sha, expected_path)):
        raise SilverStoreError(f"asset full event build evidence is missing for {table}")
    build, document = store.load_build(table, str(build_id))
    if (
        build.intent.kind is not BuildKind.FULL
        or build.intent.workflow_id != snapshot.workflow_id
        or document.sha256 != expected_sha
        or document.path != expected_path
    ):
        raise SilverStoreError(f"asset full event build evidence changed for {table}")
    store.verify_build(build, _CONTRACTS_BY_TABLE[table])
    return build, document


def _load_existing_full_runs(
    store: SilverStore,
    *,
    snapshots: Mapping[str, WorkflowSnapshot],
    plans: Mapping[str, FullRunPlan],
    intents: Mapping[str, BuildIntent],
    authorization: AssetFullRunPlanAuthorization,
) -> dict[str, AssetFullTableRun]:
    runs: dict[str, AssetFullTableRun] = {}
    for table in _TABLE_ORDER:
        build, document = _load_event_full(store, table, snapshots[table])
        if build.intent.to_dict() != intents[table].to_dict():
            raise SilverStoreError(f"asset full idempotent intent changed for {table}")
        expected_output = authorization.expected_output_rows_by_table[table]
        funnel = build.row_funnel
        if (
            funnel.input_rows != authorization.expected_input_rows
            or funnel.accepted_source_rows != authorization.expected_input_rows
            or funnel.exact_duplicate_excess != 0
            or funnel.quarantined_source_rows != 0
            or funnel.unmapped_source_rows != authorization.expected_input_rows - expected_output
            or funnel.version_preserved_rows
            != authorization.expected_output_rows_by_table[_VERSION_TABLE]
            or funnel.output_rows_by_table != {table: expected_output}
        ):
            raise SilverStoreError(f"asset full idempotent row funnel changed for {table}")
        partition_key = f"full_history:{authorization.date_start}:{authorization.date_end}"
        if (
            build.quarantine_issue_rows != 0
            or build.quarantine_unique_source_rows != 0
            or any(build.quarantine_issue_ids_by_severity.values())
            or any(item.blocks_publish for item in build.qa_checks)
            or {item.check_id for item in build.qa_checks}
            != set(_CONTRACTS_BY_TABLE[table].required_qa_checks)
            or any(item.partition_key != partition_key for item in build.qa_checks)
        ):
            raise SilverStoreError(f"asset full idempotent QA changed for {table}")
        data = tuple(artifact for artifact in build.outputs if artifact.role is ArtifactRole.DATA)
        qa_artifacts = tuple(
            artifact for artifact in build.outputs if artifact.role is ArtifactRole.QA
        )
        quarantine_artifacts = tuple(
            artifact for artifact in build.outputs if artifact.role is ArtifactRole.QUARANTINE
        )
        prefix = SilverStore.build_output_prefix(intents[table])
        if (
            len(data) != authorization.expected_session_count
            or sum(int(item.row_count or 0) for item in data) != expected_output
            or len(qa_artifacts) != 1
            or qa_artifacts[0].path != f"{prefix}/qa/qa-check-result.parquet"
            or qa_artifacts[0].row_count != len(_CONTRACTS_BY_TABLE[table].required_qa_checks)
            or len(quarantine_artifacts) != 1
            or quarantine_artifacts[0].path != f"{prefix}/quarantine/quarantine-record.parquet"
            or quarantine_artifacts[0].row_count != 0
            or len(build.outputs) != authorization.expected_session_count + 2
        ):
            raise SilverStoreError(f"asset full idempotent partitions changed for {table}")
        expected_paths = {
            (
                f"{SilverStore.build_output_prefix(intents[table])}/data/"
                f"session_year={session.year}/session_date={session.isoformat()}/"
                "part-00000.parquet"
            )
            for session in (
                item.date()
                for item in xcals.get_calendar("XNYS").sessions_in_range(
                    authorization.date_start,
                    authorization.date_end,
                )
            )
        }
        if {item.path for item in data} != expected_paths:
            raise SilverStoreError(f"asset full idempotent partition paths changed for {table}")
        runs[table] = AssetFullTableRun(
            workflow=snapshots[table],
            plan=plans[table],
            build=build,
            build_document=document,
        )
    return runs


def _call_barrier(barrier: Callable[[str], None] | None, label: str) -> None:
    if barrier is not None:
        barrier(label)


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
        raise SilverStoreError(f"cannot verify asset full Git checkout: {detail}")
    return completed.stdout.strip()


def _verify_git_checkout(repo_root: Path, git_commit: str) -> None:
    root = repo_root.expanduser().resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise SilverStoreError(
            "asset full code is not executing from the verified Git checkout"
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
        raise SilverStoreError("cannot verify asset full Git checkout") from exc
    if Path(top_level).resolve() != root:
        raise SilverStoreError("asset full repo_root is not the Git top level")
    if head != git_commit:
        raise SilverStoreError("asset full Git HEAD differs from --git-commit")
    if tracked_module != module_relative:
        raise SilverStoreError("asset full module is not verified tracked source")
    if status:
        raise SilverStoreError("asset full Git checkout is not clean")


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


__all__ = [
    "ASSET_FULL_POLICY_VERSION",
    "CURRENT_ASSET_FULL_AUTHORIZATION",
    "AssetFullAuthorization",
    "AssetFullRun",
    "AssetFullTableRun",
    "run_asset_full",
]
