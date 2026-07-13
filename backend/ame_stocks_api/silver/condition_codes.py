"""Pure S3 transformation from verified Massive condition-code snapshots."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from collections.abc import Mapping, Set
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
import pyarrow as pa

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.condition_code_contract import (
    CONDITION_CODE_CONTRACTS,
    CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT,
    CONDITION_CODE_DIM_CONTRACT,
)
from ame_stocks_api.silver.condition_code_source import ConditionCodeSourceBatch
from ame_stocks_api.silver.contracts import (
    QACheckResult,
    QASeverity,
    QuarantineRecord,
    QuarantineReviewStatus,
    RowFunnel,
    SilverContractError,
    TableContract,
)

CONDITION_CODE_TRANSFORM_VERSION = "condition-code-v1.0.0"
CONDITION_CODE_SNAPSHOT_SCOPE = "current_reference_snapshot"
CONDITION_CODE_AVAILABILITY_RULE = "first_xnys_open_after_source_capture_v1"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_NEW_YORK = ZoneInfo("America/New_York")
_EXPECTED_FIELDS = frozenset(
    {
        "asset_class",
        "data_types",
        "exchange",
        "id",
        "legacy",
        "name",
        "sip_mapping",
        "type",
        "update_rules",
    }
)
_REVIEWED_CONDITION_TYPES = frozenset(
    {
        "financial_status_indicator",
        "market_condition",
        "quote_condition",
        "sale_condition",
        "settlement_condition",
        "short_sale_restriction_indicator",
        "sip_generated_flag",
        "trade_thru_exempt",
    }
)
_REVIEWED_DATA_TYPES = frozenset({"bbo", "nbbo", "trade"})
_REVIEWED_SIP_KEYS = frozenset({"CTA", "FINRA_TDDS", "UTP"})
_UPDATE_GROUPS = ("consolidated", "market_center")
_UPDATE_FIELDS = ("updates_high_low", "updates_open_close", "updates_volume")


class ConditionCodeTransformError(SilverContractError):
    """Raised when condition-code input cannot be represented without guessing."""


@dataclass(frozen=True, slots=True)
class ConditionCodeTableTransformResult:
    table_name: str
    table: pa.Table
    qa_checks: tuple[QACheckResult, ...]
    quarantine_records: tuple[QuarantineRecord, ...]
    row_funnel: RowFunnel

    def __post_init__(self) -> None:
        contract = CONDITION_CODE_CONTRACTS.get(self.table_name)
        if contract is None:
            raise ConditionCodeTransformError("unknown condition-code output table")
        if self.table.schema != contract.arrow_schema:
            raise ConditionCodeTransformError(f"{self.table_name} output schema differs")
        if {item.check_id for item in self.qa_checks} != set(contract.required_qa_checks):
            raise ConditionCodeTransformError(f"{self.table_name} QA set differs")
        if any(item.table != self.table_name for item in self.qa_checks):
            raise ConditionCodeTransformError("condition-code QA table mismatch")
        if any(item.table_name != self.table_name for item in self.quarantine_records):
            raise ConditionCodeTransformError("condition-code quarantine table mismatch")

    def qa_by_id(self, check_id: str) -> QACheckResult:
        for check in self.qa_checks:
            if check.check_id == check_id:
                return check
        raise KeyError(check_id)


@dataclass(frozen=True, slots=True)
class ConditionCodeTransformResult:
    dim: ConditionCodeTableTransformResult
    bridge: ConditionCodeTableTransformResult

    def __post_init__(self) -> None:
        if self.dim.table_name != CONDITION_CODE_DIM_CONTRACT.table:
            raise ConditionCodeTransformError("condition-code dim result is mislabeled")
        if self.bridge.table_name != CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT.table:
            raise ConditionCodeTransformError("condition-code bridge result is mislabeled")

    def by_table(self, table_name: str) -> ConditionCodeTableTransformResult:
        if table_name == self.dim.table_name:
            return self.dim
        if table_name == self.bridge.table_name:
            return self.bridge
        raise KeyError(table_name)


@dataclass(frozen=True, slots=True)
class _Row:
    capture_date: date
    source_capture_at_utc: datetime
    available_session: date
    available_at_utc: datetime
    source_request_id: str
    source_provider_request_id: str
    source_artifact_sha256: str
    source_page_sequence: int
    source_row_ordinal: int
    source_path: str
    raw: Mapping[str, object]
    source_row_hash: str
    source_record_id: str

    @property
    def pointer(self) -> str:
        return f"{self.source_path}#page={self.source_page_sequence}&row={self.source_row_ordinal}"


def transform_condition_code_batch(
    batch: ConditionCodeSourceBatch,
    *,
    build_id: str,
    known_exchange_ids: Set[int],
    calendar_name: str = "XNYS",
) -> ConditionCodeTransformResult:
    """Return independent dim and bridge results without reading or writing state."""

    if not isinstance(build_id, str) or not _SHA256.fullmatch(build_id):
        raise ConditionCodeTransformError("build_id must be a SHA-256")
    if not isinstance(known_exchange_ids, Set) or any(
        type(value) is not int or value <= 0 for value in known_exchange_ids
    ):
        raise ConditionCodeTransformError("known_exchange_ids must contain positive integers")
    known = frozenset(known_exchange_ids)
    rows = _flatten(batch, calendar_name)
    dim_issues: dict[str, list[QuarantineRecord]] = defaultdict(list)
    bridge_issues: dict[str, list[QuarantineRecord]] = defaultdict(list)

    requests_by_date: dict[date, set[str]] = defaultdict(set)
    for snapshot in batch.snapshots:
        requests_by_date[snapshot.source_capture_at_utc.astimezone(_NEW_YORK).date()].add(
            snapshot.source_request_id
        )
    invalid_dates = {key for key, values in requests_by_date.items() if len(values) != 1}

    for row in rows:
        for issues, table in (
            (dim_issues, CONDITION_CODE_DIM_CONTRACT.table),
            (bridge_issues, CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT.table),
        ):
            if row.capture_date in invalid_dates:
                _issue(issues, row, table, build_id, "source_snapshot_cardinality_invalid")
            if "legacy" in row.raw and type(row.raw["legacy"]) is not bool:
                _issue(issues, row, table, build_id, "legacy_field_invalid_rows", "legacy")
            if not _valid_data_types(row.raw.get("data_types")):
                _issue(issues, row, table, build_id, "data_types_invalid_rows", "data_types")
        if not _valid_identity(row.raw, require_name=True):
            _issue(
                dim_issues,
                row,
                CONDITION_CODE_DIM_CONTRACT.table,
                build_id,
                "required_field_invalid_rows",
            )
        if not _valid_identity(row.raw, require_name=False):
            _issue(
                bridge_issues,
                row,
                CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT.table,
                build_id,
                "required_field_invalid_rows",
            )
        if not _valid_sip_mapping(row.raw.get("sip_mapping")):
            _issue(
                dim_issues,
                row,
                CONDITION_CODE_DIM_CONTRACT.table,
                build_id,
                "sip_mapping_invalid_rows",
                "sip_mapping",
            )
        if not _valid_update_rules(row.raw):
            _issue(
                dim_issues,
                row,
                CONDITION_CODE_DIM_CONTRACT.table,
                build_id,
                "update_rules_invalid_rows",
                "update_rules",
            )
        exchange = row.raw.get("exchange")
        if exchange is not None and (type(exchange) is not int or exchange <= 0):
            _issue(
                dim_issues,
                row,
                CONDITION_CODE_DIM_CONTRACT.table,
                build_id,
                "exchange_id_invalid_rows",
                "exchange",
            )

    groups: dict[tuple[object, ...], list[_Row]] = defaultdict(list)
    for row in rows:
        key = _source_key(row)
        if key is not None:
            groups[key].append(row)
    for grouped in groups.values():
        if len({row.source_row_hash for row in grouped}) <= 1:
            continue
        for row in grouped:
            _issue(
                dim_issues,
                row,
                CONDITION_CODE_DIM_CONTRACT.table,
                build_id,
                "primary_key_conflict_rows",
            )
            _issue(
                bridge_issues,
                row,
                CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT.table,
                build_id,
                "primary_key_conflict_rows",
            )

    dim_accepted, dim_dupes = _accepted(rows, dim_issues)
    bridge_accepted, bridge_dupes = _accepted(rows, bridge_issues)
    dim_rows = sorted(
        (_dim_output(row) for row in dim_accepted),
        key=lambda item: tuple(item[name] for name in CONDITION_CODE_DIM_CONTRACT.sort_by),
    )
    bridge_rows = sorted(
        (item for row in bridge_accepted for item in _bridge_output(row)),
        key=lambda item: tuple(
            item[name] for name in CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT.sort_by
        ),
    )
    dim_table = pa.Table.from_pylist(dim_rows, schema=CONDITION_CODE_DIM_CONTRACT.arrow_schema)
    bridge_table = pa.Table.from_pylist(
        bridge_rows,
        schema=CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT.arrow_schema,
    )
    dim_funnel = _funnel(
        len(rows), dim_accepted, dim_dupes, dim_issues, CONDITION_CODE_DIM_CONTRACT, len(dim_rows)
    )
    bridge_funnel = _funnel(
        len(rows),
        bridge_accepted,
        bridge_dupes,
        bridge_issues,
        CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT,
        len(bridge_rows),
    )
    dim_metrics = _dim_metrics(
        batch,
        rows,
        dim_accepted,
        dim_rows,
        dim_issues,
        dim_dupes,
        dim_funnel,
        known,
        calendar_name,
    )
    bridge_metrics = _bridge_metrics(
        batch,
        rows,
        bridge_accepted,
        bridge_rows,
        bridge_issues,
        bridge_dupes,
        bridge_funnel,
        dim_rows,
        calendar_name,
    )
    return ConditionCodeTransformResult(
        dim=_table_result(
            CONDITION_CODE_DIM_CONTRACT, dim_table, dim_metrics, dim_issues, dim_funnel
        ),
        bridge=_table_result(
            CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT,
            bridge_table,
            bridge_metrics,
            bridge_issues,
            bridge_funnel,
        ),
    )


def _flatten(batch: ConditionCodeSourceBatch, calendar_name: str) -> tuple[_Row, ...]:
    result: list[_Row] = []
    for snapshot in batch.snapshots:
        capture = snapshot.source_capture_at_utc.astimezone(UTC)
        session, available = _first_open(capture, calendar_name)
        for page in snapshot.pages:
            for ordinal, raw in enumerate(page.rows):
                row_hash = stable_digest(dict(raw))
                record_id = stable_digest(
                    {
                        "dataset": "condition_codes",
                        "source_request_id": snapshot.source_request_id,
                        "source_artifact_sha256": page.source_artifact_sha256,
                        "source_page_sequence": page.sequence,
                        "source_row_ordinal": ordinal,
                        "source_row_hash": row_hash,
                    }
                )
                result.append(
                    _Row(
                        capture.astimezone(_NEW_YORK).date(),
                        capture,
                        session,
                        available,
                        snapshot.source_request_id,
                        page.source_provider_request_id,
                        page.source_artifact_sha256,
                        page.sequence,
                        ordinal,
                        page.source_path,
                        raw,
                        row_hash,
                        record_id,
                    )
                )
    return tuple(result)


def _first_open(capture: datetime, calendar_name: str) -> tuple[date, datetime]:
    calendar = xcals.get_calendar(calendar_name)
    local = capture.astimezone(_NEW_YORK).date()
    for session in calendar.sessions_in_range(
        (local - timedelta(days=1)).isoformat(), (local + timedelta(days=14)).isoformat()
    ):
        opening = calendar.session_open(session)
        if opening > pd.Timestamp(capture):
            return session.date(), opening.to_pydatetime().astimezone(UTC)
    raise ConditionCodeTransformError("cannot find XNYS open after condition-code capture")


def _valid_identity(raw: Mapping[str, object], *, require_name: bool) -> bool:
    strings = ("asset_class", "type", "name") if require_name else ("asset_class", "type")
    return (
        type(raw.get("id")) is int
        and int(raw["id"]) > 0
        and all(isinstance(raw.get(field), str) and str(raw[field]).strip() for field in strings)
    )


def _valid_data_types(value: object) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(item, str) and item.strip() for item in value)
        and len(value) == len(set(value))
    )


def _valid_sip_mapping(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and bool(value)
        and all(
            isinstance(key, str) and key.strip() and isinstance(item, str)
            for key, item in value.items()
        )
    )


def _valid_update_rules(raw: Mapping[str, object]) -> bool:
    if "update_rules" not in raw:
        return True
    value = raw["update_rules"]
    return isinstance(value, Mapping) and all(
        isinstance(value.get(group), Mapping)
        and all(type(value[group].get(field)) is bool for field in _UPDATE_FIELDS)
        for group in _UPDATE_GROUPS
    )


def _source_key(row: _Row) -> tuple[object, ...] | None:
    if not _valid_identity(row.raw, require_name=False):
        return None
    if "legacy" in row.raw and type(row.raw["legacy"]) is not bool:
        return None
    return (
        row.capture_date,
        row.raw["asset_class"],
        row.raw["type"],
        row.raw["id"],
        row.raw.get("legacy", False),
    )


def _accepted(
    rows: tuple[_Row, ...], issues: Mapping[str, list[QuarantineRecord]]
) -> tuple[list[_Row], int]:
    accepted: list[_Row] = []
    seen: set[tuple[date, str, str]] = set()
    duplicates = 0
    for row in rows:
        if row.source_record_id in issues:
            continue
        key = (row.capture_date, row.source_request_id, row.source_row_hash)
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)
            accepted.append(row)
    return accepted, duplicates


def _base_output(row: _Row) -> dict[str, object]:
    return {
        "capture_date": row.capture_date,
        "asset_class": row.raw["asset_class"],
        "condition_type": row.raw["type"],
        "condition_id": row.raw["id"],
        "is_legacy": row.raw.get("legacy", False),
        "legacy_source_present": "legacy" in row.raw,
        "snapshot_scope": CONDITION_CODE_SNAPSHOT_SCOPE,
        "source_capture_at_utc": row.source_capture_at_utc,
        "available_session": row.available_session,
        "available_at_utc": row.available_at_utc,
        "availability_rule": CONDITION_CODE_AVAILABILITY_RULE,
        "source_record_id": row.source_record_id,
        "source_request_id": row.source_request_id,
        "source_provider_request_id": row.source_provider_request_id,
        "source_artifact_sha256": row.source_artifact_sha256,
        "source_page_sequence": row.source_page_sequence,
        "source_row_ordinal": row.source_row_ordinal,
        "source_row_hash": row.source_row_hash,
    }


def _canonical(value: object) -> str:
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _dim_output(row: _Row) -> dict[str, object]:
    output = _base_output(row)
    output.update(
        {
            "name": row.raw["name"],
            "exchange_id": row.raw.get("exchange"),
            "data_types_json": _canonical(row.raw["data_types"]),
            "sip_mapping_json": _canonical(row.raw["sip_mapping"]),
            "update_rules_json": (
                _canonical(row.raw["update_rules"]) if "update_rules" in row.raw else None
            ),
        }
    )
    for group in _UPDATE_GROUPS:
        for field in _UPDATE_FIELDS:
            output[f"{group}_{field}"] = (
                row.raw["update_rules"][group][field] if "update_rules" in row.raw else None
            )
    return output


def _bridge_output(row: _Row) -> list[dict[str, object]]:
    result = []
    for ordinal, data_type in enumerate(row.raw["data_types"]):
        output = _base_output(row)
        output.update({"data_type": data_type, "source_data_type_ordinal": ordinal})
        result.append(output)
    return result


def _version_preserved(rows: list[_Row]) -> int:
    groups: dict[tuple[object, ...], list[_Row]] = defaultdict(list)
    for row in rows:
        key = _source_key(row)
        if key is not None:
            groups[key[:-1]].append(row)
    return sum(
        len(grouped)
        for grouped in groups.values()
        if len({row.raw.get("legacy", False) for row in grouped}) > 1
    )


def _funnel(
    input_count: int,
    accepted: list[_Row],
    duplicates: int,
    issues: Mapping[str, list[QuarantineRecord]],
    contract: TableContract,
    output_count: int,
) -> RowFunnel:
    return RowFunnel(
        input_rows=input_count,
        accepted_source_rows=len(accepted),
        exact_duplicate_excess=duplicates,
        quarantined_source_rows=len(issues),
        unmapped_source_rows=0,
        version_preserved_rows=_version_preserved(accepted),
        output_rows_by_table={contract.table: output_count},
    )


def _dim_metrics(
    batch: ConditionCodeSourceBatch,
    rows: tuple[_Row, ...],
    accepted: list[_Row],
    output: list[dict[str, object]],
    issues: Mapping[str, list[QuarantineRecord]],
    duplicates: int,
    funnel: RowFunnel,
    known_exchange_ids: frozenset[int],
    calendar_name: str,
) -> dict[str, tuple[int, int]]:
    metrics = _base_metrics(
        batch, rows, accepted, output, issues, duplicates, funnel, calendar_name
    )
    accepted_by_id = {row.source_record_id: row for row in accepted}
    metrics.update(
        {
            "sip_mapping_invalid_rows": (
                _issue_count(issues, "sip_mapping_invalid_rows"),
                len(rows),
            ),
            "update_rules_invalid_rows": (
                _issue_count(issues, "update_rules_invalid_rows"),
                len(rows),
            ),
            "exchange_id_invalid_rows": (
                _issue_count(issues, "exchange_id_invalid_rows"),
                len(rows),
            ),
            "exchange_fk_unresolved_rows": (
                sum(
                    item["exchange_id"] is not None
                    and item["exchange_id"] not in known_exchange_ids
                    for item in output
                ),
                len(output),
            ),
            "canonical_json_invalid_rows": (
                sum(
                    not _dim_json_valid(
                        accepted_by_id[str(item["source_record_id"])],
                        item,
                    )
                    for item in output
                ),
                len(output),
            ),
            "update_rule_flatten_mismatch_rows": (
                sum(not _flatten_valid(item) for item in output),
                len(output),
            ),
            "sip_mapping_key_unreviewed_rows": (
                sum(bool(set(row.raw["sip_mapping"]) - _REVIEWED_SIP_KEYS) for row in accepted),
                len(output),
            ),
            "unexpected_update_rule_field_rows": (
                sum(_unexpected_update_rules(row.raw) for row in accepted),
                len(output),
            ),
        }
    )
    return metrics


def _bridge_metrics(
    batch: ConditionCodeSourceBatch,
    rows: tuple[_Row, ...],
    accepted: list[_Row],
    output: list[dict[str, object]],
    issues: Mapping[str, list[QuarantineRecord]],
    duplicates: int,
    funnel: RowFunnel,
    dim_output: list[dict[str, object]],
    calendar_name: str,
) -> dict[str, tuple[int, int]]:
    metrics = _base_metrics(
        batch, rows, accepted, output, issues, duplicates, funnel, calendar_name
    )
    expected = sum(len(row.raw["data_types"]) for row in accepted)
    dim_keys = {_pk(row, CONDITION_CODE_DIM_CONTRACT.primary_key) for row in dim_output}
    bridge_parent = CONDITION_CODE_DIM_CONTRACT.primary_key
    bridge_keys = {_pk(row, bridge_parent) for row in output}
    metrics.update(
        {
            "expansion_unreconciled": (int(expected != len(output)), 1),
            "source_data_type_ordinal_invalid_rows": (
                sum(not _ordinal_valid(row, accepted) for row in output),
                len(output),
            ),
            "parent_dim_missing_rows": (
                sum(_pk(row, bridge_parent) not in dim_keys for row in output),
                len(output),
            ),
            "dim_without_bridge_rows": (
                sum(key not in bridge_keys for key in dim_keys),
                len(dim_keys),
            ),
        }
    )
    return metrics


def _base_metrics(
    batch: ConditionCodeSourceBatch,
    rows: tuple[_Row, ...],
    accepted: list[_Row],
    output: list[dict[str, object]],
    issues: Mapping[str, list[QuarantineRecord]],
    duplicates: int,
    funnel: RowFunnel,
    calendar_name: str,
) -> dict[str, tuple[int, int]]:
    return {
        "schema_exact": (0, 1),
        "source_integrity_invalid": (0, batch.source_object_count),
        "source_envelope_invalid": (0, batch.page_count),
        "source_snapshot_cardinality_invalid": (
            _issue_count(issues, "source_snapshot_cardinality_invalid"),
            len(rows),
        ),
        "row_funnel_unreconciled": (int(input_rows_unreconciled(funnel)), 1),
        "required_field_invalid_rows": (
            _issue_count(issues, "required_field_invalid_rows"),
            len(rows),
        ),
        "legacy_field_invalid_rows": (_issue_count(issues, "legacy_field_invalid_rows"), len(rows)),
        "data_types_invalid_rows": (_issue_count(issues, "data_types_invalid_rows"), len(rows)),
        "primary_key_conflict_rows": (_issue_count(issues, "primary_key_conflict_rows"), len(rows)),
        "primary_key_duplicate_excess": (_duplicate_excess(output), len(output)),
        "lineage_invalid_rows": (
            sum(not _lineage_valid(row, item) for row, item in _lineage_pairs(accepted, output)),
            len(output),
        ),
        "availability_invalid_rows": (
            sum(not _availability_valid(item, calendar_name) for item in output),
            len(output),
        ),
        "snapshot_scope_invalid_rows": (
            sum(item["snapshot_scope"] != CONDITION_CODE_SNAPSHOT_SCOPE for item in output),
            len(output),
        ),
        "asset_class_domain_invalid_rows": (
            sum(item["asset_class"] != "stocks" for item in output),
            len(output),
        ),
        "condition_type_unreviewed_rows": (
            sum(item["condition_type"] not in _REVIEWED_CONDITION_TYPES for item in output),
            len(output),
        ),
        "data_type_unreviewed_rows": (_unreviewed_data_types(output), len(output)),
        "unexpected_source_field_rows": (
            sum(bool(set(row.raw) - _EXPECTED_FIELDS) for row in accepted),
            len(accepted),
        ),
        "exact_duplicate_excess_rows": (duplicates, len(rows)),
        "current_legacy_versions_unpreserved_rows": (
            _unpreserved_versions(accepted, output),
            len(accepted),
        ),
    }


def _table_result(contract, table, metrics, issues, funnel):
    if set(metrics) != set(contract.required_qa_checks):
        raise ConditionCodeTransformError(f"{contract.table} QA metric set differs")
    checks = []
    for rule in contract.qa_rules:
        numerator, denominator = metrics[rule.check_id]
        rate = None if denominator == 0 else float(numerator / denominator)
        checks.append(
            QACheckResult(
                contract.table,
                "__all__",
                rule.check_id,
                rule.severity,
                rule.expected_status(numerator=numerator, rate=rate),
                numerator,
                denominator,
                rate,
                rule.threshold_expression,
            )
        )
    records = tuple(
        sorted(
            (record for values in issues.values() for record in values),
            key=lambda item: item.issue_id,
        )
    )
    return ConditionCodeTableTransformResult(contract.table, table, tuple(checks), records, funnel)


def _issue(issues, row, table, build_id, code, field=None, severity=QASeverity.CRITICAL):
    issues[row.source_record_id].append(
        QuarantineRecord(
            row.source_record_id,
            table,
            code,
            severity,
            build_id,
            row.pointer,
            field,
            _observed(row.raw.get(field)) if field else None,
            f"S3 condition-code rule {code} must hold.",
            QuarantineReviewStatus.PENDING,
        )
    )


def _issue_count(issues, code):
    return len(
        {key for key, values in issues.items() if any(item.issue_code == code for item in values)}
    )


def _pk(row, names):
    return tuple(row[name] for name in names)


def _duplicate_excess(rows):
    if not rows:
        return 0
    contract = (
        CONDITION_CODE_DATA_TYPE_BRIDGE_CONTRACT
        if "data_type" in rows[0]
        else CONDITION_CODE_DIM_CONTRACT
    )
    return sum(
        count - 1 for count in Counter(_pk(row, contract.primary_key) for row in rows).values()
    )


def _lineage_pairs(accepted, output):
    by_id = {row.source_record_id: row for row in accepted}
    return ((by_id[item["source_record_id"]], item) for item in output)


def _lineage_valid(row, output):
    row_hash = stable_digest(dict(row.raw))
    record_id = stable_digest(
        {
            "dataset": "condition_codes",
            "source_request_id": row.source_request_id,
            "source_artifact_sha256": row.source_artifact_sha256,
            "source_page_sequence": row.source_page_sequence,
            "source_row_ordinal": row.source_row_ordinal,
            "source_row_hash": row_hash,
        }
    )
    return all(
        (
            output["source_capture_at_utc"] == row.source_capture_at_utc,
            output["source_request_id"] == row.source_request_id,
            output["source_provider_request_id"] == row.source_provider_request_id,
            output["source_artifact_sha256"] == row.source_artifact_sha256,
            output["source_page_sequence"] == row.source_page_sequence,
            output["source_row_ordinal"] == row.source_row_ordinal,
            output["source_row_hash"] == row_hash == row.source_row_hash,
            output["source_record_id"] == record_id == row.source_record_id,
        )
    )


def _availability_valid(output, calendar_name):
    session, opening = _first_open(output["source_capture_at_utc"], calendar_name)
    return (
        output["capture_date"] == output["source_capture_at_utc"].astimezone(_NEW_YORK).date()
        and output["available_session"] == session
        and output["available_at_utc"] == opening
        and output["availability_rule"] == CONDITION_CODE_AVAILABILITY_RULE
    )


def _dim_json_valid(row, output):
    data_types_json = output["data_types_json"]
    sip_mapping_json = output["sip_mapping_json"]
    if (
        not isinstance(data_types_json, str)
        or data_types_json != _canonical(row.raw["data_types"])
        or json.loads(data_types_json) != row.raw["data_types"]
        or not isinstance(sip_mapping_json, str)
        or sip_mapping_json != _canonical(row.raw["sip_mapping"])
        or json.loads(sip_mapping_json) != row.raw["sip_mapping"]
    ):
        return False
    if "update_rules" not in row.raw:
        return output["update_rules_json"] is None
    update_rules_json = output["update_rules_json"]
    return (
        isinstance(update_rules_json, str)
        and update_rules_json == _canonical(row.raw["update_rules"])
        and json.loads(update_rules_json) == row.raw["update_rules"]
    )


def _flatten_valid(output):
    if output["update_rules_json"] is None:
        return all(
            output[f"{group}_{field}"] is None
            for group in _UPDATE_GROUPS
            for field in _UPDATE_FIELDS
        )
    rules = json.loads(output["update_rules_json"])
    return all(
        output[f"{group}_{field}"] == rules[group][field]
        for group in _UPDATE_GROUPS
        for field in _UPDATE_FIELDS
    )


def _unexpected_update_rules(raw):
    if "update_rules" not in raw:
        return 0
    rules = raw["update_rules"]
    return int(
        bool(set(rules) - set(_UPDATE_GROUPS))
        or any(bool(set(rules[group]) - set(_UPDATE_FIELDS)) for group in _UPDATE_GROUPS)
    )


def _ordinal_valid(output, accepted):
    source = next(
        (row for row in accepted if row.source_record_id == output["source_record_id"]), None
    )
    ordinal = output["source_data_type_ordinal"]
    return (
        source is not None
        and ordinal < len(source.raw["data_types"])
        and source.raw["data_types"][ordinal] == output["data_type"]
    )


def _unreviewed_data_types(output):
    if not output:
        return 0
    if "data_type" in output[0]:
        return sum(row["data_type"] not in _REVIEWED_DATA_TYPES for row in output)
    return sum(
        bool(set(json.loads(row["data_types_json"])) - _REVIEWED_DATA_TYPES) for row in output
    )


def _unpreserved_versions(accepted, output):
    source = Counter(_source_key(row) for row in accepted)
    if output and "data_type" in output[0]:
        actual = Counter(_pk(row, CONDITION_CODE_DIM_CONTRACT.primary_key) for row in output)
        return sum(1 for key, count in source.items() if actual[key] < count)
    actual = Counter(_pk(row, CONDITION_CODE_DIM_CONTRACT.primary_key) for row in output)
    return sum(max(count - actual[key], 0) for key, count in source.items())


def input_rows_unreconciled(funnel):
    return (
        funnel.input_rows
        != funnel.accepted_source_rows
        + funnel.exact_duplicate_excess
        + funnel.quarantined_source_rows
    )


def _observed(value):
    if value is None:
        return None
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)[:4000]


__all__ = [
    "CONDITION_CODE_AVAILABILITY_RULE",
    "CONDITION_CODE_SNAPSHOT_SCOPE",
    "CONDITION_CODE_TRANSFORM_VERSION",
    "ConditionCodeTableTransformResult",
    "ConditionCodeTransformError",
    "ConditionCodeTransformResult",
    "transform_condition_code_batch",
]
