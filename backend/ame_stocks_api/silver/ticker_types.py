"""Pure S2 transformation from verified Massive ticker-type snapshots."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
import pyarrow as pa

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import (
    QACheckResult,
    QASeverity,
    QuarantineRecord,
    QuarantineReviewStatus,
    RowFunnel,
    SilverContractError,
)
from ame_stocks_api.silver.ticker_type_contract import TICKER_TYPE_DIM_CONTRACT
from ame_stocks_api.silver.ticker_type_source import TickerTypeSourceBatch

TICKER_TYPE_DIM_TRANSFORM_VERSION = "ticker-type-dim-v1.0.0"
TICKER_TYPE_SNAPSHOT_SCOPE = "current_reference_snapshot"
TICKER_TYPE_AVAILABILITY_RULE = "first_xnys_open_after_source_capture_v1"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_REVIEWED_CODE_FORMAT = re.compile(r"^[A-Z][A-Z0-9_]{0,31}$")
_NEW_YORK = ZoneInfo("America/New_York")
_EXPECTED_FIELDS = frozenset({"asset_class", "locale", "code", "description"})
_REQUIRED_STRING_FIELDS = ("asset_class", "locale", "code")


class TickerTypeTransformError(SilverContractError):
    """Raised when a verified source cannot be represented without guessing."""


@dataclass(frozen=True, slots=True)
class TickerTypeTransformResult:
    """In-memory code-ready result; no preview or formal Silver files are written."""

    table: pa.Table
    qa_checks: tuple[QACheckResult, ...]
    quarantine_records: tuple[QuarantineRecord, ...]
    row_funnel: RowFunnel

    def __post_init__(self) -> None:
        if self.table.schema != TICKER_TYPE_DIM_CONTRACT.arrow_schema:
            raise TickerTypeTransformError("ticker-type output differs from approved schema")
        expected_checks = set(TICKER_TYPE_DIM_CONTRACT.required_qa_checks)
        actual_checks = {item.check_id for item in self.qa_checks}
        if actual_checks != expected_checks:
            raise TickerTypeTransformError("ticker-type QA differs from approved contract")

    def qa_by_id(self, check_id: str) -> QACheckResult:
        for check in self.qa_checks:
            if check.check_id == check_id:
                return check
        raise KeyError(check_id)


@dataclass(frozen=True, slots=True)
class _RowContext:
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
    def source_pointer(self) -> str:
        return f"{self.source_path}#page={self.source_page_sequence}&row={self.source_row_ordinal}"


def transform_ticker_type_batch(
    batch: TickerTypeSourceBatch,
    *,
    build_id: str,
    calendar_name: str = "XNYS",
) -> TickerTypeTransformResult:
    """Transform only the supplied verified in-memory batch."""

    if not isinstance(build_id, str) or not _SHA256.fullmatch(build_id):
        raise TickerTypeTransformError("ticker-type transform build_id must be a SHA-256")
    rows = _flatten_rows(batch, calendar_name=calendar_name)
    issues: dict[str, list[QuarantineRecord]] = defaultdict(list)
    required_invalid: set[str] = set()
    primary_conflict: set[str] = set()

    requests_by_capture: dict[date, set[str]] = defaultdict(set)
    for snapshot in batch.snapshots:
        capture_date = snapshot.source_capture_at_utc.astimezone(_NEW_YORK).date()
        requests_by_capture[capture_date].add(snapshot.source_request_id)
    invalid_snapshot_dates = {
        capture_date
        for capture_date, request_ids in requests_by_capture.items()
        if len(request_ids) != 1
    }

    for row in rows:
        _validate_description_type(row)
        if row.capture_date in invalid_snapshot_dates:
            _add_issue(
                issues,
                row,
                build_id=build_id,
                issue_code="source_snapshot_cardinality_invalid",
                field_name=None,
                observed_value=row.source_request_id,
                expected_rule="Exactly one authoritative source request per capture date.",
            )
        invalid_fields = _invalid_required_fields(row.raw)
        if invalid_fields:
            required_invalid.add(row.source_record_id)
            for field_name in invalid_fields:
                _add_issue(
                    issues,
                    row,
                    build_id=build_id,
                    issue_code="required_field_invalid_rows",
                    field_name=field_name,
                    observed_value=_observed(row.raw.get(field_name)),
                    expected_rule=(
                        "Required ticker-type fields must be nonblank provider strings."
                    ),
                )

    rows_by_primary_key: dict[tuple[date, str, str, str], list[_RowContext]] = defaultdict(list)
    for row in rows:
        if _invalid_required_fields(row.raw):
            continue
        rows_by_primary_key[
            (
                row.capture_date,
                str(row.raw["asset_class"]),
                str(row.raw["locale"]),
                str(row.raw["code"]),
            )
        ].append(row)
    for key, grouped in rows_by_primary_key.items():
        if len({row.source_row_hash for row in grouped}) <= 1:
            continue
        for row in grouped:
            primary_conflict.add(row.source_record_id)
            _add_issue(
                issues,
                row,
                build_id=build_id,
                issue_code="primary_key_conflict_rows",
                field_name="type_code",
                observed_value=json.dumps(
                    [key[0].isoformat(), key[1], key[2], key[3]],
                    separators=(",", ":"),
                ),
                expected_rule="One canonical source row per frozen ticker-type primary key.",
            )

    accepted: list[_RowContext] = []
    exact_duplicate_excess = 0
    seen_exact: set[tuple[date, str, str]] = set()
    for row in rows:
        if row.source_record_id in issues:
            continue
        exact_key = (row.capture_date, row.source_request_id, row.source_row_hash)
        if exact_key in seen_exact:
            exact_duplicate_excess += 1
            continue
        seen_exact.add(exact_key)
        accepted.append(row)

    accepted_output_rows = [_output_row(row) for row in accepted]
    lineage_invalid = sum(
        not _lineage_is_valid(row, output)
        for row, output in zip(accepted, accepted_output_rows, strict=True)
    )
    output_rows = sorted(
        accepted_output_rows,
        key=lambda item: (
            item["capture_date"],
            item["asset_class"],
            item["locale"],
            item["type_code"],
        ),
    )
    table = pa.Table.from_pylist(output_rows, schema=TICKER_TYPE_DIM_CONTRACT.arrow_schema)
    primary_key_duplicate_excess = _primary_key_duplicate_excess(output_rows)
    availability_invalid = sum(
        not _availability_is_valid(item, calendar_name) for item in output_rows
    )
    snapshot_scope_invalid = sum(
        item["snapshot_scope"] != TICKER_TYPE_SNAPSHOT_SCOPE for item in output_rows
    )

    quarantine_records = tuple(
        sorted(
            (record for records in issues.values() for record in records),
            key=lambda item: item.issue_id,
        )
    )
    row_funnel = RowFunnel(
        input_rows=len(rows),
        accepted_source_rows=len(accepted),
        exact_duplicate_excess=exact_duplicate_excess,
        quarantined_source_rows=len(issues),
        unmapped_source_rows=0,
        version_preserved_rows=0,
        output_rows_by_table={TICKER_TYPE_DIM_CONTRACT.table: table.num_rows},
    )

    retained_contexts = {row.source_record_id: row for row in accepted}
    asset_class_invalid = sum(row["asset_class"] != "stocks" for row in output_rows)
    locale_invalid = sum(row["locale"] != "us" for row in output_rows)
    description_missing = sum(
        row["description"] is None
        or (isinstance(row["description"], str) and not row["description"].strip())
        for row in output_rows
    )
    code_unreviewed = sum(
        not _REVIEWED_CODE_FORMAT.fullmatch(str(row["type_code"])) for row in output_rows
    )
    unexpected_fields = sum(
        bool(set(retained_contexts[str(row["source_record_id"])].raw) - _EXPECTED_FIELDS)
        for row in output_rows
    )
    temporal = _temporal_metrics(
        output_rows,
        capture_dates=tuple(sorted(requests_by_capture)),
    )
    metrics = {
        "schema_exact": (0, 1),
        "source_integrity_invalid": (0, batch.source_object_count),
        "source_envelope_invalid": (0, batch.page_count),
        "source_snapshot_cardinality_invalid": (
            len(invalid_snapshot_dates),
            len(requests_by_capture),
        ),
        "row_funnel_unreconciled": (0, 1),
        "required_field_invalid_rows": (len(required_invalid), len(rows)),
        "primary_key_conflict_rows": (len(primary_conflict), len(rows)),
        "primary_key_duplicate_excess": (primary_key_duplicate_excess, table.num_rows),
        "lineage_invalid_rows": (lineage_invalid, table.num_rows),
        "availability_invalid_rows": (availability_invalid, table.num_rows),
        "snapshot_scope_invalid_rows": (snapshot_scope_invalid, table.num_rows),
        "asset_class_domain_invalid_rows": (asset_class_invalid, table.num_rows),
        "locale_domain_invalid_rows": (locale_invalid, table.num_rows),
        "description_missing_or_blank_rows": (description_missing, table.num_rows),
        "type_code_format_unreviewed_rows": (code_unreviewed, table.num_rows),
        "exact_duplicate_excess_rows": (exact_duplicate_excess, len(rows)),
        "unexpected_source_field_rows": (unexpected_fields, table.num_rows),
        **temporal,
    }
    return TickerTypeTransformResult(
        table=table,
        qa_checks=_qa_results(metrics),
        quarantine_records=quarantine_records,
        row_funnel=row_funnel,
    )


def _flatten_rows(
    batch: TickerTypeSourceBatch,
    *,
    calendar_name: str,
) -> tuple[_RowContext, ...]:
    contexts: list[_RowContext] = []
    for snapshot in batch.snapshots:
        capture_at = snapshot.source_capture_at_utc.astimezone(UTC)
        capture_date = capture_at.astimezone(_NEW_YORK).date()
        available_session, available_at = _first_market_open_after(
            capture_at,
            calendar_name=calendar_name,
        )
        for page in snapshot.pages:
            for ordinal, raw in enumerate(page.rows):
                row_hash = stable_digest(dict(raw))
                record_id = stable_digest(
                    {
                        "dataset": "ticker_types",
                        "source_request_id": snapshot.source_request_id,
                        "source_artifact_sha256": page.source_artifact_sha256,
                        "source_page_sequence": page.sequence,
                        "source_row_ordinal": ordinal,
                        "source_row_hash": row_hash,
                    }
                )
                contexts.append(
                    _RowContext(
                        capture_date=capture_date,
                        source_capture_at_utc=capture_at,
                        available_session=available_session,
                        available_at_utc=available_at,
                        source_request_id=snapshot.source_request_id,
                        source_provider_request_id=page.source_provider_request_id,
                        source_artifact_sha256=page.source_artifact_sha256,
                        source_page_sequence=page.sequence,
                        source_row_ordinal=ordinal,
                        source_path=page.source_path,
                        raw=raw,
                        source_row_hash=row_hash,
                        source_record_id=record_id,
                    )
                )
    return tuple(
        sorted(
            contexts,
            key=lambda row: (
                row.source_capture_at_utc,
                row.source_request_id,
                row.source_page_sequence,
                row.source_row_ordinal,
            ),
        )
    )


def _first_market_open_after(
    capture_at_utc: datetime,
    *,
    calendar_name: str,
) -> tuple[date, datetime]:
    calendar = xcals.get_calendar(calendar_name)
    local_date = capture_at_utc.astimezone(_NEW_YORK).date()
    start = local_date - timedelta(days=1)
    end = local_date + timedelta(days=14)
    capture = pd.Timestamp(capture_at_utc)
    for session in calendar.sessions_in_range(start.isoformat(), end.isoformat()):
        opening = calendar.session_open(session)
        if opening > capture:
            return session.date(), opening.to_pydatetime().astimezone(UTC)
    raise TickerTypeTransformError("cannot find XNYS open after ticker-type capture")


def _invalid_required_fields(raw: Mapping[str, object]) -> tuple[str, ...]:
    return tuple(
        field
        for field in _REQUIRED_STRING_FIELDS
        if not isinstance(raw.get(field), str) or not str(raw[field]).strip()
    )


def _validate_description_type(row: _RowContext) -> None:
    value = row.raw.get("description")
    if value is not None and not isinstance(value, str):
        raise TickerTypeTransformError(
            f"ticker-type description has unsafe type at {row.source_pointer}"
        )


def _output_row(row: _RowContext) -> dict[str, object]:
    return {
        "capture_date": row.capture_date,
        "asset_class": row.raw["asset_class"],
        "locale": row.raw["locale"],
        "type_code": row.raw["code"],
        "description": row.raw.get("description"),
        "snapshot_scope": TICKER_TYPE_SNAPSHOT_SCOPE,
        "source_capture_at_utc": row.source_capture_at_utc,
        "available_session": row.available_session,
        "available_at_utc": row.available_at_utc,
        "availability_rule": TICKER_TYPE_AVAILABILITY_RULE,
        "source_record_id": row.source_record_id,
        "source_request_id": row.source_request_id,
        "source_provider_request_id": row.source_provider_request_id,
        "source_artifact_sha256": row.source_artifact_sha256,
        "source_page_sequence": row.source_page_sequence,
        "source_row_ordinal": row.source_row_ordinal,
        "source_row_hash": row.source_row_hash,
    }


def _lineage_is_valid(row: _RowContext, output: Mapping[str, object]) -> bool:
    row_hash = stable_digest(dict(row.raw))
    record_id = stable_digest(
        {
            "dataset": "ticker_types",
            "source_request_id": row.source_request_id,
            "source_artifact_sha256": row.source_artifact_sha256,
            "source_page_sequence": row.source_page_sequence,
            "source_row_ordinal": row.source_row_ordinal,
            "source_row_hash": row_hash,
        }
    )
    return (
        output.get("source_row_hash") == row_hash
        and output.get("source_record_id") == record_id
        and output.get("source_request_id") == row.source_request_id
        and output.get("source_provider_request_id") == row.source_provider_request_id
        and output.get("source_artifact_sha256") == row.source_artifact_sha256
        and output.get("source_page_sequence") == row.source_page_sequence
        and output.get("source_row_ordinal") == row.source_row_ordinal
        and output.get("source_capture_at_utc") == row.source_capture_at_utc
    )


def _availability_is_valid(output: Mapping[str, object], calendar_name: str) -> bool:
    captured = output.get("source_capture_at_utc")
    if not isinstance(captured, datetime):
        return False
    session, opening = _first_market_open_after(captured, calendar_name=calendar_name)
    return (
        output.get("capture_date") == captured.astimezone(_NEW_YORK).date()
        and output.get("available_session") == session
        and output.get("available_at_utc") == opening
        and output.get("availability_rule") == TICKER_TYPE_AVAILABILITY_RULE
    )


def _primary_key_duplicate_excess(rows: list[dict[str, object]]) -> int:
    counts = Counter(
        (
            row["capture_date"],
            row["asset_class"],
            row["locale"],
            row["type_code"],
        )
        for row in rows
    )
    return sum(count - 1 for count in counts.values())


def _temporal_metrics(
    rows: list[dict[str, object]],
    *,
    capture_dates: tuple[date, ...],
) -> dict[str, tuple[int, int]]:
    by_capture: dict[date, dict[tuple[str, str, str], str | None]] = defaultdict(dict)
    for capture_date in capture_dates:
        by_capture.setdefault(capture_date, {})
    for row in rows:
        key = (str(row["asset_class"]), str(row["locale"]), str(row["type_code"]))
        description = row["description"]
        by_capture[row["capture_date"]][key] = None if description is None else str(description)
    dates = sorted(by_capture)
    new_numerator = new_denominator = 0
    disappeared_numerator = disappeared_denominator = 0
    changed_numerator = changed_denominator = 0
    for prior_date, current_date in pairwise(dates):
        prior = by_capture[prior_date]
        current = by_capture[current_date]
        prior_keys = set(prior)
        current_keys = set(current)
        common = prior_keys & current_keys
        new_numerator += len(current_keys - prior_keys)
        new_denominator += len(current_keys)
        disappeared_numerator += len(prior_keys - current_keys)
        disappeared_denominator += len(prior_keys)
        changed_numerator += sum(prior[key] != current[key] for key in common)
        changed_denominator += len(common)
    return {
        "new_type_code_rows_since_prior_capture": (new_numerator, new_denominator),
        "disappeared_type_code_rows_since_prior_capture": (
            disappeared_numerator,
            disappeared_denominator,
        ),
        "description_changed_rows_since_prior_capture": (
            changed_numerator,
            changed_denominator,
        ),
    }


def _qa_results(metrics: Mapping[str, tuple[int, int]]) -> tuple[QACheckResult, ...]:
    if set(metrics) != set(TICKER_TYPE_DIM_CONTRACT.required_qa_checks):
        raise TickerTypeTransformError("ticker-type QA metric set differs from approved policy")
    checks: list[QACheckResult] = []
    for rule in TICKER_TYPE_DIM_CONTRACT.qa_rules:
        numerator, denominator = metrics[rule.check_id]
        rate = None if denominator == 0 else float(numerator / denominator)
        checks.append(
            QACheckResult(
                table=TICKER_TYPE_DIM_CONTRACT.table,
                partition_key="__all__",
                check_id=rule.check_id,
                severity=rule.severity,
                status=rule.expected_status(numerator=numerator, rate=rate),
                numerator=numerator,
                denominator=denominator,
                rate=rate,
                threshold=rule.threshold_expression,
            )
        )
    return tuple(checks)


def _add_issue(
    issues: dict[str, list[QuarantineRecord]],
    row: _RowContext,
    *,
    build_id: str,
    issue_code: str,
    field_name: str | None,
    observed_value: str | None,
    expected_rule: str,
) -> None:
    issues[row.source_record_id].append(
        QuarantineRecord(
            source_record_id=row.source_record_id,
            table_name=TICKER_TYPE_DIM_CONTRACT.table,
            issue_code=issue_code,
            severity=QASeverity.CRITICAL,
            detected_build_id=build_id,
            source_pointer=row.source_pointer,
            field_name=field_name,
            observed_value=observed_value,
            expected_rule=expected_rule,
            review_status=QuarantineReviewStatus.PENDING,
        )
    )


def _observed(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value == value.strip():
        text = value
    else:
        text = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)
    if len(text) <= 4_000:
        return text
    return f"{text[:3900]}...[sha256={stable_digest(value)}]"


__all__ = [
    "TICKER_TYPE_AVAILABILITY_RULE",
    "TICKER_TYPE_DIM_TRANSFORM_VERSION",
    "TICKER_TYPE_SNAPSHOT_SCOPE",
    "TickerTypeTransformError",
    "TickerTypeTransformResult",
    "transform_ticker_type_batch",
]
