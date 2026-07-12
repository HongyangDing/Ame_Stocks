"""Pure S1 transformation from verified Massive exchange snapshots to exchange_dim."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from urllib.parse import urlparse
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
from ame_stocks_api.silver.exchange_contract import EXCHANGE_DIM_CONTRACT
from ame_stocks_api.silver.exchange_source import ExchangeSourceBatch

EXCHANGE_DIM_TRANSFORM_VERSION = "exchange-dim-v1.0.0"
EXCHANGE_SNAPSHOT_SCOPE = "current_reference_snapshot"
EXCHANGE_AVAILABILITY_RULE = "first_xnys_open_after_source_capture_v1"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MIC = re.compile(r"^[A-Z0-9]{4}$")
_NEW_YORK = ZoneInfo("America/New_York")
_EXPECTED_FIELDS = frozenset(
    {
        "acronym",
        "asset_class",
        "id",
        "locale",
        "mic",
        "name",
        "operating_mic",
        "participant_id",
        "type",
        "url",
    }
)
_REQUIRED_STRING_FIELDS = ("name", "type", "asset_class", "locale")
_OPTIONAL_STRING_FIELDS = ("acronym", "mic", "operating_mic", "participant_id", "url")
_REVIEWED_EXCHANGE_TYPES = frozenset({"exchange", "ORF", "SIP", "TRF"})


class ExchangeTransformError(SilverContractError):
    """Raised when a verified source cannot be represented without guessing."""


@dataclass(frozen=True, slots=True)
class ExchangeTransformResult:
    """In-memory code-ready result; no preview or formal Silver files are written."""

    table: pa.Table
    qa_checks: tuple[QACheckResult, ...]
    quarantine_records: tuple[QuarantineRecord, ...]
    row_funnel: RowFunnel

    def __post_init__(self) -> None:
        if self.table.schema != EXCHANGE_DIM_CONTRACT.arrow_schema:
            raise ExchangeTransformError("exchange output does not match the approved schema")
        expected_checks = set(EXCHANGE_DIM_CONTRACT.required_qa_checks)
        actual_checks = {item.check_id for item in self.qa_checks}
        if actual_checks != expected_checks:
            raise ExchangeTransformError("exchange QA results do not match the approved contract")

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
        return (
            f"{self.source_path}#page={self.source_page_sequence}"
            f"&row={self.source_row_ordinal}"
        )


def transform_exchange_batch(
    batch: ExchangeSourceBatch,
    *,
    build_id: str,
    calendar_name: str = "XNYS",
) -> ExchangeTransformResult:
    """Transform a verified batch without reading, writing, or registering other state."""

    if not isinstance(build_id, str) or not _SHA256.fullmatch(build_id):
        raise ExchangeTransformError("exchange transform build_id must be a SHA-256")
    rows = _flatten_rows(batch, calendar_name=calendar_name)
    issues: dict[str, list[QuarantineRecord]] = defaultdict(list)
    required_invalid: set[str] = set()
    primary_conflict: set[str] = set()
    mic_conflict: set[str] = set()
    invalid_mic_values = 0
    invalid_asset_class: set[str] = set()
    invalid_locale: set[str] = set()

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
        _validate_optional_types(row)
        if row.capture_date in invalid_snapshot_dates:
            _add_issue(
                issues,
                row,
                build_id=build_id,
                issue_code="source_snapshot_cardinality_invalid",
                severity=QASeverity.CRITICAL,
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
                    severity=QASeverity.CRITICAL,
                    field_name=field_name,
                    observed_value=_observed(row.raw.get(field_name)),
                    expected_rule="Required exchange fields must be present and correctly typed.",
                )
        for field_name in ("mic", "operating_mic"):
            value = row.raw.get(field_name)
            if isinstance(value, str) and not _MIC.fullmatch(value):
                invalid_mic_values += 1
                _add_issue(
                    issues,
                    row,
                    build_id=build_id,
                    issue_code="mic_format_invalid_values",
                    severity=QASeverity.HIGH,
                    field_name=field_name,
                    observed_value=value,
                    expected_rule="MIC values must match four uppercase ASCII letters or digits.",
                )
        if row.raw.get("asset_class") != "stocks":
            invalid_asset_class.add(row.source_record_id)
            _add_issue(
                issues,
                row,
                build_id=build_id,
                issue_code="asset_class_domain_invalid_rows",
                severity=QASeverity.HIGH,
                field_name="asset_class",
                observed_value=_observed(row.raw.get("asset_class")),
                expected_rule="The approved exchange source domain is asset_class=stocks.",
            )
        if row.raw.get("locale") != "us":
            invalid_locale.add(row.source_record_id)
            _add_issue(
                issues,
                row,
                build_id=build_id,
                issue_code="locale_domain_invalid_rows",
                severity=QASeverity.HIGH,
                field_name="locale",
                observed_value=_observed(row.raw.get("locale")),
                expected_rule="The approved exchange source domain is locale=us.",
            )

    rows_by_primary_key: dict[tuple[date, int], list[_RowContext]] = defaultdict(list)
    rows_by_mic: dict[tuple[date, str], list[_RowContext]] = defaultdict(list)
    for row in rows:
        exchange_id = row.raw.get("id")
        if type(exchange_id) is int and exchange_id > 0:
            rows_by_primary_key[(row.capture_date, exchange_id)].append(row)
            mic = row.raw.get("mic")
            if isinstance(mic, str):
                rows_by_mic[(row.capture_date, mic)].append(row)

    for (capture_date, exchange_id), grouped in rows_by_primary_key.items():
        if len({row.source_row_hash for row in grouped}) <= 1:
            continue
        for row in grouped:
            primary_conflict.add(row.source_record_id)
            _add_issue(
                issues,
                row,
                build_id=build_id,
                issue_code="primary_key_conflict_rows",
                severity=QASeverity.CRITICAL,
                field_name="exchange_id",
                observed_value=str(exchange_id),
                expected_rule=(
                    f"One canonical source row per capture_date={capture_date.isoformat()} "
                    "and exchange_id."
                ),
            )

    for (capture_date, mic), grouped in rows_by_mic.items():
        identifiers = {
            row.raw["id"]
            for row in grouped
            if type(row.raw.get("id")) is int and int(row.raw["id"]) > 0
        }
        if len(identifiers) <= 1:
            continue
        for row in grouped:
            mic_conflict.add(row.source_record_id)
            _add_issue(
                issues,
                row,
                build_id=build_id,
                issue_code="mic_conflict_rows",
                severity=QASeverity.CRITICAL,
                field_name="mic",
                observed_value=mic,
                expected_rule=(
                    f"One exchange_id per non-null MIC within capture_date="
                    f"{capture_date.isoformat()}."
                ),
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
        key=lambda item: (item["capture_date"], item["exchange_id"]),
    )
    table = pa.Table.from_pylist(output_rows, schema=EXCHANGE_DIM_CONTRACT.arrow_schema)
    primary_key_duplicate_excess = _primary_key_duplicate_excess(output_rows)
    availability_invalid = sum(
        not _availability_is_valid(item, calendar_name) for item in output_rows
    )
    snapshot_scope_invalid = sum(
        item["snapshot_scope"] != EXCHANGE_SNAPSHOT_SCOPE for item in output_rows
    )

    quarantine_records = tuple(
        sorted(
            (record for records in issues.values() for record in records),
            key=lambda item: item.issue_id,
        )
    )
    quarantined_source_ids = set(issues)
    row_funnel = RowFunnel(
        input_rows=len(rows),
        accepted_source_rows=len(accepted),
        exact_duplicate_excess=exact_duplicate_excess,
        quarantined_source_rows=len(quarantined_source_ids),
        unmapped_source_rows=0,
        version_preserved_rows=0,
        output_rows_by_table={EXCHANGE_DIM_CONTRACT.table: table.num_rows},
    )

    nonnull_mic_values = sum(
        isinstance(row.raw.get(field), str)
        for row in rows
        for field in ("mic", "operating_mic")
    )
    nonnull_urls = sum(isinstance(row.raw.get("url"), str) for row in rows)
    unreviewed_types = sum(row.raw.get("type") not in _REVIEWED_EXCHANGE_TYPES for row in rows)
    unexpected_fields = sum(bool(set(row.raw).difference(_EXPECTED_FIELDS)) for row in rows)
    empty_optional = sum(
        any(
            isinstance(row.raw.get(field), str) and not str(row.raw[field]).strip()
            for field in _OPTIONAL_STRING_FIELDS
        )
        for row in rows
    )
    invalid_urls = sum(
        isinstance(row.raw.get("url"), str) and not _valid_absolute_http_url(str(row.raw["url"]))
        for row in rows
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
        "mic_conflict_rows": (len(mic_conflict), len(rows)),
        "mic_format_invalid_values": (invalid_mic_values, nonnull_mic_values),
        "asset_class_domain_invalid_rows": (len(invalid_asset_class), len(rows)),
        "locale_domain_invalid_rows": (len(invalid_locale), len(rows)),
        "unreviewed_exchange_type_rows": (unreviewed_types, len(rows)),
        "exact_duplicate_excess_rows": (exact_duplicate_excess, len(rows)),
        "unexpected_source_field_rows": (unexpected_fields, len(rows)),
        "empty_optional_string_rows": (empty_optional, len(rows)),
        "url_invalid_rows": (invalid_urls, nonnull_urls),
    }
    checks = _qa_results(metrics)
    return ExchangeTransformResult(
        table=table,
        qa_checks=checks,
        quarantine_records=quarantine_records,
        row_funnel=row_funnel,
    )


def _flatten_rows(batch: ExchangeSourceBatch, *, calendar_name: str) -> tuple[_RowContext, ...]:
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
                        "dataset": "exchanges",
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
    raise ExchangeTransformError("cannot find an XNYS open after exchange capture")


def _invalid_required_fields(raw: Mapping[str, object]) -> tuple[str, ...]:
    invalid: list[str] = []
    exchange_id = raw.get("id")
    if type(exchange_id) is not int or exchange_id <= 0:
        invalid.append("id")
    for field in _REQUIRED_STRING_FIELDS:
        value = raw.get(field)
        if not isinstance(value, str) or not value.strip():
            invalid.append(field)
    return tuple(invalid)


def _validate_optional_types(row: _RowContext) -> None:
    for field in _OPTIONAL_STRING_FIELDS:
        value = row.raw.get(field)
        if value is not None and not isinstance(value, str):
            raise ExchangeTransformError(
                f"optional exchange field {field} has an unsafe type at {row.source_pointer}"
            )


def _output_row(row: _RowContext) -> dict[str, object]:
    raw = row.raw
    return {
        "capture_date": row.capture_date,
        "exchange_id": raw["id"],
        "name": raw["name"],
        "acronym": raw.get("acronym"),
        "mic": raw.get("mic"),
        "operating_mic": raw.get("operating_mic"),
        "participant_id": raw.get("participant_id"),
        "exchange_type": raw["type"],
        "asset_class": raw["asset_class"],
        "locale": raw["locale"],
        "url": raw.get("url"),
        "snapshot_scope": EXCHANGE_SNAPSHOT_SCOPE,
        "source_capture_at_utc": row.source_capture_at_utc,
        "available_session": row.available_session,
        "available_at_utc": row.available_at_utc,
        "availability_rule": EXCHANGE_AVAILABILITY_RULE,
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
            "dataset": "exchanges",
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
        and output.get("source_artifact_sha256") == row.source_artifact_sha256
        and output.get("source_page_sequence") == row.source_page_sequence
        and output.get("source_row_ordinal") == row.source_row_ordinal
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
        and output.get("availability_rule") == EXCHANGE_AVAILABILITY_RULE
    )


def _primary_key_duplicate_excess(rows: list[dict[str, object]]) -> int:
    counts = Counter((row["capture_date"], row["exchange_id"]) for row in rows)
    return sum(count - 1 for count in counts.values())


def _qa_results(metrics: Mapping[str, tuple[int, int]]) -> tuple[QACheckResult, ...]:
    if set(metrics) != set(EXCHANGE_DIM_CONTRACT.required_qa_checks):
        raise ExchangeTransformError("exchange QA metric set differs from approved policy")
    checks: list[QACheckResult] = []
    for rule in EXCHANGE_DIM_CONTRACT.qa_rules:
        numerator, denominator = metrics[rule.check_id]
        rate = None if denominator == 0 else float(numerator / denominator)
        checks.append(
            QACheckResult(
                table=EXCHANGE_DIM_CONTRACT.table,
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
    severity: QASeverity,
    field_name: str | None,
    observed_value: str | None,
    expected_rule: str,
) -> None:
    issues[row.source_record_id].append(
        QuarantineRecord(
            source_record_id=row.source_record_id,
            table_name=EXCHANGE_DIM_CONTRACT.table,
            issue_code=issue_code,
            severity=severity,
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


def _valid_absolute_http_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


__all__ = [
    "EXCHANGE_AVAILABILITY_RULE",
    "EXCHANGE_DIM_TRANSFORM_VERSION",
    "EXCHANGE_SNAPSHOT_SCOPE",
    "ExchangeTransformError",
    "ExchangeTransformResult",
    "transform_exchange_batch",
]
