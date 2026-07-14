from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ame_stocks_api.cli.silver_ticker_events_lifecycle import build_parser
from ame_stocks_api.silver import ticker_event_lifecycle as lifecycle
from ame_stocks_api.silver.contracts import (
    QUARANTINE_ARROW_SCHEMA,
    ArtifactRole,
    QACheckResult,
    QASeverity,
    QAStatus,
    TableContract,
)
from ame_stocks_api.silver.store import SilverStoreError
from ame_stocks_api.silver.ticker_event_contract import (
    TICKER_CHANGE_EVENT_CONTRACT,
    TICKER_EVENT_REQUEST_STATUS_CONTRACT,
)


def test_production_authorization_and_cli_surface_are_fixed() -> None:
    authorization = lifecycle.CURRENT_TICKER_EVENT_AUTHORIZATION

    assert authorization.formal_receipt_path == (
        lifecycle.PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_PATH
    )
    assert authorization.formal_receipt_sha256 == (
        lifecycle.PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_SHA256
    )
    assert authorization.pilot_receipt_path == (lifecycle.PRODUCTION_PILOT_IDENTIFIER_RECEIPT_PATH)
    assert authorization.pilot_receipt_sha256 == (
        lifecycle.PRODUCTION_PILOT_IDENTIFIER_RECEIPT_SHA256
    )
    assert (
        authorization.expected_formal_requests,
        authorization.expected_complete_requests,
        authorization.expected_not_found_requests,
        authorization.expected_raw_events,
        authorization.expected_event_rows,
        authorization.expected_blank_targets,
        authorization.expected_pilot_requests,
    ) == (15_173, 11_471, 3_702, 13_088, 12_895, 193, 100)
    assert lifecycle.S5_COMPLETION_AUTHORIZATION == (
        "我建议如果中间没发生预期外的事情，直接把S5推进到结束吧"  # noqa: RUF001
    )
    assert lifecycle.S5_DATE_QUALITY_AUTHORIZATION == (
        "批准 S5 日期质量方案，本来我们也不关心这么远的日期"  # noqa: RUF001
    )
    assert {action.dest for action in build_parser()._actions} == {
        "help",
        "data_root",
        "repo_root",
        "git_commit",
    }


@pytest.mark.parametrize(
    "overrides",
    [
        {"formal_receipt_path": "manifests/plans/ticker_events/alternate.txt"},
        {"expected_not_found_requests": 3_701},
        {"sample_limit": 101},
    ],
)
def test_production_authorization_rejects_scope_overrides(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match=r"production-authorized|cardinality|sample"):
        lifecycle.TickerEventAuthorization(**overrides)


def _small_parent_child_tables() -> tuple[pa.Table, pa.Table]:
    status = pa.Table.from_pylist(
        [
            {
                "source_request_id": "complete-a",
                "request_outcome": "complete_timeline",
                "raw_event_count": 2,
                "accepted_event_count": 1,
                "quarantined_event_count": 1,
                "backtest_identity_eligible": False,
            },
            {
                "source_request_id": "complete-b",
                "request_outcome": "complete_timeline",
                "raw_event_count": 1,
                "accepted_event_count": 1,
                "quarantined_event_count": 0,
                "backtest_identity_eligible": False,
            },
            {
                "source_request_id": "not-found",
                "request_outcome": "not_found_404",
                "raw_event_count": 0,
                "accepted_event_count": 0,
                "quarantined_event_count": 0,
                "backtest_identity_eligible": False,
            },
        ]
    )
    events = pa.Table.from_pylist(
        [
            {
                "source_record_id": "event-a",
                "source_request_id": "complete-a",
                "backtest_identity_eligible": False,
            },
            {
                "source_record_id": "event-b",
                "source_request_id": "complete-b",
                "backtest_identity_eligible": False,
            },
        ]
    )
    return status, events


def _patch_small_cardinalities(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lifecycle, "EXPECTED_FORMAL_REQUESTS", 3)
    monkeypatch.setattr(lifecycle, "EXPECTED_COMPLETE_REQUESTS", 2)
    monkeypatch.setattr(lifecycle, "EXPECTED_NOT_FOUND_REQUESTS", 1)
    monkeypatch.setattr(lifecycle, "EXPECTED_RAW_EVENTS", 3)
    monkeypatch.setattr(lifecycle, "EXPECTED_EVENT_ROWS", 2)
    monkeypatch.setattr(lifecycle, "EXPECTED_BLANK_TARGETS", 1)


def test_pair_integrity_reconciles_complete_children_and_zero_event_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_small_cardinalities(monkeypatch)
    status, events = _small_parent_child_tables()

    lifecycle._require_pair_integrity(status, events)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("not_found_child", "non-complete parent"),
        ("orphan_child", "non-complete parent"),
        ("parent_eligible", "parent became backtest identity eligible"),
        ("child_eligible", "event became backtest identity eligible"),
    ],
)
def test_pair_integrity_fails_closed_on_invalid_identity_relations(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    _patch_small_cardinalities(monkeypatch)
    status, events = _small_parent_child_tables()
    status_rows = status.to_pylist()
    event_rows = events.to_pylist()

    if mutation == "not_found_child":
        event_rows[0]["source_request_id"] = "not-found"
    elif mutation == "orphan_child":
        event_rows[0]["source_request_id"] = "absent-parent"
    elif mutation == "parent_eligible":
        status_rows[0]["backtest_identity_eligible"] = True
    else:
        event_rows[0]["backtest_identity_eligible"] = True

    with pytest.raises(SilverStoreError, match=message):
        lifecycle._require_pair_integrity(
            pa.Table.from_pylist(status_rows), pa.Table.from_pylist(event_rows)
        )


def _expected_qa(contract: TableContract) -> tuple[QACheckResult, ...]:
    expected_nonzero = lifecycle._EXPECTED_WARNING_NUMERATORS[contract.table]
    denominator = 20_000
    return tuple(
        QACheckResult(
            table=contract.table,
            partition_key=f"{contract.partition_by[0]}=2026-07-11",
            check_id=rule.check_id,
            severity=rule.severity,
            status=(QAStatus.WARNING if rule.check_id in expected_nonzero else QAStatus.PASSED),
            numerator=expected_nonzero.get(rule.check_id, 0),
            denominator=denominator,
            rate=expected_nonzero.get(rule.check_id, 0) / denominator,
            threshold=rule.threshold_expression,
        )
        for rule in contract.qa_rules
    )


@pytest.mark.parametrize(
    "contract",
    [TICKER_EVENT_REQUEST_STATUS_CONTRACT, TICKER_CHANGE_EVENT_CONTRACT],
    ids=lambda item: item.table,
)
def test_expected_qa_accepts_only_the_reviewed_warning_profile(
    contract: TableContract,
) -> None:
    lifecycle._require_expected_qa(_expected_qa(contract), contract)


@pytest.mark.parametrize("mutation", ["warning_count", "unknown_nonzero"])
def test_expected_qa_rejects_warning_profile_drift(mutation: str) -> None:
    contract = TICKER_EVENT_REQUEST_STATUS_CONTRACT
    checks = list(_expected_qa(contract))
    if mutation == "warning_count":
        index = next(
            index
            for index, item in enumerate(checks)
            if item.check_id == "identifier_not_found_404_requests"
        )
    else:
        index = next(
            index
            for index, item in enumerate(checks)
            if item.check_id == "request_outcome_changed_since_prior_capture"
        )
    check = checks[index]
    checks[index] = replace(
        check,
        status=QAStatus.WARNING,
        numerator=check.numerator + 1,
        rate=(check.numerator + 1) / check.denominator,
    )

    with pytest.raises(SilverStoreError, match="reviewed warning profile changed"):
        lifecycle._require_expected_qa(tuple(checks), contract)


def test_approval_exceptions_are_the_exact_warning_and_high_quarantine_ids() -> None:
    checks = _expected_qa(TICKER_EVENT_REQUEST_STATUS_CONTRACT)
    high_ids = ("b" * 64, "a" * 64)
    build = SimpleNamespace(
        qa_checks=checks,
        quarantine_issue_ids_by_severity={QASeverity.HIGH.value: high_ids},
    )

    waivers, accepted = lifecycle._approval_exceptions(build)

    assert waivers == tuple(
        sorted(item.result_id for item in checks if item.status is QAStatus.WARNING)
    )
    assert accepted == tuple(sorted(high_ids))


def _quarantine_row(build_id: str, *, observed_value: str = "") -> dict[str, object]:
    return {
        "source_record_id": "blank-event-1",
        "table_name": TICKER_CHANGE_EVENT_CONTRACT.table,
        "issue_code": "blank_target_ticker",
        "severity": QASeverity.HIGH.value,
        "detected_build_id": build_id,
        "source_pointer": "bronze/ticker_events/page.json.gz#events[7]",
        "field_name": "effective_ticker",
        "observed_value": observed_value,
        "expected_rule": "ticker_change.ticker must be nonblank",
        "review_status": "pending",
    }


def _write_parquet(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd")


def _parity_build(
    *, data_path: str, quarantine_path: str, checks: tuple[QACheckResult, ...]
) -> SimpleNamespace:
    return SimpleNamespace(
        outputs=(
            SimpleNamespace(role=ArtifactRole.DATA, path=data_path, sha256="d" * 64),
            SimpleNamespace(role=ArtifactRole.QUARANTINE, path=quarantine_path),
        ),
        row_funnel=("same-reviewed-funnel",),
        qa_checks=checks,
        quarantine_issue_rows=1,
        quarantine_unique_source_rows=1,
    )


def test_preview_full_parity_ignores_build_id_but_detects_semantic_quarantine_drift(
    tmp_path: Path,
) -> None:
    preview_data_path = "preview/data.parquet"
    full_data_path = "full/data.parquet"
    preview_quarantine_path = "preview/quarantine.parquet"
    full_quarantine_path = "full/quarantine.parquet"
    data = pa.table({"value": [1]})
    _write_parquet(tmp_path / preview_data_path, data)
    _write_parquet(tmp_path / full_data_path, data)
    _write_parquet(
        tmp_path / preview_quarantine_path,
        pa.Table.from_pylist([_quarantine_row("a" * 64)], schema=QUARANTINE_ARROW_SCHEMA),
    )
    _write_parquet(
        tmp_path / full_quarantine_path,
        pa.Table.from_pylist([_quarantine_row("b" * 64)], schema=QUARANTINE_ARROW_SCHEMA),
    )
    checks = _expected_qa(TICKER_CHANGE_EVENT_CONTRACT)
    preview = _parity_build(
        data_path=preview_data_path,
        quarantine_path=preview_quarantine_path,
        checks=checks,
    )
    full = _parity_build(
        data_path=full_data_path,
        quarantine_path=full_quarantine_path,
        checks=checks,
    )

    lifecycle._require_preview_parity(tmp_path, preview, full, TICKER_CHANGE_EVENT_CONTRACT)

    _write_parquet(
        tmp_path / full_quarantine_path,
        pa.Table.from_pylist(
            [_quarantine_row("b" * 64, observed_value="changed")],
            schema=QUARANTINE_ARROW_SCHEMA,
        ),
    )
    with pytest.raises(SilverStoreError, match="quarantine evidence differs"):
        lifecycle._require_preview_parity(tmp_path, preview, full, TICKER_CHANGE_EVENT_CONTRACT)
