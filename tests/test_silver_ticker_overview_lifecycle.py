from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from ame_stocks_api.cli import silver_ticker_overview_lifecycle as lifecycle_cli
from ame_stocks_api.cli.silver_ticker_overview_lifecycle import build_parser
from ame_stocks_api.silver import ticker_overview_lifecycle as lifecycle
from ame_stocks_api.silver.contracts import (
    ArtifactRef,
    ArtifactRole,
    BuildIntent,
    BuildKind,
    QACheckResult,
    QASeverity,
    QAStatus,
    SourceInventory,
    SourceInventoryItem,
    SourceLayer,
    UpstreamManifestRef,
)
from ame_stocks_api.silver.store import SilverStoreError
from ame_stocks_api.silver.ticker_overview_contract import TICKER_OVERVIEW_SAFE_CONTRACT


def test_production_authorization_and_cli_surface_are_fixed() -> None:
    authorization = lifecycle.CURRENT_TICKER_OVERVIEW_AUTHORIZATION

    assert (
        authorization.expected_source_rows,
        authorization.expected_output_rows,
        authorization.expected_unresolved_rows,
        authorization.expected_capture_date,
    ) == (30_739, 30_570, 169, date(2026, 7, 11))
    assert lifecycle.S6_COMPLETION_AUTHORIZATION == (
        "那下一步是不是可以直接走完S6，等S7的时候再回到逐步审批的模式"  # noqa: RUF001
    )
    assert lifecycle.S6_EXECUTION_AUTHORIZATION == "开始S6"
    assert {action.dest for action in build_parser()._actions} == {
        "help",
        "data_root",
        "repo_root",
        "git_commit",
    }


@pytest.mark.parametrize(
    "overrides",
    [
        {"expected_source_rows": 30_738},
        {"expected_unresolved_rows": 168},
        {"expected_capture_date": date(2026, 7, 10)},
        {"sample_limit": 101},
    ],
)
def test_production_authorization_rejects_scope_overrides(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match=r"authorized|funnel"):
        lifecycle.TickerOverviewAuthorization(**overrides)


def test_inventory_pair_has_one_row_per_lifecycle_and_one_row_per_bronze_page() -> None:
    commit = "a" * 40
    upstream = (
        UpstreamManifestRef(
            path="manifests/silver/source-coverage/ticker_overview/coverage-" + "b" * 64 + ".json",
            sha256="c" * 64,
        ),
    )
    overview_upstream = (
        *upstream,
        *(
            UpstreamManifestRef(
                path=f"manifests/massive/ticker_overview/{index!s:0>64}.json",
                sha256=str(index + 5) * 64,
            )
            for index in range(2)
        ),
    )
    lifecycle_inventory = SourceInventory(
        source_dataset="ticker_overview",
        source_layer=SourceLayer.CONTROL_MANIFEST,
        git_commit=commit,
        upstream_manifests=upstream,
        artifacts=(
            SourceInventoryItem(
                path="manifests/plans/ticker_overview/lifecycles.txt",
                sha256="d" * 64,
                bytes=100,
                row_count=2,
                media_type="text/plain",
            ),
        ),
    )
    overview_inventory = SourceInventory(
        source_dataset="ticker_overview",
        source_layer=SourceLayer.BRONZE,
        git_commit=commit,
        upstream_manifests=overview_upstream,
        artifacts=tuple(
            SourceInventoryItem(
                path=(
                    "bronze/massive/ticker_overview/request_id="
                    + str(index) * 64
                    + "/page-00000.json.gz"
                ),
                sha256=str(index + 2) * 64,
                bytes=20,
                row_count=1,
                media_type="application/gzip+json",
            )
            for index in range(2)
        ),
    )
    authorization = SimpleNamespace(expected_source_rows=2)

    lifecycle._require_source_inventory_pair(
        lifecycle_inventory,
        overview_inventory,
        git_commit=commit,
        authorization=authorization,
    )

    with pytest.raises(SilverStoreError, match="lifecycle-control inventory grain"):
        lifecycle._require_source_inventory_pair(
            replace(
                lifecycle_inventory,
                artifacts=(
                    replace(lifecycle_inventory.artifacts[0], row_count=3),
                ),
            ),
            overview_inventory,
            git_commit=commit,
            authorization=authorization,
        )


def _expected_qa() -> tuple[QACheckResult, ...]:
    expected = lifecycle._EXPECTED_WARNING_NUMERATORS
    output: list[QACheckResult] = []
    for rule in TICKER_OVERVIEW_SAFE_CONTRACT.qa_rules:
        denominator = lifecycle._EXPECTED_WARNING_DENOMINATORS.get(
            rule.check_id,
            lifecycle.EXPECTED_SOURCE_ROWS,
        )
        output.append(
            QACheckResult(
                table=TICKER_OVERVIEW_SAFE_CONTRACT.table,
                partition_key="source_capture_date=2026-07-11",
                check_id=rule.check_id,
                severity=rule.severity,
                status=(QAStatus.WARNING if rule.check_id in expected else QAStatus.PASSED),
                numerator=expected.get(rule.check_id, 0),
                denominator=denominator,
                rate=expected.get(rule.check_id, 0) / denominator,
                threshold=rule.threshold_expression,
            )
        )
    return tuple(output)


def test_expected_qa_accepts_only_reviewed_exact_warnings() -> None:
    checks = _expected_qa()

    lifecycle._require_expected_qa(
        checks,
        TICKER_OVERVIEW_SAFE_CONTRACT,
        lifecycle.CURRENT_TICKER_OVERVIEW_AUTHORIZATION,
    )
    assert {
        item.check_id: (item.severity, item.numerator)
        for item in checks
        if item.status is QAStatus.WARNING
    } == {
        "unresolved_identity_rows": (QASeverity.HIGH, 169),
        "sic_code_missing_rows": (QASeverity.MEDIUM, 14_057),
        "list_date_missing_rows": (QASeverity.MEDIUM, 7_322),
        "retrospective_query_without_archived_vintage_rows": (
            QASeverity.MEDIUM,
            30_570,
        ),
    }


def test_expected_qa_rejects_warning_count_drift() -> None:
    checks = list(_expected_qa())
    index = next(
        index for index, item in enumerate(checks) if item.check_id == "sic_code_missing_rows"
    )
    item = checks[index]
    checks[index] = replace(
        item,
        numerator=item.numerator + 1,
        rate=(item.numerator + 1) / item.denominator,
    )

    with pytest.raises(SilverStoreError, match="reviewed warning profile changed"):
        lifecycle._require_expected_qa(
            tuple(checks),
            TICKER_OVERVIEW_SAFE_CONTRACT,
            lifecycle.CURRENT_TICKER_OVERVIEW_AUTHORIZATION,
        )


def test_expected_qa_rejects_warning_denominator_drift() -> None:
    checks = list(_expected_qa())
    index = next(
        index for index, item in enumerate(checks) if item.check_id == "sic_code_missing_rows"
    )
    item = checks[index]
    checks[index] = replace(
        item,
        denominator=item.denominator - 1,
        rate=item.numerator / (item.denominator - 1),
    )

    with pytest.raises(SilverStoreError, match="warning denominator changed"):
        lifecycle._require_expected_qa(
            tuple(checks),
            TICKER_OVERVIEW_SAFE_CONTRACT,
            lifecycle.CURRENT_TICKER_OVERVIEW_AUTHORIZATION,
        )


def test_quarantine_semantics_are_exactly_unresolved_identity_high() -> None:
    records = tuple(
        SimpleNamespace(
            table_name=TICKER_OVERVIEW_SAFE_CONTRACT.table,
            issue_code="identity_evidence_unresolved",
            severity=QASeverity.HIGH,
            detected_build_id="a" * 64,
        )
        for _ in range(169)
    )

    lifecycle._require_expected_quarantine(
        records,
        TICKER_OVERVIEW_SAFE_CONTRACT,
        build_id="a" * 64,
    )
    broken = list(records)
    broken[0] = SimpleNamespace(
        table_name=TICKER_OVERVIEW_SAFE_CONTRACT.table,
        issue_code="different",
        severity=QASeverity.HIGH,
        detected_build_id="a" * 64,
    )
    with pytest.raises(SilverStoreError, match="quarantine semantics changed"):
        lifecycle._require_expected_quarantine(
            tuple(broken),
            TICKER_OVERVIEW_SAFE_CONTRACT,
            build_id="a" * 64,
        )


def test_approval_exceptions_are_exact_warning_and_high_quarantine_ids() -> None:
    checks = _expected_qa()
    build = SimpleNamespace(
        qa_checks=checks,
        quarantine_issue_ids_by_severity={QASeverity.HIGH.value: ("b" * 64, "a" * 64)},
    )

    waivers, accepted = lifecycle._approval_exceptions(build)

    assert waivers == tuple(
        sorted(item.result_id for item in checks if item.status is QAStatus.WARNING)
    )
    assert accepted == ("a" * 64, "b" * 64)


def test_build_parameters_preserve_s7_hard_stop() -> None:
    prepared = SimpleNamespace(
        coverage_receipt_path="manifests/silver/source-coverage/ticker_overview/test.json",
        coverage_receipt_sha256="a" * 64,
        lifecycle_inventory=SimpleNamespace(inventory_id="b" * 64),
        overview_inventory=SimpleNamespace(inventory_id="c" * 64),
        profile_sha256="d" * 64,
    )

    parameters = lifecycle._parameters(
        prepared,
        lifecycle.CURRENT_TICKER_OVERVIEW_AUTHORIZATION,
    )

    assert parameters["backtest_identity_eligible"] is False
    assert parameters["full_formal_scope_preview"] is True
    assert parameters["s7_started"] is False
    assert parameters["execution_instruction"] == "开始S6"

    intent = BuildIntent(
        workflow_id="e" * 64,
        domain=TICKER_OVERVIEW_SAFE_CONTRACT.domain,
        table=TICKER_OVERVIEW_SAFE_CONTRACT.table,
        schema_version=TICKER_OVERVIEW_SAFE_CONTRACT.schema_version,
        contract_id=TICKER_OVERVIEW_SAFE_CONTRACT.contract_id,
        kind=BuildKind.PREVIEW,
        attempt=1,
        retry_of_build_id=None,
        transform_version="test-transform-v1",
        git_commit="f" * 40,
        exchange_calendar_version="exchange-calendars==test",
        inputs=(
            ArtifactRef(
                path=(
                    "bronze/massive/ticker_overview/request_id="
                    + "1" * 64
                    + "/page-00000.json.gz"
                ),
                sha256="2" * 64,
                bytes=20,
                row_count=1,
                media_type="application/gzip+json",
                role=ArtifactRole.SOURCE,
                source_dataset="ticker_overview",
                source_layer=SourceLayer.BRONZE,
                lineage_manifest_path=(
                    "manifests/silver/source-inventories/ticker_overview/" + "3" * 64 + ".json"
                ),
                lineage_manifest_sha256="4" * 64,
            ),
        ),
        parameters=parameters,
    )
    assert intent.parameters["execution_instruction"] == "开始S6"


def test_cli_reports_two_bound_inventories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_path = tmp_path / "silver" / "part-00000.parquet"
    run = SimpleNamespace(
        contract=SimpleNamespace(contract_id="a" * 64),
        coverage_receipt_path="manifests/silver/source-coverage/ticker_overview/test.json",
        coverage_receipt_sha256="b" * 64,
        published=SimpleNamespace(data_paths=(data_path,)),
        full=SimpleNamespace(
            build_id="c" * 64,
            qa_checks=(),
            quarantine_issue_rows=169,
            row_funnel=SimpleNamespace(to_dict=lambda: {"input_rows": 30_739}),
        ),
        full_document=SimpleNamespace(sha256="d" * 64),
        preview=SimpleNamespace(build_id="e" * 64),
        preview_document=SimpleNamespace(sha256="f" * 64),
        profile_sha256="1" * 64,
        lifecycle_inventory=SimpleNamespace(
            inventory_id="2" * 64, source_layer=SourceLayer.CONTROL_MANIFEST
        ),
        lifecycle_inventory_document=SimpleNamespace(path="control.json", sha256="3" * 64),
        overview_inventory=SimpleNamespace(inventory_id="4" * 64, source_layer=SourceLayer.BRONZE),
        overview_inventory_document=SimpleNamespace(path="bronze.json", sha256="5" * 64),
        release=SimpleNamespace(release_id="6" * 64),
        release_document=SimpleNamespace(sha256="7" * 64),
        workflow=SimpleNamespace(
            sequence=9,
            state=SimpleNamespace(value="published"),
            event_sha256="8" * 64,
            workflow_id="9" * 64,
        ),
    )
    monkeypatch.setattr(
        lifecycle_cli,
        "complete_ticker_overview_lifecycle",
        lambda *_args, **_kwargs: run,
    )

    assert lifecycle_cli.main(
        [
            "--data-root",
            str(tmp_path),
            "--repo-root",
            str(tmp_path),
            "--git-commit",
            "0" * 40,
        ]
    ) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["inventories"] == {
        "lifecycle_control": {
            "inventory_id": "2" * 64,
            "manifest_path": "control.json",
            "manifest_sha256": "3" * 64,
            "source_layer": "control_manifest",
        },
        "ticker_overview_bronze": {
            "inventory_id": "4" * 64,
            "manifest_path": "bronze.json",
            "manifest_sha256": "5" * 64,
            "source_layer": "bronze",
        },
    }
    assert output["s7_started"] is False
