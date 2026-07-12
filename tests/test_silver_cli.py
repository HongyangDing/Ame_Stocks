from __future__ import annotations

import json
from pathlib import Path

import pytest

from ame_stocks_api.cli.silver import main
from ame_stocks_api.silver.contracts import (
    ArrowType,
    ColumnSpec,
    QAMetric,
    QAOperator,
    QARule,
    QASeverity,
    QAStatus,
    TableContract,
)
from ame_stocks_api.silver.store import SilverStore


def _contract() -> TableContract:
    return TableContract(
        domain="reference",
        table="cli_fixture_dim",
        schema_version=1,
        description="CLI fixture contract",
        grain="One row per identifier",
        columns=(ColumnSpec("identifier", ArrowType.STRING, False, "Stable identifier"),),
        primary_key=("identifier",),
        partition_by=(),
        sort_by=("identifier",),
        source_datasets=("synthetic_source",),
        qa_rules=(
            QARule(
                check_id="schema_exact",
                severity=QASeverity.CRITICAL,
                metric=QAMetric.NUMERATOR,
                operator=QAOperator.EQUAL,
                limit=0.0,
                failure_status=QAStatus.FAILED,
                description="No schema mismatches are allowed.",
            ),
        ),
    )


def test_fixed_cases_cli_is_metadata_only(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["fixed-cases"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["mode"] == "metadata_only"
    assert output["case_count"] == len(output["cases"]) == 14


def test_validate_contract_cli_does_not_register_it(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "contract.json"
    contract = _contract()
    path.write_text(json.dumps(contract.to_dict()), encoding="utf-8")

    assert main(["validate-contract", "--contract", str(path)]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "valid"
    assert output["contract_id"] == contract.contract_id
    assert not (tmp_path / "manifests").exists()


def test_status_cli_verifies_workflow_chain(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot = SilverStore(tmp_path).create_workflow(
        _contract(),
        actor="cli-test",
        created_at="2026-07-12T00:00:00+00:00",
    )
    assert (
        main(
            [
                "status",
                "--data-root",
                str(tmp_path),
                "--workflow-id",
                snapshot.workflow_id,
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert output["state"] == "planned"
    assert output["event_sha256"] == snapshot.event_sha256


def test_inspect_release_cli_rejects_unknown_release(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        main(
            [
                "inspect-release",
                "--data-root",
                str(tmp_path),
                "--release-id",
                "a" * 64,
            ]
        )
    assert raised.value.code == 2
    assert "ame-silver:" in capsys.readouterr().err
