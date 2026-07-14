from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from ame_stocks_api.silver.contracts import (
    ArrowType,
    ArtifactRef,
    ArtifactRole,
    BuildIntent,
    BuildKind,
    ColumnSpec,
    QAMetric,
    QAOperator,
    QARule,
    QASeverity,
    QAStatus,
    QuarantineRecord,
    QuarantineReviewStatus,
    SilverContractError,
    SourceLayer,
    TableContract,
    ensure_json_safe,
)
from ame_stocks_api.silver.fixed_cases import (
    FIXED_CASE_IDS,
    FIXED_CASES,
    FIXED_CASES_BY_ID,
    get_fixed_case,
)


def _contract(*, description: str = "A reviewed synthetic table") -> TableContract:
    return TableContract(
        domain="reference",
        table="synthetic_dim",
        schema_version=1,
        description=description,
        grain="One row per session and asset",
        columns=(
            ColumnSpec("session_date", ArrowType.DATE32, False, "Trading session"),
            ColumnSpec("asset_id", ArrowType.STRING, False, "Permanent security ID"),
            ColumnSpec("value", ArrowType.FLOAT64, True, "Synthetic value"),
        ),
        primary_key=("session_date", "asset_id"),
        partition_by=("session_date",),
        sort_by=("session_date", "asset_id"),
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
            QARule(
                check_id="primary_key_unique",
                severity=QASeverity.CRITICAL,
                metric=QAMetric.NUMERATOR,
                operator=QAOperator.EQUAL,
                limit=0.0,
                failure_status=QAStatus.FAILED,
                description="No duplicate primary keys are allowed.",
            ),
        ),
    )


def test_table_contract_round_trip_and_digest_are_deterministic() -> None:
    first = _contract()
    second = _contract()

    assert first.contract_id == second.contract_id
    assert first.schema_digest == second.schema_digest
    assert TableContract.from_dict(first.to_dict()) == first


def test_table_contract_rejects_digest_tampering_and_nullable_key() -> None:
    document = _contract().to_dict()
    document["description"] = "changed after digest"
    with pytest.raises(SilverContractError, match="digest mismatch"):
        TableContract.from_dict(document)

    with pytest.raises(SilverContractError, match="cannot be nullable"):
        TableContract(
            domain="reference",
            table="bad_dim",
            schema_version=1,
            description="Bad contract",
            grain="One row",
            columns=(ColumnSpec("asset_id", ArrowType.STRING, True, "ID"),),
            primary_key=("asset_id",),
            partition_by=(),
            sort_by=("asset_id",),
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


@pytest.mark.parametrize(
    "payload",
    [
        {"api_key": "do-not-store"},
        {"nested": {"password": "do-not-store"}},
        {"header": "Bearer do-not-store"},
        {"url": "https://example.test/file?token=do-not-store"},
        {"value": float("nan")},
    ],
)
def test_json_contract_rejects_secrets_and_nonfinite_numbers(payload: object) -> None:
    with pytest.raises(SilverContractError):
        ensure_json_safe(payload, label="test payload")


def test_json_contract_allows_an_explicit_bounded_large_list_without_weakening_safety() -> None:
    payload = {"items": list(range(2_513))}

    with pytest.raises(SilverContractError, match="too many items"):
        ensure_json_safe(payload, label="test payload")

    observed = ensure_json_safe(
        payload,
        label="test payload",
        max_list_items=5_000,
    )
    assert len(observed["items"]) == 2_513
    with pytest.raises(SilverContractError, match="sensitive key"):
        ensure_json_safe(
            {"api_key": ["do-not-store"] * 2_513},
            label="test payload",
            max_list_items=5_000,
        )


def test_quarantine_contract_round_trip_and_issue_identity() -> None:
    record = QuarantineRecord(
        source_record_id="source-row-7",
        table_name="synthetic_dim",
        issue_code="timestamp.out_of_session",
        severity=QASeverity.HIGH,
        detected_build_id="a" * 64,
        source_pointer="bronze/example.json#row=7",
        field_name="event_at_utc",
        observed_value="2019-08-12T03:00:00Z",
        expected_rule="Timestamp must map to the requested market session.",
        review_status=QuarantineReviewStatus.PENDING,
    )

    restored = QuarantineRecord.from_dict(record.to_dict())
    assert restored == record
    assert restored.issue_id == record.issue_id


def test_data_artifacts_cannot_bypass_parquet_schema_validation() -> None:
    with pytest.raises(SilverContractError, match="data artifacts must be Parquet"):
        ArtifactRef(
            path="silver/fake-data.json",
            sha256="a" * 64,
            bytes=2,
            row_count=2,
            media_type="application/json",
            role=ArtifactRole.DATA,
            table="synthetic_dim",
        )


def test_fixed_case_registry_is_complete_unique_and_immutable() -> None:
    assert len(FIXED_CASES) == len(FIXED_CASE_IDS) == len(FIXED_CASES_BY_ID) == 15
    assert len(set(FIXED_CASE_IDS)) == 15
    assert get_fixed_case("half_day").case_id == "half_day"
    snapshot_case = get_fixed_case("current_reference_snapshot")
    assert snapshot_case.family == "reference"
    with pytest.raises(FrozenInstanceError):
        FIXED_CASES[0].title = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        FIXED_CASES_BY_ID["new"] = FIXED_CASES[0]  # type: ignore[index]
    with pytest.raises(KeyError, match="unknown Silver fixed case"):
        get_fixed_case("unknown")


def test_plain_text_media_is_source_only() -> None:
    source = ArtifactRef(
        path="manifests/plans/ticker_events/identifiers.txt",
        sha256="a" * 64,
        bytes=13,
        row_count=1,
        media_type="text/plain",
        role=ArtifactRole.SOURCE,
        source_dataset="ticker_events",
        source_layer=SourceLayer.CONTROL_MANIFEST,
        lineage_manifest_path=(
            f"manifests/silver/source-inventories/ticker_events/inventory-{'b' * 64}.json"
        ),
        lineage_manifest_sha256="c" * 64,
    )
    assert source.media_type == "text/plain"
    assert source.source_layer is SourceLayer.CONTROL_MANIFEST
    with pytest.raises(SilverContractError, match="source-only"):
        ArtifactRef(
            path="samples/identifiers.txt",
            sha256="a" * 64,
            bytes=13,
            row_count=1,
            media_type="text/plain",
            role=ArtifactRole.SAMPLE,
        )


def test_trust_anchor_sequences_and_nested_parameters_are_deeply_frozen() -> None:
    base = _contract()
    columns = list(base.columns)
    primary_key = list(base.primary_key)
    contract = TableContract(
        domain=base.domain,
        table=base.table,
        schema_version=base.schema_version,
        description=base.description,
        grain=base.grain,
        columns=columns,  # type: ignore[arg-type]
        primary_key=primary_key,  # type: ignore[arg-type]
        partition_by=list(base.partition_by),  # type: ignore[arg-type]
        sort_by=list(base.sort_by),  # type: ignore[arg-type]
        source_datasets=list(base.source_datasets),  # type: ignore[arg-type]
        qa_rules=list(base.qa_rules),  # type: ignore[arg-type]
    )
    contract_id = contract.contract_id
    columns.append(ColumnSpec("late", ArrowType.STRING, True, "Late mutation"))
    primary_key.append("late")
    assert contract.contract_id == contract_id
    assert isinstance(contract.columns, tuple)

    parameters: dict[str, object] = {"nested": {"values": [1, 2]}}
    source = ArtifactRef(
        path="fixtures/source.json",
        sha256="b" * 64,
        bytes=10,
        row_count=1,
        media_type="application/json",
        role=ArtifactRole.SOURCE,
        source_dataset="synthetic_source",
        source_layer=SourceLayer.SYNTHETIC_FIXTURE,
        lineage_manifest_path=(
            f"manifests/silver/source-inventories/synthetic_source/inventory-{'c' * 64}.json"
        ),
        lineage_manifest_sha256="d" * 64,
    )
    intent = BuildIntent(
        workflow_id="e" * 64,
        domain=contract.domain,
        table=contract.table,
        schema_version=contract.schema_version,
        contract_id=contract.contract_id,
        kind=BuildKind.PREVIEW,
        attempt=1,
        retry_of_build_id=None,
        transform_version="fixture-v1",
        git_commit="f" * 40,
        exchange_calendar_version="fixture-calendar-v1",
        inputs=[source],  # type: ignore[arg-type]
        parameters=parameters,
    )
    build_id = intent.build_id
    nested = parameters["nested"]
    assert isinstance(nested, dict)
    values = nested["values"]
    assert isinstance(values, list)
    values.append(3)
    parameters["new"] = True
    assert intent.build_id == build_id
    with pytest.raises(TypeError):
        intent.parameters["new"] = True  # type: ignore[index]
