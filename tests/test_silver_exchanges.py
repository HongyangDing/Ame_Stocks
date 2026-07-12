from __future__ import annotations

import gzip
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import QAStatus, SourceLayer
from ame_stocks_api.silver.exchange_contract import EXCHANGE_DIM_CONTRACT
from ame_stocks_api.silver.exchange_source import (
    ExchangeSourceBatch,
    ExchangeSourceError,
    ExchangeSourcePage,
    ExchangeSourceSnapshot,
    build_exchange_source_inventory,
    read_exchange_source_inventory,
)
from ame_stocks_api.silver.exchanges import (
    EXCHANGE_AVAILABILITY_RULE,
    EXCHANGE_SNAPSHOT_SCOPE,
    ExchangeTransformError,
    transform_exchange_batch,
)
from ame_stocks_api.silver.store import SilverStore, WorkflowState

BUILD_ID = "c" * 64
CAPTURE_AT = datetime(2026, 7, 11, 15, 37, 41, tzinfo=UTC)


def _row(
    exchange_id: int = 1,
    *,
    name: str = "NYSE American, LLC",
    mic: str | None = "XASE",
    exchange_type: str = "exchange",
    **extra: object,
) -> dict[str, object]:
    row: dict[str, object] = {
        "asset_class": "stocks",
        "id": exchange_id,
        "locale": "us",
        "name": name,
        "operating_mic": "XNYS",
        "type": exchange_type,
        "url": "https://example.test/exchange",
    }
    if mic is not None:
        row["mic"] = mic
    row.update(extra)
    return row


def _snapshot(
    rows: tuple[dict[str, object], ...],
    *,
    request_id: str = "a" * 64,
    artifact_sha: str = "b" * 64,
    capture_at: datetime = CAPTURE_AT,
) -> ExchangeSourceSnapshot:
    return ExchangeSourceSnapshot(
        source_request_id=request_id,
        source_capture_at_utc=capture_at,
        pages=(
            ExchangeSourcePage(
                source_path=f"fixtures/{request_id}/page-00000.json.gz",
                source_artifact_sha256=artifact_sha,
                sequence=0,
                source_provider_request_id=f"provider-{request_id[:8]}",
                rows=rows,
            ),
        ),
    )


def _transform(*snapshots: ExchangeSourceSnapshot):
    return transform_exchange_batch(ExchangeSourceBatch(tuple(snapshots)), build_id=BUILD_ID)


def test_current_reference_snapshot_maps_point_in_time_and_preserves_orf() -> None:
    source_rows = (
        _row(acronym="AMEX", participant_id="A"),
        _row(
            5,
            name="Unlisted Trading Privileges",
            mic=None,
            exchange_type="SIP",
            participant_id="E",
        ),
        _row(
            62,
            name="OTC Equity Security",
            mic="OOTC",
            exchange_type="ORF",
            operating_mic="FINR",
        ),
    )
    result = _transform(_snapshot(source_rows))

    assert result.table.schema == EXCHANGE_DIM_CONTRACT.arrow_schema
    assert result.table.num_rows == 3
    rows = result.table.to_pylist()
    assert {row["exchange_type"] for row in rows} == {"exchange", "SIP", "ORF"}
    sip = next(row for row in rows if row["exchange_id"] == 5)
    assert sip["mic"] is None
    assert sip["capture_date"].isoformat() == "2026-07-11"
    assert sip["available_session"].isoformat() == "2026-07-13"
    assert sip["available_at_utc"] == datetime(2026, 7, 13, 13, 30, tzinfo=UTC)
    assert sip["snapshot_scope"] == EXCHANGE_SNAPSHOT_SCOPE
    assert sip["availability_rule"] == EXCHANGE_AVAILABILITY_RULE
    first = next(row for row in rows if row["exchange_id"] == 1)
    expected_row_hash = stable_digest(source_rows[0])
    assert first["source_row_hash"] == expected_row_hash
    assert first["source_record_id"] == stable_digest(
        {
            "dataset": "exchanges",
            "source_request_id": "a" * 64,
            "source_artifact_sha256": "b" * 64,
            "source_page_sequence": 0,
            "source_row_ordinal": 0,
            "source_row_hash": expected_row_hash,
        }
    )
    assert result.row_funnel.to_dict() == {
        "accepted_source_rows": 3,
        "exact_duplicate_excess": 0,
        "input_rows": 3,
        "output_rows_by_table": {"exchange_dim": 3},
        "quarantined_source_rows": 0,
        "unmapped_source_rows": 0,
        "version_preserved_rows": 0,
    }
    assert all(check.status is QAStatus.PASSED for check in result.qa_checks)
    assert result.quarantine_records == ()

    repeated = _transform(_snapshot(source_rows))
    assert repeated.table.equals(result.table)
    assert repeated.qa_checks == result.qa_checks


def test_nonblocking_source_drift_is_preserved_and_reported() -> None:
    row = _row(
        exchange_type="DARK",
        acronym="",
        url="ftp://invalid.example/exchange",
        provider_new_field={"nested": True},
    )
    result = _transform(_snapshot((row, dict(row))))

    assert result.table.num_rows == 1
    assert result.table.to_pylist()[0]["exchange_type"] == "DARK"
    assert result.table.to_pylist()[0]["acronym"] == ""
    assert result.row_funnel.exact_duplicate_excess == 1
    assert result.qa_by_id("exact_duplicate_excess_rows").numerator == 1
    assert result.qa_by_id("unreviewed_exchange_type_rows").numerator == 2
    assert result.qa_by_id("unexpected_source_field_rows").numerator == 2
    assert result.qa_by_id("empty_optional_string_rows").numerator == 2
    assert result.qa_by_id("url_invalid_rows").numerator == 2
    assert result.qa_by_id("unreviewed_exchange_type_rows").status is QAStatus.WARNING


def test_conflicting_primary_key_and_mic_rows_are_quarantined() -> None:
    primary = _transform(
        _snapshot(
            (
                _row(1, name="First representation"),
                _row(1, name="Conflicting representation"),
            )
        )
    )
    assert primary.table.num_rows == 0
    assert primary.row_funnel.quarantined_source_rows == 2
    assert primary.qa_by_id("primary_key_conflict_rows").numerator == 2
    assert primary.qa_by_id("primary_key_conflict_rows").status is QAStatus.FAILED
    assert {item.issue_code for item in primary.quarantine_records} == {
        "primary_key_conflict_rows"
    }

    mic = _transform(_snapshot((_row(1, mic="XASE"), _row(2, mic="XASE"))))
    assert mic.table.num_rows == 0
    assert mic.qa_by_id("mic_conflict_rows").numerator == 2
    assert {item.issue_code for item in mic.quarantine_records} == {"mic_conflict_rows"}


def test_invalid_required_domain_and_mic_values_are_fail_closed() -> None:
    invalid = _row(1, mic="bad", asset_class="crypto", locale="global", name=" ")
    result = _transform(_snapshot((invalid,)))

    assert result.table.num_rows == 0
    assert result.row_funnel.quarantined_source_rows == 1
    assert result.qa_by_id("required_field_invalid_rows").numerator == 1
    assert result.qa_by_id("mic_format_invalid_values").numerator == 1
    assert result.qa_by_id("asset_class_domain_invalid_rows").numerator == 1
    assert result.qa_by_id("locale_domain_invalid_rows").numerator == 1
    assert all(record.review_status.value == "pending" for record in result.quarantine_records)


def test_same_capture_date_has_exactly_one_source_request() -> None:
    result = _transform(
        _snapshot((_row(1),), request_id="a" * 64, artifact_sha="1" * 64),
        _snapshot((_row(2),), request_id="b" * 64, artifact_sha="2" * 64),
    )

    assert result.table.num_rows == 0
    assert result.row_funnel.quarantined_source_rows == 2
    check = result.qa_by_id("source_snapshot_cardinality_invalid")
    assert (check.numerator, check.denominator, check.status) == (1, 1, QAStatus.FAILED)


def test_later_capture_appends_a_new_partition_without_historical_backfill() -> None:
    result = _transform(
        _snapshot((_row(1),), request_id="a" * 64, artifact_sha="1" * 64),
        _snapshot(
            (_row(1),),
            request_id="b" * 64,
            artifact_sha="2" * 64,
            capture_at=datetime(2026, 7, 12, 15, 37, 41, tzinfo=UTC),
        ),
    )

    assert result.table.num_rows == 2
    assert [item.isoformat() for item in result.table.column("capture_date").to_pylist()] == [
        "2026-07-11",
        "2026-07-12",
    ]
    assert result.qa_by_id("source_snapshot_cardinality_invalid").status is QAStatus.PASSED


def test_optional_nonstring_value_is_not_silently_coerced() -> None:
    with pytest.raises(ExchangeTransformError, match="optional exchange field acronym"):
        _transform(_snapshot((_row(acronym=7),)))


def _write_bronze_fixture(
    root: Path,
    *,
    response_count: int | None = None,
) -> str:
    request_id = "1" * 64
    rows = [_row()]
    response = {
        "count": len(rows) if response_count is None else response_count,
        "request_id": "provider-request",
        "results": rows,
        "status": "OK",
    }
    raw = json.dumps(response, separators=(",", ":"), sort_keys=True).encode()
    compressed = gzip.compress(raw, mtime=0)
    artifact_relative = (
        f"bronze/massive/exchanges/request_id={request_id}/page-00000.json.gz"
    )
    artifact_path = root / artifact_relative
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(compressed)
    artifact_path.with_name(".page-00000.json.gz.swp").write_bytes(b"not-authoritative")
    manifest = {
        "artifacts": [
            {
                "compressed_bytes": len(compressed),
                "content_type": "application/json",
                "is_last": True,
                "next_continuation": None,
                "path": artifact_relative,
                "raw_bytes": len(raw),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "record_count": len(rows),
                "sequence": 0,
                "stored_sha256": hashlib.sha256(compressed).hexdigest(),
            }
        ],
        "checkpoint": None,
        "completed_at": CAPTURE_AT.isoformat(),
        "created_at": CAPTURE_AT.isoformat(),
        "dataset": "exchanges",
        "manifest_schema_version": 1,
        "provider": "massive",
        "provider_contract_version": "1.1",
        "provider_version": "1.2.0",
        "request": {
            "adjusted": False,
            "asset_ids": [],
            "dataset": "exchanges",
            "end": "2026-07-09",
            "parameters": {},
            "start": "2026-07-09",
        },
        "request_id": request_id,
        "status": "complete",
        "updated_at": CAPTURE_AT.isoformat(),
    }
    manifest_relative = f"manifests/massive/exchanges/{request_id}.json"
    manifest_path = root / manifest_relative
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return manifest_relative


def test_manifest_bound_inventory_ignores_unlisted_files_and_detects_mutation(
    tmp_path: Path,
) -> None:
    manifest_path = _write_bronze_fixture(tmp_path)
    inventory = build_exchange_source_inventory(
        tmp_path,
        manifest_paths=(manifest_path,),
        git_commit="a" * 40,
    )

    assert inventory.source_dataset == "exchanges"
    assert inventory.source_layer is SourceLayer.BRONZE
    assert len(inventory.upstream_manifests) == len(inventory.artifacts) == 1
    assert not inventory.artifacts[0].path.endswith(".swp")
    batch = read_exchange_source_inventory(tmp_path, inventory)
    assert (batch.source_object_count, batch.page_count, batch.row_count) == (2, 1, 1)
    assert _transform(*batch.snapshots).table.num_rows == 1

    artifact = tmp_path / inventory.artifacts[0].path
    artifact.write_bytes(artifact.read_bytes() + b"tampered")
    with pytest.raises(ExchangeSourceError, match="byte count mismatch"):
        read_exchange_source_inventory(tmp_path, inventory)


def test_manifest_reader_rejects_optional_envelope_count_mismatch(tmp_path: Path) -> None:
    manifest_path = _write_bronze_fixture(tmp_path, response_count=2)
    inventory = build_exchange_source_inventory(
        tmp_path,
        manifest_paths=(manifest_path,),
        git_commit="a" * 40,
    )
    with pytest.raises(ExchangeSourceError, match="count does not match"):
        read_exchange_source_inventory(tmp_path, inventory)


def test_approved_contract_can_enter_code_ready_without_a_preview(tmp_path: Path) -> None:
    store = SilverStore(tmp_path)
    snapshot = store.create_workflow(
        EXCHANGE_DIM_CONTRACT,
        actor="ame-stocks-author",
        created_at="2026-07-13T00:00:00+00:00",
    )
    snapshot = store.submit_schema_review(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="ame-stocks-author",
        created_at="2026-07-13T00:01:00+00:00",
    )
    snapshot = store.approve_schema(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        approver="user-approved-contract-1803d28f",
        decided_at="2026-07-13T00:02:00+00:00",
        note="Exact exchange_dim schema approved; no preview has run.",
    )

    assert snapshot.state is WorkflowState.CODE_READY
    approval, _ = store.load_approval(str(snapshot.evidence["approval_id"]))
    assert approval.subject_id == EXCHANGE_DIM_CONTRACT.contract_id
    assert approval.expected_event_sha256 is not None
    assert not (tmp_path / "staging").exists()
    assert not (tmp_path / "silver").exists()
