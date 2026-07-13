from __future__ import annotations

import gzip
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import QAStatus, SourceLayer
from ame_stocks_api.silver.ticker_type_contract import TICKER_TYPE_DIM_CONTRACT
from ame_stocks_api.silver.ticker_type_source import (
    TickerTypeSourceBatch,
    TickerTypeSourceError,
    TickerTypeSourcePage,
    TickerTypeSourceSnapshot,
    build_ticker_type_source_inventory,
    read_ticker_type_source_inventory,
)
from ame_stocks_api.silver.ticker_types import (
    TICKER_TYPE_AVAILABILITY_RULE,
    TICKER_TYPE_SNAPSHOT_SCOPE,
    TickerTypeTransformError,
    transform_ticker_type_batch,
)

BUILD_ID = "c" * 64
CAPTURE_AT = datetime(2026, 7, 11, 15, 37, 40, tzinfo=UTC)
_MISSING = object()


def _row(
    code: object = "CS",
    description: object = "Common Stock",
    *,
    asset_class: object = "stocks",
    locale: object = "us",
    **extra: object,
) -> dict[str, object]:
    row: dict[str, object] = {
        "asset_class": asset_class,
        "locale": locale,
        "code": code,
    }
    if description is not _MISSING:
        row["description"] = description
    row.update(extra)
    return row


def _page(
    rows: tuple[dict[str, object], ...],
    *,
    request_id: str = "a" * 64,
    sequence: int = 0,
    artifact_sha: str = "b" * 64,
) -> TickerTypeSourcePage:
    return TickerTypeSourcePage(
        source_path=(f"fixtures/ticker_types/request_id={request_id}/page-{sequence:05d}.json.gz"),
        source_artifact_sha256=artifact_sha,
        sequence=sequence,
        source_provider_request_id=f"provider-{request_id[:8]}",
        rows=rows,
    )


def _snapshot(
    rows: tuple[dict[str, object], ...] = (),
    *,
    request_id: str = "a" * 64,
    capture_at: datetime = CAPTURE_AT,
    pages: tuple[TickerTypeSourcePage, ...] | None = None,
) -> TickerTypeSourceSnapshot:
    if pages is None:
        pages = (_page(rows, request_id=request_id),)
    return TickerTypeSourceSnapshot(
        source_request_id=request_id,
        source_capture_at_utc=capture_at,
        pages=pages,
    )


def _transform(*snapshots: TickerTypeSourceSnapshot):
    return transform_ticker_type_batch(
        TickerTypeSourceBatch(tuple(snapshots)),
        build_id=BUILD_ID,
    )


def test_current_snapshot_preserves_fields_lineage_and_is_deterministic() -> None:
    source_rows = (
        _row("OS", "Ordinary Shares"),
        _row("CS", "Common Stock"),
    )
    snapshot = _snapshot(source_rows)
    result = _transform(snapshot)

    assert result.table.schema == TICKER_TYPE_DIM_CONTRACT.arrow_schema
    assert result.table.num_rows == 2
    assert result.table.column_names == [
        "capture_date",
        "asset_class",
        "locale",
        "type_code",
        "description",
        "snapshot_scope",
        "source_capture_at_utc",
        "available_session",
        "available_at_utc",
        "availability_rule",
        "source_record_id",
        "source_request_id",
        "source_provider_request_id",
        "source_artifact_sha256",
        "source_page_sequence",
        "source_row_ordinal",
        "source_row_hash",
    ]
    rows = result.table.to_pylist()
    assert [row["type_code"] for row in rows] == ["CS", "OS"]
    first = rows[0]
    assert first["description"] == "Common Stock"
    assert first["capture_date"].isoformat() == "2026-07-11"
    assert first["available_session"].isoformat() == "2026-07-13"
    assert first["available_at_utc"] == datetime(2026, 7, 13, 13, 30, tzinfo=UTC)
    assert first["snapshot_scope"] == TICKER_TYPE_SNAPSHOT_SCOPE
    assert first["availability_rule"] == TICKER_TYPE_AVAILABILITY_RULE
    expected_row_hash = stable_digest(source_rows[1])
    assert first["source_row_hash"] == expected_row_hash
    assert first["source_record_id"] == stable_digest(
        {
            "dataset": "ticker_types",
            "source_request_id": "a" * 64,
            "source_artifact_sha256": "b" * 64,
            "source_page_sequence": 0,
            "source_row_ordinal": 1,
            "source_row_hash": expected_row_hash,
        }
    )
    assert result.row_funnel.to_dict() == {
        "accepted_source_rows": 2,
        "exact_duplicate_excess": 0,
        "input_rows": 2,
        "output_rows_by_table": {"ticker_type_dim": 2},
        "quarantined_source_rows": 0,
        "unmapped_source_rows": 0,
        "version_preserved_rows": 0,
    }
    assert len(result.qa_checks) == 20
    assert all(check.status is QAStatus.PASSED for check in result.qa_checks)
    for check_id in (
        "new_type_code_rows_since_prior_capture",
        "disappeared_type_code_rows_since_prior_capture",
        "description_changed_rows_since_prior_capture",
    ):
        check = result.qa_by_id(check_id)
        assert (check.numerator, check.denominator, check.rate) == (0, 0, None)

    repeated = _transform(snapshot)
    assert repeated.table.equals(result.table)
    assert repeated.qa_checks == result.qa_checks
    assert repeated.quarantine_records == result.quarantine_records == ()
    assert repeated.row_funnel == result.row_funnel


def test_point_in_time_uses_new_york_date_and_strictly_later_open() -> None:
    near_utc_midnight = _transform(
        _snapshot(
            (_row("CS"),),
            capture_at=datetime(2026, 7, 12, 1, 0, tzinfo=UTC),
        )
    ).table.to_pylist()[0]
    assert near_utc_midnight["capture_date"].isoformat() == "2026-07-11"
    assert near_utc_midnight["available_session"].isoformat() == "2026-07-13"

    exactly_at_open = _transform(
        _snapshot(
            (_row("CS"),),
            capture_at=datetime(2026, 7, 13, 13, 30, tzinfo=UTC),
        )
    ).table.to_pylist()[0]
    assert exactly_at_open["capture_date"].isoformat() == "2026-07-13"
    assert exactly_at_open["available_session"].isoformat() == "2026-07-14"
    assert exactly_at_open["available_at_utc"] == datetime(2026, 7, 14, 13, 30, tzinfo=UTC)


def test_nullable_and_blank_descriptions_are_preserved_as_review_warnings() -> None:
    result = _transform(
        _snapshot(
            (
                _row("A", _MISSING),
                _row("B", None),
                _row("C", ""),
                _row("D", "  "),
            )
        )
    )

    assert [row["description"] for row in result.table.to_pylist()] == [
        None,
        None,
        "",
        "  ",
    ]
    check = result.qa_by_id("description_missing_or_blank_rows")
    assert (check.numerator, check.denominator, check.rate, check.status) == (
        4,
        4,
        1.0,
        QAStatus.WARNING,
    )


def test_nonstring_description_is_never_silently_coerced() -> None:
    with pytest.raises(TickerTypeTransformError, match=r"description.*unsafe type"):
        _transform(_snapshot((_row("CS", 7),)))


def test_warning_denominators_use_retained_rows_after_quarantine_and_dedup() -> None:
    reviewed = _row("bad-code", "", provider_new_field={"nested": True})
    invalid_required = _row(" ", "", provider_new_field=True)
    pages = (
        _page((reviewed, invalid_required), sequence=0),
        _page((dict(reviewed),), sequence=1, artifact_sha="d" * 64),
    )
    result = _transform(_snapshot(pages=pages))

    assert result.table.num_rows == 1
    assert result.table.to_pylist()[0]["type_code"] == "bad-code"
    assert result.row_funnel.input_rows == 3
    assert result.row_funnel.accepted_source_rows == 1
    assert result.row_funnel.exact_duplicate_excess == 1
    assert result.row_funnel.quarantined_source_rows == 1
    for check_id in (
        "description_missing_or_blank_rows",
        "type_code_format_unreviewed_rows",
        "unexpected_source_field_rows",
    ):
        check = result.qa_by_id(check_id)
        assert (check.numerator, check.denominator, check.status) == (
            1,
            1,
            QAStatus.WARNING,
        )
    duplicate = result.qa_by_id("exact_duplicate_excess_rows")
    assert (duplicate.numerator, duplicate.denominator, duplicate.status) == (
        1,
        3,
        QAStatus.WARNING,
    )


def test_domain_violations_are_retained_and_fail_without_quarantine() -> None:
    result = _transform(
        _snapshot(
            (
                _row("CS", asset_class="crypto"),
                _row("OS", locale="global"),
            )
        )
    )

    assert result.table.num_rows == 2
    assert result.row_funnel.accepted_source_rows == 2
    assert result.row_funnel.quarantined_source_rows == 0
    assert result.quarantine_records == ()
    asset = result.qa_by_id("asset_class_domain_invalid_rows")
    locale = result.qa_by_id("locale_domain_invalid_rows")
    assert (asset.numerator, asset.denominator, asset.status) == (1, 2, QAStatus.FAILED)
    assert (locale.numerator, locale.denominator, locale.status) == (1, 2, QAStatus.FAILED)


def test_required_fields_are_quarantined_without_string_coercion() -> None:
    missing_asset = _row("A")
    missing_asset.pop("asset_class")
    result = _transform(
        _snapshot(
            (
                missing_asset,
                _row("B", locale=None),
                _row(3),
                _row(" "),
            )
        )
    )

    assert result.table.num_rows == 0
    assert result.row_funnel.quarantined_source_rows == 4
    check = result.qa_by_id("required_field_invalid_rows")
    assert (check.numerator, check.denominator, check.status) == (4, 4, QAStatus.FAILED)
    assert {item.issue_code for item in result.quarantine_records} == {
        "required_field_invalid_rows"
    }


def test_exact_duplicates_keep_earliest_lineage_but_conflicts_precede_dedup() -> None:
    duplicate = _row("CS", "Common Stock")
    duplicate_pages = (
        _page((duplicate,), sequence=0, artifact_sha="1" * 64),
        _page((dict(duplicate),), sequence=1, artifact_sha="2" * 64),
    )
    deduplicated = _transform(_snapshot(pages=duplicate_pages))
    assert deduplicated.table.num_rows == 1
    output = deduplicated.table.to_pylist()[0]
    assert output["source_page_sequence"] == 0
    assert output["source_row_ordinal"] == 0
    assert output["source_artifact_sha256"] == "1" * 64
    assert deduplicated.row_funnel.exact_duplicate_excess == 1
    assert deduplicated.qa_by_id("primary_key_conflict_rows").numerator == 0

    conflict_pages = (
        _page((duplicate, dict(duplicate)), sequence=0, artifact_sha="1" * 64),
        _page(
            (_row("CS", "Changed label"),),
            sequence=1,
            artifact_sha="2" * 64,
        ),
    )
    conflicted = _transform(_snapshot(pages=conflict_pages))
    assert conflicted.table.num_rows == 0
    assert conflicted.row_funnel.quarantined_source_rows == 3
    assert conflicted.row_funnel.exact_duplicate_excess == 0
    conflict = conflicted.qa_by_id("primary_key_conflict_rows")
    assert (conflict.numerator, conflict.denominator, conflict.status) == (
        3,
        3,
        QAStatus.FAILED,
    )


def test_same_capture_date_rejects_multiple_requests_but_allows_multiple_pages() -> None:
    same_capture = _transform(
        _snapshot((_row("A"),), request_id="a" * 64),
        _snapshot((_row("B"),), request_id="b" * 64),
    )
    assert same_capture.table.num_rows == 0
    assert same_capture.row_funnel.quarantined_source_rows == 2
    check = same_capture.qa_by_id("source_snapshot_cardinality_invalid")
    assert (check.numerator, check.denominator, check.status) == (
        1,
        1,
        QAStatus.FAILED,
    )

    pages = (
        _page((_row("A"),), sequence=0),
        _page((_row("B"),), sequence=1, artifact_sha="d" * 64),
    )
    one_request = _transform(_snapshot(pages=pages))
    assert one_request.table.num_rows == 2
    assert one_request.qa_by_id("source_snapshot_cardinality_invalid").status is (QAStatus.PASSED)


def test_temporal_drift_compares_adjacent_accepted_captures() -> None:
    prior = _snapshot(
        (
            _row("A", "Alpha"),
            _row("B", "Beta"),
            _row("C", "Charlie"),
        ),
        request_id="a" * 64,
        capture_at=datetime(2026, 7, 10, 20, 0, tzinfo=UTC),
    )
    current = _snapshot(
        (
            _row("A", "Alpha changed"),
            _row("B", "Beta"),
            _row("D", "Delta"),
        ),
        request_id="b" * 64,
        capture_at=datetime(2026, 7, 13, 20, 0, tzinfo=UTC),
    )

    result = _transform(current, prior)
    expected = {
        "new_type_code_rows_since_prior_capture": (1, 3, 1 / 3),
        "disappeared_type_code_rows_since_prior_capture": (1, 3, 1 / 3),
        "description_changed_rows_since_prior_capture": (1, 2, 1 / 2),
    }
    for check_id, (numerator, denominator, rate) in expected.items():
        check = result.qa_by_id(check_id)
        assert (check.numerator, check.denominator, check.rate, check.status) == (
            numerator,
            denominator,
            rate,
            QAStatus.WARNING,
        )


def _write_bronze_fixture(
    root: Path,
    *,
    response: object | None = None,
    artifact_overrides: dict[str, object] | None = None,
) -> tuple[str, Path]:
    request_id = "1" * 64
    if response is None:
        response = {
            "count": 1,
            "request_id": "provider-request",
            "results": [_row()],
            "status": "OK",
        }
    raw = json.dumps(response, separators=(",", ":"), sort_keys=True).encode()
    compressed = gzip.compress(raw, mtime=0)
    artifact_relative = f"bronze/massive/ticker_types/request_id={request_id}/page-00000.json.gz"
    artifact_path = root / artifact_relative
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(compressed)
    artifact_path.with_name(".page-00000.json.gz.swp").write_bytes(b"not-authoritative")
    results = response.get("results") if isinstance(response, dict) else None
    artifact: dict[str, object] = {
        "compressed_bytes": len(compressed),
        "content_type": "application/json",
        "is_last": True,
        "next_continuation": None,
        "path": artifact_relative,
        "raw_bytes": len(raw),
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
        "record_count": len(results) if isinstance(results, list) else 0,
        "sequence": 0,
        "stored_sha256": hashlib.sha256(compressed).hexdigest(),
    }
    artifact.update(artifact_overrides or {})
    manifest = {
        "artifacts": [artifact],
        "checkpoint": None,
        "completed_at": CAPTURE_AT.isoformat(),
        "created_at": CAPTURE_AT.isoformat(),
        "dataset": "ticker_types",
        "manifest_schema_version": 1,
        "provider": "massive",
        "provider_contract_version": "1.1",
        "provider_version": "1.2.0",
        "request": {
            "adjusted": False,
            "asset_ids": [],
            "dataset": "ticker_types",
            "end": "2020-01-02",
            "parameters": {},
            "start": "2020-01-02",
        },
        "request_id": request_id,
        "status": "complete",
        "updated_at": CAPTURE_AT.isoformat(),
    }
    manifest_relative = f"manifests/massive/ticker_types/{request_id}.json"
    manifest_path = root / manifest_relative
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return manifest_relative, artifact_path


def test_manifest_bound_source_ignores_strays_and_detects_same_size_mutation(
    tmp_path: Path,
) -> None:
    manifest_path, artifact_path = _write_bronze_fixture(tmp_path)
    inventory = build_ticker_type_source_inventory(
        tmp_path,
        manifest_paths=(manifest_path,),
        git_commit="a" * 40,
    )

    assert inventory.source_dataset == "ticker_types"
    assert inventory.source_layer is SourceLayer.BRONZE
    assert len(inventory.upstream_manifests) == len(inventory.artifacts) == 1
    assert not inventory.artifacts[0].path.endswith(".swp")
    batch = read_ticker_type_source_inventory(tmp_path, inventory)
    assert (batch.source_object_count, batch.page_count, batch.row_count) == (2, 1, 1)
    assert _transform(*batch.snapshots).table.num_rows == 1

    mutated = bytearray(artifact_path.read_bytes())
    mutated[-1] ^= 1
    artifact_path.write_bytes(bytes(mutated))
    with pytest.raises(TickerTypeSourceError, match="checksum mismatch"):
        read_ticker_type_source_inventory(tmp_path, inventory)


@pytest.mark.parametrize(
    ("artifact_overrides", "message"),
    [
        ({"raw_sha256": "0" * 64}, "raw checksum mismatch"),
        ({"raw_bytes": 1}, "raw byte count mismatch"),
        ({"record_count": 2}, "rows differ from manifest"),
    ],
)
def test_manifest_reader_rejects_raw_integrity_and_declared_row_drift(
    tmp_path: Path,
    artifact_overrides: dict[str, object],
    message: str,
) -> None:
    manifest_path, _ = _write_bronze_fixture(
        tmp_path,
        artifact_overrides=artifact_overrides,
    )
    inventory = build_ticker_type_source_inventory(
        tmp_path,
        manifest_paths=(manifest_path,),
        git_commit="a" * 40,
    )
    with pytest.raises(TickerTypeSourceError, match=message):
        read_ticker_type_source_inventory(tmp_path, inventory)


@pytest.mark.parametrize(
    "response",
    [
        {"status": "ERROR", "request_id": "provider", "results": [], "count": 0},
        {"status": "OK", "results": [], "count": 0},
        {"status": "OK", "request_id": " provider ", "results": [], "count": 0},
        {"status": "OK", "request_id": "provider", "results": "bad", "count": 0},
        {"status": "OK", "request_id": "provider", "results": [1], "count": 1},
        {"status": "OK", "request_id": "provider", "results": [], "count": True},
        {"status": "OK", "request_id": "provider", "results": [], "count": 1},
    ],
)
def test_manifest_reader_rejects_unsafe_response_envelopes(
    tmp_path: Path,
    response: object,
) -> None:
    manifest_path, _ = _write_bronze_fixture(tmp_path, response=response)
    inventory = build_ticker_type_source_inventory(
        tmp_path,
        manifest_paths=(manifest_path,),
        git_commit="a" * 40,
    )
    with pytest.raises(TickerTypeSourceError):
        read_ticker_type_source_inventory(tmp_path, inventory)
