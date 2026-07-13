from __future__ import annotations

import gzip
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ame_stocks_api.silver.condition_code_source import (
    ConditionCodeSourceBatch,
    ConditionCodeSourceError,
    ConditionCodeSourcePage,
    ConditionCodeSourceSnapshot,
    build_condition_code_source_inventory,
    read_condition_code_source_inventory,
)
from ame_stocks_api.silver.contracts import SourceLayer

CAPTURE_AT = datetime(2026, 7, 11, 18, 54, 40, tzinfo=UTC)
REQUEST_ID = "1" * 64


def _condition_row() -> dict[str, object]:
    return {
        "asset_class": "stocks",
        "data_types": ["bbo", "nbbo"],
        "exchange": 10,
        "id": 30,
        "legacy": True,
        "name": "Equipment Changeover",
        "sip_mapping": {"CTA": "D", "UTP": "D"},
        "type": "quote_condition",
        "update_rules": {
            "consolidated": {
                "updates_high_low": False,
                "updates_open_close": False,
                "updates_volume": False,
            },
            "market_center": {
                "updates_high_low": False,
                "updates_open_close": False,
                "updates_volume": False,
            },
        },
    }


def _write_bronze_fixture(
    root: Path,
    *,
    response: object | None = None,
    manifest_overrides: dict[str, object] | None = None,
    request_overrides: dict[str, object] | None = None,
    artifact_overrides: dict[str, object] | None = None,
) -> tuple[str, Path]:
    rows = [_condition_row()]
    if response is None:
        response = {
            "count": len(rows),
            "request_id": "provider-request",
            "results": rows,
            "status": "OK",
        }
    raw = json.dumps(response, separators=(",", ":"), sort_keys=True).encode()
    compressed = gzip.compress(raw, mtime=0)
    artifact_relative = f"bronze/massive/condition_codes/request_id={REQUEST_ID}/page-00000.json.gz"
    artifact_path = root / artifact_relative
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(compressed)
    artifact_path.with_name(".page-00000.json.gz.swp").write_bytes(b"not-authoritative")

    artifact: dict[str, object] = {
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
    artifact.update(artifact_overrides or {})
    request: dict[str, object] = {
        "adjusted": False,
        "asset_ids": [],
        "dataset": "condition_codes",
        "end": "2026-07-09",
        "parameters": {},
        "start": "2026-07-09",
    }
    request.update(request_overrides or {})
    manifest: dict[str, object] = {
        "artifacts": [artifact],
        "checkpoint": None,
        "completed_at": CAPTURE_AT.isoformat(),
        "created_at": CAPTURE_AT.isoformat(),
        "dataset": "condition_codes",
        "manifest_schema_version": 1,
        "provider": "massive",
        "provider_contract_version": "1.1",
        "provider_version": "1.2.0",
        "request": request,
        "request_id": REQUEST_ID,
        "status": "complete",
        "updated_at": CAPTURE_AT.isoformat(),
    }
    manifest.update(manifest_overrides or {})
    manifest_relative = f"manifests/massive/condition_codes/{REQUEST_ID}.json"
    manifest_path = root / manifest_relative
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return manifest_relative, artifact_path


def test_manifest_bound_reader_preserves_complete_row_mappings_and_ignores_strays(
    tmp_path: Path,
) -> None:
    manifest_path, _ = _write_bronze_fixture(tmp_path)
    inventory = build_condition_code_source_inventory(
        tmp_path,
        manifest_paths=(manifest_path,),
        git_commit="a" * 40,
    )

    assert inventory.source_dataset == "condition_codes"
    assert inventory.source_layer is SourceLayer.BRONZE
    assert len(inventory.upstream_manifests) == len(inventory.artifacts) == 1
    assert not inventory.artifacts[0].path.endswith(".swp")

    batch = read_condition_code_source_inventory(tmp_path, inventory)
    assert isinstance(batch, ConditionCodeSourceBatch)
    assert (batch.source_object_count, batch.page_count, batch.row_count) == (2, 1, 1)
    snapshot = batch.snapshots[0]
    assert snapshot.source_request_id == REQUEST_ID
    assert snapshot.source_capture_at_utc == CAPTURE_AT
    assert snapshot.pages[0].source_provider_request_id == "provider-request"
    assert dict(snapshot.pages[0].rows[0]) == _condition_row()


def test_source_page_detaches_rows_from_caller_and_rejects_unsafe_json() -> None:
    row = _condition_row()
    page = ConditionCodeSourcePage(
        source_path="fixtures/page-00000.json.gz",
        source_artifact_sha256="b" * 64,
        sequence=0,
        source_provider_request_id="provider-request",
        rows=(row,),
    )
    row["name"] = "mutated"
    assert page.rows[0]["name"] == "Equipment Changeover"

    unsafe = _condition_row()
    unsafe["bad"] = float("nan")
    with pytest.raises(ConditionCodeSourceError, match="not safe JSON"):
        ConditionCodeSourcePage(
            source_path="fixtures/page-00000.json.gz",
            source_artifact_sha256="b" * 64,
            sequence=0,
            source_provider_request_id="provider-request",
            rows=(unsafe,),
        )


def test_reader_detects_same_size_artifact_mutation(tmp_path: Path) -> None:
    manifest_path, artifact_path = _write_bronze_fixture(tmp_path)
    inventory = build_condition_code_source_inventory(
        tmp_path,
        manifest_paths=(manifest_path,),
        git_commit="a" * 40,
    )

    mutated = bytearray(artifact_path.read_bytes())
    mutated[-1] ^= 1
    artifact_path.write_bytes(bytes(mutated))
    with pytest.raises(ConditionCodeSourceError, match="checksum mismatch"):
        read_condition_code_source_inventory(tmp_path, inventory)


@pytest.mark.parametrize(
    ("artifact_overrides", "message"),
    [
        ({"raw_sha256": "0" * 64}, "raw checksum mismatch"),
        ({"raw_bytes": 1}, "raw byte count mismatch"),
        ({"record_count": 2}, "rows differ from manifest"),
    ],
)
def test_reader_rejects_raw_integrity_and_declared_row_drift(
    tmp_path: Path,
    artifact_overrides: dict[str, object],
    message: str,
) -> None:
    manifest_path, _ = _write_bronze_fixture(
        tmp_path,
        artifact_overrides=artifact_overrides,
    )
    inventory = build_condition_code_source_inventory(
        tmp_path,
        manifest_paths=(manifest_path,),
        git_commit="a" * 40,
    )
    with pytest.raises(ConditionCodeSourceError, match=message):
        read_condition_code_source_inventory(tmp_path, inventory)


@pytest.mark.parametrize(
    ("manifest_overrides", "request_overrides"),
    [
        ({"status": "running"}, {}),
        ({"provider": "other"}, {}),
        ({"dataset": "exchanges"}, {}),
        ({}, {"dataset": "exchanges"}),
        ({}, {"start": "2026-07-08"}),
        ({}, {"asset_ids": ["AAPL"]}),
        ({}, {"parameters": {"limit": 1000}}),
        ({}, {"adjusted": True}),
    ],
)
def test_inventory_rejects_noncanonical_manifest_identity(
    tmp_path: Path,
    manifest_overrides: dict[str, object],
    request_overrides: dict[str, object],
) -> None:
    manifest_path, _ = _write_bronze_fixture(
        tmp_path,
        manifest_overrides=manifest_overrides,
        request_overrides=request_overrides,
    )
    with pytest.raises(ConditionCodeSourceError):
        build_condition_code_source_inventory(
            tmp_path,
            manifest_paths=(manifest_path,),
            git_commit="a" * 40,
        )


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
def test_reader_rejects_unsafe_response_envelopes(tmp_path: Path, response: object) -> None:
    manifest_path, _ = _write_bronze_fixture(tmp_path, response=response)
    inventory = build_condition_code_source_inventory(
        tmp_path,
        manifest_paths=(manifest_path,),
        git_commit="a" * 40,
    )
    with pytest.raises(ConditionCodeSourceError):
        read_condition_code_source_inventory(tmp_path, inventory)


def test_snapshot_and_batch_require_ordered_unique_provenance() -> None:
    page = ConditionCodeSourcePage(
        source_path="fixtures/page-00001.json.gz",
        source_artifact_sha256="b" * 64,
        sequence=1,
        source_provider_request_id="provider-request",
        rows=(),
    )
    with pytest.raises(ConditionCodeSourceError, match="contiguous and ordered"):
        ConditionCodeSourceSnapshot(
            source_request_id=REQUEST_ID,
            source_capture_at_utc=CAPTURE_AT,
            pages=(page,),
        )

    valid_page = ConditionCodeSourcePage(
        source_path="fixtures/page-00000.json.gz",
        source_artifact_sha256="b" * 64,
        sequence=0,
        source_provider_request_id="provider-request",
        rows=(),
    )
    snapshot = ConditionCodeSourceSnapshot(
        source_request_id=REQUEST_ID,
        source_capture_at_utc=CAPTURE_AT,
        pages=(valid_page,),
    )
    with pytest.raises(ConditionCodeSourceError, match="request IDs must be unique"):
        ConditionCodeSourceBatch((snapshot, snapshot))
