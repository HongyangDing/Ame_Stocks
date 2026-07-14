from __future__ import annotations

import gzip
import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from ame_stocks_api.downloads.plan import build_download_plan
from ame_stocks_api.silver.contracts import SourceLayer
from ame_stocks_api.silver.store import SilverStore
from ame_stocks_api.silver.ticker_event_source import (
    TickerEventSourceError,
    build_ticker_event_source_inventory,
    load_ticker_event_coverage_receipt,
    read_ticker_event_source_inventory,
    ticker_event_coverage_receipt_path,
    ticker_event_occurrence_transform_inputs,
    ticker_event_request_transform_inputs,
    ticker_event_transform_inputs,
)
from ame_stocks_api.silver.ticker_event_source_profile import (
    TickerEventCoverageExpectation,
    accepted_coverage_receipt,
    coverage_receipt_bytes,
    profile_ticker_event_source,
)
from ame_stocks_core import ProviderDataset

START = date(2003, 9, 10)
END = date(2026, 7, 9)
FORMAL = ("BBG000000001", "BBG000000002")
PILOT = ("TEST",)
FORMAL_RECEIPT = "manifests/plans/ticker_events/formal-source-fixture.txt"
PILOT_RECEIPT = "manifests/plans/ticker_events/pilot-source-fixture.txt"
EXPECTED = TickerEventCoverageExpectation(
    formal_identifiers=2,
    formal_complete=1,
    formal_not_found_404=1,
    pilot_identifiers=1,
    pilot_complete=1,
    pilot_not_found_404=0,
    formal_events=2,
    formal_blank_targets=1,
    formal_sentinel_dates=1,
    formal_coverage_floor_dates=0,
    formal_after_declared_end_dates=0,
)


def _fixture(root: Path) -> tuple[str, str, Path, Path]:
    for relative, identifiers in ((FORMAL_RECEIPT, FORMAL), (PILOT_RECEIPT, PILOT)):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(f"{item}\n" for item in identifiers), encoding="utf-8")
    formal_plan = build_download_plan(
        dataset=ProviderDataset.TICKER_EVENTS,
        start=START,
        end=END,
        tickers=FORMAL,
    )
    pilot_plan = build_download_plan(
        dataset=ProviderDataset.TICKER_EVENTS,
        start=START,
        end=END,
        tickers=PILOT,
    )
    requests = {item.asset_ids[0]: item for item in formal_plan.requests}
    formal_manifest, formal_page = _complete(
        root,
        requests[FORMAL[0]],
        {
            "composite_figi": FORMAL[0],
            "events": [
                {
                    "date": "1969-12-31",
                    "ticker_change": {"ticker": "OLD"},
                    "type": "ticker_change",
                },
                {
                    "date": "2023-11-18",
                    "ticker_change": {"ticker": ""},
                    "type": "ticker_change",
                },
            ],
            "name": "Formal fixture",
        },
    )
    _failed(root, requests[FORMAL[1]])
    _complete(
        root,
        pilot_plan.requests[0],
        {
            "cik": "0000000001",
            "events": [
                {
                    "date": "2025-01-02",
                    "ticker_change": {"ticker": "TEST"},
                    "type": "ticker_change",
                }
            ],
            "name": "Pilot fixture",
        },
    )
    report = profile_ticker_event_source(
        root,
        formal_receipt_path=FORMAL_RECEIPT,
        pilot_receipt_path=PILOT_RECEIPT,
        expected=EXPECTED,
        request_start=START,
        request_end=END,
    )
    receipt = accepted_coverage_receipt(report)
    relative = ticker_event_coverage_receipt_path(receipt)
    content = coverage_receipt_bytes(receipt)
    path = root / relative
    path.parent.mkdir(parents=True)
    path.write_bytes(content)
    return relative, hashlib.sha256(content).hexdigest(), formal_manifest, formal_page


def _complete(root: Path, request, results: dict[str, object]) -> tuple[Path, Path]:
    response = {"request_id": "provider-request", "results": results, "status": "OK"}
    raw = json.dumps(response, separators=(",", ":"), sort_keys=True).encode()
    compressed = gzip.compress(raw, mtime=0)
    relative = f"bronze/massive/ticker_events/request_id={request.request_id}/page-00000.json.gz"
    page = root / relative
    page.parent.mkdir(parents=True)
    page.write_bytes(compressed)
    manifest = _manifest_base(request)
    manifest.update(
        {
            "artifacts": [
                {
                    "compressed_bytes": len(compressed),
                    "content_type": "application/json",
                    "is_last": True,
                    "next_continuation": None,
                    "path": relative,
                    "raw_bytes": len(raw),
                    "raw_sha256": hashlib.sha256(raw).hexdigest(),
                    "record_count": len(results["events"]),
                    "sequence": 0,
                    "stored_sha256": hashlib.sha256(compressed).hexdigest(),
                }
            ],
            "completed_at": "2026-07-11T16:00:01+00:00",
            "status": "complete",
        }
    )
    path = root / f"manifests/massive/ticker_events/{request.request_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, separators=(",", ":"), sort_keys=True))
    return path, page


def _failed(root: Path, request) -> Path:
    manifest = _manifest_base(request)
    manifest.update(
        {
            "artifacts": [],
            "failure": {
                "error_type": "MassiveRequestError",
                "message": "download interrupted; retrying this request is safe",
                "provider_status_code": 404,
            },
            "status": "failed",
        }
    )
    path = root / f"manifests/massive/ticker_events/{request.request_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, separators=(",", ":"), sort_keys=True))
    return path


def _manifest_base(request) -> dict[str, object]:
    return {
        "checkpoint": None,
        "created_at": "2026-07-11T16:00:00+00:00",
        "dataset": "ticker_events",
        "manifest_schema_version": 1,
        "provider": "massive",
        "provider_contract_version": "1.1",
        "provider_version": "1.2.0",
        "request": request.canonical_dict(),
        "request_id": request.request_id,
        "updated_at": "2026-07-11T19:00:00+00:00",
    }


def test_coverage_receipt_is_only_upstream_and_reader_reverifies_all(tmp_path: Path) -> None:
    receipt_path, receipt_sha, _, _ = _fixture(tmp_path)
    receipt = load_ticker_event_coverage_receipt(
        tmp_path,
        coverage_receipt_path=receipt_path,
        coverage_receipt_sha256=receipt_sha,
    )
    assert (receipt.request_count, receipt.complete_count, receipt.not_found_count) == (2, 1, 1)

    inventory = build_ticker_event_source_inventory(
        tmp_path,
        coverage_receipt_path=receipt_path,
        coverage_receipt_sha256=receipt_sha,
        git_commit="a" * 40,
    )
    assert inventory.source_dataset == "ticker_events"
    assert inventory.source_layer is SourceLayer.BRONZE
    assert [(item.path, item.sha256) for item in inventory.upstream_manifests] == [
        (receipt_path, receipt_sha)
    ]
    assert len(inventory.artifacts) == 1
    assert PILOT[0] not in inventory.artifacts[0].path
    SilverStore(tmp_path).register_source_inventory(inventory)

    batch = read_ticker_event_source_inventory(tmp_path, inventory)
    assert (batch.request_count, batch.page_count, batch.not_found_count, batch.row_count) == (
        2,
        1,
        1,
        2,
    )
    request_inputs = ticker_event_request_transform_inputs(batch)
    occurrence_inputs = ticker_event_occurrence_transform_inputs(batch)
    assert ticker_event_transform_inputs(batch) == (request_inputs, occurrence_inputs)
    assert [item["outcome"] for item in request_inputs] == ["complete", "not_found_404"]
    assert request_inputs[0]["result_composite_figi"] == FORMAL[0]
    assert request_inputs[0]["source_page_count"] == 1
    assert request_inputs[1]["result_composite_figi"] is None
    assert request_inputs[1]["source_page_count"] == 0
    assert [item["date_quality"] for item in occurrence_inputs] == [
        "provider_sentinel_unknown_date",
        "blank_target_placeholder",
    ]
    assert all(item["result_composite_figi"] == FORMAL[0] for item in occurrence_inputs)
    assert all(len(str(item["source_event_hash"])) == 64 for item in occurrence_inputs)
    assert all(len(str(item["source_result_hash"])) == 64 for item in occurrence_inputs)


@pytest.mark.parametrize("target", ["manifest", "page", "receipt"])
def test_reader_detects_post_receipt_mutation(tmp_path: Path, target: str) -> None:
    receipt_path, receipt_sha, manifest_path, page_path = _fixture(tmp_path)
    inventory = build_ticker_event_source_inventory(
        tmp_path,
        coverage_receipt_path=receipt_path,
        coverage_receipt_sha256=receipt_sha,
        git_commit="a" * 40,
    )
    path = {
        "manifest": manifest_path,
        "page": page_path,
        "receipt": tmp_path / receipt_path,
    }[target]
    content = bytearray(path.read_bytes())
    content[-1] ^= 1
    path.write_bytes(content)
    with pytest.raises(TickerEventSourceError):
        read_ticker_event_source_inventory(tmp_path, inventory)


def test_production_builder_has_no_raw_manifest_fallback(tmp_path: Path) -> None:
    with pytest.raises(TypeError):
        build_ticker_event_source_inventory(  # type: ignore[call-arg]
            tmp_path,
            manifest_paths=("manifests/massive/ticker_events/unsafe.json",),
            git_commit="a" * 40,
        )
