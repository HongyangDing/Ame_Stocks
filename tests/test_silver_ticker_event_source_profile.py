from __future__ import annotations

import gzip
import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from ame_stocks_api.downloads.plan import build_download_plan
from ame_stocks_api.silver.ticker_event_source_profile import (
    TickerEventCoverageExpectation,
    TickerEventSourceProfileError,
    accepted_coverage_receipt,
    profile_ticker_event_source,
    validate_ticker_event_coverage_receipt,
)
from ame_stocks_core import ProviderDataset

START = date(2003, 9, 10)
END = date(2026, 7, 9)
FORMAL_RECEIPT = "manifests/plans/ticker_events/identifiers.txt"
PILOT_RECEIPT = "manifests/plans/ticker_events/pilot-fixture.txt"
FORMAL_COMPLETE = "BBG000000001"
FORMAL_MISSING = "BBG000000002"
PILOT_COMPLETE = "TEST"
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


def build_ticker_event_fixture(root: Path) -> dict[str, Path]:
    formal_path = root / FORMAL_RECEIPT
    pilot_path = root / PILOT_RECEIPT
    formal_path.parent.mkdir(parents=True, exist_ok=True)
    formal_path.write_text(f"{FORMAL_COMPLETE}\n{FORMAL_MISSING}\n", encoding="utf-8")
    pilot_path.write_text(f"{PILOT_COMPLETE}\n", encoding="utf-8")
    formal_plan = build_download_plan(
        dataset=ProviderDataset.TICKER_EVENTS,
        start=START,
        end=END,
        tickers=(FORMAL_COMPLETE, FORMAL_MISSING),
    )
    pilot_plan = build_download_plan(
        dataset=ProviderDataset.TICKER_EVENTS,
        start=START,
        end=END,
        tickers=(PILOT_COMPLETE,),
    )
    requests = {item.asset_ids[0]: item for item in formal_plan.requests}
    complete_manifest, complete_page = _write_complete(
        root,
        requests[FORMAL_COMPLETE],
        results={
            "composite_figi": FORMAL_COMPLETE,
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
            "name": "Fixture formal",
        },
    )
    failed_manifest = _write_failed(root, requests[FORMAL_MISSING])
    pilot_manifest, pilot_page = _write_complete(
        root,
        pilot_plan.requests[0],
        results={
            "cik": "0000000001",
            "events": [
                {
                    "date": "2025-01-02",
                    "ticker_change": {"ticker": "ABC"},
                    "type": "ticker_change",
                }
            ],
            "name": "Fixture pilot",
        },
    )
    return {
        "complete_manifest": complete_manifest,
        "complete_page": complete_page,
        "failed_manifest": failed_manifest,
        "formal_receipt": formal_path,
        "pilot_manifest": pilot_manifest,
        "pilot_page": pilot_page,
        "pilot_receipt": pilot_path,
    }


def _write_complete(root: Path, request, *, results: dict[str, object]) -> tuple[Path, Path]:
    response = {
        "request_id": f"provider-{request.request_id[:12]}",
        "results": results,
        "status": "OK",
    }
    raw = json.dumps(response, separators=(",", ":"), sort_keys=True).encode()
    compressed = gzip.compress(raw, mtime=0)
    relative_page = (
        f"bronze/massive/ticker_events/request_id={request.request_id}/page-00000.json.gz"
    )
    page = root / relative_page
    page.parent.mkdir(parents=True)
    page.write_bytes(compressed)
    manifest = {
        "artifacts": [
            {
                "compressed_bytes": len(compressed),
                "content_type": "application/json",
                "is_last": True,
                "next_continuation": None,
                "path": relative_page,
                "raw_bytes": len(raw),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "record_count": len(results["events"]),
                "sequence": 0,
                "stored_sha256": hashlib.sha256(compressed).hexdigest(),
            }
        ],
        "checkpoint": None,
        "completed_at": "2026-07-11T16:00:01+00:00",
        "created_at": "2026-07-11T16:00:00+00:00",
        "dataset": "ticker_events",
        "manifest_schema_version": 1,
        "provider": "massive",
        "provider_contract_version": "1.1",
        "provider_version": "1.2.0",
        "request": request.canonical_dict(),
        "request_id": request.request_id,
        "status": "complete",
        "updated_at": "2026-07-11T16:00:01+00:00",
    }
    path = root / f"manifests/massive/ticker_events/{request.request_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    return path, page


def _write_failed(root: Path, request) -> Path:
    manifest = {
        "artifacts": [],
        "checkpoint": None,
        "created_at": "2026-07-11T16:00:00+00:00",
        "dataset": "ticker_events",
        "failure": {
            "error_type": "MassiveRequestError",
            "message": "download interrupted; retrying this request is safe",
            "provider_status_code": 404,
        },
        "manifest_schema_version": 1,
        "provider": "massive",
        "provider_contract_version": "1.1",
        "provider_version": "1.2.0",
        "request": request.canonical_dict(),
        "request_id": request.request_id,
        "status": "failed",
        "updated_at": "2026-07-11T19:00:00+00:00",
    }
    path = root / f"manifests/massive/ticker_events/{request.request_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    return path


def profile_fixture(root: Path) -> dict[str, object]:
    return profile_ticker_event_source(
        root,
        formal_receipt_path=FORMAL_RECEIPT,
        pilot_receipt_path=PILOT_RECEIPT,
        expected=EXPECTED,
        request_start=START,
        request_end=END,
    )


def test_profile_is_read_only_reconciled_and_excludes_pilot(tmp_path: Path) -> None:
    build_ticker_event_fixture(tmp_path)
    before = {
        path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    report = profile_fixture(tmp_path)
    receipt = accepted_coverage_receipt(report)
    assert report["status"] == "passed_with_warnings"
    assert set(report["hard_gate_counts"].values()) == {0}
    assert receipt["formal_counts"] == {
        "artifacts": 1,
        "complete": 1,
        "events": 2,
        "identifiers": 2,
        "not_found_404": 1,
    }
    assert len(receipt["formal_manifest_refs"]) == 2
    assert len(receipt["artifacts"]) == 1
    formal_path = tmp_path / FORMAL_RECEIPT
    assert receipt["formal_identifier_receipt"] == {
        "bytes": formal_path.stat().st_size,
        "identifier_count": 2,
        "path": FORMAL_RECEIPT,
        "row_count": 2,
        "sha256": hashlib.sha256(formal_path.read_bytes()).hexdigest(),
    }
    assert formal_path.stat().st_size > 0
    assert receipt["pilot_exclusion"]["identifier_count"] == 1
    assert receipt["pilot_exclusion"]["included_in_inventory"] is False
    assert receipt["diagnostics"]["date_quality"] == {
        "blank_target_placeholder": 1,
        "provider_sentinel_unknown_date": 1,
    }
    assert receipt["diagnostics"]["cik_coverage"] == {"missing": 1}
    assert validate_ticker_event_coverage_receipt(receipt) == receipt
    after = {
        path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    assert after == before


@pytest.mark.parametrize(
    "mutation",
    ["missing_row_count", "count_mismatch", "wrong_namespace", "legacy_version"],
)
def test_coverage_receipt_rejects_invalid_formal_identifier_binding(
    tmp_path: Path, mutation: str
) -> None:
    build_ticker_event_fixture(tmp_path)
    receipt = accepted_coverage_receipt(profile_fixture(tmp_path))
    formal = receipt["formal_identifier_receipt"]
    assert isinstance(formal, dict)
    if mutation == "missing_row_count":
        formal.pop("row_count")
    elif mutation == "count_mismatch":
        formal["row_count"] = 1
    elif mutation == "wrong_namespace":
        formal["path"] = "manifests/plans/not_ticker_events/identifiers.txt"
    else:
        receipt["coverage_receipt_schema_version"] = 1
    with pytest.raises(TickerEventSourceProfileError):
        validate_ticker_event_coverage_receipt(receipt)


@pytest.mark.parametrize("mutation", ["schema", "non_404", "checksum", "figi"])
def test_profile_fails_closed_on_unreviewed_drift(tmp_path: Path, mutation: str) -> None:
    files = build_ticker_event_fixture(tmp_path)
    if mutation == "schema":
        document = json.loads(files["complete_manifest"].read_text())
        document["unexpected"] = True
        files["complete_manifest"].write_text(json.dumps(document))
    elif mutation == "non_404":
        document = json.loads(files["failed_manifest"].read_text())
        document["failure"]["provider_status_code"] = 500
        files["failed_manifest"].write_text(json.dumps(document))
    elif mutation == "checksum":
        content = bytearray(files["complete_page"].read_bytes())
        content[-1] ^= 1
        files["complete_page"].write_bytes(content)
    else:
        manifest = json.loads(files["complete_manifest"].read_text())
        page = files["complete_page"]
        response = json.loads(gzip.decompress(page.read_bytes()))
        response["results"]["composite_figi"] = "BBG999999999"
        raw = json.dumps(response, separators=(",", ":"), sort_keys=True).encode()
        compressed = gzip.compress(raw, mtime=0)
        page.write_bytes(compressed)
        artifact = manifest["artifacts"][0]
        artifact.update(
            {
                "compressed_bytes": len(compressed),
                "raw_bytes": len(raw),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "stored_sha256": hashlib.sha256(compressed).hexdigest(),
            }
        )
        files["complete_manifest"].write_text(json.dumps(manifest))
    with pytest.raises(TickerEventSourceProfileError):
        profile_fixture(tmp_path)
