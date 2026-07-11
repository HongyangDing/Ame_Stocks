import asyncio
import gzip
import hashlib
import json
from collections.abc import AsyncIterator
from datetime import date
from pathlib import Path

from ame_stocks_api.artifacts import write_json_atomic
from ame_stocks_api.audit import BronzeAuditor
from ame_stocks_api.downloads import BronzeDownloader, build_download_plan
from ame_stocks_api.flatfiles import FlatFileDataset, FlatFileObject
from ame_stocks_core import ProviderBatch, ProviderDataset, ProviderRequest

MINIMAL_REQUIRED = (ProviderDataset.ASSETS,)


class StaticProvider:
    name = "massive"
    version = "audit-fixture"

    def __init__(self, results: list[dict[str, object]]) -> None:
        self.payload = json.dumps({"results": results, "status": "OK"}).encode()

    async def fetch(
        self,
        request: ProviderRequest,
        *,
        checkpoint=None,
    ) -> AsyncIterator[ProviderBatch]:
        yield ProviderBatch(
            provider=self.name,
            provider_version=self.version,
            dataset=request.dataset,
            request_id=request.request_id,
            sequence=checkpoint.next_sequence if checkpoint else 0,
            payload=self.payload,
        )


def _write_assets(root: Path, session: date, *, overlap: bool = False) -> None:
    requests = {
        dict(request.parameters)["active"]: request
        for request in build_download_plan(
            dataset=ProviderDataset.ASSETS,
            start=session,
            end=session,
            active="both",
        ).requests
    }
    asyncio.run(
        BronzeDownloader(root, minimum_free_bytes=0).download(
            StaticProvider([{"active": True, "ticker": "AAPL"}]), requests["true"]
        )
    )
    inactive = [{"active": False, "ticker": "AAPL"}] if overlap else []
    asyncio.run(
        BronzeDownloader(root, minimum_free_bytes=0).download(
            StaticProvider(inactive), requests["false"]
        )
    )


def _write_flat_file(root: Path, dataset: FlatFileDataset, session: date) -> Path:
    item = FlatFileObject(dataset, session)
    timestamp = 1_772_452_600_000_000_000
    csv_bytes = (
        b"ticker,volume,open,close,high,low,window_start,transactions\n"
        + f"AAPL,100,10,10.5,11,9.5,{timestamp},4\n".encode()
    )
    compressed = gzip.compress(csv_bytes, mtime=0)
    relative = f"bronze/massive/flatfiles/{item.object_key}"
    output = root / relative
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(compressed)
    manifest = {
        "bucket": "flatfiles",
        "created_at": "2026-07-01T00:00:00+00:00",
        "dataset": dataset.value,
        "endpoint": "https://files.massive.com",
        "flat_file_manifest_schema_version": 1,
        "object_id": item.object_id,
        "object_key": item.object_key,
        "partial_bytes": 0,
        "remote": {
            "content_length": len(compressed),
            "etag": "fixture",
            "last_modified": "2026-07-01T00:00:00+00:00",
        },
        "session_date": session.isoformat(),
        "status": "complete",
        "updated_at": "2026-07-01T00:00:00+00:00",
        "completed_at": "2026-07-01T00:00:00+00:00",
        "output": {
            "bytes": len(compressed),
            "csv_header": [
                "ticker",
                "volume",
                "open",
                "close",
                "high",
                "low",
                "window_start",
                "transactions",
            ],
            "path": relative,
            "sha256": hashlib.sha256(compressed).hexdigest(),
        },
    }
    manifest_path = (
        root / "manifests" / "massive" / "flatfiles" / dataset.value / f"{session.isoformat()}.json"
    )
    write_json_atomic(manifest_path, manifest)
    return output


def _complete_fixture(root: Path, *, overlap: bool = False) -> tuple[date, Path]:
    session = date(2026, 6, 30)
    _write_assets(root, session, overlap=overlap)
    minute = _write_flat_file(root, FlatFileDataset.MINUTE_AGGREGATES, session)
    _write_flat_file(root, FlatFileDataset.DAY_AGGREGATES, session)
    return session, minute


def test_full_audit_verifies_complete_fixture(tmp_path: Path) -> None:
    session, _ = _complete_fixture(tmp_path)

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        mode="full",
        workers=2,
        required_rest_datasets=MINIMAL_REQUIRED,
    ).run()

    assert report["status"] == "passed"
    assert report["summary"]["verified_files"] == 4
    assert report["summary"]["issue_counts"] == {}
    stats = {item["dataset"]: item for item in report["datasets"]}
    assert stats["minute_aggregates"]["flat_file_rows"] == 1
    assert stats["assets"]["expected_objects"] == 2


def test_full_audit_detects_file_corruption(tmp_path: Path) -> None:
    session, minute = _complete_fixture(tmp_path)
    content = bytearray(minute.read_bytes())
    content[len(content) // 2] ^= 0x01
    minute.write_bytes(content)

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        required_rest_datasets=MINIMAL_REQUIRED,
    ).run()

    assert report["status"] == "failed"
    codes = report["summary"]["issue_code_counts"]
    assert codes["stored_sha256_mismatch"] == 1
    assert codes["gzip_corrupt"] == 1


def test_full_audit_detects_active_inactive_overlap(tmp_path: Path) -> None:
    session, _ = _complete_fixture(tmp_path, overlap=True)

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        required_rest_datasets=MINIMAL_REQUIRED,
    ).run()

    assert report["status"] == "failed"
    assert report["summary"]["issue_code_counts"]["asset_active_inactive_overlap"] == 1


def test_ticker_event_404_is_accounted_terminal_gap(tmp_path: Path) -> None:
    session, _ = _complete_fixture(tmp_path)
    request = ProviderRequest(
        dataset=ProviderDataset.TICKER_EVENTS,
        start=date(2003, 9, 10),
        end=session,
        asset_ids=("BBG000MISSING",),
        parameters=(("types", "ticker_change"),),
    )
    manifest = {
        "artifacts": [],
        "checkpoint": None,
        "created_at": "2026-07-01T00:00:00+00:00",
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
        "updated_at": "2026-07-01T00:00:00+00:00",
    }
    write_json_atomic(
        tmp_path / "manifests" / "massive" / "ticker_events" / f"{request.request_id}.json",
        manifest,
    )
    receipt = tmp_path / "manifests" / "plans" / "ticker_events" / "identifiers.txt"
    receipt.parent.mkdir(parents=True)
    receipt.write_text("BBG000MISSING\n", encoding="utf-8")

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        required_rest_datasets=MINIMAL_REQUIRED,
    ).run()

    assert report["status"] == "passed_with_warnings"
    stats = {item["dataset"]: item for item in report["datasets"]}
    assert stats["ticker_events"]["missing_expected"] == 0
    assert stats["ticker_events"]["unavailable_expected"] == 1
    assert report["summary"]["issue_code_counts"] == {"ticker_event_identifier_not_found": 1}


def test_required_dataset_is_not_silently_dropped_when_its_directory_is_missing(
    tmp_path: Path,
) -> None:
    session, _ = _complete_fixture(tmp_path)

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        required_rest_datasets=(ProviderDataset.ASSETS, ProviderDataset.CONDITION_CODES),
    ).run()

    assert report["status"] == "failed"
    assert report["gates"]["authoritative_plan"] == "failed"
    stats = {item["dataset"]: item for item in report["datasets"]}
    assert stats["condition_codes"]["expected_objects"] == 1
    assert stats["condition_codes"]["missing_expected"] == 1
    assert report["required_profile"]["rest_datasets"] == ["assets", "condition_codes"]


def test_corrupt_asset_page_returns_a_failed_report_instead_of_aborting(tmp_path: Path) -> None:
    session, _ = _complete_fixture(tmp_path)
    manifest_path = next((tmp_path / "manifests" / "massive" / "assets").glob("*.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifact_path = tmp_path / manifest["artifacts"][0]["path"]
    content = bytearray(artifact_path.read_bytes())
    content[len(content) // 2] ^= 0x01
    artifact_path.write_bytes(content)

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        required_rest_datasets=MINIMAL_REQUIRED,
    ).run()

    assert report["status"] == "failed"
    codes = report["summary"]["issue_code_counts"]
    assert codes["gzip_corrupt"] == 1
    assert codes["asset_reconciliation_unreadable"] == 1


def test_non_iso_nonblank_row_date_fails_semantic_gate(tmp_path: Path) -> None:
    session, _ = _complete_fixture(tmp_path)
    request = ProviderRequest(
        dataset=ProviderDataset.SPLITS,
        start=date(2003, 9, 10),
        end=session,
    )
    asyncio.run(
        BronzeDownloader(tmp_path, minimum_free_bytes=0).download(
            StaticProvider([{"execution_date": "not-a-date", "ticker": "AAPL"}]), request
        )
    )

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        required_rest_datasets=MINIMAL_REQUIRED,
    ).run()

    assert report["status"] == "failed"
    assert report["gates"]["semantic_consistency"] == "failed"
    assert report["summary"]["issue_code_counts"]["invalid_date_value"] == 1
