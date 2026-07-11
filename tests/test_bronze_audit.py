import asyncio
import gzip
import hashlib
import json
from collections import Counter
from collections.abc import AsyncIterator
from datetime import date
from pathlib import Path

from ame_stocks_api.artifacts import write_json_atomic
from ame_stocks_api.audit import BronzeAuditor
from ame_stocks_api.audit.bronze import _gate_issue_code_counts, _iso_datetime_date
from ame_stocks_api.cli.audit import main as bronze_audit_main
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


class PagedProvider:
    name = "massive"
    version = "audit-paged-fixture"

    def __init__(self, pages: list[list[dict[str, object]]]) -> None:
        self.payloads = [
            json.dumps({"results": rows, "status": "OK"}).encode() for rows in pages
        ]

    async def fetch(
        self,
        request: ProviderRequest,
        *,
        checkpoint=None,
    ) -> AsyncIterator[ProviderBatch]:
        start = checkpoint.next_sequence if checkpoint else 0
        for sequence in range(start, len(self.payloads)):
            is_last = sequence == len(self.payloads) - 1
            yield ProviderBatch(
                provider=self.name,
                provider_version=self.version,
                dataset=request.dataset,
                request_id=request.request_id,
                sequence=sequence,
                payload=self.payloads[sequence],
                next_cursor=None if is_last else f"page-{sequence + 1}",
                is_last=is_last,
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


def _rewrite_single_page(
    root: Path,
    request: ProviderRequest,
    document: dict[str, object],
    *,
    record_count: int,
) -> None:
    manifest_path = (
        root
        / "manifests"
        / "massive"
        / request.dataset.value
        / f"{request.request_id}.json"
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifact = manifest["artifacts"][0]
    raw = json.dumps(document, separators=(",", ":")).encode()
    compressed = gzip.compress(raw, mtime=0)
    (root / artifact["path"]).write_bytes(compressed)
    artifact.update(
        {
            "compressed_bytes": len(compressed),
            "raw_bytes": len(raw),
            "raw_sha256": hashlib.sha256(raw).hexdigest(),
            "record_count": record_count,
            "stored_sha256": hashlib.sha256(compressed).hexdigest(),
        }
    )
    write_json_atomic(manifest_path, manifest)


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


def test_ticker_event_receipt_rejects_duplicate_normalized_identifiers(
    tmp_path: Path,
) -> None:
    session, _ = _complete_fixture(tmp_path)
    receipt = tmp_path / "manifests" / "plans" / "ticker_events" / "identifiers.txt"
    receipt.parent.mkdir(parents=True)
    receipt.write_text("BBG000DUPLICATE\n BBG000DUPLICATE \n", encoding="utf-8")

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        required_rest_datasets=(ProviderDataset.ASSETS, ProviderDataset.TICKER_EVENTS),
    ).run()

    assert report["status"] == "failed"
    assert report["gates"]["authoritative_plan"] == "failed"
    assert report["summary"]["issue_code_counts"]["ticker_event_plan_invalid"] == 1


def test_ticker_overview_receipt_rejects_duplicate_normalized_requests(
    tmp_path: Path,
) -> None:
    session, _ = _complete_fixture(tmp_path)
    request = build_download_plan(
        dataset=ProviderDataset.TICKER_OVERVIEW,
        start=session,
        end=session,
        ticker_dates=(("AAPL", session),),
    ).requests[0]
    asyncio.run(
        BronzeDownloader(tmp_path, minimum_free_bytes=0).download(
            StaticProvider([{"ticker": "AAPL"}]), request
        )
    )
    receipt = tmp_path / "overview-requests.csv"
    receipt.write_text(
        f"ticker,query_date\nAAPL,{session.isoformat()}\n AAPL ,{session.isoformat()}\n",
        encoding="utf-8",
    )

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        ticker_overview_plan=receipt,
        required_rest_datasets=(ProviderDataset.ASSETS, ProviderDataset.TICKER_OVERVIEW),
    ).run()

    assert report["status"] == "failed"
    assert report["gates"]["authoritative_plan"] == "failed"
    assert report["summary"]["issue_code_counts"]["ticker_overview_plan_invalid"] == 1


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


def test_every_failure_code_is_mapped_to_at_least_one_gate(tmp_path: Path) -> None:
    session, _ = _complete_fixture(tmp_path)
    manifest_path = next((tmp_path / "manifests" / "massive" / "assets").glob("*.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["provider"] = "unexpected-provider"
    write_json_atomic(manifest_path, manifest)

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        required_rest_datasets=MINIMAL_REQUIRED,
    ).run()

    assert report["status"] == "failed"
    assert report["gates"]["physical_integrity"] == "failed"
    assert report["gate_issue_code_counts"]["physical_integrity"] == {
        "provider_mismatch": 1
    }
    mapped_codes = {
        code
        for gate_counts in report["gate_issue_code_counts"].values()
        for code in gate_counts
    }
    failure_codes = {
        issue["code"]
        for issue in report["issue_samples"]
        if issue["severity"] in {"error", "critical"}
    }
    assert failure_codes <= mapped_codes
    assert "failed" in report["gates"].values()


def test_unclassified_future_failure_conservatively_maps_to_physical_gate() -> None:
    gate_counts = _gate_issue_code_counts(Counter({"future_failure_code": 2}))

    assert gate_counts["physical_integrity"] == {"future_failure_code": 2}
    assert gate_counts["authoritative_plan"] == {}
    assert gate_counts["semantic_consistency"] == {}


def test_asset_reconciliation_uses_only_authoritative_request_ids(tmp_path: Path) -> None:
    session, _ = _complete_fixture(tmp_path)
    formal_true = next(
        request
        for request in build_download_plan(
            dataset=ProviderDataset.ASSETS,
            start=session,
            end=session,
            active="both",
        ).requests
        if dict(request.parameters)["active"] == "true"
    )
    pilot = None
    for index in range(100_000):
        candidate = ProviderRequest(
            dataset=ProviderDataset.ASSETS,
            start=session,
            end=session,
            parameters=(("active", "true"), ("pilot", str(index))),
        )
        if candidate.request_id > formal_true.request_id:
            pilot = candidate
            break
    assert pilot is not None
    asyncio.run(
        BronzeDownloader(tmp_path, minimum_free_bytes=0).download(
            StaticProvider([{"active": False, "ticker": "PILOT"}]), pilot
        )
    )

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        required_rest_datasets=MINIMAL_REQUIRED,
    ).run()

    assert report["status"] == "passed_with_warnings"
    assert report["gates"]["semantic_consistency"] == "passed"
    assert "asset_active_flag_mismatch" not in report["summary"]["issue_code_counts"]
    stats = {item["dataset"]: item for item in report["datasets"]}
    assert stats["assets"]["extra_objects"] == 1


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


def test_news_published_utc_requires_a_full_timezone_aware_datetime(tmp_path: Path) -> None:
    session, _ = _complete_fixture(tmp_path)
    request = build_download_plan(
        dataset=ProviderDataset.NEWS,
        start=date(2016, 6, 22),
        end=session,
    ).requests[-1]
    asyncio.run(
        BronzeDownloader(tmp_path, minimum_free_bytes=0).download(
            StaticProvider(
                [
                    {
                        "id": "news-1",
                        "published_utc": f"{session.isoformat()}T12:00:00",
                    }
                ]
            ),
            request,
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
    assert report["summary"]["issue_code_counts"]["invalid_timestamp_value"] == 1
    assert _iso_datetime_date(f"{session.isoformat()}T12:00:00Z") == session.isoformat()


def test_observed_date_bounds_accumulate_across_pages(tmp_path: Path) -> None:
    session, _ = _complete_fixture(tmp_path)
    request = ProviderRequest(
        dataset=ProviderDataset.SPLITS,
        start=date(2003, 9, 10),
        end=session,
    )
    asyncio.run(
        BronzeDownloader(tmp_path, minimum_free_bytes=0).download(
            PagedProvider(
                [
                    [{"execution_date": "2004-01-02", "ticker": "OLD"}],
                    [{"execution_date": session.isoformat(), "ticker": "NEW"}],
                ]
            ),
            request,
        )
    )

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        required_rest_datasets=MINIMAL_REQUIRED,
    ).run()

    stats = {item["dataset"]: item for item in report["datasets"]}
    assert stats["splits"]["observed_min_date"] == "2004-01-02"
    assert stats["splits"]["observed_max_date"] == session.isoformat()


def test_missing_results_key_is_not_treated_as_an_empty_response(tmp_path: Path) -> None:
    session, _ = _complete_fixture(tmp_path)
    active_request = next(
        request
        for request in build_download_plan(
            dataset=ProviderDataset.ASSETS,
            start=session,
            end=session,
            active="both",
        ).requests
        if dict(request.parameters)["active"] == "true"
    )
    _rewrite_single_page(
        tmp_path,
        active_request,
        {"status": "OK"},
        record_count=0,
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
    assert report["summary"]["issue_code_counts"]["results_shape_invalid"] == 1


def test_ticker_events_require_the_endpoint_envelope_and_event_contract(
    tmp_path: Path,
) -> None:
    session, _ = _complete_fixture(tmp_path)
    identifier = "BBG000EVENT"
    request = build_download_plan(
        dataset=ProviderDataset.TICKER_EVENTS,
        start=date(2003, 9, 10),
        end=session,
        tickers=(identifier,),
    ).requests[0]
    asyncio.run(
        BronzeDownloader(tmp_path, minimum_free_bytes=0).download(
            StaticProvider([{}]), request
        )
    )
    _rewrite_single_page(
        tmp_path,
        request,
        {"status": "OK", "results": {"events": [{}]}},
        record_count=1,
    )
    receipt = tmp_path / "manifests" / "plans" / "ticker_events" / "identifiers.txt"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(f"{identifier}\n", encoding="utf-8")

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        required_rest_datasets=(ProviderDataset.ASSETS, ProviderDataset.TICKER_EVENTS),
    ).run()

    assert report["status"] == "failed"
    assert report["summary"]["issue_code_counts"]["ticker_event_contract_invalid"] == 1


def test_ticker_overview_response_must_match_requested_ticker(tmp_path: Path) -> None:
    session, _ = _complete_fixture(tmp_path)
    request = build_download_plan(
        dataset=ProviderDataset.TICKER_OVERVIEW,
        start=session,
        end=session,
        ticker_dates=(("RIGHT", session),),
    ).requests[0]
    asyncio.run(
        BronzeDownloader(tmp_path, minimum_free_bytes=0).download(
            StaticProvider([{"ticker": "WRONG"}]), request
        )
    )
    _rewrite_single_page(
        tmp_path,
        request,
        {"status": "OK", "results": {"ticker": "WRONG"}},
        record_count=1,
    )
    receipt = tmp_path / "overview.csv"
    receipt.write_text(
        f"ticker,query_date\nRIGHT,{session.isoformat()}\n",
        encoding="utf-8",
    )

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        ticker_overview_plan=receipt,
        required_rest_datasets=(ProviderDataset.ASSETS, ProviderDataset.TICKER_OVERVIEW),
    ).run()

    assert report["status"] == "failed"
    assert report["summary"]["issue_code_counts"]["ticker_overview_identity_mismatch"] == 1


def test_form_13f_requires_research_identity_and_holding_fields(tmp_path: Path) -> None:
    session, _ = _complete_fixture(tmp_path)
    request = build_download_plan(
        dataset=ProviderDataset.FORM_13F,
        start=session,
        end=session,
    ).requests[0]
    asyncio.run(
        BronzeDownloader(tmp_path, minimum_free_bytes=0).download(
            StaticProvider([{"filing_date": session.isoformat()}]), request
        )
    )

    report = BronzeAuditor(
        tmp_path,
        start=session,
        end=session,
        workers=1,
        required_rest_datasets=(ProviderDataset.ASSETS, ProviderDataset.FORM_13F),
    ).run()

    assert report["status"] == "failed"
    assert report["gates"]["semantic_consistency"] == "failed"
    assert report["summary"]["issue_code_counts"]["required_fields_missing"] == 1


def test_bronze_cli_persists_the_same_report_it_prints(tmp_path: Path, capsys) -> None:
    session, _ = _complete_fixture(tmp_path)
    relative_output = Path("manifests/audits/bronze-cli.json")

    result = bronze_audit_main(
        [
            "--data-root",
            str(tmp_path),
            "--start",
            session.isoformat(),
            "--end",
            session.isoformat(),
            "--mode",
            "structural",
            "--workers",
            "1",
            "--output",
            str(relative_output),
        ]
    )

    printed = json.loads(capsys.readouterr().out)
    stored = json.loads((tmp_path / relative_output).read_text(encoding="utf-8"))
    assert result == 1
    assert stored == printed
    assert stored["report_path"] == str((tmp_path / relative_output).resolve())
