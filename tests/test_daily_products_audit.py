import asyncio
import gzip
import hashlib
import json
from collections.abc import AsyncIterator
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from ame_stocks_api.artifacts import write_json_atomic
from ame_stocks_api.audit.daily_products import DailyProductCrossAuditor
from ame_stocks_api.cli.daily_products_audit import main as daily_products_audit_main
from ame_stocks_api.downloads import BronzeDownloader, build_download_plan
from ame_stocks_api.flatfiles import FlatFileDataset, FlatFileObject
from ame_stocks_core import ProviderBatch, ProviderDataset, ProviderRequest

SESSION = date(2026, 6, 30)
NEW_YORK = ZoneInfo("America/New_York")
HEADER = "ticker,volume,open,close,high,low,window_start,transactions\n"


class _RestProvider:
    name = "massive"
    version = "daily-products-fixture"

    def __init__(self, document: dict[str, object]) -> None:
        self.payload = json.dumps(document, separators=(",", ":"), sort_keys=True).encode()

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
            sequence=0,
            payload=self.payload,
        )


def _session_start(session: date) -> datetime:
    return datetime.combine(session, time.min, tzinfo=NEW_YORK)


def _session_start_ns(session: date) -> int:
    return int(_session_start(session).timestamp() * 1_000_000_000)


def _session_start_ms(session: date) -> int:
    return int(_session_start(session).timestamp() * 1000)


def _write_flat(root: Path, rows: list[str], *, session: date = SESSION) -> Path:
    item = FlatFileObject(FlatFileDataset.DAY_AGGREGATES, session)
    compressed = gzip.compress((HEADER + "".join(rows)).encode(), mtime=0)
    relative = f"bronze/massive/flatfiles/{item.object_key}"
    artifact_path = root / relative
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(compressed)
    manifest_path = (
        root
        / "manifests"
        / "massive"
        / "flatfiles"
        / FlatFileDataset.DAY_AGGREGATES.value
        / f"{session.isoformat()}.json"
    )
    write_json_atomic(
        manifest_path,
        {
            "dataset": FlatFileDataset.DAY_AGGREGATES.value,
            "flat_file_manifest_schema_version": 1,
            "object_id": item.object_id,
            "object_key": item.object_key,
            "output": {
                "bytes": len(compressed),
                "csv_header": HEADER.strip().split(","),
                "path": relative,
                "sha256": hashlib.sha256(compressed).hexdigest(),
            },
            "remote": {"content_length": len(compressed)},
            "session_date": session.isoformat(),
            "status": "complete",
        },
    )
    return artifact_path


def _write_rest(
    root: Path,
    rows: list[dict[str, object]],
    *,
    session: date = SESSION,
    query_count: int | None = None,
) -> Path:
    request = build_download_plan(
        dataset=ProviderDataset.DAILY_BARS,
        start=session,
        end=session,
    ).requests[0]
    document = {
        "adjusted": False,
        "queryCount": len(rows) if query_count is None else query_count,
        "request_id": "provider-fixture",
        "results": rows,
        "resultsCount": len(rows),
        "status": "OK",
    }
    result = asyncio.run(
        BronzeDownloader(root, minimum_free_bytes=0).download(
            _RestProvider(document), request
        )
    )
    return result.manifest_path


def _flat_row(
    ticker: str,
    *,
    session: date = SESSION,
    close: float = 11.0,
    transactions: int = 10,
) -> str:
    return (
        f"{ticker},250,10,{close},11.25,9.75,{_session_start_ns(session)},"
        f"{transactions}\n"
    )


def _rest_row(
    ticker: str,
    *,
    session: date = SESSION,
    close: float = 11.0,
    transactions: int | None = 10,
    include_vwap: bool = True,
) -> dict[str, object]:
    row: dict[str, object] = {
        "T": ticker,
        "c": close,
        "h": 11.25,
        "l": 9.75,
        "o": 10.0,
        "t": _session_start_ms(session),
        "v": 250.0,
    }
    if transactions is not None:
        row["n"] = transactions
    if include_vwap:
        row["vw"] = 10.5
    return row


def _write_matching_fixture(root: Path, *, session: date = SESSION) -> None:
    _write_flat(
        root,
        [_flat_row("AAPL", session=session), _flat_row("MSFT", session=session)],
        session=session,
    )
    _write_rest(
        root,
        [_rest_row("AAPL", session=session), _rest_row("MSFT", session=session)],
        session=session,
    )


def _audit(root: Path) -> dict[str, object]:
    return DailyProductCrossAuditor(
        root,
        start=SESSION,
        end=SESSION,
        workers=1,
    ).run()


def test_matching_products_pass_and_reuse_independent_cache(tmp_path: Path) -> None:
    _write_matching_fixture(tmp_path)

    first = _audit(tmp_path)
    second = _audit(tmp_path)

    assert first["status"] == "passed"
    assert first["gates"] == {
        "numerical_reconciliation": "matched",
        "source_integrity": "passed",
        "ticker_coverage": "matched",
    }
    assert first["sessions"][0]["comparison"]["common_tickers"] == 2
    assert first["summary"]["field_comparison_counts"]["transactions"] == 2
    assert second["summary"]["cache_reused"] == 1
    assert (
        tmp_path
        / "manifests"
        / "audits"
        / "daily_product_crosscheck"
        / "schema=v1"
        / f"{SESSION.isoformat()}.json"
    ).is_file()
    assert not (tmp_path / "manifests" / "audits" / "market_crosscheck").exists()

    _write_flat(
        tmp_path,
        [_flat_row("AAPL", close=10.5), _flat_row("MSFT")],
    )
    changed = _audit(tmp_path)
    assert changed["sessions"][0]["cache_status"] == "computed"
    assert changed["gates"]["numerical_reconciliation"] == "different"


def test_coverage_and_numeric_product_differences_do_not_fail_integrity(
    tmp_path: Path,
) -> None:
    _write_flat(tmp_path, [_flat_row("AAPL"), _flat_row("FLATONLY")])
    _write_rest(
        tmp_path,
        [
            _rest_row("AAPL", close=10.5, transactions=9),
            _rest_row("RESTONLY"),
        ],
    )

    report = _audit(tmp_path)

    assert report["status"] == "passed_with_differences"
    assert report["gates"] == {
        "numerical_reconciliation": "different",
        "source_integrity": "passed",
        "ticker_coverage": "different",
    }
    comparison = report["sessions"][0]["comparison"]
    assert comparison["flat_only"] == {"count": 1, "examples": ["FLATONLY"]}
    assert comparison["rest_only"] == {"count": 1, "examples": ["RESTONLY"]}
    assert comparison["field_mismatches"]["close"]["count"] == 1
    assert comparison["field_mismatches"]["transactions"]["count"] == 1
    assert {issue["kind"] for issue in report["sessions"][0]["issues"]} == {
        "product_difference"
    }


def test_optional_rest_transactions_and_vwap_are_coverage_not_corruption(
    tmp_path: Path,
) -> None:
    _write_flat(tmp_path, [_flat_row("AAPL")])
    _write_rest(
        tmp_path,
        [_rest_row("AAPL", transactions=None, include_vwap=False)],
    )

    report = _audit(tmp_path)

    assert report["status"] == "passed"
    rest_stats = report["sessions"][0]["datasets"]["rest_daily"]
    assert rest_stats["transactions_missing"] == 1
    assert rest_stats["vwap_missing"] == 1
    transactions = report["sessions"][0]["comparison"]["field_mismatches"][
        "transactions"
    ]
    assert transactions["compared"] == 0
    assert transactions["rest_missing_on_common"] == 1


def test_rest_artifact_checksum_failure_is_source_integrity_failure(
    tmp_path: Path,
) -> None:
    _write_matching_fixture(tmp_path)
    manifest_path = next((tmp_path / "manifests" / "massive" / "daily_bars").glob("*.json"))
    manifest = json.loads(manifest_path.read_text())
    artifact_path = tmp_path / manifest["artifacts"][0]["path"]
    damaged = bytearray(artifact_path.read_bytes())
    damaged[len(damaged) // 2] ^= 1
    artifact_path.write_bytes(damaged)

    report = _audit(tmp_path)

    assert report["status"] == "failed"
    assert report["gates"]["source_integrity"] == "failed"
    assert report["gates"]["ticker_coverage"] == "not_run"
    assert report["gates"]["numerical_reconciliation"] == "not_run"
    assert report["summary"]["issue_code_counts"] == {"source_unavailable": 1}


def test_rest_timestamp_outside_requested_et_session_fails_integrity(
    tmp_path: Path,
) -> None:
    _write_flat(tmp_path, [_flat_row("AAPL")])
    row = _rest_row("AAPL")
    row["t"] = int(row["t"]) + 60_000
    _write_rest(tmp_path, [row])

    report = _audit(tmp_path)

    assert report["status"] == "failed"
    assert report["gates"]["source_integrity"] == "failed"
    assert report["sessions"][0]["gates"]["ticker_coverage"] == "matched"
    assert report["summary"]["issue_code_counts"] == {
        "noncanonical_session_timestamp": 1
    }


def test_rest_query_count_must_match_decoded_results(tmp_path: Path) -> None:
    _write_flat(tmp_path, [_flat_row("AAPL")])
    _write_rest(tmp_path, [_rest_row("AAPL")], query_count=2)

    report = _audit(tmp_path)

    assert report["status"] == "failed"
    assert report["gates"]["source_integrity"] == "failed"
    assert report["summary"]["issue_code_counts"] == {"source_parse_failed": 1}


def test_effective_window_starts_on_verified_daily_rest_boundary(tmp_path: Path) -> None:
    for session in (date(2016, 7, 13), date(2016, 7, 14)):
        _write_matching_fixture(tmp_path, session=session)

    report = DailyProductCrossAuditor(
        tmp_path,
        start=date(2016, 7, 11),
        end=date(2016, 7, 14),
        workers=1,
    ).run()

    assert report["status"] == "passed"
    assert report["config"]["effective_start"] == "2016-07-13"
    assert [item["session_date"] for item in report["sessions"]] == [
        "2016-07-13",
        "2016-07-14",
    ]


def test_spawn_workers_recycle_and_reuse_multiple_session_caches(tmp_path: Path) -> None:
    sessions = (
        date(2016, 7, 13),
        date(2016, 7, 14),
        date(2016, 7, 15),
        date(2016, 7, 18),
    )
    for session in sessions:
        _write_matching_fixture(tmp_path, session=session)

    def run() -> dict[str, object]:
        return DailyProductCrossAuditor(
            tmp_path,
            start=sessions[0],
            end=sessions[-1],
            workers=1,
            max_tasks_per_child=1,
        ).run()

    first = run()
    second = run()

    assert first["status"] == "passed"
    assert first["config"]["execution"] == {
        "max_in_flight": 2,
        "max_tasks_per_child": 1,
        "max_workers": 1,
        "process_start_method": "spawn",
    }
    assert first["execution"] == {
        "peak_in_flight": 2,
        "worker_processes_observed": 4,
    }
    assert len({item["execution"]["worker_pid"] for item in first["sessions"]}) == 4
    assert all(
        item["execution"]["process_start_method"] == "spawn"
        for item in first["sessions"]
    )
    assert second["summary"]["cache_reused"] == 4
    assert len({item["execution"]["worker_pid"] for item in second["sessions"]}) == 4


def test_cli_writes_bounded_report_and_accepts_product_differences(
    tmp_path: Path, capsys
) -> None:
    _write_flat(tmp_path, [_flat_row("AAPL"), _flat_row("FLATONLY")])
    _write_rest(tmp_path, [_rest_row("AAPL")])
    output = Path("manifests/audits/daily-products-test.json")

    assert (
        daily_products_audit_main(
            [
                "--data-root",
                str(tmp_path),
                "--start",
                SESSION.isoformat(),
                "--end",
                SESSION.isoformat(),
                "--output",
                str(output),
            ]
        )
        == 0
    )
    printed = json.loads(capsys.readouterr().out)
    stored = json.loads((tmp_path / output).read_text())
    assert printed["status"] == "passed_with_differences"
    assert stored == printed
