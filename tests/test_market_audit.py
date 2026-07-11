import gzip
import hashlib
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

from ame_stocks_api.artifacts import write_json_atomic
from ame_stocks_api.audit.market import MarketCrossAuditor
from ame_stocks_api.cli.market_audit import main as market_audit_main
from ame_stocks_api.flatfiles import FlatFileDataset, FlatFileObject

SESSION = date(2026, 6, 30)
HEADER = "ticker,volume,open,close,high,low,window_start,transactions\n"


def _timestamp(hour: int, minute: int, *, seconds: int = 0) -> int:
    observed = datetime(2026, 6, 30, hour, minute, seconds, tzinfo=UTC)
    return int(observed.timestamp() * 1_000_000_000)


def _write_flat_file(
    root: Path,
    dataset: FlatFileDataset,
    rows: list[str],
    *,
    session: date = SESSION,
) -> tuple[Path, Path]:
    item = FlatFileObject(dataset=dataset, session_date=session)
    compressed = gzip.compress((HEADER + "".join(rows)).encode(), mtime=0)
    relative = f"bronze/massive/flatfiles/{item.object_key}"
    output = root / relative
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(compressed)
    manifest_path = (
        root / "manifests" / "massive" / "flatfiles" / dataset.value / f"{session.isoformat()}.json"
    )
    write_json_atomic(
        manifest_path,
        {
            "dataset": dataset.value,
            "flat_file_manifest_schema_version": 1,
            "object_id": item.object_id,
            "object_key": item.object_key,
            "output": {
                "bytes": len(compressed),
                "path": relative,
                "sha256": hashlib.sha256(compressed).hexdigest(),
            },
            "remote": {"content_length": len(compressed)},
            "session_date": session.isoformat(),
            "status": "complete",
        },
    )
    return output, manifest_path


def _valid_fixture(root: Path, *, day_close: float = 11.0) -> None:
    first = _timestamp(13, 30)
    second = _timestamp(13, 31)
    _write_flat_file(
        root,
        FlatFileDataset.MINUTE_AGGREGATES,
        [
            f"AAPL,100,10,10.5,10.75,9.75,{first},4\n",
            f"AAPL,150,10.5,11,11.25,10.25,{second},6\n",
            f"MSFT,50,20,20,20,20,{first},2\n",
        ],
    )
    midnight_et = _timestamp(4, 0)
    _write_flat_file(
        root,
        FlatFileDataset.DAY_AGGREGATES,
        [
            f"AAPL,250,10,{day_close},11.25,9.75,{midnight_et},10\n",
            f"MSFT,50,20,20,20,20,{midnight_et},2\n",
        ],
    )


def _audit(root: Path) -> dict[str, object]:
    return MarketCrossAuditor(
        root,
        start=SESSION,
        end=SESSION,
        workers=2,
    ).run()


def test_matching_minute_and_day_files_pass(tmp_path: Path) -> None:
    _valid_fixture(tmp_path)

    report = _audit(tmp_path)

    assert report["status"] == "passed"
    assert report["summary"]["sessions"] == 1
    assert report["summary"]["minute_rows"] == 3
    session = report["sessions"][0]
    assert session["comparison"]["compared_tickers"] == 2
    assert session["comparison"]["missing_tickers"] == {
        "day_only": {"count": 0, "examples": []},
        "minute_only": {"count": 0, "examples": []},
    }
    assert report["summary"]["ticker_session_pairs_in_both_files"] == 2
    assert report["summary"]["field_comparison_counts"]["close"] == 2
    assert all(
        details["count"] == 0 for details in session["comparison"]["field_mismatches"].values()
    )


def test_numeric_mismatch_is_reported_separately_from_integrity(
    tmp_path: Path, capsys
) -> None:
    _valid_fixture(tmp_path, day_close=10.75)
    output = tmp_path / "market-audit.json"

    report = _audit(tmp_path)

    assert report["status"] == "passed_with_differences"
    assert report["summary"]["difference_sessions"] == 1
    assert report["summary"]["failed_sessions"] == 0
    assert report["summary"]["issue_code_counts"]["close_mismatch"] == 1
    mismatch = report["sessions"][0]["comparison"]["field_mismatches"]["close"]
    assert mismatch["count"] == 1
    assert mismatch["examples"][0]["ticker"] == "AAPL"
    assert (
        market_audit_main(
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
    stored = json.loads(output.read_text(encoding="utf-8"))
    assert printed["status"] == "passed_with_differences"
    assert stored == printed
    assert stored["report_path"] == str(output.resolve())


def test_invalid_minute_alignment_and_duplicate_conflict_fail(tmp_path: Path) -> None:
    first = _timestamp(13, 30)
    invalid = _timestamp(13, 31, seconds=1)
    _write_flat_file(
        tmp_path,
        FlatFileDataset.MINUTE_AGGREGATES,
        [
            f"AAPL,100,10,10,10,10,{first},4\n",
            f"AAPL,100,10,10,10,10,{invalid},4\n",
            f"AAPL,100,10,10,10,10,{invalid},4\n",
            f"AAPL,101,10,10,10,10,{invalid},4\n",
        ],
    )
    midnight_et = _timestamp(4, 0)
    _write_flat_file(
        tmp_path,
        FlatFileDataset.DAY_AGGREGATES,
        [f"AAPL,200,10,10,10,10,{midnight_et},8\n"],
    )

    report = _audit(tmp_path)

    codes = report["summary"]["issue_code_counts"]
    assert report["status"] == "failed"
    assert codes["minute_timestamp_unaligned"] == 3
    assert codes["duplicate_keys"] == 1
    assert codes["conflicting_duplicate_keys"] == 1


def test_second_run_reuses_cache_bound_to_both_manifest_hashes(tmp_path: Path) -> None:
    _valid_fixture(tmp_path)
    first = _audit(tmp_path)

    second = _audit(tmp_path)

    assert first["sessions"][0]["cache_status"] == "computed"
    assert second["sessions"][0]["cache_status"] == "reused"
    assert second["summary"]["cache_reused"] == 1
    cache = json.loads(
        (
            tmp_path
            / "manifests"
            / "audits"
            / "market_crosscheck"
            / "schema=v5"
            / f"{SESSION.isoformat()}.json"
        ).read_text()
    )
    assert len(cache["binding"]["minute"]["manifest_sha256"]) == 64
    assert len(cache["binding"]["day"]["manifest_sha256"]) == 64

    _, day_manifest = _write_flat_file(
        tmp_path,
        FlatFileDataset.DAY_AGGREGATES,
        [f"AAPL,250,10,11.5,11.5,9.75,{_timestamp(4, 0)},10\n"],
    )
    assert day_manifest.is_file()
    changed = _audit(tmp_path)
    assert changed["sessions"][0]["cache_status"] == "computed"
    assert changed["status"] == "passed_with_differences"


def test_cache_rehashes_artifact_even_when_size_and_mtime_are_preserved(
    tmp_path: Path,
) -> None:
    _valid_fixture(tmp_path)
    first = _audit(tmp_path)
    minute_path = tmp_path / str(
        first["sessions"][0]["sources"]["minute"]["artifact_path"]
    )
    stat = minute_path.stat()
    damaged = bytearray(minute_path.read_bytes())
    damaged[len(damaged) // 2] ^= 0x01
    minute_path.write_bytes(damaged)
    os.utime(minute_path, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    second = _audit(tmp_path)

    assert second["sessions"][0]["cache_status"] == "computed"
    assert second["status"] == "failed"
    assert second["summary"]["issue_code_counts"]["artifact_sha256_mismatch"] == 1


def test_extended_hours_prices_do_not_replace_regular_session_ohlc(tmp_path: Path) -> None:
    premarket = _timestamp(12, 0)
    open_time = _timestamp(13, 30)
    close_time = _timestamp(19, 59)
    after_hours = _timestamp(21, 0)
    _write_flat_file(
        tmp_path,
        FlatFileDataset.MINUTE_AGGREGATES,
        [
            f"AAPL,10,5,5,5,5,{premarket},1\n",
            f"AAPL,100,10,10.5,10.75,9.75,{open_time},4\n",
            f"AAPL,150,10.5,11,11.25,10.25,{close_time},6\n",
            f"AAPL,20,20,20,20,20,{after_hours},2\n",
        ],
    )
    _write_flat_file(
        tmp_path,
        FlatFileDataset.DAY_AGGREGATES,
        [f"AAPL,280,10,11,11.25,9.75,{_timestamp(4, 0)},13\n"],
    )

    report = _audit(tmp_path)

    assert report["status"] == "passed"
    reconstruction = report["sessions"][0]["comparison"]["reconstruction"]
    assert reconstruction["ohlc"].startswith("XNYS regular-session")


def test_extended_hours_only_ticker_is_not_silently_matched(tmp_path: Path) -> None:
    _write_flat_file(
        tmp_path,
        FlatFileDataset.MINUTE_AGGREGATES,
        [f"AAPL,10,5,5,5,5,{_timestamp(12, 0)},1\n"],
    )
    _write_flat_file(
        tmp_path,
        FlatFileDataset.DAY_AGGREGATES,
        [f"AAPL,10,5,5,5,5,{_timestamp(4, 0)},1\n"],
    )

    report = _audit(tmp_path)

    assert report["status"] == "passed_with_differences"
    assert (
        report["summary"]["issue_code_counts"][
            "ticker_without_regular_session_minutes"
        ]
        == 1
    )
    assert report["sessions"][0]["comparison"]["regular_session_missing"] == {
        "count": 1,
        "examples": ["AAPL"],
    }


def test_material_no_regular_session_coverage_fails_coverage_gate(tmp_path: Path) -> None:
    premarket = _timestamp(12, 0)
    tickers = ("AAA", "BBB", "CCC")
    _write_flat_file(
        tmp_path,
        FlatFileDataset.MINUTE_AGGREGATES,
        [f"{ticker},10,5,5,5,5,{premarket},1\n" for ticker in tickers],
    )
    _write_flat_file(
        tmp_path,
        FlatFileDataset.DAY_AGGREGATES,
        [f"{ticker},10,5,5,5,5,{_timestamp(4, 0)},1\n" for ticker in tickers],
    )

    report = _audit(tmp_path)

    assert report["status"] == "failed"
    assert report["gates"]["ticker_coverage"] == "failed"
    coverage = report["sessions"][0]["comparison"]["coverage"]
    assert coverage["no_regular_session_fraction"] == 1.0


def test_noncanonical_day_timestamp_fails_source_and_row_integrity(tmp_path: Path) -> None:
    _valid_fixture(tmp_path)
    _write_flat_file(
        tmp_path,
        FlatFileDataset.DAY_AGGREGATES,
        [f"AAPL,250,10,11,11.25,9.75,{_timestamp(14, 0)},10\n"],
    )

    report = _audit(tmp_path)

    assert report["status"] == "failed"
    assert report["gates"]["source_and_row_integrity"] == "failed"
    assert report["summary"]["issue_code_counts"]["noncanonical_day_timestamp"] == 1


def test_material_ticker_coverage_loss_fails_coverage_gate(tmp_path: Path) -> None:
    first = _timestamp(13, 30)
    _write_flat_file(
        tmp_path,
        FlatFileDataset.MINUTE_AGGREGATES,
        [
            f"AAA,100,10,10,10,10,{first},4\n",
            f"BBB,100,10,10,10,10,{first},4\n",
            f"CCC,100,10,10,10,10,{first},4\n",
        ],
    )
    _write_flat_file(
        tmp_path,
        FlatFileDataset.DAY_AGGREGATES,
        [f"AAA,100,10,10,10,10,{_timestamp(4, 0)},4\n"],
    )

    report = _audit(tmp_path)

    assert report["status"] == "failed"
    assert report["gates"]["ticker_coverage"] == "failed"
    coverage = report["sessions"][0]["comparison"]["coverage"]
    assert coverage["missing_tickers"] == 2
    assert coverage["missing_fraction"] == 2 / 3


def test_market_manifest_cannot_point_day_identity_at_minute_path(tmp_path: Path) -> None:
    _valid_fixture(tmp_path)
    minute_path = next(
        (tmp_path / "bronze" / "massive" / "flatfiles").glob("**/minute_aggs_v1/**/*.gz")
    )
    day_manifest_path = (
        tmp_path
        / "manifests"
        / "massive"
        / "flatfiles"
        / FlatFileDataset.DAY_AGGREGATES.value
        / f"{SESSION.isoformat()}.json"
    )
    manifest = json.loads(day_manifest_path.read_text())
    manifest["output"]["path"] = str(minute_path.relative_to(tmp_path))
    write_json_atomic(day_manifest_path, manifest)

    report = _audit(tmp_path)

    assert report["status"] == "failed"
    assert report["gates"]["source_and_row_integrity"] == "failed"
    assert report["summary"]["issue_code_counts"]["source_unavailable"] == 1
