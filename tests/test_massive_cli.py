import asyncio
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

from ame_stocks_api.cli import massive
from ame_stocks_api.cli.massive import main
from ame_stocks_api.downloads import DownloadResult
from ame_stocks_api.providers import MassiveRequestError
from ame_stocks_core import ProviderDataset, ProviderRequest


def test_plan_command_needs_no_api_key_or_network(monkeypatch, capsys) -> None:
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    exit_code = main(
        [
            "plan",
            "--dataset",
            "daily_bars",
            "--start",
            "2026-06-30",
            "--end",
            "2026-06-30",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["request_count"] == 1
    assert output["requests_per_minute"] == 600.0
    assert output["note"].endswith("never contacts Massive.")


def test_rest_plan_defaults_to_ten_years(monkeypatch, capsys) -> None:
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    exit_code = main(
        [
            "plan",
            "--dataset",
            "assets",
            "--active",
            "both",
            "--end",
            "2026-06-30",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["start"] == "2016-06-30"
    assert output["end"] == "2026-06-30"
    assert output["requests"][0]["start"] == "2016-06-30"
    assert output["request_count"] > 4_900


def test_ticker_date_csv_builds_lifecycle_overview_plan(
    monkeypatch,
    capsys,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    request_file = tmp_path / "requests.csv"
    request_file.write_text(
        "ticker,query_date\nBCpC,2018-02-01\nAAPL,2026-07-09\n",
        encoding="utf-8",
    )

    exit_code = main(
        [
            "plan",
            "--dataset",
            "ticker_overview",
            "--start",
            "2016-07-11",
            "--end",
            "2026-07-09",
            "--ticker-date-file",
            str(request_file),
            "--show-all",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["request_count"] == 2
    assert [(item["asset_ids"], item["start"]) for item in output["requests"]] == [
        (["BCpC"], "2018-02-01"),
        (["AAPL"], "2026-07-09"),
    ]


def test_continue_on_error_finishes_independent_request_streams(
    monkeypatch,
    capsys,
    tmp_path: Path,
) -> None:
    attempted: list[str] = []

    class FakeProvider:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    class FakeDownloader:
        def __init__(self, data_root):
            assert data_root == tmp_path

        async def download(self, provider, request):
            identifier = request.asset_ids[0]
            attempted.append(identifier)
            if identifier == "MISSING":
                raise MassiveRequestError("fixture 404")
            return DownloadResult(
                status="downloaded",
                manifest_path=tmp_path / "manifest.json",
                page_count=1,
                record_count=2,
                compressed_bytes=3,
            )

    monkeypatch.setattr(massive, "BronzeDownloader", FakeDownloader)
    monkeypatch.setattr(
        massive.MassiveProvider,
        "from_env",
        lambda **kwargs: FakeProvider(),
    )
    arguments = SimpleNamespace(
        concurrency=2,
        continue_on_error=True,
        data_root=tmp_path,
        max_attempts=1,
        requests_per_minute=600.0,
        timeout_seconds=1.0,
    )
    requests = tuple(
        ProviderRequest(
            dataset=ProviderDataset.TICKER_EVENTS,
            start=date(2003, 9, 10),
            end=date(2026, 7, 9),
            asset_ids=(identifier,),
        )
        for identifier in ("MISSING", "AAPL")
    )

    exit_code = asyncio.run(massive._execute_downloads(arguments, requests))
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    assert exit_code == 0
    assert attempted == ["MISSING", "AAPL"]
    assert output[-1]["status"] == "complete_with_failures"
    assert output[-1]["failed_requests"] == 1
    assert output[-1]["downloaded_requests"] == 1
