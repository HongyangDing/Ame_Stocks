import json

from ame_stocks_api.cli.massive import main


def test_plan_command_needs_no_api_key_or_network(monkeypatch, capsys) -> None:
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    exit_code = main(
        [
            "plan",
            "--dataset",
            "daily_bars",
            "--ticker",
            "AAPL",
            "--start",
            "2024-07-01",
            "--end",
            "2026-06-30",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["request_count"] == 1
    assert output["note"].endswith("never contacts Massive.")
