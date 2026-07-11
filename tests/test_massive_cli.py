import json

from ame_stocks_api.cli.massive import main


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


def test_rest_plan_defaults_to_five_years(monkeypatch, capsys) -> None:
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
    assert output["start"] == "2021-06-30"
    assert output["end"] == "2026-06-30"
    assert output["requests"][0]["start"] == "2021-06-30"
    assert output["request_count"] > 2_400
