from ame_stocks_api.audit.row_contracts import (
    epoch_millisecond_date,
    valid_daily_bar,
    valid_legacy_financials,
)


def test_grouped_daily_bar_contract_uses_massive_compact_keys() -> None:
    row = {
        "T": "AAPL",
        "t": 1_751_322_000_000,
        "o": 200.0,
        "h": 205.0,
        "l": 198.0,
        "c": 203.0,
        "v": 1_000_000,
        "vw": 202.5,
    }

    assert epoch_millisecond_date(row["t"]) == "2025-06-30"
    assert valid_daily_bar(row)
    assert not valid_daily_bar({**row, "vw": None})
    assert not valid_daily_bar({**row, "h": 199.0})
    assert not valid_daily_bar({**row, "T": " AAPL"})


def test_legacy_financials_contract_requires_point_in_time_identity_fields() -> None:
    row = {
        "acceptance_datetime": "2026-02-02T18:17:05Z",
        "cik": "0000320193",
        "end_date": "2025-12-31",
        "filing_date": "2026-02-02",
        "financials": {"income_statement": {"revenues": {"value": 1}}},
        "source_filing_file_url": "https://www.sec.gov/Archives/fixture.htm",
        "source_filing_url": "https://www.sec.gov/Archives/fixture.txt",
        "timeframe": "annual",
    }

    assert valid_legacy_financials(row)
    assert valid_legacy_financials(
        {
            key: value
            for key, value in row.items()
            if key
            not in {
                "acceptance_datetime",
                "source_filing_file_url",
                "source_filing_url",
            }
        }
    )
    assert not valid_legacy_financials(
        {key: value for key, value in row.items() if key != "end_date"}
    )
    assert not valid_legacy_financials({**row, "financials": {}})
    assert not valid_legacy_financials({**row, "acceptance_datetime": "2026-02-02T18:17:05"})
