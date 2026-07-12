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
        "company_name": "Apple Inc.",
        "end_date": "2025-12-31",
        "filing_date": "2026-02-02",
        "financials": {
            "income_statement": {
                "revenues": {
                    "label": "Revenue",
                    "order": 1,
                    "source": "direct_report",
                    "unit": "USD",
                    "value": 1,
                    "xpath": "//Revenue",
                }
            }
        },
        "fiscal_period": "FY",
        "fiscal_year": "2025",
        "sic": "3571",
        "source_filing_file_url": (
            "http://api.polygon.io/v1/reference/sec/filings/"
            "0000320193-26-000006/files/aapl.xml"
        ),
        "source_filing_url": (
            "https://api.polygon.io/v1/reference/sec/filings/0000320193-26-000006"
        ),
        "start_date": "2025-01-01",
        "tickers": ["AAPL"],
        "timeframe": "annual",
    }

    assert valid_legacy_financials(row)
    assert valid_legacy_financials(
        {key: value for key, value in row.items() if key != "acceptance_datetime"}
    )
    assert not valid_legacy_financials(
        {key: value for key, value in row.items() if key != "end_date"}
    )
    assert not valid_legacy_financials({**row, "financials": {}})
    assert not valid_legacy_financials({**row, "acceptance_datetime": "2026-02-02T18:17:05"})
    assert not valid_legacy_financials({**row, "tickers": ["AAPL", "AAPL"]})
    assert not valid_legacy_financials({**row, "end_date": "2026-12-31"})


def test_legacy_financials_contract_accepts_all_provenance_branches() -> None:
    base = {
        "cik": "0000320193",
        "company_name": "Apple Inc.",
        "end_date": "2025-12-31",
        "filing_date": "2026-02-02",
        "fiscal_period": "Q1",
        "fiscal_year": "2026",
        "sic": "",
        "source_filing_file_url": (
            "http://api.polygon.io/v1/reference/sec/filings/"
            "0000320193-26-000006/files/aapl.xml"
        ),
        "source_filing_url": (
            "https://api.polygon.io/v1/reference/sec/filings/0000320193-26-000006"
        ),
        "start_date": "2025-10-01",
        "tickers": None,
        "timeframe": "quarterly",
    }
    metrics = {
        "direct": {
            "label": "Direct",
            "order": 1,
            "source": "direct_report",
            "unit": "USD",
            "value": -1.0,
            "xpath": "//Direct",
        },
        "intra": {
            "formula": "a-b",
            "label": "Intra",
            "order": 2,
            "source": "intra_report_impute",
            "unit": "USD",
            "value": 2,
        },
        "inter": {
            "derived_from": ["0000320193-25-000001", "0000320193-25-000002"],
            "label": "Inter",
            "order": 3,
            "source": "inter_report_derive",
            "unit": "USD",
            "value": 3,
        },
    }

    assert valid_legacy_financials(
        {**base, "financials": {"income_statement": metrics}}
    )
