from fastapi.testclient import TestClient

from ame_stocks_api.main import app

client = TestClient(app)


def test_health_endpoint() -> None:
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {
        "service": "ame-stocks-api",
        "status": "ok",
        "version": "0.1.0",
    }


def test_contract_endpoint_is_explicit_and_versioned() -> None:
    response = client.get("/api/v1/contracts")

    assert response.status_code == 200
    assert response.json() == {
        "factor_contract_version": "1.0",
        "factor_output_columns": ["signal_date", "asset_id", "raw_value"],
        "provider_contract_version": "1.1",
        "provider_datasets": [
            "assets",
            "daily_bars",
            "minute_bars",
            "splits",
            "dividends",
            "short_interest",
            "short_volume",
            "float",
            "legacy_financials",
            "income_statements",
            "balance_sheets",
            "cash_flow_statements",
            "ratios",
            "ipos",
            "ticker_overview",
            "ticker_events",
            "ticker_types",
            "exchanges",
            "condition_codes",
            "edgar_index",
            "form_3",
            "form_4",
            "form_13f",
            "risk_factors",
            "ten_k_sections",
            "eight_k_text",
            "eight_k_disclosures",
            "disclosure_taxonomy",
            "news",
            "treasury_yields",
            "inflation",
            "inflation_expectations",
            "labor_market",
            "risk_taxonomy",
        ],
    }
