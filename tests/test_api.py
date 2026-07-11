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
        "provider_contract_version": "1.0",
        "provider_datasets": ["assets", "minute_bars", "splits", "dividends"],
    }
