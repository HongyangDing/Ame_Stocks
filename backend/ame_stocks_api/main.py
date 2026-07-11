"""Ame Stocks API application entrypoint."""

from __future__ import annotations

import os
from typing import Literal

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from ame_stocks_api import __version__
from ame_stocks_core import (
    FACTOR_CONTRACT_VERSION,
    FACTOR_OUTPUT_COLUMNS,
    PROVIDER_CONTRACT_VERSION,
    ProviderDataset,
)


class HealthResponse(BaseModel):
    service: str
    status: Literal["ok"]
    version: str


class ContractSummary(BaseModel):
    factor_contract_version: str
    factor_output_columns: list[str]
    provider_contract_version: str
    provider_datasets: list[str]


def _allowed_origins() -> list[str]:
    raw_origins = os.getenv(
        "AME_ALLOWED_ORIGINS",
        "http://127.0.0.1:3000,http://localhost:3000",
    )
    return [origin.strip() for origin in raw_origins.split(",") if origin.strip()]


def create_app() -> FastAPI:
    application = FastAPI(
        title="Ame Stocks API",
        description="U.S. equity factor research and backtesting platform",
        version=__version__,
    )
    application.add_middleware(
        CORSMiddleware,
        allow_origins=_allowed_origins(),
        allow_credentials=True,
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    @application.get("/healthz", response_model=HealthResponse, tags=["system"])
    def health() -> HealthResponse:
        return HealthResponse(service="ame-stocks-api", status="ok", version=__version__)

    @application.get(
        "/api/v1/contracts",
        response_model=ContractSummary,
        tags=["system"],
    )
    def contracts() -> ContractSummary:
        return ContractSummary(
            factor_contract_version=FACTOR_CONTRACT_VERSION,
            factor_output_columns=list(FACTOR_OUTPUT_COLUMNS),
            provider_contract_version=PROVIDER_CONTRACT_VERSION,
            provider_datasets=[dataset.value for dataset in ProviderDataset],
        )

    return application


app = create_app()
