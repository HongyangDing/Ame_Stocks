"""Massive REST adapter with safe authentication, pagination, and retry behavior."""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, quote, urljoin, urlsplit, urlunsplit

import httpx2

from ame_stocks_core import FetchCheckpoint, ProviderBatch, ProviderDataset, ProviderRequest

MASSIVE_BASE_URL = "https://api.massive.com"
MASSIVE_API_KEY_ENV = "MASSIVE_API_KEY"
# Paid plans have unlimited calls; 10 requests/second remains well below Massive's
# published recommendation to stay under 100 requests/second.
DEFAULT_REQUESTS_PER_MINUTE = 600.0
_RETRYABLE_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})
_ALLOWED_PARAMETERS = {
    ProviderDataset.ASSETS: frozenset({"active", "exchange", "search", "type"}),
    ProviderDataset.DAILY_BARS: frozenset({"include_otc"}),
    ProviderDataset.MINUTE_BARS: frozenset(),
    ProviderDataset.SPLITS: frozenset({"adjustment_type"}),
    ProviderDataset.DIVIDENDS: frozenset({"distribution_type", "frequency"}),
    ProviderDataset.SHORT_INTEREST: frozenset({"avg_daily_volume", "days_to_cover"}),
    ProviderDataset.SHORT_VOLUME: frozenset({"short_volume_ratio"}),
    ProviderDataset.FLOAT: frozenset({"free_float_percent"}),
    ProviderDataset.IPOS: frozenset({"ipo_status", "isin", "us_code"}),
    ProviderDataset.TICKER_EVENTS: frozenset({"types"}),
    ProviderDataset.TICKER_TYPES: frozenset(),
    ProviderDataset.EXCHANGES: frozenset(),
    ProviderDataset.EDGAR_INDEX: frozenset({"cik", "form_type"}),
    ProviderDataset.FORM_3: frozenset({"form_type", "issuer_cik", "owner_cik"}),
    ProviderDataset.FORM_4: frozenset(
        {"form_type", "issuer_cik", "owner_cik", "transaction_code"}
    ),
    ProviderDataset.FORM_13F: frozenset({"filer_cik"}),
    ProviderDataset.RISK_FACTORS: frozenset({"cik"}),
    ProviderDataset.TEN_K_SECTIONS: frozenset({"cik", "period_end", "section"}),
    ProviderDataset.EIGHT_K_TEXT: frozenset({"cik", "form_type"}),
    ProviderDataset.NEWS: frozenset(),
}


@dataclass(frozen=True, slots=True)
class _BulkEndpoint:
    path: str
    date_field: str
    limit: int
    sort: str
    asset_parameter: str | None = "ticker"
    order: str | None = None


_BULK_ENDPOINTS = {
    ProviderDataset.SHORT_INTEREST: _BulkEndpoint(
        "/stocks/v1/short-interest", "settlement_date", 50_000, "settlement_date.asc,ticker.asc"
    ),
    ProviderDataset.SHORT_VOLUME: _BulkEndpoint(
        "/stocks/v1/short-volume", "date", 50_000, "date.asc,ticker.asc"
    ),
    ProviderDataset.IPOS: _BulkEndpoint(
        "/vX/reference/ipos", "listing_date", 1_000, "listing_date", order="asc"
    ),
    ProviderDataset.EDGAR_INDEX: _BulkEndpoint(
        "/stocks/filings/vX/index", "filing_date", 10_000, "filing_date.asc"
    ),
    ProviderDataset.FORM_3: _BulkEndpoint(
        "/stocks/filings/vX/form-3", "filing_date", 10_000, "filing_date.asc", "tickers"
    ),
    ProviderDataset.FORM_4: _BulkEndpoint(
        "/stocks/filings/vX/form-4", "filing_date", 10_000, "filing_date.asc", "tickers"
    ),
    ProviderDataset.FORM_13F: _BulkEndpoint(
        "/stocks/filings/vX/13-F", "filing_date", 1_000, "filing_date.asc", None
    ),
    ProviderDataset.RISK_FACTORS: _BulkEndpoint(
        "/stocks/filings/vX/risk-factors", "filing_date", 49_999, "filing_date.asc"
    ),
    ProviderDataset.TEN_K_SECTIONS: _BulkEndpoint(
        "/stocks/filings/10-K/vX/sections", "filing_date", 100, "filing_date.asc"
    ),
    ProviderDataset.EIGHT_K_TEXT: _BulkEndpoint(
        "/stocks/filings/8-K/vX/text", "filing_date", 100, "filing_date.asc"
    ),
    ProviderDataset.NEWS: _BulkEndpoint(
        "/v2/reference/news", "published_utc", 1_000, "published_utc", order="asc"
    ),
}

Sleep = Callable[[float], Awaitable[None]]
Clock = Callable[[], float]
RandomFloat = Callable[[], float]


class MassiveProviderError(RuntimeError):
    """Base class for safe-to-display Massive adapter errors."""


class MassiveConfigurationError(MassiveProviderError):
    """Raised when local configuration is incomplete or unsafe."""


class MassiveRequestError(MassiveProviderError):
    """Raised when a request cannot be completed after retry handling."""


class MassiveResponseError(MassiveProviderError):
    """Raised when Massive returns malformed or unsafe response metadata."""


class _RateLimiter:
    """Serialize requests at a fixed interval across one provider instance."""

    def __init__(self, requests_per_minute: float, *, sleep: Sleep, clock: Clock) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")
        self._interval = 60.0 / requests_per_minute
        self._sleep = sleep
        self._clock = clock
        self._next_allowed = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = self._clock()
            delay = max(0.0, self._next_allowed - now)
            if delay:
                await self._sleep(delay)
                now = self._clock()
            self._next_allowed = max(self._next_allowed, now) + self._interval


class MassiveProvider:
    """Stream successful Massive JSON pages without exposing credentials."""

    name = "massive"
    version = "1.2.0"

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = MASSIVE_BASE_URL,
        requests_per_minute: float | None = DEFAULT_REQUESTS_PER_MINUTE,
        timeout_seconds: float = 30.0,
        max_attempts: int = 5,
        transport: httpx2.AsyncBaseTransport | None = None,
        sleep: Sleep = asyncio.sleep,
        clock: Clock = time.monotonic,
        random_float: RandomFloat = random.random,
    ) -> None:
        clean_key = api_key.strip()
        if not clean_key:
            raise MassiveConfigurationError("MASSIVE_API_KEY is not configured")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")

        self._api_key = clean_key
        self._base_url = self._validate_base_url(base_url)
        self._origin = self._origin_tuple(self._base_url)
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._random_float = random_float
        self._rate_limiter = (
            _RateLimiter(requests_per_minute, sleep=sleep, clock=clock)
            if requests_per_minute is not None
            else None
        )
        self._client = httpx2.AsyncClient(
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {clean_key}",
                "User-Agent": "ame-stocks/0.1",
            },
            timeout=httpx2.Timeout(timeout_seconds),
            transport=transport,
            follow_redirects=False,
        )

    @classmethod
    def from_env(cls, **kwargs: Any) -> MassiveProvider:
        """Construct from the untracked MASSIVE_API_KEY environment variable."""

        return cls(os.getenv(MASSIVE_API_KEY_ENV, ""), **kwargs)

    def __repr__(self) -> str:
        return (
            f"MassiveProvider(base_url={self._base_url!r}, "
            f"max_attempts={self._max_attempts}, api_key='[REDACTED]')"
        )

    async def __aenter__(self) -> MassiveProvider:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch(
        self,
        request: ProviderRequest,
        *,
        checkpoint: FetchCheckpoint | None = None,
    ) -> AsyncIterator[ProviderBatch]:
        """Yield pages, following Massive next_url values on the same origin only."""

        if checkpoint:
            url = urljoin(f"{self._base_url}/", checkpoint.continuation)
            self._validate_continuation(url)
            parameters: Mapping[str, str] | None = None
            sequence = checkpoint.next_sequence
        else:
            path, parameters = self._request_target(request)
            url = urljoin(f"{self._base_url}/", path.lstrip("/"))
            sequence = 0

        seen_urls: set[str] = set()
        while True:
            request_fingerprint = url
            if request_fingerprint in seen_urls:
                raise MassiveResponseError("Massive pagination repeated a previous URL")
            seen_urls.add(request_fingerprint)

            response = await self._request_with_retry(url, parameters)
            payload = response.content
            document = self._parse_document(payload)
            next_url = document.get("next_url")
            continuation = self._continuation_from_next_url(next_url)

            yield ProviderBatch(
                provider=self.name,
                provider_version=self.version,
                dataset=request.dataset,
                request_id=request.request_id,
                sequence=sequence,
                payload=payload,
                content_type=response.headers.get("content-type", "application/json").split(";", 1)[
                    0
                ],
                next_cursor=continuation,
                is_last=continuation is None,
            )

            if continuation is None:
                return
            url = urljoin(f"{self._base_url}/", continuation)
            parameters = None
            sequence += 1

    def _request_target(self, request: ProviderRequest) -> tuple[str, dict[str, str]]:
        extras = self._validated_parameters(request)

        if request.dataset is ProviderDataset.ASSETS:
            if request.start != request.end:
                raise MassiveConfigurationError("assets requests require start and end to match")
            self._require_at_most_one_asset(request)
            parameters = {
                "date": request.end.isoformat(),
                "limit": "1000",
                "locale": "us",
                "market": "stocks",
                "order": "asc",
                "sort": "ticker",
                **extras,
            }
            if request.asset_ids:
                parameters["ticker"] = request.asset_ids[0]
            return "/v3/reference/tickers", parameters

        if request.dataset is ProviderDataset.DAILY_BARS:
            if request.start != request.end:
                raise MassiveConfigurationError(
                    "daily_bars grouped requests require start and end to match"
                )
            self._require_at_most_one_asset(request)
            if request.asset_ids:
                raise MassiveConfigurationError(
                    "daily_bars grouped requests do not accept asset_ids"
                )
            path = f"/v2/aggs/grouped/locale/us/market/stocks/{request.start.isoformat()}"
            return path, {
                "adjusted": str(request.adjusted).lower(),
                "include_otc": extras.get("include_otc", "false"),
            }

        if request.dataset is ProviderDataset.MINUTE_BARS:
            self._require_exactly_one_asset(request)
            ticker = quote(request.asset_ids[0], safe="")
            path = (
                f"/v2/aggs/ticker/{ticker}/range/1/minute/"
                f"{request.start.isoformat()}/{request.end.isoformat()}"
            )
            return path, {
                "adjusted": str(request.adjusted).lower(),
                "limit": "50000",
                "sort": "asc",
            }

        if request.dataset is ProviderDataset.SPLITS:
            self._require_at_most_one_asset(request)
            parameters = {
                "execution_date.gte": request.start.isoformat(),
                "execution_date.lte": request.end.isoformat(),
                "limit": "5000",
                "sort": "execution_date.asc,ticker.asc",
                **extras,
            }
            if request.asset_ids:
                parameters["ticker"] = request.asset_ids[0]
            return "/stocks/v1/splits", parameters

        if request.dataset is ProviderDataset.DIVIDENDS:
            self._require_at_most_one_asset(request)
            parameters = {
                "ex_dividend_date.gte": request.start.isoformat(),
                "ex_dividend_date.lte": request.end.isoformat(),
                "limit": "5000",
                "sort": "ex_dividend_date.asc,ticker.asc",
                **extras,
            }
            if request.asset_ids:
                parameters["ticker"] = request.asset_ids[0]
            return "/stocks/v1/dividends", parameters

        endpoint = _BULK_ENDPOINTS.get(request.dataset)
        if endpoint is not None:
            self._require_at_most_one_asset(request)
            if request.asset_ids and endpoint.asset_parameter is None:
                raise MassiveConfigurationError(
                    f"{request.dataset.value} only supports full-market requests"
                )
            parameters = {
                f"{endpoint.date_field}.gte": request.start.isoformat(),
                f"{endpoint.date_field}.lte": request.end.isoformat(),
                "limit": str(endpoint.limit),
                "sort": endpoint.sort,
                **extras,
            }
            if endpoint.order:
                parameters["order"] = endpoint.order
            if request.asset_ids:
                parameters[str(endpoint.asset_parameter)] = request.asset_ids[0]
            return endpoint.path, parameters

        if request.dataset is ProviderDataset.FLOAT:
            if request.start != request.end:
                raise MassiveConfigurationError("float is a latest-only snapshot")
            self._require_at_most_one_asset(request)
            parameters = {"limit": "5000", "sort": "ticker.asc", **extras}
            if request.asset_ids:
                parameters["ticker"] = request.asset_ids[0]
            return "/stocks/vX/float", parameters

        if request.dataset is ProviderDataset.TICKER_EVENTS:
            self._require_exactly_one_asset(request)
            identifier = quote(request.asset_ids[0], safe="")
            return f"/vX/reference/tickers/{identifier}/events", extras

        if request.dataset is ProviderDataset.TICKER_TYPES:
            if request.start != request.end:
                raise MassiveConfigurationError("ticker_types is a latest-only snapshot")
            self._require_at_most_one_asset(request)
            if request.asset_ids:
                raise MassiveConfigurationError("ticker_types does not accept asset_ids")
            return "/v3/reference/tickers/types", {
                "asset_class": "stocks",
                "locale": "us",
            }

        if request.dataset is ProviderDataset.EXCHANGES:
            if request.start != request.end:
                raise MassiveConfigurationError("exchanges is a latest-only snapshot")
            self._require_at_most_one_asset(request)
            if request.asset_ids:
                raise MassiveConfigurationError("exchanges does not accept asset_ids")
            return "/v3/reference/exchanges", {
                "asset_class": "stocks",
                "locale": "us",
            }

        raise MassiveConfigurationError(f"unsupported Massive dataset: {request.dataset.value}")

    def _validated_parameters(self, request: ProviderRequest) -> dict[str, str]:
        allowed = _ALLOWED_PARAMETERS[request.dataset]
        supplied = dict(request.parameters)
        if any(key.lower() == "apikey" for key in supplied):
            raise MassiveConfigurationError("API keys must use the Authorization header")
        unexpected = sorted(set(supplied) - allowed)
        if unexpected:
            raise MassiveConfigurationError(
                f"unsupported parameters for {request.dataset.value}: {', '.join(unexpected)}"
            )
        return supplied

    async def _request_with_retry(
        self,
        url: str,
        parameters: Mapping[str, str] | None,
    ) -> httpx2.Response:
        for attempt in range(1, self._max_attempts + 1):
            if self._rate_limiter:
                await self._rate_limiter.acquire()
            try:
                response = await self._client.get(url, params=parameters)
            except httpx2.TransportError as exc:
                if attempt == self._max_attempts:
                    raise MassiveRequestError(
                        f"Massive transport failed after {attempt} attempts"
                    ) from exc
                await self._sleep(self._backoff_seconds(attempt))
                continue

            if 200 <= response.status_code < 300:
                return response
            if response.status_code not in _RETRYABLE_STATUS_CODES:
                raise MassiveRequestError(
                    f"Massive returned HTTP {response.status_code} for "
                    f"{self._url_without_query(url)}"
                )
            if attempt == self._max_attempts:
                raise MassiveRequestError(
                    f"Massive returned HTTP {response.status_code} after {attempt} attempts"
                )
            await self._sleep(self._retry_delay(response, attempt))

        raise AssertionError("retry loop exited unexpectedly")

    def _parse_document(self, payload: bytes) -> dict[str, Any]:
        try:
            document = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MassiveResponseError("Massive returned invalid JSON") from exc
        if not isinstance(document, dict):
            raise MassiveResponseError("Massive response root must be an object")
        status = document.get("status")
        if status is not None and str(status).upper() != "OK":
            detail = self._redact(str(document.get("error") or "unknown API error"))
            raise MassiveResponseError(f"Massive response status was not OK: {detail[:240]}")
        return document

    def _continuation_from_next_url(self, next_url: object) -> str | None:
        if next_url is None or next_url == "":
            return None
        if not isinstance(next_url, str):
            raise MassiveResponseError("Massive next_url must be a string")
        absolute = urljoin(f"{self._base_url}/", next_url)
        self._validate_continuation(absolute)
        parsed = urlsplit(absolute)
        return urlunsplit(("", "", parsed.path, parsed.query, ""))

    def _validate_continuation(self, absolute_url: str) -> None:
        parsed = urlsplit(absolute_url)
        if self._origin_tuple(absolute_url) != self._origin:
            raise MassiveResponseError("Massive next_url changed API origin")
        if parsed.fragment:
            raise MassiveResponseError("Massive next_url cannot contain a fragment")
        if any(key.lower() == "apikey" for key, _ in parse_qsl(parsed.query)):
            raise MassiveResponseError("Massive next_url unexpectedly contained an API key")

    def _retry_delay(self, response: httpx2.Response, attempt: int) -> float:
        retry_after = response.headers.get("retry-after")
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                pass
        return self._backoff_seconds(attempt)

    def _backoff_seconds(self, attempt: int) -> float:
        return min(60.0, 2.0 ** (attempt - 1)) + (self._random_float() * 0.25)

    def _redact(self, value: str) -> str:
        return value.replace(self._api_key, "[REDACTED]")

    @staticmethod
    def _validate_base_url(base_url: str) -> str:
        parsed = urlsplit(base_url.strip())
        if parsed.scheme != "https" or not parsed.hostname:
            raise MassiveConfigurationError("Massive base_url must be an HTTPS origin")
        if parsed.path not in {"", "/"}:
            raise MassiveConfigurationError("Massive base_url cannot include a path")
        if parsed.query or parsed.fragment:
            raise MassiveConfigurationError("Massive base_url cannot include query or fragment")
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))

    @staticmethod
    def _origin_tuple(url: str) -> tuple[str, str, int | None]:
        parsed = urlsplit(url)
        return parsed.scheme.lower(), (parsed.hostname or "").lower(), parsed.port

    @staticmethod
    def _url_without_query(url: str) -> str:
        parsed = urlsplit(url)
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))

    @staticmethod
    def _require_exactly_one_asset(request: ProviderRequest) -> None:
        if len(request.asset_ids) != 1:
            raise MassiveConfigurationError(
                f"{request.dataset.value} requests require exactly one asset_id"
            )

    @staticmethod
    def _require_at_most_one_asset(request: ProviderRequest) -> None:
        if len(request.asset_ids) > 1:
            raise MassiveConfigurationError(
                f"{request.dataset.value} requests allow at most one asset_id"
            )
