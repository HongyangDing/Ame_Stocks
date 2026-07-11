"""Build efficient Massive request plans before any credentials are read."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import exchange_calendars as xcals

from ame_stocks_core import ProviderDataset, ProviderRequest


@dataclass(frozen=True, slots=True)
class DownloadPlan:
    """A deterministic list of provider requests and a lower-bound API estimate."""

    dataset: ProviderDataset
    requests: tuple[ProviderRequest, ...]
    requests_per_minute: float

    @property
    def minimum_api_calls(self) -> int:
        return len(self.requests)

    @property
    def minimum_minutes(self) -> float:
        if not self.requests:
            return 0.0
        return self.minimum_api_calls / self.requests_per_minute

    def summary(self, *, show_all: bool = False) -> dict[str, object]:
        displayed = self.requests if show_all else self.requests[:10]
        return {
            "dataset": self.dataset.value,
            "minimum_api_calls": self.minimum_api_calls,
            "minimum_minutes_at_configured_rate": round(self.minimum_minutes, 2),
            "note": "Pagination can increase actual API calls; plan output never contacts Massive.",
            "request_count": len(self.requests),
            "requests": [
                {
                    "asset_ids": list(request.asset_ids),
                    "end": request.end.isoformat(),
                    "parameters": dict(request.parameters),
                    "request_id": request.request_id,
                    "start": request.start.isoformat(),
                }
                for request in displayed
            ],
            "requests_per_minute": self.requests_per_minute,
            "truncated": not show_all and len(self.requests) > len(displayed),
        }


def build_download_plan(
    *,
    dataset: ProviderDataset,
    start: date,
    end: date,
    tickers: tuple[str, ...] = (),
    active: str = "both",
    requests_per_minute: float = 600.0,
) -> DownloadPlan:
    """Build deterministic requests while avoiding unnecessary API fragmentation."""

    if start > end:
        raise ValueError("start must be on or before end")
    if requests_per_minute <= 0:
        raise ValueError("requests_per_minute must be positive")
    normalized_tickers = _normalize_tickers(tickers)

    if dataset is ProviderDataset.ASSETS:
        if normalized_tickers:
            raise ValueError("assets plans do not accept ticker filters")
        if active not in {"true", "false", "both", "history"}:
            raise ValueError("active must be true, false, both, or history")
        sessions = market_session_dates(start, end)
        if active == "history":
            # The daily active snapshots are the point-in-time tradable universe. One
            # inactive snapshot at the window end adds delisted and former symbols to
            # the historical download union without repeating that large list daily.
            requests = (
                *(
                    ProviderRequest(
                        dataset=dataset,
                        start=session,
                        end=session,
                        parameters=(("active", "true"),),
                    )
                    for session in sessions
                ),
                ProviderRequest(
                    dataset=dataset,
                    start=sessions[-1],
                    end=sessions[-1],
                    parameters=(("active", "false"),),
                ),
            )
        else:
            active_values = ("true", "false") if active == "both" else (active,)
            requests = tuple(
                ProviderRequest(
                    dataset=dataset,
                    start=session,
                    end=session,
                    parameters=(("active", value),),
                )
                for session in sessions
                for value in active_values
            )
    elif dataset is ProviderDataset.DAILY_BARS:
        if normalized_tickers:
            raise ValueError("daily_bars uses the full-market endpoint and rejects ticker filters")
        requests = tuple(
            ProviderRequest(
                dataset=dataset,
                start=session,
                end=session,
                adjusted=False,
            )
            for session in market_session_dates(start, end)
        )
    elif dataset is ProviderDataset.MINUTE_BARS:
        if not normalized_tickers:
            raise ValueError(f"{dataset.value} plans require at least one ticker")
        requests = tuple(
            ProviderRequest(
                dataset=dataset,
                start=start,
                end=end,
                asset_ids=(ticker,),
                adjusted=False,
            )
            for ticker in normalized_tickers
        )
    elif dataset in {ProviderDataset.SPLITS, ProviderDataset.DIVIDENDS}:
        # No ticker filter is the efficient full-market path: one request stream for the date range.
        requests = (
            tuple(
                ProviderRequest(
                    dataset=dataset,
                    start=start,
                    end=end,
                    asset_ids=(ticker,),
                )
                for ticker in normalized_tickers
            )
            if normalized_tickers
            else (ProviderRequest(dataset=dataset, start=start, end=end),)
        )
    else:
        raise ValueError(f"unsupported dataset: {dataset.value}")

    return DownloadPlan(
        dataset=dataset,
        requests=requests,
        requests_per_minute=requests_per_minute,
    )


def _normalize_tickers(tickers: tuple[str, ...]) -> tuple[str, ...]:
    # Duplicates are harmless for callers, but removing them avoids wasted requests.
    return tuple(sorted({ticker.strip().upper() for ticker in tickers if ticker.strip()}))


def market_session_dates(start: date, end: date) -> tuple[date, ...]:
    """Return deterministic XNYS sessions, including scheduled half days."""

    calendar = xcals.get_calendar("XNYS")
    sessions = tuple(
        timestamp.date()
        for timestamp in calendar.sessions_in_range(start.isoformat(), end.isoformat())
    )
    if not sessions:
        raise ValueError("date range contains no XNYS trading sessions")
    return sessions
