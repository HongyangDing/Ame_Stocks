"""Shared semantic contracts for REST Bronze rows."""

from __future__ import annotations

import math
import re
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import urlsplit

_ACCESSION_PATTERN = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_FINANCIAL_SECTIONS = frozenset(
    {
        "balance_sheet",
        "cash_flow_statement",
        "comprehensive_income",
        "income_statement",
    }
)
_METRIC_KEYS = {
    "direct_report": frozenset({"label", "order", "source", "unit", "value", "xpath"}),
    "intra_report_impute": frozenset(
        {"formula", "label", "order", "source", "unit", "value"}
    ),
    "inter_report_derive": frozenset(
        {"derived_from", "label", "order", "source", "unit", "value"}
    ),
}
_FISCAL_PERIODS = {
    "annual": frozenset({"FY"}),
    "quarterly": frozenset({"Q1", "Q2", "Q3", "Q4"}),
    "ttm": frozenset({"TTM"}),
}


def epoch_millisecond_date(value: object) -> str | None:
    """Return the UTC date for a non-negative integer Unix-millisecond timestamp."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    try:
        return datetime.fromtimestamp(value / 1000, tz=UTC).date().isoformat()
    except (OSError, OverflowError, ValueError):
        return None


def valid_daily_bar(row: dict[str, Any]) -> bool:
    """Validate Massive's raw grouped-daily compact-key response contract."""

    ticker = row.get("T")
    if not isinstance(ticker, str) or not ticker.strip() or ticker != ticker.strip():
        return False
    if epoch_millisecond_date(row.get("t")) is None:
        return False
    values = {field: row.get(field) for field in ("o", "h", "l", "c", "v")}
    if any(not _finite_nonnegative_number(value) for value in values.values()):
        return False
    transactions = row.get("n")
    if transactions is not None and (
        isinstance(transactions, bool)
        or not isinstance(transactions, int)
        or transactions < 0
    ):
        return False
    vwap = row.get("vw")
    if vwap is not None and not _finite_nonnegative_number(vwap):
        return False
    otc = row.get("otc")
    if otc is not None and not isinstance(otc, bool):
        return False
    open_price = float(values["o"])
    high_price = float(values["h"])
    low_price = float(values["l"])
    close_price = float(values["c"])
    return high_price >= max(open_price, close_price, low_price) and low_price <= min(
        open_price, close_price, high_price
    )


def valid_legacy_financials(row: dict[str, Any]) -> bool:
    """Validate fields needed for point-in-time use of the legacy financials endpoint."""

    start_date = _iso_date(row.get("start_date"))
    filing_date = _iso_date(row.get("filing_date"))
    end_date = _iso_date(row.get("end_date"))
    if (
        start_date is None
        or filing_date is None
        or end_date is None
        or start_date > end_date
        or end_date > filing_date
    ):
        return False
    cik = row.get("cik")
    if (
        not isinstance(cik, str)
        or cik != cik.strip()
        or not cik.isdigit()
        or len(cik) != 10
    ):
        return False
    company_name = row.get("company_name")
    if not _trimmed_text(company_name):
        return False
    timeframe = row.get("timeframe")
    fiscal_period = row.get("fiscal_period")
    fiscal_year = row.get("fiscal_year")
    if (
        not _trimmed_text(timeframe)
        or timeframe not in _FISCAL_PERIODS
        or fiscal_period not in _FISCAL_PERIODS[timeframe]
        or not _trimmed_text(fiscal_year)
    ):
        return False
    if "sic" not in row or not isinstance(row["sic"], str) or row["sic"] != row["sic"].strip():
        return False
    if "tickers" not in row or not _valid_optional_tickers(row["tickers"]):
        return False
    source_url = row.get("source_filing_url")
    source_file_url = row.get("source_filing_file_url")
    source_accession = legacy_filing_accession(source_url)
    file_accession = legacy_filing_accession(source_file_url)
    if source_accession is None or file_accession != source_accession:
        return False
    financials = row.get("financials")
    if not _valid_financial_sections(financials):
        return False
    acceptance_datetime = row.get("acceptance_datetime")
    return acceptance_datetime is None or _aware_iso_datetime(acceptance_datetime)


def _valid_optional_tickers(value: object) -> bool:
    if value is None:
        return True
    if not isinstance(value, list) or not value:
        return False
    normalized: list[str] = []
    for ticker in value:
        if not _trimmed_text(ticker):
            return False
        normalized.append(ticker)
    return len(normalized) == len(set(normalized))


def legacy_filing_accession(value: object) -> str | None:
    """Extract the one canonical SEC accession embedded in a legacy filing URL."""

    if not _trimmed_text(value):
        return None
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.query
        or parsed.fragment
    ):
        return None
    matches = _ACCESSION_PATTERN.findall(parsed.path)
    return matches[0] if len(matches) == 1 else None


def _valid_financial_sections(value: object) -> bool:
    if not isinstance(value, dict) or not value or not set(value).issubset(_FINANCIAL_SECTIONS):
        return False
    for section_name, section in value.items():
        if not _trimmed_text(section_name) or not isinstance(section, dict) or not section:
            return False
        for metric_name, metric in section.items():
            if not _trimmed_text(metric_name) or not _valid_financial_metric(metric):
                return False
    return True


def _valid_financial_metric(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    source = value.get("source")
    expected_keys = _METRIC_KEYS.get(source)
    if expected_keys is None or set(value) != expected_keys:
        return False
    if (
        not _trimmed_text(value.get("label"))
        or not _trimmed_text(value.get("unit"))
        or isinstance(value.get("order"), bool)
        or not isinstance(value.get("order"), int)
        or not _finite_number(value.get("value"))
    ):
        return False
    if source == "direct_report":
        return _trimmed_text(value.get("xpath"))
    if source == "intra_report_impute":
        return _trimmed_text(value.get("formula"))
    derived_from = value.get("derived_from")
    return (
        isinstance(derived_from, list)
        and bool(derived_from)
        and all(
            isinstance(accession, str)
            and accession == accession.strip()
            and _ACCESSION_PATTERN.fullmatch(accession) is not None
            for accession in derived_from
        )
        and len(derived_from) == len(set(derived_from))
    )


def _finite_nonnegative_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    if isinstance(value, int):
        return value >= 0
    return math.isfinite(value) and value >= 0


def _finite_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return isinstance(value, int) or math.isfinite(value)


def _trimmed_text(value: object) -> bool:
    return isinstance(value, str) and bool(value) and value == value.strip()


def _iso_date(value: object) -> str | None:
    if not isinstance(value, str) or len(value) != 10:
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        return None


def _aware_iso_datetime(value: object) -> bool:
    if not isinstance(value, str) or len(value) < 12 or value[10] not in {"T", " "}:
        return False
    candidate = f"{value[:-1]}+00:00" if value.endswith(("Z", "z")) else value
    try:
        observed = datetime.fromisoformat(candidate)
    except ValueError:
        return False
    return observed.tzinfo is not None and observed.utcoffset() is not None


__all__ = [
    "epoch_millisecond_date",
    "legacy_filing_accession",
    "valid_daily_bar",
    "valid_legacy_financials",
]
