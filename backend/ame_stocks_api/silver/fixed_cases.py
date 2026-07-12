"""Immutable metadata for the mandatory Silver review cases.

S0 registers the cases and their review invariants only.  It deliberately does
not load Bronze data, synthesize fixtures, or run a Silver transformation.
Dataset-specific stages select the relevant case IDs when they build a preview.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final


@dataclass(frozen=True, slots=True)
class FixedCase:
    """Review metadata for one scenario that a later preview must evidence."""

    case_id: str
    title: str
    family: str
    purpose: str
    expected_invariants: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        """Return a detached JSON-compatible representation for CLI output."""

        return {
            "case_id": self.case_id,
            "expected_invariants": list(self.expected_invariants),
            "family": self.family,
            "purpose": self.purpose,
            "title": self.title,
        }


FIXED_CASES: Final[tuple[FixedCase, ...]] = (
    FixedCase(
        case_id="normal_session",
        title="Normal U.S. equity session",
        family="market_calendar",
        purpose="Establish the baseline calendar, session-segment, and sparse-bar behavior.",
        expected_invariants=(
            "The exchange calendar, not weekday arithmetic, defines the session.",
            "RTH boundaries follow America/New_York and are persisted as UTC-aware timestamps.",
            "Missing trades do not create zero-filled or forward-filled bars.",
        ),
    ),
    FixedCase(
        case_id="half_day",
        title="U.S. equity half-day session",
        family="market_calendar",
        purpose="Prove that an early close does not inherit a normal-session boundary.",
        expected_invariants=(
            "The exchange calendar supplies the early close and its UTC conversion.",
            "The scheduled RTH window has about 210 minute slots, not 390.",
            "Bars after the early close are not classified as RTH.",
        ),
    ),
    FixedCase(
        case_id="current_reference_snapshot",
        title="Current-only reference snapshot",
        family="reference",
        purpose=(
            "Prevent a latest-only provider dictionary from being backfilled as historical fact."
        ),
        expected_invariants=(
            "Capture date comes from the immutable manifest completion time, not a request label.",
            "Availability begins at the first XNYS open strictly after the capture instant.",
            "A later capture appends a new date partition and does not overwrite prior evidence.",
            "Every row retains exact request, artifact, page, ordinal, and raw-row lineage.",
        ),
    ),
    FixedCase(
        case_id="forward_split_2_for_1",
        title="2-for-1 forward split",
        family="corporate_actions",
        purpose="Keep the event ratio and any derived adjustment chain explicit and auditable.",
        expected_invariants=(
            "The event ratio is independently derived as two new shares per old share.",
            "Raw market data remains raw and the split is not applied twice.",
            "Event date, source lineage, and adjustment convention remain visible.",
        ),
    ),
    FixedCase(
        case_id="reverse_split",
        title="Reverse split",
        family="corporate_actions",
        purpose="Exercise a share ratio below one without confusing its direction.",
        expected_invariants=(
            "The new-per-old ratio is positive and below one for the selected case.",
            "Price and share transformations use reciprocal directions where appropriate.",
            "The event remains distinct from ticker and identity changes on nearby dates.",
        ),
    ),
    FixedCase(
        case_id="regular_dividend",
        title="Regular cash dividend",
        family="corporate_actions",
        purpose="Validate ordinary cash-event dates, amount basis, and return availability.",
        expected_invariants=(
            "Raw cash and any current-share-basis amount are separate fields.",
            "Ex-date, declaration date, record date, and pay date are not interchangeable.",
            "A dividend is not used before the approved availability rule permits it.",
        ),
    ),
    FixedCase(
        case_id="special_dividend",
        title="Special cash dividend",
        family="corporate_actions",
        purpose="Prevent a non-recurring distribution from being treated as an ordinary dividend.",
        expected_invariants=(
            "The provider classification and cash amount are preserved with lineage.",
            "Special and regular distributions remain distinguishable in Silver.",
            "Any return adjustment is applied exactly once under a named convention.",
        ),
    ),
    FixedCase(
        case_id="halt_or_missing_minutes",
        title="Trading halt or missing minutes",
        family="market_calendar",
        purpose="Preserve sparse observations without inventing transactions or prices.",
        expected_invariants=(
            "No zero bar or forward-filled trade is synthesized.",
            "Observed absence, a known halt, and a source-coverage gap remain distinguishable.",
            "Coverage and bounded QA evidence retain the affected asset and interval.",
        ),
    ),
    FixedCase(
        case_id="ticker_change",
        title="Ticker change",
        family="identity",
        purpose="Show that a symbol can change while the tradable security identity continues.",
        expected_invariants=(
            "Ticker is an alias, never the permanent security key.",
            "Alias validity uses a reviewed half-open session interval.",
            "Both source tickers and the evidence used to link them are retained.",
        ),
    ),
    FixedCase(
        case_id="ticker_reuse",
        title="Ticker reuse",
        family="identity",
        purpose="Prevent two historical securities that share a symbol from being merged.",
        expected_invariants=(
            "Non-overlapping aliases may resolve to different asset IDs.",
            "Symbol equality alone is insufficient identity evidence.",
            "Ambiguous links remain provisional or quarantined instead of silently merged.",
        ),
    ),
    FixedCase(
        case_id="delisting",
        title="Delisted security",
        family="identity",
        purpose="Retain historical securities after they leave the active ticker snapshot.",
        expected_invariants=(
            "Historical membership is evaluated as of each session, not from today's active list.",
            "Last-seen and alias end boundaries remain explicit.",
            "The security is not dropped from prior universes, avoiding survivorship bias.",
        ),
    ),
    FixedCase(
        case_id="case_sensitive_tickers",
        title="Case-sensitive ticker distinction",
        family="identity",
        purpose="Prove that normalization does not collapse distinct provider symbols.",
        expected_invariants=(
            "The provider ticker spelling and case are preserved verbatim.",
            "Case folding is not used as a primary-key or deduplication rule.",
            "Any normalized search field is separate from the canonical source ticker.",
        ),
    ),
    FixedCase(
        case_id="provider_timestamp_2019_08_12",
        title="2019-08-12 provider timestamp anomaly",
        family="market_data_quality",
        purpose="Retain the reproducible Flat Day timestamp anomaly as review evidence.",
        expected_invariants=(
            "The 29 noncanonical Flat Day timestamps are flagged or quarantined.",
            "The source timestamp is preserved and is not silently rewritten.",
            "The anomaly is not mislabeled as local corruption because the source hash is stable.",
        ),
    ),
    FixedCase(
        case_id="date_only_filing",
        title="Date-only filing availability",
        family="point_in_time",
        purpose="Prevent a filing with no intraday timestamp from entering a same-day signal.",
        expected_invariants=(
            "The source filing date remains distinct from derived availability fields.",
            "The default rule treats a date-only filing as public after that session's close.",
            "The filing first becomes eligible on the next valid trading session.",
        ),
    ),
    FixedCase(
        case_id="form_13f_header_only",
        title="Form 13-F header-only filing",
        family="sec",
        purpose=(
            "Represent a filing whose holding fields are absent without inventing zero holdings."
        ),
        expected_invariants=(
            "The filing header is retained with holdings_status=not_public_or_unavailable.",
            "No holding fact row is emitted from the absent information table.",
            (
                "Header-only, a genuine zero position, and a partially malformed holding stay "
                "distinct."
            ),
        ),
    ),
)

FIXED_CASE_IDS: Final[tuple[str, ...]] = tuple(case.case_id for case in FIXED_CASES)

if len(FIXED_CASE_IDS) != 15 or len(set(FIXED_CASE_IDS)) != len(FIXED_CASE_IDS):
    raise RuntimeError("the Silver fixed-case registry must contain 15 unique case IDs")

FIXED_CASES_BY_ID: Final[Mapping[str, FixedCase]] = MappingProxyType(
    {case.case_id: case for case in FIXED_CASES}
)


def get_fixed_case(case_id: str) -> FixedCase:
    """Return registered metadata, raising a descriptive error for an unknown ID."""

    try:
        return FIXED_CASES_BY_ID[case_id]
    except KeyError as exc:
        raise KeyError(f"unknown Silver fixed case: {case_id}") from exc
