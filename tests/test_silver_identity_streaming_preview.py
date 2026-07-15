from __future__ import annotations

import inspect
import random
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import pytest

from ame_stocks_api.silver.identity_bounce import (
    IdentityObservation,
    SourceSession,
    detect_provider_figi_bounces,
)
from ame_stocks_api.silver.identity_streaming_preview import (
    BoundedIdentityPreviewEngine,
    BoundedIdentityPreviewLimits,
    IdentityStreamingPreviewError,
    build_bounded_identity_preview_artifact,
    run_source_bound_identity_streaming_preview,
)

BINDING = "49f3d20725f2609b43d6736df78993b2975c9f1b71947af93190dc0658366c64"
A = "BBG00000000A"
B = "BBG00000000B"
C = "BBG00000000C"
AVAILABLE = date(2024, 2, 1)


def _sessions(count: int) -> tuple[SourceSession, ...]:
    start = date(2024, 1, 2)
    return tuple(SourceSession(start + timedelta(days=index)) for index in range(count))


def _row(
    session: SourceSession,
    ticker: str,
    figi: str | None,
    ordinal: int,
    *,
    active: bool = True,
    available: date | None = None,
) -> IdentityObservation:
    return IdentityObservation(
        session_date=session.session_date,
        ticker=ticker,
        observed_composite_figi=figi,
        source_record_id=f"{ticker}-source-{ordinal:04d}",
        source_available_session=available or session.session_date,
        active_on_date=active,
    )


def _engine(
    tickers: tuple[str, ...] = ("AAA",),
    *,
    limits: BoundedIdentityPreviewLimits | None = None,
) -> BoundedIdentityPreviewEngine:
    return BoundedIdentityPreviewEngine(
        six_release_binding_id=BINDING,
        preview_manifest_available_session=AVAILABLE,
        scoped_tickers=tickers,
        limits=limits,
    )


def _feed(
    engine: BoundedIdentityPreviewEngine,
    sessions: tuple[SourceSession, ...],
    rows: tuple[IdentityObservation, ...],
) -> None:
    by_session: dict[date, list[IdentityObservation]] = {
        session.session_date: [] for session in sessions
    }
    for row in rows:
        by_session[row.session_date].append(row)
    for session in sessions:
        selected = by_session[session.session_date]
        engine.consume_session(
            session,
            iter(selected),
            scanned_row_count=len(selected) + 3,
            scanned_artifact_count=1,
            scanned_bytes=100 + len(selected),
        )


def test_streaming_fixed_case_is_byte_rule_equivalent_to_existing_detector() -> None:
    sessions = _sessions(4)
    rows = (
        _row(sessions[0], "AAPL", "BBG000B9XRY4", 0),
        _row(sessions[1], "AAPL", "BBG000BPH459", 1),
        _row(sessions[2], "AAPL", "BBG000BPH459", 2),
        _row(
            sessions[3],
            "AAPL",
            "BBG000B9XRY4",
            3,
            available=date(2024, 1, 8),
        ),
    )
    expected = detect_provider_figi_bounces(
        sessions,
        rows,
        six_release_binding_id=BINDING,
        candidate_manifest_available_session=AVAILABLE,
    )
    engine = _engine(("AAPL",))

    _feed(engine, sessions, rows)
    actual = engine.finalize()

    assert tuple(item.identity_case_id for item in actual.cases) == tuple(
        item.identity_case_id for item in expected.cases
    )
    assert actual.cases[0].reason_codes == (
        "hierarchy_support_not_evaluated",
        "s5_and_s6_support_not_evaluated",
        "s5_ticker_change_event_support_not_evaluated",
        "s6_overview_identity_support_not_evaluated",
    )
    assert actual.selected_observation_count == expected.observation_count
    assert actual.valid_active_observation_count == expected.valid_active_observation_count
    assert dict(actual.session_band_counts) == dict(expected.session_band_counts)
    assert actual.corroboration_evaluation_state == "not_evaluated"
    assert actual.support_absence_verified is False


def test_finalize_binds_case_availability_to_actual_artifact_session() -> None:
    sessions = _sessions(3)
    rows = (
        _row(sessions[0], "AAA", A, 0),
        _row(sessions[1], "AAA", B, 1),
        _row(sessions[2], "AAA", A, 2),
    )
    engine = _engine()
    _feed(engine, sessions, rows)
    finalized_available = AVAILABLE + timedelta(days=1)

    result = engine.finalize(
        preview_manifest_available_session=finalized_available,
    )

    assert result.preview_manifest_available_session == finalized_available
    assert result.cases[0].identity_case_available_session == finalized_available


@pytest.mark.parametrize("seed", range(12))
def test_streaming_random_scoped_inputs_match_existing_detector(seed: int) -> None:
    rng = random.Random(seed)
    base_sessions = _sessions(20)
    sessions = tuple(
        SourceSession(session.session_date, source_complete=rng.random() > 0.08)
        for session in base_sessions
    )
    tickers = ("AAA", "BBB", "CCC", "DDD")
    rows: list[IdentityObservation] = []
    ordinal = 0
    choices: tuple[tuple[str | None, bool] | None, ...] = (
        (A, True),
        (A, True),
        (B, True),
        (C, True),
        (None, True),
        ("MALFORMED", True),
        (A, False),
        None,
    )
    for session in sessions:
        for ticker in tickers:
            choice = rng.choice(choices)
            if choice is None:
                continue
            figi, active = choice
            rows.append(_row(session, ticker, figi, ordinal, active=active))
            ordinal += 1
    frozen_rows = tuple(rows)
    expected = detect_provider_figi_bounces(
        sessions,
        frozen_rows,
        six_release_binding_id=BINDING,
        candidate_manifest_available_session=AVAILABLE,
    )
    engine = _engine(tickers)

    _feed(engine, sessions, frozen_rows)
    actual = engine.finalize()

    assert tuple(item.identity_case_id for item in actual.cases) == tuple(
        item.identity_case_id for item in expected.cases
    )
    assert actual.selected_observation_count == expected.observation_count
    assert actual.valid_active_observation_count == expected.valid_active_observation_count
    assert actual.incomplete_source_session_count == expected.incomplete_source_session_count


@pytest.mark.parametrize("breaker", ["missing", "inactive", "null", "malformed", "gap"])
def test_every_reviewed_break_condition_prevents_cross_boundary_case(breaker: str) -> None:
    source_sessions = list(_sessions(5))
    if breaker == "gap":
        source_sessions[2] = SourceSession(
            source_sessions[2].session_date,
            source_complete=False,
        )
    sessions = tuple(source_sessions)
    rows = [
        _row(sessions[0], "AAA", A, 0),
        _row(sessions[1], "AAA", B, 1),
        _row(sessions[3], "AAA", B, 3),
        _row(sessions[4], "AAA", A, 4),
    ]
    if breaker != "missing":
        figi = B
        active = True
        if breaker == "inactive":
            active = False
        elif breaker == "null":
            figi = None
        elif breaker == "malformed":
            figi = "NOT-A-FIGI"
        rows.append(_row(sessions[2], "AAA", figi, 2, active=active))
    engine = _engine()

    _feed(engine, sessions, tuple(rows))

    assert engine.finalize().cases == ()


def test_middle_run_over_twenty_is_not_a_case_and_state_stays_bounded() -> None:
    sessions = _sessions(23)
    rows = [_row(sessions[0], "AAA", A, 0)]
    rows.extend(_row(session, "AAA", B, index) for index, session in enumerate(sessions[1:22], 1))
    rows.append(_row(sessions[22], "AAA", A, 22))
    engine = _engine()

    _feed(engine, sessions, tuple(rows))

    assert engine.buffered_source_record_id_count <= 21
    assert engine.finalize().cases == ()


def test_streaming_engine_rejects_duplicate_out_of_scope_and_wrong_session_rows() -> None:
    session = _sessions(1)[0]
    duplicate = _engine()
    row = _row(session, "AAA", A, 0)
    with pytest.raises(IdentityStreamingPreviewError, match="duplicate ticker/session"):
        duplicate.consume_session(
            session,
            iter((row, row)),
            scanned_row_count=2,
            scanned_artifact_count=1,
            scanned_bytes=10,
        )
    with pytest.raises(IdentityStreamingPreviewError, match="poisoned"):
        duplicate.finalize()

    out_of_scope = _engine()
    with pytest.raises(IdentityStreamingPreviewError, match="outside the exact preview scope"):
        out_of_scope.consume_session(
            session,
            iter((_row(session, "ZZZ", A, 0),)),
            scanned_row_count=1,
            scanned_artifact_count=1,
            scanned_bytes=10,
        )

    wrong_session = _engine()
    later = SourceSession(session.session_date + timedelta(days=1))
    with pytest.raises(IdentityStreamingPreviewError, match="differs"):
        wrong_session.consume_session(
            session,
            iter((_row(later, "AAA", A, 0),)),
            scanned_row_count=1,
            scanned_artifact_count=1,
            scanned_bytes=10,
        )


def test_scope_is_exact_sorted_unique_and_never_wildcard() -> None:
    with pytest.raises(IdentityStreamingPreviewError, match="sorted order"):
        _engine(("BBB", "AAA"))
    with pytest.raises(IdentityStreamingPreviewError, match="unique"):
        _engine(("AAA", "AAA"))
    with pytest.raises(IdentityStreamingPreviewError, match="exact nonempty"):
        _engine(("*",))


def test_session_cap_poisons_engine_before_result_or_artifact() -> None:
    limits = replace(BoundedIdentityPreviewLimits(), max_sessions=1)
    engine = _engine(limits=limits)
    sessions = _sessions(2)
    engine.consume_session(
        sessions[0],
        iter((_row(sessions[0], "AAA", A, 0),)),
        scanned_row_count=1,
        scanned_artifact_count=1,
        scanned_bytes=1,
    )
    with pytest.raises(IdentityStreamingPreviewError, match="session cap"):
        engine.consume_session(
            sessions[1],
            iter((_row(sessions[1], "AAA", A, 1),)),
            scanned_row_count=1,
            scanned_artifact_count=1,
            scanned_bytes=1,
        )
    with pytest.raises(IdentityStreamingPreviewError, match="poisoned"):
        engine.finalize()


def test_ticker_cap_fails_before_any_session_is_consumed() -> None:
    limits = replace(BoundedIdentityPreviewLimits(), max_tickers=1)
    with pytest.raises(IdentityStreamingPreviewError, match="ticker cap"):
        _engine(("AAA", "BBB"), limits=limits)


def test_selected_row_cap_poisons_before_final_artifact() -> None:
    limits = replace(BoundedIdentityPreviewLimits(), max_selected_rows=1)
    engine = _engine(("AAA", "BBB"), limits=limits)
    session = _sessions(1)[0]
    with pytest.raises(IdentityStreamingPreviewError, match="selected-row cap"):
        engine.consume_session(
            session,
            iter((_row(session, "AAA", A, 0), _row(session, "BBB", A, 1))),
            scanned_row_count=2,
            scanned_artifact_count=1,
            scanned_bytes=1,
        )
    with pytest.raises(IdentityStreamingPreviewError, match="poisoned"):
        engine.finalize()


@pytest.mark.parametrize(
    ("limit_field", "consume_field", "error"),
    [
        ("max_scanned_rows", "scanned_row_count", "scanned-row cap"),
        ("max_artifacts", "scanned_artifact_count", "artifact cap"),
        ("max_bytes", "scanned_bytes", "byte cap"),
    ],
)
def test_physical_scan_caps_fail_before_rows_are_iterated(
    limit_field: str,
    consume_field: str,
    error: str,
) -> None:
    limits = replace(BoundedIdentityPreviewLimits(), **{limit_field: 1})
    engine = _engine(limits=limits)
    session = _sessions(1)[0]
    consumed = False

    def rows() -> object:
        nonlocal consumed
        consumed = True
        yield _row(session, "AAA", A, 0)

    metrics = {
        "scanned_row_count": 1,
        "scanned_artifact_count": 1,
        "scanned_bytes": 1,
    }
    metrics[consume_field] = 2
    with pytest.raises(IdentityStreamingPreviewError, match=error):
        engine.consume_session(session, rows(), **metrics)
    assert consumed is False


def test_selected_rows_cannot_exceed_reported_physical_rows() -> None:
    engine = _engine(("AAA", "BBB"))
    session = _sessions(1)[0]
    with pytest.raises(IdentityStreamingPreviewError, match="exceed physically scanned"):
        engine.consume_session(
            session,
            iter((_row(session, "AAA", A, 0), _row(session, "BBB", A, 1))),
            scanned_row_count=1,
            scanned_artifact_count=1,
            scanned_bytes=1,
        )
    with pytest.raises(IdentityStreamingPreviewError, match="poisoned"):
        engine.finalize()


def test_case_cap_prevents_final_result_after_partial_detection() -> None:
    limits = replace(BoundedIdentityPreviewLimits(), max_cases=1)
    sessions = _sessions(5)
    rows = tuple(
        _row(session, "AAA", figi, index)
        for index, (session, figi) in enumerate(zip(sessions, (A, B, A, B, A), strict=True))
    )
    engine = _engine(limits=limits)
    by_date = {row.session_date: row for row in rows}
    with pytest.raises(IdentityStreamingPreviewError, match="case cap"):
        for session in sessions:
            engine.consume_session(
                session,
                iter((by_date[session.session_date],)),
                scanned_row_count=1,
                scanned_artifact_count=1,
                scanned_bytes=1,
            )
    with pytest.raises(IdentityStreamingPreviewError, match="poisoned"):
        engine.finalize()


def test_final_result_and_artifact_are_explicitly_review_only_and_deterministic() -> None:
    sessions = _sessions(3)
    rows = tuple(
        _row(session, "AAA", figi, index)
        for index, (session, figi) in enumerate(zip(sessions, (A, B, A), strict=True))
    )
    engine = _engine()
    _feed(engine, sessions, rows)
    result = engine.finalize()

    first = build_bounded_identity_preview_artifact(result)
    second = build_bounded_identity_preview_artifact(result)

    assert result.status == "awaiting_review"
    assert result.scope_kind == "bounded_preview"
    assert result.adjudication_eligible is False
    assert result.source_attested is False
    assert result.canonical_candidate_eligible is False
    assert first.preview_artifact_id == second.preview_artifact_id
    assert first.sha256 == second.sha256
    assert first.content == second.content
    assert first.document["status"] == "awaiting_review"
    assert first.document["adjudication_eligible"] is False
    assert "identity-case-candidates" not in first.relative_path


def test_empty_or_already_finalized_engine_cannot_produce_another_result() -> None:
    empty = _engine()
    with pytest.raises(IdentityStreamingPreviewError, match="empty scope"):
        empty.finalize()
    with pytest.raises(IdentityStreamingPreviewError, match="poisoned"):
        empty.finalize()

    session = _sessions(1)[0]
    complete = _engine()
    complete.consume_session(
        session,
        iter((_row(session, "AAA", A, 0),)),
        scanned_row_count=1,
        scanned_artifact_count=1,
        scanned_bytes=1,
    )
    complete.finalize()
    with pytest.raises(IdentityStreamingPreviewError, match="already finalized"):
        complete.finalize()


def test_production_shaped_entry_accepts_no_bundle_or_rows_and_delegates_exact_controls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signature = inspect.signature(run_source_bound_identity_streaming_preview)
    assert tuple(signature.parameters) == (
        "data_root",
        "plan_id",
        "expected_plan_sha256",
        "approval_id",
        "expected_approval_sha256",
    )
    assert "bundle" not in signature.parameters
    assert "observations" not in signature.parameters
    assert "corroboration" not in signature.parameters

    calls: list[tuple[object, ...]] = []
    sentinel = object()

    def fake_run(data_root: Path, **kwargs: str) -> object:
        calls.append((data_root, kwargs))
        return sentinel

    monkeypatch.setattr(
        "ame_stocks_api.silver.identity_preview_runner.run_source_bound_identity_streaming_preview",
        fake_run,
    )
    result = run_source_bound_identity_streaming_preview(
        tmp_path,
        plan_id="a" * 64,
        expected_plan_sha256="b" * 64,
        approval_id="c" * 64,
        expected_approval_sha256="d" * 64,
    )

    assert result is sentinel
    assert calls == [
        (
            tmp_path,
            {
                "approval_id": "c" * 64,
                "expected_approval_sha256": "d" * 64,
                "expected_plan_sha256": "b" * 64,
                "plan_id": "a" * 64,
            },
        )
    ]


def test_configurable_limits_can_only_tighten_repository_hard_ceilings() -> None:
    with pytest.raises(IdentityStreamingPreviewError, match="hard ceiling"):
        replace(
            BoundedIdentityPreviewLimits(),
            max_sessions=BoundedIdentityPreviewLimits().max_sessions + 1,
        )
