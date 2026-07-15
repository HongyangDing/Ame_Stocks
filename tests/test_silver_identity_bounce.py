from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from ame_stocks_api.artifacts import ArtifactError, stable_digest
from ame_stocks_api.silver.identity_bounce import (
    CANDIDATE_MANIFEST_RULE_VERSION,
    CASE_ID_RULE_VERSION,
    DETECTOR_RULE_VERSION,
    MAX_BOUNDED_OBSERVATIONS,
    BounceCorroboration,
    IdentityBounceError,
    IdentityObservation,
    SourceSession,
    build_identity_case_candidate_manifest,
    detect_provider_figi_bounces,
    identity_case_candidate_manifest_path,
    read_identity_case_candidate_manifest,
    write_identity_case_candidate_manifest,
)

A = "BBG00000000A"
B = "BBG00000000B"
C = "BBG00000000C"
BINDING = "49f3d20725f2609b43d6736df78993b2975c9f1b71947af93190dc0658366c64"
NON_PRODUCTION_BINDING = "1" * 64


def _sessions(count: int, *, start: date = date(2024, 1, 2)) -> tuple[SourceSession, ...]:
    return tuple(SourceSession(start + timedelta(days=index)) for index in range(count))


def _observations(
    sessions: tuple[SourceSession, ...],
    *,
    ticker: str,
    figis: tuple[str | None, ...],
    prefix: str,
) -> tuple[IdentityObservation, ...]:
    assert len(sessions) == len(figis)
    return tuple(
        IdentityObservation(
            session_date=session.session_date,
            ticker=ticker,
            observed_composite_figi=figi,
            source_record_id=f"{prefix}-{index:03d}",
            source_available_session=session.session_date,
        )
        for index, (session, figi) in enumerate(zip(sessions, figis, strict=True))
    )


def test_detector_reproduces_reviewed_case_id_and_keeps_discovery_fail_closed() -> None:
    sessions = tuple(
        SourceSession(value)
        for value in (
            date(2024, 1, 2),
            date(2024, 1, 3),
            date(2024, 1, 4),
            date(2024, 1, 5),
        )
    )
    rows = (
        IdentityObservation(
            date(2024, 1, 2),
            "AAPL",
            "BBG000B9XRY4",
            "s4-a-left",
            date(2024, 1, 2),
        ),
        IdentityObservation(
            date(2024, 1, 3),
            "AAPL",
            "BBG000BPH459",
            "s4-b-001",
            date(2024, 1, 3),
        ),
        IdentityObservation(
            date(2024, 1, 4),
            "AAPL",
            "BBG000BPH459",
            "s4-b-002",
            date(2024, 1, 4),
        ),
        IdentityObservation(
            date(2024, 1, 5),
            "AAPL",
            "BBG000B9XRY4",
            "s4-a-right",
            date(2024, 1, 8),
        ),
    )

    result = detect_provider_figi_bounces(
        sessions,
        rows,
        six_release_binding_id=BINDING,
        candidate_manifest_available_session=date(2024, 1, 9),
    )

    assert len(result.cases) == 1
    case = result.cases[0]
    assert case.identity_case_id == (
        "8cd333b4fb72b62e4534ddb316d2ebf30f3cc6d852e19ea778375c13b7daa46e"
    )
    assert case.episode_source_record_ids == ("s4-b-001", "s4-b-002")
    assert case.episode_source_record_set_digest == stable_digest(["s4-b-001", "s4-b-002"])
    assert case.identity_case_available_session == date(2024, 1, 9)
    assert case.session_band == "2-5"
    assert case.s5_support_count == case.s6_support_count == 0
    assert "s5_and_s6_support_absent" in case.reason_codes
    assert result.support_reason_counts["s5_ticker_change_event_support_absent"] == 1
    assert result.suspected_provider_figi_bounce_rows == 2
    assert case.to_manifest_dict()["detector_disposition"] == ("review_required_no_auto_decision")


@pytest.mark.parametrize(
    ("breaker", "replacement"),
    (
        ("ticker_absent", None),
        ("inactive", "inactive"),
        ("null_figi", "null"),
        ("malformed_figi", "malformed"),
        ("global_source_gap", "gap"),
    ),
)
def test_detector_never_stitches_across_a_broken_run(
    breaker: str,
    replacement: str | None,
) -> None:
    sessions = list(_sessions(5))
    rows = list(
        _observations(
            tuple(sessions),
            ticker="XYZ",
            figis=(A, B, B, B, A),
            prefix="xyz",
        )
    )
    if replacement is None:
        rows.pop(2)
    elif replacement == "inactive":
        rows[2] = replace(rows[2], active_on_date=False)
    elif replacement == "null":
        rows[2] = replace(rows[2], observed_composite_figi=None)
    elif replacement == "malformed":
        rows[2] = replace(rows[2], observed_composite_figi="NOT-A-FIGI")
    else:
        assert replacement == "gap" and breaker == "global_source_gap"
        sessions[2] = replace(sessions[2], source_complete=False)

    result = detect_provider_figi_bounces(
        sessions,
        rows,
        six_release_binding_id=BINDING,
        candidate_manifest_available_session=date(2024, 1, 9),
    )

    assert result.cases == ()
    if replacement == "gap":
        assert result.incomplete_source_session_count == 1


def test_detector_uses_maximal_runs_case_sensitive_tickers_and_twenty_session_bound() -> None:
    sessions = _sessions(23)
    too_long = _observations(
        sessions,
        ticker="LONG",
        figis=(A, *(B for _ in range(21)), A),
        prefix="long",
    )
    upper_case = _observations(
        sessions[:3],
        ticker="Case",
        figis=(A, B, A),
        prefix="upper",
    )
    lower_case = _observations(
        sessions[:3],
        ticker="case",
        figis=(A, C, A),
        prefix="lower",
    )

    result = detect_provider_figi_bounces(
        sessions,
        (*too_long, *upper_case, *lower_case),
        six_release_binding_id=BINDING,
        candidate_manifest_available_session=date(2024, 2, 1),
    )

    assert {case.ticker for case in result.cases} == {"Case", "case"}
    assert all(case.middle_session_count == 1 for case in result.cases)
    assert result.session_band_counts == {"1": 2, "2-5": 0, "6-20": 0}


def test_corroboration_is_exact_lineage_and_only_changes_reason_counts() -> None:
    sessions = _sessions(4)
    rows = _observations(
        sessions,
        ticker="ABC",
        figis=(A, B, B, A),
        prefix="abc",
    )
    support = BounceCorroboration(
        ticker="ABC",
        middle_observed_composite_figi=B,
        episode_valid_from_session=sessions[1].session_date,
        episode_valid_through_session=sessions[2].session_date,
        s5_source_record_ids=("s5-event-001",),
        s6_source_record_ids=("s6-overview-001",),
        hierarchy_source_record_ids=("s4-hierarchy-001",),
    )

    supported = detect_provider_figi_bounces(
        sessions,
        rows,
        six_release_binding_id=BINDING,
        candidate_manifest_available_session=date(2024, 1, 9),
        corroboration=(support,),
    )
    unsupported = detect_provider_figi_bounces(
        sessions,
        rows,
        six_release_binding_id=BINDING,
        candidate_manifest_available_session=date(2024, 1, 9),
    )

    assert supported.cases[0].identity_case_id == unsupported.cases[0].identity_case_id
    assert supported.cases[0].s5_support_count == 1
    assert supported.cases[0].s6_support_count == 1
    assert supported.cases[0].hierarchy_support_count == 1
    assert supported.support_reason_counts["s5_and_s6_support_present"] == 1
    assert "s5_and_s6_support_absent" not in supported.support_reason_counts
    assert supported.cases[0].to_manifest_dict()["detector_disposition"] == (
        "review_required_no_auto_decision"
    )

    with pytest.raises(IdentityBounceError, match="does not match any exact detected"):
        detect_provider_figi_bounces(
            sessions,
            rows,
            six_release_binding_id=BINDING,
            candidate_manifest_available_session=date(2024, 1, 9),
            corroboration=(replace(support, episode_valid_through_session=date(2024, 1, 20)),),
        )


def test_candidate_manifest_is_content_addressed_bounded_and_immutable(
    tmp_path: Path,
) -> None:
    sessions = _sessions(8)
    one = _observations(
        sessions[:3],
        ticker="ONE",
        figis=(A, B, A),
        prefix="one",
    )
    three = _observations(
        sessions[:5],
        ticker="THREE",
        figis=(A, B, B, B, A),
        prefix="three",
    )
    six = _observations(
        sessions,
        ticker="SIX",
        figis=(A, *(B for _ in range(6)), A),
        prefix="six",
    )
    detection = detect_provider_figi_bounces(
        sessions,
        (*one, *three, *six),
        six_release_binding_id=NON_PRODUCTION_BINDING,
        candidate_manifest_available_session=date(2024, 1, 16),
    )
    created = datetime(2024, 1, 12, 21, 0, tzinfo=UTC)

    first = build_identity_case_candidate_manifest(
        detection,
        created_at_utc=created,
        bounded_example_limit=2,
    )
    repeated = build_identity_case_candidate_manifest(
        detection,
        created_at_utc=created,
        bounded_example_limit=2,
    )
    with pytest.raises(IdentityBounceError, match="first XNYS open"):
        build_identity_case_candidate_manifest(
            replace(detection, candidate_manifest_available_session=date(2024, 1, 15)),
            created_at_utc=created,
            bounded_example_limit=2,
        )

    assert first.candidate_manifest_id == repeated.candidate_manifest_id
    assert first.content == repeated.content
    document = json.loads(first.content)
    logical_payload = dict(document)
    del logical_payload["candidate_manifest_id"]
    assert first.candidate_manifest_id == stable_digest(logical_payload)
    assert document["candidate_manifest_rule_version"] == CANDIDATE_MANIFEST_RULE_VERSION
    assert document["detector_rule_version"] == DETECTOR_RULE_VERSION
    assert document["case_count"] == 3
    assert len(document["bounded_examples"]) == 2
    assert document["session_band_counts"] == {"1": 1, "2-5": 1, "6-20": 1}
    assert document["suspected_provider_figi_bounce_rows"] == 10
    assert all(
        row["detector_disposition"] == "review_required_no_auto_decision"
        for row in document["cases"]
    )

    receipt = write_identity_case_candidate_manifest(tmp_path, first)
    assert receipt == {
        "bytes": len(first.content),
        "candidate_manifest_id": first.candidate_manifest_id,
        "media_type": "application/json",
        "path": identity_case_candidate_manifest_path(first.candidate_manifest_id),
        "sha256": first.sha256,
    }
    assert write_identity_case_candidate_manifest(tmp_path, first) == receipt
    loaded = read_identity_case_candidate_manifest(
        tmp_path,
        candidate_manifest_id=first.candidate_manifest_id,
        expected_sha256=first.sha256,
    )
    assert loaded.content == first.content
    assert loaded.document == first.document
    assert loaded.candidate_manifest_available_session == date(2024, 1, 16)
    assert loaded.six_release_binding_id == NON_PRODUCTION_BINDING
    assert tuple(item.to_manifest_dict() for item in loaded.cases) == tuple(
        item.to_manifest_dict() for item in detection.cases
    )

    path = tmp_path / first.relative_path
    path.chmod(0o644)
    path.write_bytes(b"{}\n")
    with pytest.raises(ArtifactError, match="refusing to overwrite immutable artifact"):
        write_identity_case_candidate_manifest(tmp_path, first)


def test_production_candidate_ingress_remains_hard_gated(tmp_path: Path) -> None:
    sessions = _sessions(3)
    detection = detect_provider_figi_bounces(
        sessions,
        _observations(sessions, ticker="BLOCK", figis=(A, B, A), prefix="block"),
        six_release_binding_id=BINDING,
        candidate_manifest_available_session=date(2024, 1, 16),
    )
    manifest = build_identity_case_candidate_manifest(
        detection,
        created_at_utc=datetime(2024, 1, 12, 21, tzinfo=UTC),
    )

    with pytest.raises(
        IdentityBounceError, match="production candidate writing remains hard-gated"
    ):
        write_identity_case_candidate_manifest(tmp_path, manifest)
    assert not (tmp_path / manifest.relative_path).exists()

    path = tmp_path / manifest.relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(manifest.content)
    with pytest.raises(
        IdentityBounceError, match="production candidate reading remains hard-gated"
    ):
        read_identity_case_candidate_manifest(
            tmp_path,
            candidate_manifest_id=manifest.candidate_manifest_id,
            expected_sha256=manifest.sha256,
        )


def test_detector_rejects_ignored_or_ambiguous_input() -> None:
    sessions = _sessions(3)
    rows = _observations(
        sessions,
        ticker="DUP",
        figis=(A, B, A),
        prefix="dup",
    )

    with pytest.raises(IdentityBounceError, match="duplicate ticker/session"):
        detect_provider_figi_bounces(
            sessions,
            (*rows, rows[1]),
            six_release_binding_id=BINDING,
            candidate_manifest_available_session=date(2024, 1, 9),
        )

    with pytest.raises(IdentityBounceError, match="observation limit"):
        detect_provider_figi_bounces(
            sessions[:1],
            (rows[0] for _ in range(MAX_BOUNDED_OBSERVATIONS + 1)),
            six_release_binding_id=BINDING,
            candidate_manifest_available_session=date(2024, 1, 9),
        )
    with pytest.raises(IdentityBounceError, match="strictly increasing"):
        detect_provider_figi_bounces(
            (sessions[1], sessions[0], sessions[2]),
            rows,
            six_release_binding_id=BINDING,
            candidate_manifest_available_session=date(2024, 1, 9),
        )
    with pytest.raises(IdentityBounceError, match="outside the supplied global spine"):
        detect_provider_figi_bounces(
            sessions,
            (
                *rows,
                replace(
                    rows[0],
                    session_date=date(2024, 2, 1),
                    source_available_session=date(2024, 2, 1),
                ),
            ),
            six_release_binding_id=BINDING,
            candidate_manifest_available_session=date(2024, 1, 9),
        )


def test_manifest_reload_recomputes_candidate_availability(tmp_path: Path) -> None:
    sessions = _sessions(3)
    detection = detect_provider_figi_bounces(
        sessions,
        _observations(
            sessions,
            ticker="TIME",
            figis=(A, B, A),
            prefix="time",
        ),
        six_release_binding_id=BINDING,
        candidate_manifest_available_session=date(2024, 1, 16),
    )
    manifest = build_identity_case_candidate_manifest(
        detection,
        created_at_utc=datetime(2024, 1, 12, 21, 0, tzinfo=UTC),
    )
    document = json.loads(manifest.content)
    document["created_at_utc"] = "2024-01-12T13:00:00+00:00"
    logical = dict(document)
    logical.pop("candidate_manifest_id")
    document["candidate_manifest_id"] = stable_digest(logical)
    content = (
        json.dumps(document, allow_nan=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    )
    path = tmp_path / identity_case_candidate_manifest_path(document["candidate_manifest_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)

    with pytest.raises(IdentityBounceError, match="first XNYS open"):
        read_identity_case_candidate_manifest(
            tmp_path,
            candidate_manifest_id=document["candidate_manifest_id"],
            expected_sha256=hashlib.sha256(content).hexdigest(),
        )


def test_fixed_versions_and_case_payload_are_explicit() -> None:
    assert DETECTOR_RULE_VERSION == "s7_provider_figi_bounce_detector_v1"
    assert CASE_ID_RULE_VERSION == "s7_provider_figi_bounce_case_id_v1"
    assert CANDIDATE_MANIFEST_RULE_VERSION == "s7_identity_case_candidate_manifest_v1"
