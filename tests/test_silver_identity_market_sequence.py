from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ame_stocks_api.artifacts import sha256_file, stable_digest
from ame_stocks_api.silver import identity_market_sequence as subject
from ame_stocks_api.silver.calendar_artifact import (
    build_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.identity_market_sequence import (
    IdentityMarketSequenceError,
    S7MarketSequenceResourceCaps,
    authorize_market_sequence_plan_under_standing_grant,
    prepare_market_sequence_plan,
    run_source_bound_market_sequence,
)

US_A = "BBG000000001"
US_B = "BBG000000002"
FOREIGN_A = "BBG000000003"
FOREIGN_B = "BBG000000004"
FOREIGN_LONG = "BBG000000005"
FOREIGN_LEGAL = "BBG000000006"
UNRESOLVED = "BBG000000007"
SHARE = "BBG000000101"


def _compact_canonical(value: dict[str, object]) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _canonical(value: dict[str, object]) -> bytes:
    return _compact_canonical(value) + b"\n"


def _write_compact_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_compact_canonical(value))


def _write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical(value))


def _receipt(root: Path, relative: str, *, rows: int | None = None) -> dict[str, object]:
    path = root / relative
    result: dict[str, object] = {
        "bytes": path.stat().st_size,
        "path": relative,
        "sha256": sha256_file(path),
    }
    if rows is not None:
        result["row_count"] = rows
    return result


def _source_id(session: date, ticker: str) -> str:
    return stable_digest({"session": session.isoformat(), "ticker": ticker})


def _row(
    session: date,
    ticker: str,
    composite: str,
    *,
    locale: str = "us",
    mic: str = "XNAS",
    active: bool = True,
) -> dict[str, object]:
    return {
        "active_on_date": active,
        "composite_figi": composite,
        "locale": locale,
        "market": "stocks",
        "primary_exchange_mic": mic,
        "selected_source_record_id": _source_id(session, ticker),
        "session_date": session,
        "share_class_figi": SHARE,
        "ticker": ticker,
    }


def _fixture_rows(sessions: list[date]) -> list[list[dict[str, object]]]:
    ordinary = [US_A, FOREIGN_A, US_A, US_A, US_A]
    inverse = [FOREIGN_B, US_B, FOREIGN_B, FOREIGN_B, FOREIGN_B]
    output: list[list[dict[str, object]]] = []
    for index, session in enumerate(sessions):
        output.append(
            [
                _row(session, "ORD", ordinary[index]),
                _row(session, "INV", inverse[index]),
                _row(
                    session,
                    "LONG",
                    FOREIGN_LONG,
                    active=False,
                ),
                _row(
                    session,
                    "LEGAL",
                    FOREIGN_LEGAL,
                    locale="ca",
                    mic="XTSE",
                ),
                _row(session, "UNK", UNRESOLVED),
            ]
        )
    return output


def _write_universe_artifacts(
    root: Path, sessions: list[date]
) -> tuple[list[dict[str, object]], int]:
    refs: list[dict[str, object]] = []
    total_rows = 0
    for session, rows in zip(sessions, _fixture_rows(sessions), strict=True):
        relative = (
            "silver/releases/universe_source_daily/"
            f"session_date={session.isoformat()}/part-00000.parquet"
        )
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")
        total_rows += len(rows)
        refs.append(
            {
                "bytes": path.stat().st_size,
                "path": relative,
                "release_id": subject.UNIVERSE_RELEASE_ID,
                "release_manifest_sha256": subject.UNIVERSE_RELEASE_MANIFEST_SHA256,
                "row_count": len(rows),
                "session_date": session.isoformat(),
                "sha256": sha256_file(path),
                "table": subject.UNIVERSE_TABLE,
            }
        )
    return refs, total_rows


def _write_gate_a(
    root: Path, universe_refs: list[dict[str, object]], sessions: list[date]
) -> tuple[subject._SourceExpectations, str]:
    asset_refs = [
        {
            "bytes": 0,
            "path": (
                "silver/releases/asset_observation_daily/"
                f"session_date={session.isoformat()}/part-00000.parquet"
            ),
            "release_id": stable_digest({"release": "asset"}),
            "release_manifest_sha256": stable_digest({"manifest": "asset"}),
            "row_count": 1,
            "session_date": session.isoformat(),
            "sha256": stable_digest({"asset": session.isoformat()}),
            "table": "asset_observation_daily",
        }
        for session in sessions
    ]
    refs = sorted(
        asset_refs + universe_refs,
        key=lambda item: (item["table"], item["session_date"]),
    )
    source_digest = stable_digest(refs)
    source_rows = sum(int(item["row_count"]) for item in refs)
    source_bytes = sum(int(item["bytes"]) for item in refs)

    artifacts = []
    provisional = {
        "artifact_type": "fixture_gate_a_candidate",
        "artifacts": artifacts,
        "candidate_state": "awaiting_review",
        "counts": {
            "source_artifact_count": len(refs),
            "source_bytes": source_bytes,
            "source_row_count": source_rows,
        },
        "source_artifact_set_digest": source_digest,
        "source_artifacts": refs,
    }
    # Candidate output paths are relative to a content-addressed directory, while the
    # refs themselves are part of the logical payload.  Their bytes are independent of
    # the directory name, so write them first in a temporary fixture slot.
    fixture_output = root / "fixture-gate-a-output"
    fixture_output.mkdir()
    data_path = fixture_output / "part-00000.parquet"
    pq.write_table(pa.table({"observed_composite_figi": [US_A]}), data_path)
    qa_path = fixture_output / "qa.json"
    examples_path = fixture_output / "examples.json"
    _write_json(qa_path, {"critical_failure_count": 0})
    _write_json(examples_path, {"examples": []})
    artifacts.extend(
        [
            {
                "bytes": data_path.stat().st_size,
                "media_type": "application/vnd.apache.parquet",
                "path": "data/part-00000.parquet",
                "role": "data",
                "row_count": 1,
                "sha256": sha256_file(data_path),
            },
            {
                "bytes": qa_path.stat().st_size,
                "media_type": "application/json",
                "path": "qa/qa.json",
                "role": "qa",
                "sha256": sha256_file(qa_path),
            },
            {
                "bytes": examples_path.stat().st_size,
                "media_type": "application/json",
                "path": "examples/invalid-figi.json",
                "role": "bounded_examples",
                "sha256": sha256_file(examples_path),
            },
        ]
    )
    candidate_id = stable_digest(provisional)
    prefix = f"manifests/silver/identity/composite-inventory-candidates/candidate_id={candidate_id}"
    for source, relative in (
        (data_path, "data/part-00000.parquet"),
        (qa_path, "qa/qa.json"),
        (examples_path, "examples/invalid-figi.json"),
    ):
        target = root / prefix / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
    candidate = {
        **provisional,
        "candidate_id": candidate_id,
        "canonical_paths": {
            "manifest": f"{prefix}/manifest.json",
        },
    }
    candidate_relative = f"{prefix}/manifest.json"
    _write_json(root / candidate_relative, candidate)
    candidate_sha = sha256_file(root / candidate_relative)

    plan_id = stable_digest({"fixture": "plan"})
    approval_id = stable_digest({"fixture": "approval"})
    completion_payload = {
        "approval_id": approval_id,
        "candidate": {
            "candidate_id": candidate_id,
            "data": {
                "bytes": artifacts[0]["bytes"],
                "path": f"{prefix}/{artifacts[0]['path']}",
                "sha256": artifacts[0]["sha256"],
            },
            "path": candidate_relative,
            "sha256": candidate_sha,
            "state": "awaiting_review",
        },
        "completion_state": "awaiting_review",
        "counts": {
            "reconciliation_row_count": sum(int(item["row_count"]) for item in universe_refs),
            "source_artifact_count": len(refs),
            "source_bytes": source_bytes,
            "source_row_count": source_rows,
        },
        "plan_id": plan_id,
        "source_artifact_set_digest": source_digest,
    }
    completion_id = stable_digest(completion_payload)
    completion = {**completion_payload, "completion_id": completion_id}
    completion_relative = (
        "manifests/silver/identity/composite-inventory-execution-completions/"
        f"plan_id={plan_id}/approval_id={approval_id}/manifest.json"
    )
    _write_json(root / completion_relative, completion)
    expectations = subject._SourceExpectations(
        inventory_candidate_id=candidate_id,
        inventory_candidate_sha256=candidate_sha,
        inventory_completion_id=completion_id,
        inventory_completion_sha256=sha256_file(root / completion_relative),
        inventory_completion_path=completion_relative,
        source_artifact_set_digest=source_digest,
        source_artifact_count=len(refs),
        source_row_count=source_rows,
        source_bytes=source_bytes,
        universe_artifact_count=len(universe_refs),
        universe_row_count=sum(int(item["row_count"]) for item in universe_refs),
        universe_release_id=subject.UNIVERSE_RELEASE_ID,
        universe_release_manifest_sha256=subject.UNIVERSE_RELEASE_MANIFEST_SHA256,
        start_session=sessions[0],
        end_session=sessions[-1],
    )
    return expectations, completion_relative


def _write_gate_b(root: Path) -> dict[str, object]:
    source_available_session = "2026-07-20"
    rows = [
        {
            "classification": "us_composite",
            "composite_figi": US_A,
            "market_codes": ["US"],
            "source_available_session": source_available_session,
        },
        {
            "classification": "us_composite",
            "composite_figi": US_B,
            "market_codes": ["US"],
            "source_available_session": source_available_session,
        },
        {
            "classification": "non_us_composite",
            "composite_figi": FOREIGN_A,
            "market_codes": ["GR"],
            "source_available_session": source_available_session,
        },
        {
            "classification": "non_us_composite",
            "composite_figi": FOREIGN_B,
            "market_codes": ["LN"],
            "source_available_session": source_available_session,
        },
        {
            "classification": "non_us_composite",
            "composite_figi": FOREIGN_LONG,
            "market_codes": ["GR"],
            "source_available_session": source_available_session,
        },
        {
            "classification": "non_us_composite",
            "composite_figi": FOREIGN_LEGAL,
            "market_codes": ["CN"],
            "source_available_session": source_available_session,
        },
        {
            "classification": "unresolved_no_exact_current_mapping",
            "composite_figi": UNRESOLVED,
            "market_codes": [],
            "source_available_session": source_available_session,
        },
    ]
    expected = subject.PRODUCTION_EXPECTATIONS
    gate_a_relative = (
        "manifests/silver/identity/composite-inventory-candidates/"
        f"candidate_id={expected.inventory_candidate_id}/manifest.json"
    )
    gate_a = json.loads((root / gate_a_relative).read_text())
    data_ref = next(item for item in gate_a["artifacts"] if item["role"] == "data")
    gate_a_path = root / gate_a_relative
    completion_path = root / expected.inventory_completion_path
    inventory_binding = {
        "candidate": {
            "bytes": gate_a_path.stat().st_size,
            "candidate_id": expected.inventory_candidate_id,
            "path": gate_a_relative,
            "sha256": expected.inventory_candidate_sha256,
        },
        "completion": {
            "bytes": completion_path.stat().st_size,
            "completion_id": expected.inventory_completion_id,
            "path": expected.inventory_completion_path,
            "sha256": expected.inventory_completion_sha256,
        },
        "data": {
            "bytes": data_ref["bytes"],
            "path": str((Path(gate_a_relative).parent / data_ref["path"]).as_posix()),
            "row_count": data_ref["row_count"],
            "sha256": data_ref["sha256"],
        },
        "mode": "production",
    }
    basis = {
        "classification_row_digest": stable_digest(rows),
        "classification_version": "fixture_v1",
        "composite_count": len(rows),
        "inventory_binding": inventory_binding,
        "source_capture_manifest_id": stable_digest({"fixture": "capture"}),
        "source_capture_manifest_sha256": stable_digest({"fixture": "capture-bytes"}),
        "source_run_id": stable_digest({"fixture": "run"}),
    }
    candidate_id = stable_digest(basis)
    prefix = (
        "manifests/silver/identity/openfigi-market-consistency-candidates/"
        f"candidate_id={candidate_id}"
    )
    data_relative = f"{prefix}/data/classification.parquet"
    data_path = root / data_relative
    data_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), data_path, compression="zstd")
    payload = {
        "artifact_type": "s7_openfigi_market_consistency_candidate",
        "candidate_basis": basis,
        "candidate_id": candidate_id,
        "data": _receipt(root, data_relative, rows=len(rows)),
        "inventory_binding": inventory_binding,
        "source_available_session": source_available_session,
        "state": "awaiting_review",
    }
    manifest = {**payload, "manifest_id": stable_digest(payload)}
    manifest_relative = f"{prefix}/manifest.json"
    _write_compact_json(root / manifest_relative, manifest)
    return {
        "candidate_id": candidate_id,
        "candidate_path": manifest_relative,
        "candidate_sha256": sha256_file(root / manifest_relative),
        "data_bytes": data_path.stat().st_size,
        "data_path": data_relative,
        "data_row_count": len(rows),
        "data_sha256": sha256_file(data_path),
        "source_available_session": source_available_session,
    }


def _reviewed_evidence_fixture() -> tuple[list[dict[str, object]], dict[str, object]]:
    source_id = stable_digest({"reviewed": "source"})
    asset_attestation = stable_digest({"reviewed": "asset"})
    universe_attestation = stable_digest({"reviewed": "universe"})
    relation = {
        "identity_case_id": stable_digest({"reviewed": "case"}),
        "identity_case_resolution_role": "contaminated_middle_episode",
    }
    binding_payload = {
        "asset_observation_attestation_id": asset_attestation,
        "related_cases": [relation],
        "selected_source_record_id": source_id,
        "universe_membership_attestation_id": universe_attestation,
    }
    row = {
        "active_on_date": True,
        "asset_observation_artifact_path": "silver/fixture/asset.parquet",
        "asset_observation_artifact_sha256": stable_digest({"asset": "file"}),
        "asset_observation_attestation_id": asset_attestation,
        "asset_observation_attestation_json": '{"fixture":"asset-attestation"}',
        "asset_observation_full_row_digest": stable_digest({"asset": "row"}),
        "asset_observation_full_row_json": '{"fixture":"asset"}',
        "asset_observation_parquet_row_group": 0,
        "asset_observation_row_index_in_row_group": 1,
        "locale": "us",
        "market": "stocks",
        "observed_composite_figi": FOREIGN_A,
        "observed_share_class_figi": SHARE,
        "primary_exchange_mic": "XNAS",
        "provider": "massive",
        "related_case_bindings_json": json.dumps(
            binding_payload, separators=(",", ":"), sort_keys=True
        ),
        "related_case_resolution_roles": ["contaminated_middle_episode"],
        "related_identity_case_ids": [relation["identity_case_id"]],
        "selected_source_record_id": source_id,
        "session_date": date(2024, 1, 3),
        "source_available_session": "2026-07-20",
        "source_snapshot_binding_digest": stable_digest(binding_payload),
        "ticker": "ORD",
        "universe_membership_artifact_path": "silver/fixture/universe.parquet",
        "universe_membership_artifact_sha256": stable_digest({"universe": "file"}),
        "universe_membership_attestation_id": universe_attestation,
        "universe_membership_attestation_json": '{"fixture":"universe-attestation"}',
        "universe_membership_full_row_digest": stable_digest({"universe": "row"}),
        "universe_membership_full_row_json": '{"fixture":"universe"}',
        "universe_membership_parquet_row_group": 0,
        "universe_membership_row_index_in_row_group": 1,
    }
    return [row], {
        "detector_preview": {"fixture": True},
        "detector_preview_completion": {"fixture": True},
        "reviewed_case_evidence": {"fixture": True},
        "reviewed_external_evidence": {"fixture": True},
    }


def _fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, object], str]:
    calendar = build_xnys_calendar_artifact(date(2023, 1, 3), date(2026, 12, 31))
    write_xnys_calendar_artifact(tmp_path, calendar)
    monkeypatch.setattr(subject, "CALENDAR_ARTIFACT_ID", calendar.calendar_artifact_id)
    monkeypatch.setattr(subject, "CALENDAR_ARTIFACT_SHA256", calendar.sha256)
    sessions = [date(2024, 1, 2) + timedelta(days=index) for index in range(5)]
    universe_refs, _ = _write_universe_artifacts(tmp_path, sessions)
    expectations, completion_path = _write_gate_a(tmp_path, universe_refs, sessions)
    monkeypatch.setattr(subject, "PRODUCTION_EXPECTATIONS", expectations)
    monkeypatch.setattr(subject, "LONG_STANDING_MIN_SESSIONS", 4)
    monkeypatch.setattr(
        subject,
        "_runtime_provenance",
        lambda: {
            "git": {
                "commit": "a" * 40,
                "head_tree": "b" * 40,
                "index_listing_sha256": "c" * 64,
                "index_matches_head": True,
                "index_tree": "b" * 40,
                "repository_clean": True,
            },
            "runtime_files": [{"fixture": True}],
            "versions": {
                "exchange_calendars": subject.CALENDAR_ENGINE_VERSION,
                "polars": "fixture",
                "pyarrow": pa.__version__,
                "python_cache_tag": "fixture",
                "python_implementation": "CPython",
                "python_version": "fixture",
            },
        },
    )
    monkeypatch.setattr(
        subject, "_load_reviewed_case_evidence", lambda _root: _reviewed_evidence_fixture()
    )
    monkeypatch.setattr(
        subject,
        "_utc_now",
        lambda: subject.datetime(2026, 7, 20, 0, 0, tzinfo=subject.UTC),
    )
    gate_b = _write_gate_b(tmp_path)
    expected_gate_b = dict(gate_b)

    def verify_fixture_gate_b(
        data_root: Path,
        *,
        candidate_path: str,
        candidate_id: str,
        candidate_sha256: str,
        require_production_approval: bool,
    ) -> SimpleNamespace:
        if (
            Path(data_root) != tmp_path
            or require_production_approval is not True
            or candidate_path != expected_gate_b["candidate_path"]
            or candidate_id != expected_gate_b["candidate_id"]
            or candidate_sha256 != expected_gate_b["candidate_sha256"]
            or sha256_file(tmp_path / candidate_path) != expected_gate_b["candidate_sha256"]
            or sha256_file(tmp_path / str(expected_gate_b["data_path"]))
            != expected_gate_b["data_sha256"]
        ):
            raise subject.market_consistency_module.IdentityMarketConsistencyError(
                "fixture official replay differs"
            )
        return SimpleNamespace(
            candidate_id=candidate_id,
            manifest_path=candidate_path,
            data_path=expected_gate_b["data_path"],
        )

    monkeypatch.setattr(
        subject.market_consistency_module,
        "verify_market_classification_candidate",
        verify_fixture_gate_b,
        raising=False,
    )
    return gate_b, completion_path


def _caps() -> S7MarketSequenceResourceCaps:
    return S7MarketSequenceResourceCaps(
        batch_rows=2,
        disk_free_floor_bytes=1,
        max_classification_rows=100,
        max_examples=100,
        max_interval_rows=1_000,
        max_output_bytes=10 * 1024**2,
        max_tmp_bytes=10 * 1024**2,
        rss_bytes_cap=16 * 1024**3,
        wall_clock_seconds_cap=60,
        worker_count=1,
    )


def _prepare_plan(
    root: Path,
    gate_b: dict[str, object],
    *,
    prepared_by: str = "gate_c_fixture",
):
    return prepare_market_sequence_plan(
        root,
        classification_candidate_path=str(gate_b["candidate_path"]),
        classification_candidate_id=str(gate_b["candidate_id"]),
        classification_candidate_sha256=str(gate_b["candidate_sha256"]),
        classification_data_path=str(gate_b["data_path"]),
        classification_data_sha256=str(gate_b["data_sha256"]),
        classification_data_bytes=int(gate_b["data_bytes"]),
        classification_data_row_count=int(gate_b["data_row_count"]),
        classification_source_available_session=str(gate_b["source_available_session"]),
        prepared_by=prepared_by,
        resource_caps=_caps(),
    )


def _authorize(root: Path, plan):
    return authorize_market_sequence_plan_under_standing_grant(
        root,
        plan_path=plan.plan_path,
        plan_id=plan.plan_id,
        plan_sha256=plan.plan_sha256,
        request_path=plan.request_path,
        request_id=plan.request_id,
        request_sha256=plan.request_sha256,
        recorded_by="gate_c_fixture_authorizer",
    )


def _run(root: Path, plan):
    authorization = _authorize(root, plan)
    return run_source_bound_market_sequence(
        root,
        plan_path=plan.plan_path,
        plan_id=plan.plan_id,
        plan_sha256=plan.plan_sha256,
        authorization_path=authorization.authorization_path,
        authorization_id=authorization.authorization_id,
        authorization_sha256=authorization.authorization_sha256,
    )


def test_full_sequence_fixture_preserves_lineage_and_fail_closed_eligibility(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, completion_path = _fixture(tmp_path, monkeypatch)
    assert completion_path == subject.PRODUCTION_EXPECTATIONS.inventory_completion_path
    gate_b_path = tmp_path / str(gate_b["candidate_path"])
    gate_b_bytes = gate_b_path.read_bytes()
    gate_b_document, gate_b_sha256 = subject._load_compact_canonical_json_file(
        gate_b_path, "fixture Gate-B candidate"
    )
    assert gate_b_bytes == _compact_canonical(gate_b_document)
    assert not gate_b_bytes.endswith(b"\n")
    assert gate_b_sha256 == gate_b["candidate_sha256"]
    plan = _prepare_plan(tmp_path, gate_b)

    result = _run(tmp_path, plan)

    assert len(result.completion_id) == 64
    assert result.completion_sha256 == sha256_file(tmp_path / result.completion_path)
    completion = json.loads((tmp_path / result.completion_path).read_text())
    assert completion["completion_id"] == result.completion_id
    assert completion["completion_state"] == "awaiting_review"
    assert completion["candidate"]["candidate_id"] == result.candidate_id
    assert (
        completion["outputs"]
        == json.loads((tmp_path / result.manifest_path).read_text())["outputs"]
    )
    assert result.source_row_count == 25
    assert result.us_locale_non_us_composite_figi_rows == 10
    assert result.unresolved_rows == 5
    assert result.long_standing_foreign_rows == 5
    assert result.reviewed_foreign_row_count == 1
    assert result.reviewed_case_count == subject.PREVIEW_CASE_COUNT
    reviewed = pq.read_table(tmp_path / result.reviewed_evidence_path).to_pylist()
    assert len(reviewed) == 1
    assert reviewed[0]["selected_source_record_id"]
    assert reviewed[0]["related_identity_case_ids"]
    assert json.loads(reviewed[0]["asset_observation_attestation_json"])
    assert json.loads(reviewed[0]["universe_membership_attestation_json"])
    manifest = json.loads((tmp_path / result.manifest_path).read_text())
    assert manifest["outputs"]["reviewed_foreign_source_evidence"]["row_count"] == 1
    assert (
        manifest["registry_loader_source_refs"]
        == manifest["candidate_basis"]["registry_loader_source_refs"]
    )
    interval_rows = pq.read_table(tmp_path / result.interval_data_path).to_pylist()
    assert interval_rows
    assert all(row["membership_preserved"] for row in interval_rows)
    assert not any(row["identity_quality_inactive_inferred"] for row in interval_rows)
    assert not any(row["liquidation_signal"] for row in interval_rows)
    assert {row["transition_disposition"] for row in interval_rows} == {
        "not_evaluated_no_transition_adjudication"
    }
    assert all(len(row["source_record_lineage_digest"]) == 64 for row in interval_rows)

    legal = [row for row in interval_rows if row["ticker"] == "LEGAL"]
    assert len(legal) == 1
    assert legal[0]["market_classification"] == "known_non_us"
    assert legal[0]["foreign_market_identity_legal"] is True
    assert legal[0]["proposed_backtest_identity_eligible"] is False
    flagged = [
        row
        for row in interval_rows
        if row["market_classification"] in {"known_non_us", "unresolved"}
    ]
    assert flagged
    assert not any(row["proposed_backtest_identity_eligible"] for row in flagged)
    qa = json.loads((tmp_path / result.qa_path).read_text())
    by_id = {item["check_id"]: item for item in qa["results"]}
    assert by_id["unapproved_cross_market_composite_eligible_rows"]["numerator"] == 0
    assert by_id["inverse_bounce_misclassified_as_genuine_transition_rows"] == {
        "check_id": "inverse_bounce_misclassified_as_genuine_transition_rows",
        "denominator": 1,
        "numerator": 0,
        "severity": "critical",
        "status": "passed",
    }
    assert qa["diagnostics"]["ordinary_bounce_detected_case_count"] == 1
    assert by_id["long_standing_non_us_composite_figi_rows"]["numerator"] == 5
    assert qa["critical_failure_count"] == 0
    examples = json.loads((tmp_path / result.examples_path).read_text())
    assert len(examples["examples"]) <= 100
    daily = pq.read_table(tmp_path / result.daily_reason_counts_path).to_pylist()
    assert sum(row["row_count"] for row in daily) == (
        result.us_locale_non_us_composite_figi_rows + result.unresolved_rows
    )


def test_idempotent_replay_revalidates_exact_inputs_and_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, completion_path = _fixture(tmp_path, monkeypatch)
    assert completion_path
    plan = _prepare_plan(tmp_path, gate_b)
    first = _run(tmp_path, plan)
    repeated = _run(tmp_path, plan)
    assert first.candidate_id == repeated.candidate_id
    assert repeated.idempotent is True

    output = tmp_path / first.qa_path
    output.chmod(0o644)
    output.write_bytes(b"{}\n")
    with pytest.raises(IdentityMarketSequenceError, match="artifact receipt differs"):
        _run(tmp_path, plan)


def test_tampered_gate_b_data_fails_before_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, completion_path = _fixture(tmp_path, monkeypatch)
    assert completion_path
    plan = _prepare_plan(tmp_path, gate_b)
    data = tmp_path / str(gate_b["data_path"])
    data.write_bytes(data.read_bytes() + b"tamper")

    with pytest.raises(
        IdentityMarketSequenceError,
        match=r"official candidate replay verification failed|artifact receipt differs",
    ):
        _run(tmp_path, plan)


def test_tampered_or_symlinked_universe_artifact_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, completion_path = _fixture(tmp_path, monkeypatch)
    assert completion_path
    plan = _prepare_plan(tmp_path, gate_b)
    source = next(tmp_path.glob("silver/releases/universe_source_daily/**/*.parquet"))
    original = source.with_suffix(".original")
    source.rename(original)
    source.symlink_to(original)

    with pytest.raises(IdentityMarketSequenceError, match=r"symlink|unsafe"):
        _run(tmp_path, plan)


def test_missing_gate_b_classification_is_critical_and_produces_no_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, completion_path = _fixture(tmp_path, monkeypatch)
    assert completion_path
    table = pq.read_table(tmp_path / str(gate_b["data_path"]))
    filtered = table.filter(pa.compute.not_equal(table["composite_figi"], US_A))
    pq.write_table(filtered, tmp_path / str(gate_b["data_path"]), compression="zstd")
    # Re-sign the explicitly bound Gate-B candidate so this exercises reference
    # completeness, not a byte-level tamper failure.
    gate_b["data_bytes"] = (tmp_path / str(gate_b["data_path"])).stat().st_size
    gate_b["data_sha256"] = sha256_file(tmp_path / str(gate_b["data_path"]))
    gate_b["data_row_count"] = filtered.num_rows
    manifest_path = tmp_path / str(gate_b["candidate_path"])
    manifest = json.loads(manifest_path.read_text())
    manifest["data"] = {
        "bytes": gate_b["data_bytes"],
        "path": gate_b["data_path"],
        "row_count": gate_b["data_row_count"],
        "sha256": gate_b["data_sha256"],
    }
    manifest.pop("manifest_id")
    manifest["manifest_id"] = stable_digest(manifest)
    _write_compact_json(manifest_path, manifest)
    gate_b["candidate_sha256"] = sha256_file(manifest_path)
    monkeypatch.setattr(
        subject.market_consistency_module,
        "verify_market_classification_candidate",
        lambda *_args, **_kwargs: SimpleNamespace(
            candidate_id=gate_b["candidate_id"],
            manifest_path=gate_b["candidate_path"],
            data_path=gate_b["data_path"],
        ),
    )
    plan = _prepare_plan(tmp_path, gate_b)

    with pytest.raises(IdentityMarketSequenceError, match="unattempted_rows"):
        _run(tmp_path, plan)
    assert not list(
        tmp_path.glob("manifests/silver/identity/full-market-sequence-candidates/candidate_id=*")
    )


def test_resigned_all_us_gate_b_candidate_fails_official_replay_before_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    original_manifest = json.loads((tmp_path / str(gate_b["candidate_path"])).read_text())
    rows = pq.read_table(tmp_path / str(gate_b["data_path"])).to_pylist()
    for row in rows:
        row["classification"] = "us_composite"
        row["market_codes"] = ["US"]

    basis = dict(original_manifest["candidate_basis"])
    basis["classification_row_digest"] = stable_digest(rows)
    candidate_id = stable_digest(basis)
    prefix = (
        "manifests/silver/identity/openfigi-market-consistency-candidates/"
        f"candidate_id={candidate_id}"
    )
    data_relative = f"{prefix}/data/classification.parquet"
    data_path = tmp_path / data_relative
    data_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), data_path, compression="zstd")
    payload = {
        **original_manifest,
        "candidate_basis": basis,
        "candidate_id": candidate_id,
        "data": _receipt(tmp_path, data_relative, rows=len(rows)),
    }
    payload.pop("manifest_id", None)
    manifest = {**payload, "manifest_id": stable_digest(payload)}
    manifest_relative = f"{prefix}/manifest.json"
    _write_compact_json(tmp_path / manifest_relative, manifest)
    forged = {
        **gate_b,
        "candidate_id": candidate_id,
        "candidate_path": manifest_relative,
        "candidate_sha256": sha256_file(tmp_path / manifest_relative),
        "data_bytes": data_path.stat().st_size,
        "data_path": data_relative,
        "data_row_count": len(rows),
        "data_sha256": sha256_file(data_path),
    }
    plan = _prepare_plan(tmp_path, forged)
    monkeypatch.setattr(
        subject,
        "_load_classifications",
        lambda *_args, **_kwargs: pytest.fail(
            "forged Gate-B DATA reached the Gate-C classification loader"
        ),
    )
    monkeypatch.setattr(
        subject,
        "_execute_scan",
        lambda *_args, **_kwargs: pytest.fail("forged Gate-B DATA reached the Gate-C scan"),
    )

    with pytest.raises(
        IdentityMarketSequenceError,
        match="Gate-B official candidate replay verification failed",
    ):
        _run(tmp_path, plan)
    assert not list(
        tmp_path.glob("manifests/silver/identity/full-market-sequence-candidates/candidate_id=*")
    )


@pytest.mark.parametrize("label", ["known_us", "known_non_us"])
def test_gate_c_rejects_derived_labels_as_gate_b_source_states(label: str) -> None:
    with pytest.raises(IdentityMarketSequenceError, match="unsupported Gate-B classification"):
        subject._normalize_classification(label)


def test_gate_b_compact_canonical_loader_accepts_exact_producer_dialect(
    tmp_path: Path,
) -> None:
    document = {"a": [1, True, None], "nested": {"z": "值"}}
    content = _compact_canonical(document)
    path = tmp_path / "gate-b-manifest.json"
    path.write_bytes(content)

    loaded, loaded_sha256 = subject._load_compact_canonical_json_file(
        path, "fixture Gate-B candidate"
    )

    assert loaded == document
    assert loaded_sha256 == sha256_file(path)
    assert path.read_bytes() == content
    assert not content.endswith(b"\n")


@pytest.mark.parametrize(
    ("variant", "expected_error"),
    [
        ("newline", "not compact canonical JSON"),
        ("pretty", "not compact canonical JSON"),
        ("duplicate", "duplicate JSON keys"),
        ("tamper", "not valid JSON"),
    ],
)
def test_gate_b_compact_canonical_loader_rejects_other_json_dialects_and_tamper(
    tmp_path: Path,
    variant: str,
    expected_error: str,
) -> None:
    document = {"a": 1, "nested": {"z": "value"}}
    exact = _compact_canonical(document)
    variants = {
        "newline": exact + b"\n",
        "pretty": json.dumps(document, indent=2, sort_keys=True).encode(),
        "duplicate": b'{"a":1,"a":2}',
        "tamper": exact + b"#",
    }
    path = tmp_path / f"gate-b-{variant}.json"
    path.write_bytes(variants[variant])

    with pytest.raises(IdentityMarketSequenceError, match=expected_error):
        subject._load_compact_canonical_json_file(path, "fixture Gate-B candidate")


def test_plan_freezes_exact_authorization_before_any_parquet_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        subject.pq,
        "ParquetFile",
        lambda *_args, **_kwargs: pytest.fail("plan preparation must not read Parquet"),
    )

    plan = _prepare_plan(tmp_path, gate_b)

    document = json.loads((tmp_path / plan.plan_path).read_text())
    assert document["authorization_requirement"] == {
        "authorized_action": subject.AUTHORIZED_ACTION,
        "accepted_modes": ["current_standing_receipt", "exact_plan_literal"],
        "execution_without_plan_bound_receipt": False,
    }
    assert document["capabilities"] == subject._fail_closed_capabilities()
    assert document["state"] == "draft_awaiting_authorization"
    request = json.loads((tmp_path / plan.request_path).read_text())
    assert request["plan"] == {
        "path": plan.plan_path,
        "plan_id": plan.plan_id,
        "sha256": plan.plan_sha256,
    }
    assert request["request_id"] == plan.request_id


def test_fixed_preparation_slot_recovers_missing_plan_and_request_without_fork(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    first = _prepare_plan(tmp_path, gate_b)
    first_plan = json.loads((tmp_path / first.plan_path).read_text())
    preparation_path = tmp_path / first_plan["preparation_scope"]["path"]
    preparation_before = preparation_path.read_bytes()
    (tmp_path / first.plan_path).unlink()
    (tmp_path / first.request_path).unlink()
    monkeypatch.setattr(
        subject,
        "_utc_now",
        lambda: subject.datetime(2026, 7, 21, 0, 0, tzinfo=subject.UTC),
    )

    recovered = _prepare_plan(tmp_path, gate_b)

    assert recovered.plan_id == first.plan_id
    assert recovered.plan_sha256 == first.plan_sha256
    assert recovered.request_id == first.request_id
    assert recovered.request_sha256 == first.request_sha256
    assert recovered.intent_captured_at_utc == first.intent_captured_at_utc
    assert recovered.idempotent is True
    assert preparation_path.read_bytes() == preparation_before
    assert (tmp_path / first.plan_path).is_file()
    assert (tmp_path / first.request_path).is_file()
    assert (
        len(
            list(
                tmp_path.glob(
                    "manifests/silver/identity/full-market-sequence-preparations/**/manifest.json"
                )
            )
        )
        == 1
    )


def test_fixed_preparation_slot_rejects_actor_fork(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    first = _prepare_plan(tmp_path, gate_b)

    with pytest.raises(IdentityMarketSequenceError, match="actor or scope differs"):
        _prepare_plan(tmp_path, gate_b, prepared_by="different_actor")

    assert (
        len(
            list(
                tmp_path.glob(
                    "manifests/silver/identity/full-market-sequence-preparations/**/manifest.json"
                )
            )
        )
        == 1
    )
    assert (tmp_path / first.plan_path).is_file()


@pytest.mark.parametrize("field", ["grant", "reaffirmation"])
def test_standing_authorization_string_drift_fails_before_parquet_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    authorization = _authorize(tmp_path, plan)
    original_path = tmp_path / authorization.authorization_path
    document = json.loads(original_path.read_text())
    document["authorization_basis"][field] += " drift"
    document.pop("authorization_id")
    authorization_id = stable_digest(document)
    document["authorization_id"] = authorization_id
    relative = (
        "manifests/silver/identity/full-market-sequence-authorizations/"
        f"authorization_id={authorization_id}/manifest.json"
    )
    _write_json(tmp_path / relative, document)
    monkeypatch.setattr(
        subject.pq,
        "ParquetFile",
        lambda *_args, **_kwargs: pytest.fail("authorization drift must fail before Parquet"),
    )

    with pytest.raises(IdentityMarketSequenceError, match="standing authorization basis"):
        run_source_bound_market_sequence(
            tmp_path,
            plan_path=plan.plan_path,
            plan_id=plan.plan_id,
            plan_sha256=plan.plan_sha256,
            authorization_path=relative,
            authorization_id=authorization_id,
            authorization_sha256=sha256_file(tmp_path / relative),
        )


def test_standing_authorization_uses_one_fixed_retry_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    first = _authorize(tmp_path, plan)
    monkeypatch.setattr(
        subject,
        "_utc_now",
        lambda: subject.datetime(2026, 7, 21, 0, 0, tzinfo=subject.UTC),
    )
    second = _authorize(tmp_path, plan)
    assert second.authorization_path == first.authorization_path
    assert second.authorization_id == first.authorization_id
    assert second.authorization_sha256 == first.authorization_sha256
    assert second.recorded_at_utc == first.recorded_at_utc
    assert second.idempotent is True
    slots = list(
        tmp_path.glob(
            "manifests/silver/identity/full-market-sequence-authorizations/standing/**/manifest.json"
        )
    )
    assert len(slots) == 1


def test_plan_intent_and_standing_receipt_exist_before_first_parquet_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    authorization = _authorize(tmp_path, plan)
    original_parquet_file = subject.pq.ParquetFile
    original_sha256_file = subject.sha256_file
    parquet_accesses = 0

    def require_controls() -> None:
        assert (tmp_path / plan.plan_path).is_file()
        assert (tmp_path / plan.request_path).is_file()
        assert (tmp_path / authorization.authorization_path).is_file()

    def checked_parquet_file(*args, **kwargs):
        nonlocal parquet_accesses
        require_controls()
        parquet_accesses += 1
        return original_parquet_file(*args, **kwargs)

    def checked_sha256_file(path):
        if Path(path).suffix == ".parquet":
            require_controls()
        return original_sha256_file(path)

    monkeypatch.setattr(subject.pq, "ParquetFile", checked_parquet_file)
    monkeypatch.setattr(subject, "sha256_file", checked_sha256_file)
    result = run_source_bound_market_sequence(
        tmp_path,
        plan_path=plan.plan_path,
        plan_id=plan.plan_id,
        plan_sha256=plan.plan_sha256,
        authorization_path=authorization.authorization_path,
        authorization_id=authorization.authorization_id,
        authorization_sha256=authorization.authorization_sha256,
    )
    assert result.source_row_count == 25
    assert parquet_accesses > 0


def test_run_wide_wall_clock_cap_starts_before_input_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    authorization = _authorize(tmp_path, plan)
    original = subject._load_and_verify_inputs
    observed_monitor = None

    def expired_before_input(*args, **kwargs):
        nonlocal observed_monitor
        observed_monitor = kwargs["monitor"]
        observed_monitor.started -= _caps().wall_clock_seconds_cap + 1
        return original(*args, **kwargs)

    monkeypatch.setattr(subject, "_load_and_verify_inputs", expired_before_input)
    with pytest.raises(IdentityMarketSequenceError, match="resource_cap_exceeded: wall clock"):
        run_source_bound_market_sequence(
            tmp_path,
            plan_path=plan.plan_path,
            plan_id=plan.plan_id,
            plan_sha256=plan.plan_sha256,
            authorization_path=authorization.authorization_path,
            authorization_id=authorization.authorization_id,
            authorization_sha256=authorization.authorization_sha256,
        )
    assert observed_monitor is not None
    assert not list(tmp_path.glob("tmp/silver-s7-market-sequence/*.staging"))


def test_plan_or_runtime_provenance_drift_fails_before_parquet_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    authorization = _authorize(tmp_path, plan)
    planned = json.loads((tmp_path / plan.plan_path).read_text())["runtime_provenance"]
    drifted = json.loads(json.dumps(planned))
    drifted["versions"]["pyarrow"] = "drifted"
    monkeypatch.setattr(subject, "_runtime_provenance", lambda: drifted)
    monkeypatch.setattr(
        subject.pq,
        "ParquetFile",
        lambda *_args, **_kwargs: pytest.fail("runtime drift must fail before Parquet"),
    )

    with pytest.raises(IdentityMarketSequenceError, match="runtime provenance differs"):
        run_source_bound_market_sequence(
            tmp_path,
            plan_path=plan.plan_path,
            plan_id=plan.plan_id,
            plan_sha256=plan.plan_sha256,
            authorization_path=authorization.authorization_path,
            authorization_id=authorization.authorization_id,
            authorization_sha256=authorization.authorization_sha256,
        )


@pytest.mark.parametrize("target", ["plan", "request"])
def test_plan_or_request_byte_drift_fails_before_parquet_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    authorization = _authorize(tmp_path, plan)
    path = tmp_path / (plan.plan_path if target == "plan" else plan.request_path)
    path.chmod(0o644)
    path.write_bytes(path.read_bytes() + b" ")
    monkeypatch.setattr(
        subject.pq,
        "ParquetFile",
        lambda *_args, **_kwargs: pytest.fail("control drift must fail before Parquet"),
    )
    with pytest.raises(IdentityMarketSequenceError, match=r"not canonical|exact binding"):
        run_source_bound_market_sequence(
            tmp_path,
            plan_path=plan.plan_path,
            plan_id=plan.plan_id,
            plan_sha256=plan.plan_sha256,
            authorization_path=authorization.authorization_path,
            authorization_id=authorization.authorization_id,
            authorization_sha256=authorization.authorization_sha256,
        )


def test_runtime_path_set_includes_cli_and_every_identity_dependency() -> None:
    names = {path.name for path in subject._runtime_paths()}
    assert names.issuperset(
        {
            "artifacts.py",
            "calendar_artifact.py",
            "identity_market_consistency.py",
            "identity_market_inventory_plan.py",
            "identity_market_sequence.py",
            "identity_preview_runner.py",
            "identity_provider_evidence.py",
            "identity_source.py",
            "silver_identity_market_sequence.py",
        }
    )


def test_runtime_provenance_rejects_dirty_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subject, "_git", lambda repository, *_args: str(repository))
    monkeypatch.setattr(
        subject,
        "_git_bytes",
        lambda _repository, *arguments: (
            b" M runtime.py\0" if arguments and arguments[0] == "status" else b""
        ),
    )
    with pytest.raises(IdentityMarketSequenceError, match="repository is not clean"):
        subject._runtime_provenance()


def test_interrupted_staging_requires_forensic_review_before_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)

    def interrupted(*_args, **_kwargs):
        raise IdentityMarketSequenceError("simulated interruption")

    monkeypatch.setattr(subject, "_execute_scan", interrupted)
    with pytest.raises(IdentityMarketSequenceError, match="simulated interruption"):
        _run(tmp_path, plan)
    staging = list(tmp_path.glob("tmp/silver-s7-market-sequence/candidate_id=*.staging"))
    assert len(staging) == 1
    with pytest.raises(IdentityMarketSequenceError, match="prior incomplete Gate-C staging"):
        _run(tmp_path, plan)


def test_existing_candidate_replay_rechecks_source_parquet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    first = _run(tmp_path, plan)
    assert first.idempotent is False
    (tmp_path / first.completion_path).unlink()
    source = next(tmp_path.glob("silver/releases/universe_source_daily/**/*.parquet"))
    source.write_bytes(source.read_bytes() + b"tamper-after-first-run")

    with pytest.raises(IdentityMarketSequenceError, match="receipt differs"):
        _run(tmp_path, plan)


def test_resigned_candidate_capabilities_or_availability_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    first = _run(tmp_path, plan)
    (tmp_path / first.completion_path).unlink()
    manifest_path = tmp_path / first.manifest_path
    manifest_path.chmod(0o644)
    manifest = json.loads(manifest_path.read_text())
    manifest["capabilities"]["identity_registry_authorized"] = True
    manifest.pop("manifest_id")
    manifest["manifest_id"] = stable_digest(manifest)
    _write_json(manifest_path, manifest)
    with pytest.raises(IdentityMarketSequenceError, match="candidate identity differs"):
        _run(tmp_path, plan)


def test_resigned_candidate_backdated_availability_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    first = _run(tmp_path, plan)
    (tmp_path / first.completion_path).unlink()
    manifest_path = tmp_path / first.manifest_path
    manifest_path.chmod(0o644)
    manifest = json.loads(manifest_path.read_text())
    manifest["availability"]["execution_completed_at_utc"] = "2026-07-19T23:59:59+00:00"
    manifest.pop("manifest_id")
    manifest["manifest_id"] = stable_digest(manifest)
    _write_json(manifest_path, manifest)
    with pytest.raises(IdentityMarketSequenceError, match="predates durable intent"):
        _run(tmp_path, plan)


def test_resigned_candidate_between_intent_and_authorization_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    monkeypatch.setattr(
        subject,
        "_utc_now",
        lambda: subject.datetime(2026, 7, 20, 0, 5, tzinfo=subject.UTC),
    )
    authorization = _authorize(tmp_path, plan)
    monkeypatch.setattr(
        subject,
        "_utc_now",
        lambda: subject.datetime(2026, 7, 20, 0, 6, tzinfo=subject.UTC),
    )
    first = run_source_bound_market_sequence(
        tmp_path,
        plan_path=plan.plan_path,
        plan_id=plan.plan_id,
        plan_sha256=plan.plan_sha256,
        authorization_path=authorization.authorization_path,
        authorization_id=authorization.authorization_id,
        authorization_sha256=authorization.authorization_sha256,
    )
    (tmp_path / first.completion_path).unlink()
    manifest_path = tmp_path / first.manifest_path
    manifest_path.chmod(0o644)
    manifest = json.loads(manifest_path.read_text())
    manifest["availability"]["execution_completed_at_utc"] = "2026-07-20T00:04:00+00:00"
    manifest.pop("manifest_id")
    manifest["manifest_id"] = stable_digest(manifest)
    _write_json(manifest_path, manifest)

    with pytest.raises(
        IdentityMarketSequenceError,
        match="candidate completion predates Gate-C authorization",
    ):
        run_source_bound_market_sequence(
            tmp_path,
            plan_path=plan.plan_path,
            plan_id=plan.plan_id,
            plan_sha256=plan.plan_sha256,
            authorization_path=authorization.authorization_path,
            authorization_id=authorization.authorization_id,
            authorization_sha256=authorization.authorization_sha256,
        )


def test_resigned_existing_output_is_rejected_by_full_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    first = _run(tmp_path, plan)
    (tmp_path / first.completion_path).unlink()
    qa_path = tmp_path / first.qa_path
    qa_path.chmod(0o644)
    _write_json(qa_path, {"resigned_tamper": True})
    manifest_path = tmp_path / first.manifest_path
    manifest_path.chmod(0o644)
    manifest = json.loads(manifest_path.read_text())
    manifest["outputs"]["qa"] = _receipt(tmp_path, first.qa_path)
    manifest.pop("manifest_id")
    manifest["manifest_id"] = stable_digest(manifest)
    _write_json(manifest_path, manifest)

    with pytest.raises(
        IdentityMarketSequenceError,
        match=r"candidate resource measurements|full replay output receipts differ",
    ):
        _run(tmp_path, plan)


def test_missing_completion_requires_exact_full_replay_before_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    first = _run(tmp_path, plan)
    completion_path = tmp_path / first.completion_path
    completion_bytes = completion_path.read_bytes()
    completion_path.unlink()
    replay_calls = 0
    original_replay = subject._replay_and_compare_existing

    def counted_replay(*args, **kwargs):
        nonlocal replay_calls
        replay_calls += 1
        return original_replay(*args, **kwargs)

    monkeypatch.setattr(subject, "_replay_and_compare_existing", counted_replay)

    recovered = _run(tmp_path, plan)

    assert replay_calls == 1
    assert recovered.candidate_id == first.candidate_id
    assert recovered.completion_id == first.completion_id
    assert recovered.completion_sha256 == first.completion_sha256
    assert recovered.idempotent is True
    assert completion_path.read_bytes() == completion_bytes


def test_existing_completion_skips_source_inputs_and_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    first = _run(tmp_path, plan)

    def forbidden(*_args, **_kwargs):
        pytest.fail("completed retry must not load source inputs or execute a scan")

    monkeypatch.setattr(subject, "_load_and_verify_inputs", forbidden)
    monkeypatch.setattr(subject, "_execute_scan", forbidden)

    repeated = _run(tmp_path, plan)

    assert repeated.candidate_id == first.candidate_id
    assert repeated.completion_id == first.completion_id
    assert repeated.idempotent is True


def test_tampered_completion_fails_closed_without_source_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    first = _run(tmp_path, plan)
    completion_path = tmp_path / first.completion_path
    completion_path.chmod(0o644)
    completion_path.write_bytes(completion_path.read_bytes() + b" ")
    monkeypatch.setattr(
        subject,
        "_load_and_verify_inputs",
        lambda *_args, **_kwargs: pytest.fail("tampered completion must fail before source input"),
    )

    with pytest.raises(IdentityMarketSequenceError, match=r"canonical|completion"):
        _run(tmp_path, plan)


def test_gate_b_cannot_rebind_to_another_gate_a_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    manifest_path = tmp_path / str(gate_b["candidate_path"])
    manifest = json.loads(manifest_path.read_text())
    manifest["inventory_binding"]["candidate"]["candidate_id"] = stable_digest(
        {"different": "Gate-A inventory"}
    )
    manifest.pop("manifest_id")
    manifest["manifest_id"] = stable_digest(manifest)
    _write_compact_json(manifest_path, manifest)
    gate_b["candidate_sha256"] = sha256_file(manifest_path)
    plan = _prepare_plan(tmp_path, gate_b)

    with pytest.raises(IdentityMarketSequenceError, match="inventory binding differs"):
        _run(tmp_path, plan)


@pytest.mark.parametrize("variant", ["membership_gap", "middle_share_class"])
def test_inverse_bounce_requires_strict_continuity_and_one_share_class(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    variant: str,
) -> None:
    original = _fixture_rows

    def altered(sessions: list[date]) -> list[list[dict[str, object]]]:
        rows = original(sessions)
        if variant == "membership_gap":
            rows[1] = [row for row in rows[1] if row["ticker"] != "INV"]
        else:
            middle = next(row for row in rows[1] if row["ticker"] == "INV")
            middle["share_class_figi"] = "BBG000000102"
        return rows

    monkeypatch.setattr(
        __import__(__name__, fromlist=["_fixture_rows"]),
        "_fixture_rows",
        altered,
    )
    gate_b, _completion_path = _fixture(tmp_path, monkeypatch)
    plan = _prepare_plan(tmp_path, gate_b)
    result = _run(tmp_path, plan)
    qa = json.loads((tmp_path / result.qa_path).read_text())
    by_id = {item["check_id"]: item for item in qa["results"]}
    assert by_id["inverse_bounce_misclassified_as_genuine_transition_rows"]["denominator"] == 0
    assert qa["diagnostics"]["inverse_bounce_detected_case_count"] == 0
