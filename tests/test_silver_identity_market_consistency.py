from __future__ import annotations

import fcntl
import hashlib
import io
import json
import urllib.error
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ame_stocks_api.artifacts import sha256_file, stable_digest
from ame_stocks_api.cli.silver_identity_market_consistency import build_parser
from ame_stocks_api.silver import identity_market_consistency as market_module
from ame_stocks_api.silver.calendar_artifact import (
    build_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.identity_market_consistency import (
    S7_CONTINUING_AUTHORIZATION_SHA256,
    S7_CONTINUING_AUTHORIZATION_TEXT,
    HttpResult,
    IdentityMarketConsistencyError,
    classify_market_consistency_run,
    execute_market_consistency_run,
    materialize_market_classification_candidate,
    prepare_approved_market_consistency_run,
    prepare_market_consistency_run,
    verify_market_classification_candidate,
)


class _Clock:
    def __init__(self, value: datetime) -> None:
        self.value = value
        self.sleeps: list[float] = []

    def __call__(self) -> datetime:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += timedelta(seconds=seconds)


@pytest.fixture(scope="module")
def exact_calendar():
    artifact = build_xnys_calendar_artifact(date(2016, 7, 11), date(2026, 12, 31))
    assert artifact.calendar_artifact_id == market_module.XNYS_CALENDAR_ARTIFACT_ID
    assert artifact.sha256 == market_module.XNYS_CALENDAR_ARTIFACT_SHA256
    return artifact


def _figis(count: int) -> tuple[str, ...]:
    return tuple(f"BBG{index:09d}" for index in range(count))


def _inventory(root: Path, figis: tuple[str, ...]) -> tuple[str, str]:
    relative = "manifests/fixtures/composite-inventory.parquet"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "active_row_count": [index + 1 for index in range(len(figis))],
                "first_session": [date(2024, 1, 2)] * len(figis),
                "inactive_row_count": [0] * len(figis),
                "last_session": [date(2024, 1, 3)] * len(figis),
                "observed_composite_figi": figis,
                "source_record_lineage_digest": [
                    stable_digest({"lineage": figi}) for figi in figis
                ],
            }
        ),
        path,
    )
    return relative, sha256_file(path)


def _prepare(
    root: Path,
    figis: tuple[str, ...],
    exact_calendar,
    *,
    authenticated: bool = False,
):
    write_xnys_calendar_artifact(root, exact_calendar)
    path, sha256 = _inventory(root, figis)
    return prepare_market_consistency_run(
        root,
        inventory_data_path=path,
        inventory_data_sha256=sha256,
        inventory_candidate_id=stable_digest({"candidate": list(figis)}),
        inventory_candidate_sha256=stable_digest({"bytes": list(figis)}),
        prepared_at_utc="2026-07-19T16:00:00+00:00",
        prepared_by="test_market_consistency_preparer",
        authenticated=authenticated,
    )


def _mapping_row(
    query: str,
    *,
    market: str,
    share: str,
    figi: str | None = None,
    security_type: str = "Common Stock",
) -> dict[str, str]:
    return {
        "compositeFIGI": query,
        "exchCode": market,
        "figi": figi or query,
        "marketSector": "Equity",
        "securityDescription": "Common Stock",
        "securityType": security_type,
        "securityType2": "Common Stock",
        "shareClassFIGI": share,
    }


def _fake_runtime_binding(marker: str = "a") -> dict[str, object]:
    files = [
        {
            "bytes": index + 1,
            "git_blob": marker * 40,
            "git_mode": "100644",
            "path": path,
            "sha256": marker * 64,
        }
        for index, path in enumerate(market_module._RUNTIME_SOURCE_PATHS)
    ]
    return {
        "binding_version": "s7_gate_b_runtime_git_binding_v1",
        "exact_checkout_clean": True,
        "repository_commit": marker * 40,
        "repository_tree": marker * 40,
        "runtime_file_set_digest": stable_digest(files),
        "runtime_files": files,
    }


def _normal_post(calls: list[dict[str, object]]):
    def post(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        jobs = json.loads(body)
        calls.append({"headers": dict(headers), "jobs": jobs, "url": url})
        results = []
        for job in jobs:
            query = job["idValue"]
            ordinal = int(query.removeprefix("BBG"))
            if ordinal == 2:
                results.append({"error": "No identifier found."})
                continue
            market = "GR" if ordinal == 1 else "US"
            share = f"BBG{ordinal + 100:09d}"
            venue = f"BBG{ordinal + 500:09d}"
            results.append(
                {
                    "data": [
                        _mapping_row(query, market=market, share=share),
                        _mapping_row(
                            query,
                            market="GF" if market == "GR" else "UN",
                            share=share,
                            figi=venue,
                        ),
                    ]
                }
            )
        return HttpResult(
            status=200,
            headers={
                "Content-Type": "application/json",
                "RateLimit-Remaining": "24",
                "Set-Cookie": "must-not-be-persisted",
            },
            body=json.dumps(results, separators=(",", ":")).encode(),
        )

    return post


def _execute_complete(root: Path, prepared, *, post=None, api_key: str | None = None):
    clock = _Clock(datetime(2026, 7, 19, 16, 2, tzinfo=UTC))
    result = execute_market_consistency_run(
        root,
        run_id=prepared.run_id,
        api_key=api_key,
        http_post=post or _normal_post([]),
        sleep=clock.sleep,
        now=clock,
    )
    return result, clock


def _write_canonical_predecessor_control(
    root: Path,
    relative: str,
    document: dict[str, object],
) -> dict[str, object]:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(market_module._canonical_json(document))
    return {
        "bytes": path.stat().st_size,
        "path": relative,
        "sha256": sha256_file(path),
    }


def _install_recovery_predecessor_fixture(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, dict[str, object]]:
    """Write one fully content-addressed v2 predecessor chain to disk."""

    source_binding: dict[str, object] = {
        "composite_count": 18_421,
        "run_id": "0" * 64,
    }
    runtime = _fake_runtime_binding("c")
    runtime_digest = stable_digest(runtime)
    approval_scope: dict[str, object] = {
        "approval_slot_id": "4" * 64,
        "approval_slot_version": "s7_gate_b_offline_reclassification_slot_v2",
        "artifact_type": "s7_openfigi_market_consistency_offline_replay_approval",
        "authorized_action": market_module.OFFLINE_REPLAY_ACTION,
        "authorization": {
            "continuing_literal_text": market_module.S7_CONTINUING_AUTHORIZATION_TEXT,
            "continuing_literal_text_sha256": (market_module.S7_CONTINUING_AUTHORIZATION_SHA256),
            "reaffirmation_literal_text": market_module.S7_REAFFIRMATION_TEXT,
            "reaffirmation_literal_text_sha256": market_module.S7_REAFFIRMATION_SHA256,
        },
        "candidate_state": "awaiting_review",
        "capture_write": False,
        "classification_version": market_module.MARKET_CLASSIFICATION_VERSION,
        "classifier_algorithm_digest": market_module.CLASSIFIER_ALGORITHM_DIGEST,
        "classifier_qa_digest": market_module.CLASSIFIER_QA_DIGEST,
        "false_capabilities": dict(market_module._FALSE_CAPABILITIES),
        "network_access": False,
        "recovery_predecessor": {"disposition": "fixture_v1_blocked_by_false_positive"},
        "source_capture_binding": source_binding,
        "source_mutation": False,
        "transform_runtime_binding": runtime,
        "transform_runtime_binding_digest": runtime_digest,
    }
    approval_id = stable_digest(approval_scope)
    replay_id = stable_digest(
        {
            "approval_id": approval_id,
            "classification_version": market_module.MARKET_CLASSIFICATION_VERSION,
            "source_capture_binding_digest": stable_digest(source_binding),
            "transform_runtime_binding_digest": runtime_digest,
        }
    )
    approved_at = "2026-07-20T02:37:20+00:00"
    approval = {
        **approval_scope,
        "approval_availability": {"source_available_session": "2026-07-20"},
        "approval_id": approval_id,
        "approved_at_utc": approved_at,
        "approved_by": "fixture_approver",
        "prepared_by": "fixture_preparer",
        "replay_id": replay_id,
    }
    approval_file = _write_canonical_predecessor_control(
        root,
        "manifests/fixtures/gate-b-v2-predecessor/approval.json",
        approval,
    )
    approval_receipt = {**approval_file, "approval_id": approval_id}

    intent_payload: dict[str, object] = {
        "approval": approval_receipt,
        "approval_id": approval_id,
        "artifact_type": "s7_openfigi_market_consistency_offline_replay_intent",
        "capabilities": dict(market_module._FALSE_CAPABILITIES),
        "capture_write": False,
        "created_at_utc": approved_at,
        "created_by": "fixture_preparer",
        "network_access": False,
        "replay_id": replay_id,
        "source_capture_binding": source_binding,
        "source_mutation": False,
        "state": "running",
        "transform_runtime_binding_digest": runtime_digest,
    }
    intent_id = stable_digest(intent_payload)
    intent = {**intent_payload, "intent_id": intent_id}
    intent_file = _write_canonical_predecessor_control(
        root,
        "manifests/fixtures/gate-b-v2-predecessor/intent.json",
        intent,
    )

    candidate_basis = {
        "replay_id": replay_id,
        "source_capture_binding_digest": stable_digest(source_binding),
        "transform_runtime_binding_digest": runtime_digest,
    }
    candidate_id = stable_digest(candidate_basis)
    qa: dict[str, object] = {
        "artifact_type": "s7_openfigi_market_consistency_qa",
        "candidate_id": candidate_id,
        "critical_failure_count": 0,
        "results": [
            {
                "check_id": "resolved_composite_hierarchy_invalid_rows",
                "numerator": 0,
                "severity": "critical",
                "status": "passed",
            },
            {
                "check_id": "approved_relationship_seed_drift",
                "numerator": 0,
                "severity": "high",
                "status": "passed",
            },
            {
                "check_id": "exact_group_openfigi_seed_drift",
                "numerator": 0,
                "severity": "high",
                "status": "passed",
            },
            {
                "check_id": "unresolved_unique_self_missing_share_class_rows",
                "composite_count": 867,
                "numerator": 923_408,
                "severity": "high",
                "status": "warning",
            },
        ],
    }
    qa_receipt = _write_canonical_predecessor_control(
        root,
        "manifests/fixtures/gate-b-v2-predecessor/qa.json",
        qa,
    )
    candidate_payload: dict[str, object] = {
        "artifact_type": "s7_openfigi_market_consistency_candidate",
        "candidate_basis": candidate_basis,
        "candidate_id": candidate_id,
        "offline_replay_approval": approval_receipt,
        "qa": qa_receipt,
        "replay_id": replay_id,
        "state": "awaiting_review",
    }
    candidate = {
        **candidate_payload,
        "manifest_id": stable_digest(candidate_payload),
    }
    candidate_file = _write_canonical_predecessor_control(
        root,
        "manifests/fixtures/gate-b-v2-predecessor/candidate.json",
        candidate,
    )
    candidate_receipt = {**candidate_file, "candidate_id": candidate_id}

    intent_receipt = dict(intent_file)
    completion_payload: dict[str, object] = {
        "approval": approval_receipt,
        "approval_id": approval_id,
        "artifact_type": "s7_openfigi_market_consistency_offline_replay_completion",
        "candidate": candidate_receipt,
        "candidate_qa": qa_receipt,
        "capabilities": dict(market_module._FALSE_CAPABILITIES),
        "completed_at_utc": "2026-07-20T02:37:53+00:00",
        "intent": intent_receipt,
        "network_request_count": 0,
        "replay_id": replay_id,
        "source_capture_binding": source_binding,
        "source_mutation": False,
        "state": "awaiting_review",
        "transform_runtime_binding_digest": runtime_digest,
    }
    completion = {
        **completion_payload,
        "completion_id": stable_digest(completion_payload),
    }
    completion_file = _write_canonical_predecessor_control(
        root,
        market_module._offline_replay_completion_path(replay_id),
        completion,
    )
    predecessor: dict[str, object] = {
        "approval": approval_receipt,
        "candidate": candidate_receipt,
        "candidate_qa": {
            **qa_receipt,
            "critical_failure_count": 0,
        },
        "completion": {
            **completion_file,
            "completion_id": completion["completion_id"],
        },
        "disposition": "completed_v2_candidate_rejected_by_downstream_reader",
        "intent": {**intent_file, "intent_id": intent_id},
        "replay_id": replay_id,
        "runtime_commit": runtime["repository_commit"],
    }
    monkeypatch.setattr(
        market_module,
        "_OFFLINE_REPLAY_RECOVERY_PREDECESSOR",
        predecessor,
    )
    monkeypatch.setattr(
        market_module,
        "_production_replay_source_binding",
        lambda: source_binding,
    )
    return {
        "candidate": candidate,
        "completion": completion,
        "predecessor": predecessor,
        "qa": qa,
    }


def test_offline_replay_recovery_predecessor_valid_chain_passes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_recovery_predecessor_fixture(tmp_path, monkeypatch)

    market_module._verify_offline_replay_recovery_predecessor(tmp_path)


@pytest.mark.parametrize("tamper_target", ["completion", "qa", "receipt"])
def test_offline_replay_recovery_predecessor_resigned_tamper_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper_target: str,
) -> None:
    chain = _install_recovery_predecessor_fixture(tmp_path, monkeypatch)
    predecessor = chain["predecessor"]
    candidate = chain["candidate"]
    completion = chain["completion"]
    qa = chain["qa"]

    if tamper_target == "qa":
        results = qa["results"]
        assert isinstance(results, list)
        hierarchy = next(
            item
            for item in results
            if isinstance(item, dict)
            and item.get("check_id") == "resolved_composite_hierarchy_invalid_rows"
        )
        hierarchy["numerator"] = 1
        hierarchy["status"] = "failed"
        qa["critical_failure_count"] = 1
        old_qa_ref = predecessor["candidate_qa"]
        assert isinstance(old_qa_ref, dict)
        qa_receipt = _write_canonical_predecessor_control(
            tmp_path,
            str(old_qa_ref["path"]),
            qa,
        )
        predecessor["candidate_qa"] = {
            **qa_receipt,
            "critical_failure_count": 1,
        }

        candidate["qa"] = qa_receipt
        candidate_payload = dict(candidate)
        candidate_payload.pop("manifest_id")
        candidate["manifest_id"] = stable_digest(candidate_payload)
        old_candidate_ref = predecessor["candidate"]
        assert isinstance(old_candidate_ref, dict)
        candidate_receipt = _write_canonical_predecessor_control(
            tmp_path,
            str(old_candidate_ref["path"]),
            candidate,
        )
        candidate_receipt = {
            **candidate_receipt,
            "candidate_id": candidate["candidate_id"],
        }
        predecessor["candidate"] = candidate_receipt
        completion["candidate"] = candidate_receipt
        completion["candidate_qa"] = qa_receipt
    elif tamper_target == "receipt":
        completion["intent"] = dict(completion["candidate_qa"])
    else:
        completion["network_request_count"] = 1

    completion_payload = dict(completion)
    completion_payload.pop("completion_id")
    completion["completion_id"] = stable_digest(completion_payload)
    old_completion_ref = predecessor["completion"]
    assert isinstance(old_completion_ref, dict)
    completion_receipt = _write_canonical_predecessor_control(
        tmp_path,
        str(old_completion_ref["path"]),
        completion,
    )
    predecessor["completion"] = {
        **completion_receipt,
        "completion_id": completion["completion_id"],
    }
    monkeypatch.setattr(
        market_module,
        "_OFFLINE_REPLAY_RECOVERY_PREDECESSOR",
        predecessor,
    )

    with pytest.raises(
        IdentityMarketConsistencyError,
        match="offline replay recovery predecessor controls differ",
    ):
        market_module._verify_offline_replay_recovery_predecessor(tmp_path)


def _install_offline_replay_fixture(
    root: Path,
    exact_calendar,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[object, object, dict[str, object], list[Path]]:
    """Bind the fixed replay controls to one local capture without production data."""

    prepared = _prepare(root, _figis(3), exact_calendar)
    completed, _clock = _execute_complete(root, prepared)
    assert completed.final_manifest_path is not None
    request_path = root / prepared.request_manifest_path
    final_path = root / completed.final_manifest_path
    runtime = _fake_runtime_binding()
    source_binding = {
        "batch_count": completed.batch_count,
        "composite_count": completed.composite_count,
        "final_manifest": {
            "bytes": final_path.stat().st_size,
            "path": completed.final_manifest_path,
            "sha256": sha256_file(final_path),
        },
        "request_manifest": {
            "bytes": request_path.stat().st_size,
            "path": prepared.request_manifest_path,
            "sha256": sha256_file(request_path),
        },
        "source_run_id": prepared.run_id,
    }
    frozen_reads: list[Path] = []

    def verify_frozen_capture(actual_root: Path):
        frozen_reads.append(actual_root)
        return (
            json.loads(request_path.read_text()),
            json.loads(final_path.read_text()),
        )

    monkeypatch.setattr(market_module, "_require_canonical_production_root", lambda _root: None)
    monkeypatch.setattr(
        market_module,
        "_verify_offline_replay_recovery_predecessor",
        lambda _root: None,
        raising=False,
    )
    monkeypatch.setattr(market_module, "PRODUCTION_REPLAY_RUN_ID", prepared.run_id)
    monkeypatch.setattr(
        market_module,
        "_production_replay_source_binding",
        lambda: source_binding,
    )
    monkeypatch.setattr(market_module, "_repository_runtime_binding", lambda: runtime)
    monkeypatch.setattr(
        market_module,
        "_verify_frozen_replay_capture",
        verify_frozen_capture,
    )
    return prepared, completed, runtime, frozen_reads


def test_prepare_is_network_free_and_binds_fixed_evidence_and_calendar(
    tmp_path: Path, exact_calendar
) -> None:
    result = _prepare(tmp_path, _figis(12), exact_calendar)

    assert result.composite_count == 12
    assert result.batch_count == 2
    manifest = json.loads((tmp_path / result.request_manifest_path).read_text())
    assert manifest["run_id"] == result.run_id
    assert manifest["direct_approval"] is None
    assert manifest["runtime_binding"] is None
    assert manifest["external_evidence_binding"]["cross_market"] == {
        "manifest_id": market_module.CROSS_MARKET_EVIDENCE_ID,
        "path": market_module.CROSS_MARKET_EVIDENCE_PATH,
        "relationship_seed_count": 18,
        "relationship_seed_set_digest": manifest["external_evidence_binding"]["cross_market"][
            "relationship_seed_set_digest"
        ],
        "sha256": market_module.CROSS_MARKET_EVIDENCE_SHA256,
    }
    assert manifest["calendar_binding"]["artifact_id"] == exact_calendar.calendar_artifact_id
    assert manifest["source_capabilities"] == market_module._FALSE_CAPABILITIES

    repeated = prepare_market_consistency_run(
        tmp_path,
        inventory_data_path=manifest["inventory_binding"]["data"]["path"],
        inventory_data_sha256=manifest["inventory_binding"]["data"]["sha256"],
        inventory_candidate_id=manifest["inventory_binding"]["candidate"]["candidate_id"],
        inventory_candidate_sha256=manifest["inventory_binding"]["candidate"]["sha256"],
        prepared_at_utc=manifest["prepared_at_utc"],
        prepared_by=manifest["prepared_by"],
        authenticated=False,
    )
    assert repeated.run_id == result.run_id


def test_capture_resumes_classifies_multivenue_and_materializes_full_lineage(
    tmp_path: Path, exact_calendar
) -> None:
    prepared = _prepare(tmp_path, _figis(12), exact_calendar)
    calls: list[dict[str, object]] = []
    first_clock = _Clock(datetime(2026, 7, 19, 16, 1, tzinfo=UTC))
    first = execute_market_consistency_run(
        tmp_path,
        run_id=prepared.run_id,
        http_post=_normal_post(calls),
        sleep=first_clock.sleep,
        now=first_clock,
        max_batches=1,
    )
    assert first.completed_batch_count == 1
    assert first.final_manifest_path is None

    second_clock = _Clock(datetime(2026, 7, 19, 16, 2, tzinfo=UTC))
    completed = execute_market_consistency_run(
        tmp_path,
        run_id=prepared.run_id,
        http_post=_normal_post(calls),
        sleep=second_clock.sleep,
        now=second_clock,
    )
    assert completed.completed_batch_count == 2
    assert completed.final_manifest_path is not None
    assert len(calls) == 2

    by_figi = {
        row.composite_figi: row
        for row in classify_market_consistency_run(tmp_path, run_id=prepared.run_id)
    }
    us = by_figi["BBG000000000"]
    foreign = by_figi["BBG000000001"]
    assert us.classification == "us_composite"
    assert us.market_codes == ("US",)
    assert us.returned_exchange_codes == ("UN", "US")
    assert us.self_row_count == 1
    assert foreign.classification == "non_us_composite"
    assert foreign.market_codes == ("GR",)
    assert by_figi["BBG000000002"].classification == "unresolved_no_mapping"

    candidate = materialize_market_classification_candidate(
        tmp_path,
        run_id=prepared.run_id,
        materialized_at_utc="2026-07-19T16:03:00+00:00",
        materialized_by="test_market_classifier",
    )
    assert candidate.composite_count == 12
    assert candidate.us_composite_count == 10
    assert candidate.non_us_composite_count == 1
    assert candidate.unresolved_composite_count == 1
    data = pq.read_table(tmp_path / candidate.data_path).to_pylist()
    first_row = data[0]
    assert first_row["reference_version"] == market_module.MARKET_CLASSIFICATION_VERSION
    assert first_row["reference_build_run_id"] == prepared.run_id
    assert first_row["raw_response_attempt_path"].endswith("/attempt.json")
    assert first_row["request_started_at_utc"] <= first_row["response_received_at_utc"]
    assert first_row["source_publication_status"] == (
        "unavailable_current_snapshot_not_point_in_time"
    )
    assert first_row["source_available_session"] == "2026-07-20"
    assert first_row["inventory_candidate_id"]
    assert first_row["inventory_data_sha256"]
    assert json.loads(first_row["relation_projection_json"])[0]["exchCode"] == "US"
    verified = verify_market_classification_candidate(
        tmp_path,
        candidate_path=candidate.manifest_path,
        candidate_id=candidate.candidate_id,
        candidate_sha256=sha256_file(tmp_path / candidate.manifest_path),
        require_production_approval=False,
    )
    assert verified.candidate_id == candidate.candidate_id

    replay = materialize_market_classification_candidate(
        tmp_path,
        run_id=prepared.run_id,
        materialized_at_utc="2026-07-19T17:00:00+00:00",
        materialized_by="ignored_on_replay",
    )
    assert replay.candidate_id == candidate.candidate_id
    assert replay.idempotent is True
    assert execute_market_consistency_run(
        tmp_path,
        run_id=prepared.run_id,
        http_post=lambda *_args: pytest.fail("idempotent replay called network"),
    ).idempotent


def test_materialization_handles_values_after_polars_inference_window(
    tmp_path: Path, exact_calendar
) -> None:
    figis = _figis(203)
    prepared = _prepare(tmp_path, figis, exact_calendar)

    def post(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        del url, headers
        results: list[dict[str, object]] = []
        for job in json.loads(body):
            query = job["idValue"]
            ordinal = int(query.removeprefix("BBG"))
            if ordinal == 101:
                results.append(
                    {
                        "data": [
                            _mapping_row(
                                query,
                                market="US",
                                share="BBG000000999",
                            )
                        ]
                    }
                )
            elif ordinal == 202:
                results.append({"error": "No identifier found."})
            else:
                results.append({"data": []})
        return HttpResult(200, {}, json.dumps(results).encode())

    _execute_complete(tmp_path, prepared, post=post)
    candidate = materialize_market_classification_candidate(
        tmp_path,
        run_id=prepared.run_id,
        materialized_at_utc="2026-07-19T16:05:00+00:00",
        materialized_by="test_late_schema_values",
    )

    table = pq.read_table(tmp_path / candidate.data_path)
    rows = {row["composite_figi"]: row for row in table.to_pylist()}
    late_mapping = rows[figis[101]]
    late_error = rows[figis[202]]
    assert late_mapping["selected_figi"] == figis[101]
    assert late_mapping["selected_share_class_figi"] == "BBG000000999"
    assert late_mapping["returned_figis"] == [figis[101]]
    assert late_error["classification"] == "unresolved_no_mapping"
    assert late_error["job_error"] == "No identifier found."
    assert table.schema.field("job_error").type == pa.large_string()
    assert table.schema.field("returned_figis").type == pa.large_list(pa.large_string())

    verified = verify_market_classification_candidate(
        tmp_path,
        candidate_path=candidate.manifest_path,
        candidate_id=candidate.candidate_id,
        candidate_sha256=sha256_file(tmp_path / candidate.manifest_path),
        require_production_approval=False,
    )
    assert verified.candidate_id == candidate.candidate_id


def test_attempt_ledger_rate_limits_retries_and_ignores_partial_staging(
    tmp_path: Path, exact_calendar
) -> None:
    prepared = _prepare(tmp_path, _figis(1), exact_calendar)
    calls = 0

    def interrupted(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        nonlocal calls
        del url, body, headers
        calls += 1
        assert len(list(tmp_path.rglob("attempt_index=*/intent.json"))) == calls
        if calls == 1:
            return HttpResult(429, {"Retry-After": "0"}, b'{"error":"rate"}')
        raise RuntimeError("simulated process interruption")

    clock = _Clock(datetime(2026, 7, 19, 16, 0, tzinfo=UTC))
    with pytest.raises(RuntimeError, match="interruption"):
        execute_market_consistency_run(
            tmp_path,
            run_id=prepared.run_id,
            http_post=interrupted,
            sleep=clock.sleep,
            now=clock,
        )
    attempt0 = next(tmp_path.rglob("attempt_index=000000/attempt.json"))
    first = json.loads(attempt0.read_text())
    assert first["http_status"] == 429
    orphan_intent = next(tmp_path.rglob("attempt_index=000001/intent.json"))
    assert not (orphan_intent.parent / "attempt.json").exists()
    partial = orphan_intent.parent / ".attempt.json.tmp-crash"
    partial.write_bytes(b"partial")

    def resumed(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        del url, headers
        jobs = json.loads(body)
        response = [
            {"data": [_mapping_row(job["idValue"], market="US", share="BBG000000100")]}
            for job in jobs
        ]
        return HttpResult(200, {"Content-Type": "application/json"}, json.dumps(response).encode())

    result = execute_market_consistency_run(
        tmp_path,
        run_id=prepared.run_id,
        http_post=resumed,
        sleep=clock.sleep,
        now=clock,
    )
    assert result.final_manifest_path
    attempts = sorted(tmp_path.rglob("attempt_index=*/attempt.json"))
    intents = sorted(tmp_path.rglob("attempt_index=*/intent.json"))
    assert len(attempts) == len(intents) == 3
    second = json.loads(attempts[1].read_text())
    third = json.loads(attempts[2].read_text())
    assert second["outcome"] == "unknown_network_outcome"
    assert second["http_status"] is None
    first_received = datetime.fromisoformat(first["response_received_at_utc"])
    third_started = datetime.fromisoformat(third["request_started_at_utc"])
    assert (third_started - first_received).total_seconds() >= 6.4
    assert third["http_status"] == 200
    assert [json.loads(path.read_text())["attempt_index"] for path in intents] == [0, 1, 2]


def test_retry_after_is_durable_across_restart_and_never_clamped(
    tmp_path: Path, exact_calendar
) -> None:
    prepared = _prepare(tmp_path, _figis(1), exact_calendar)
    clock = _Clock(datetime(2026, 7, 19, 16, 0, tzinfo=UTC))
    calls = 0

    def first(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        nonlocal calls
        del url, body, headers
        calls += 1
        return HttpResult(429, {"Retry-After": "600"}, b'{"error":"rate"}')

    def interrupted_sleep(seconds: float) -> None:
        assert seconds == pytest.approx(600.0)
        raise RuntimeError("stop after durable 429")

    with pytest.raises(RuntimeError, match="durable 429"):
        execute_market_consistency_run(
            tmp_path,
            run_id=prepared.run_id,
            http_post=first,
            sleep=interrupted_sleep,
            now=clock,
        )
    first_attempt = json.loads(
        next(tmp_path.rglob("attempt_index=000000/attempt.json")).read_text()
    )
    assert (
        datetime.fromisoformat(first_attempt["retry_not_before_utc"])
        - datetime.fromisoformat(first_attempt["response_received_at_utc"])
    ).total_seconds() == 600.0

    def success(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        nonlocal calls
        del url, headers
        calls += 1
        jobs = json.loads(body)
        return HttpResult(
            200,
            {},
            json.dumps(
                [
                    {"data": [_mapping_row(job["idValue"], market="US", share="BBG000000100")]}
                    for job in jobs
                ]
            ).encode(),
        )

    result = execute_market_consistency_run(
        tmp_path,
        run_id=prepared.run_id,
        http_post=success,
        sleep=clock.sleep,
        now=clock,
    )
    assert result.final_manifest_path
    assert calls == 2
    assert 600.0 in clock.sleeps


def test_retry_after_http_date_is_supported_without_downward_clamp() -> None:
    received = datetime(2026, 7, 19, 16, 0, tzinfo=UTC)
    assert market_module._retry_after_seconds(
        {"retry-after": "Sun, 19 Jul 2026 16:10:00 GMT"}, received
    ) == pytest.approx(600.0)


def test_resigned_attempt_cannot_shorten_persisted_retry_boundary(
    tmp_path: Path, exact_calendar
) -> None:
    prepared = _prepare(tmp_path, _figis(1), exact_calendar)
    _execute_complete(tmp_path, prepared)
    request = json.loads((tmp_path / prepared.request_manifest_path).read_text())
    _, specs = market_module._rebuild_batch_specs(tmp_path, request)
    spec = specs[0]
    request_receipt = market_module._file_receipt(tmp_path, spec.request_path)
    attempt_path = tmp_path / spec.attempt_path(0)
    attempt = json.loads(attempt_path.read_text())
    attempt["retry_not_before_utc"] = attempt["response_received_at_utc"]
    payload = dict(attempt)
    payload.pop("attempt_id")
    attempt["attempt_id"] = stable_digest(payload)

    with pytest.raises(IdentityMarketConsistencyError, match="retry boundary was shortened"):
        market_module._verify_attempt(
            attempt,
            spec=spec,
            expected_index=0,
            request_receipt=request_receipt,
            run_id=prepared.run_id,
            expected_path=spec.attempt_path(0),
            root=tmp_path,
        )


def test_transport_and_5xx_attempts_are_immutable_before_success(
    tmp_path: Path, exact_calendar
) -> None:
    prepared = _prepare(tmp_path, _figis(1), exact_calendar)
    outcomes: list[str] = []

    def post(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        del url, headers
        if not outcomes:
            outcomes.append("transport")
            raise TimeoutError("timeout")
        if len(outcomes) == 1:
            outcomes.append("server")
            return HttpResult(503, {"Retry-After": "0"}, b"unavailable")
        outcomes.append("success")
        jobs = json.loads(body)
        response = [
            {"data": [_mapping_row(job["idValue"], market="US", share="BBG000000100")]}
            for job in jobs
        ]
        return HttpResult(200, {}, json.dumps(response).encode())

    clock = _Clock(datetime(2026, 7, 19, 16, 0, tzinfo=UTC))
    result = execute_market_consistency_run(
        tmp_path,
        run_id=prepared.run_id,
        http_post=post,
        sleep=clock.sleep,
        now=clock,
    )
    assert result.final_manifest_path
    attempts = [json.loads(path.read_text()) for path in sorted(tmp_path.rglob("attempt.json"))]
    assert [item["outcome"] for item in attempts] == [
        "transport_error",
        "http_response",
        "http_response",
    ]
    assert [item["http_status"] for item in attempts] == [None, 503, 200]


def test_retry_exhaustion_commits_source_unavailable_without_losing_coverage(
    tmp_path: Path, exact_calendar
) -> None:
    prepared = _prepare(tmp_path, _figis(1), exact_calendar)
    calls = 0

    def unavailable(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        nonlocal calls
        del url, body, headers
        calls += 1
        if calls % 2 == 0:
            raise TimeoutError("transient timeout")
        return HttpResult(503, {"Retry-After": "0"}, b"temporarily unavailable")

    result, _ = _execute_complete(tmp_path, prepared, post=unavailable)
    assert result.final_manifest_path is not None
    assert result.completed_batch_count == 1
    assert calls == market_module.MAX_ATTEMPTS_PER_BATCH
    final = json.loads((tmp_path / result.final_manifest_path).read_text())
    commit = final["batches"][0]
    assert commit["terminal_status"] == "source_unavailable"
    assert commit["accepted_attempt_id"] is None
    assert len(commit["attempts"]) == market_module.MAX_ATTEMPTS_PER_BATCH
    assert commit["attempt"] == commit["attempts"][-1]

    row = classify_market_consistency_run(tmp_path, run_id=prepared.run_id)[0]
    assert row.classification == "unresolved_source_unavailable"
    assert row.projection_classification == "unresolved_source_unavailable"
    assert row.projection_reason_codes == ("source_unavailable_attempts_exhausted",)
    assert row.raw_response_attempt_path == commit["attempt"]["path"]

    candidate = materialize_market_classification_candidate(
        tmp_path,
        run_id=prepared.run_id,
        materialized_at_utc="2026-07-19T17:00:00+00:00",
        materialized_by="test_market_classifier",
    )
    assert candidate.composite_count == 1
    assert candidate.unresolved_composite_count == 1
    qa = json.loads((tmp_path / candidate.qa_path).read_text())
    unattempted = next(
        item for item in qa["results"] if item["check_id"] == "reference_inventory_unattempted_rows"
    )
    assert unattempted["numerator"] == 0


def test_cumulative_response_cap_preserves_pre_send_intent_without_response(
    tmp_path: Path, exact_calendar, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(market_module, "MAX_CUMULATIVE_RESPONSE_BYTES", 16)
    prepared = _prepare(tmp_path, _figis(1), exact_calendar)

    def oversized(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        del url, body, headers
        return HttpResult(503, {}, b"x" * 17)

    with pytest.raises(IdentityMarketConsistencyError, match="cumulative-response-byte cap"):
        _execute_complete(tmp_path, prepared, post=oversized)
    assert not list(tmp_path.rglob("attempt.json"))
    intents = list(tmp_path.rglob("intent.json"))
    assert len(intents) == 1
    assert json.loads(intents[0].read_text())["attempt_index"] == 0


def test_resource_cap_tamper_fails_before_network(tmp_path: Path, exact_calendar) -> None:
    prepared = _prepare(tmp_path, _figis(1), exact_calendar)
    request_path = tmp_path / prepared.request_manifest_path
    request_path.chmod(0o644)
    document = json.loads(request_path.read_text())
    document["resource_caps"]["max_total_attempts"] += 1
    request_path.write_text(json.dumps(document, separators=(",", ":"), sort_keys=True))

    with pytest.raises(IdentityMarketConsistencyError, match="ID recomputation"):
        execute_market_consistency_run(
            tmp_path,
            run_id=prepared.run_id,
            http_post=lambda *_args: pytest.fail("cap tamper reached network"),
        )


def test_classification_states_and_unique_self_row_rule(tmp_path: Path, exact_calendar) -> None:
    figis = _figis(7)
    prepared = _prepare(tmp_path, figis, exact_calendar)

    def post(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        del url, headers
        jobs = json.loads(body)
        q = [job["idValue"] for job in jobs]
        results = [
            {
                "data": [
                    _mapping_row(q[0], market="US", share="BBG000000100"),
                    _mapping_row(
                        q[0],
                        market="UN",
                        share="BBG000000100",
                        figi="BBG000000900",
                    ),
                ]
            },
            {"data": [_mapping_row(q[1], market="GR", share="BBG000000101")]},
            {"warning": "No identifier found."},
            {"error": "Internal mapping job error."},
            {
                "data": [
                    _mapping_row(q[4], market="US", share="BBG000000104"),
                    _mapping_row(q[4], market="GR", share="BBG000000104"),
                ]
            },
            {
                "data": [
                    _mapping_row(q[5], market="US", share="BBG000000105"),
                    _mapping_row(
                        q[5],
                        market="UN",
                        share="BBG000000999",
                        figi="BBG000000905",
                    ),
                ]
            },
            {
                "data": [
                    _mapping_row(
                        q[6],
                        market="US",
                        share="BBG000000106",
                        figi="BBG000000906",
                    )
                ]
            },
        ]
        return HttpResult(200, {}, json.dumps(results).encode())

    _execute_complete(tmp_path, prepared, post=post)
    rows = classify_market_consistency_run(tmp_path, run_id=prepared.run_id)
    assert [item.classification for item in rows] == [
        "us_composite",
        "non_us_composite",
        "unresolved_no_mapping",
        "unresolved_job_error",
        "unresolved_mixed_market",
        "us_composite",
        "unresolved_invalid_projection",
    ]
    assert rows[0].market_codes == ("US",)
    assert rows[0].returned_exchange_codes == ("UN", "US")
    assert rows[5].market_codes == ("US",)
    assert rows[5].selected_share_class_figi == "BBG000000105"
    assert rows[5].relation_share_class_conflict is True
    assert rows[5].projection_reason_codes == (
        "multiple_relation_share_classes",
        "unique_exact_self_row",
    )


def test_unique_self_missing_share_class_stays_high_unresolved_not_critical(
    tmp_path: Path, exact_calendar
) -> None:
    query = "BBG000000060"
    prepared = _prepare(tmp_path, (query,), exact_calendar)

    def post(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        del url, body, headers
        self_row = _mapping_row(query, market="US", share="BBG000000160")
        self_row.pop("shareClassFIGI")
        return HttpResult(200, {}, json.dumps([{"data": [self_row]}]).encode())

    _execute_complete(tmp_path, prepared, post=post)
    classified = classify_market_consistency_run(tmp_path, run_id=prepared.run_id)
    assert len(classified) == 1
    row = classified[0]
    assert row.classification == "unresolved_invalid_projection"
    assert row.self_row_count == 1
    assert row.selected_figi == query
    assert row.selected_share_class_figi is None
    assert row.projection_reason_codes == ("self_row_missing_market_or_share_class",)

    candidate = materialize_market_classification_candidate(
        tmp_path,
        run_id=prepared.run_id,
        materialized_at_utc="2026-07-19T16:03:00+00:00",
        materialized_by="test_missing_share_class_classifier",
    )
    qa = json.loads((tmp_path / candidate.qa_path).read_text())
    checks = {item["check_id"]: item for item in qa["results"]}
    hierarchy = checks["resolved_composite_hierarchy_invalid_rows"]
    missing_share = checks["unresolved_unique_self_missing_share_class_rows"]
    unresolved = checks["openfigi_market_classification_unresolved_rows"]

    assert qa["critical_failure_count"] == 0
    assert hierarchy["numerator"] == 0
    assert hierarchy["status"] == "passed"
    assert missing_share["severity"] == "high"
    assert missing_share["composite_count"] == 1
    assert missing_share["numerator"] == 1
    assert missing_share["status"] == "warning"
    assert unresolved["severity"] == "high"
    assert unresolved["numerator"] == 1
    assert unresolved["reason_counts"] == {"unresolved_invalid_projection": 1}
    assert unresolved["status"] == "warning"


@pytest.mark.parametrize(
    ("classification", "selected_figi", "selected_share", "returned_shares"),
    [
        ("us_composite", "BBG000000071", "BBG000000170", ["BBG000000170"]),
        ("non_us_composite", "BBG000000070", "BBG000000171", ["BBG000000170"]),
    ],
)
def test_resolved_unique_self_inconsistent_hierarchy_remains_critical(
    classification: str,
    selected_figi: str,
    selected_share: str,
    returned_shares: list[str],
) -> None:
    row = {
        "classification": classification,
        "composite_figi": "BBG000000070",
        "market_codes": ["US" if classification == "us_composite" else "GR"],
        "provider_observation_row_count": 1,
        "relation_share_class_conflict": False,
        "relationship_seed_status": "not_seed",
        "returned_share_class_figis": returned_shares,
        "selected_figi": selected_figi,
        "selected_market_code": "US" if classification == "us_composite" else "GR",
        "selected_share_class_figi": selected_share,
        "self_openfigi_row_count": 1,
    }

    qa = market_module._qa_document(
        candidate_id="0" * 64,
        rows=[row],
        example_path="manifests/fixtures/examples.json",
        production=False,
    )
    checks = {item["check_id"]: item for item in qa["results"]}
    hierarchy = checks["resolved_composite_hierarchy_invalid_rows"]

    assert qa["critical_failure_count"] == 1
    assert hierarchy["severity"] == "critical"
    assert hierarchy["numerator"] == 1
    assert hierarchy["status"] == "failed"


def test_tnxp_is_the_only_frozen_no_self_relation_exception(tmp_path: Path, exact_calendar) -> None:
    query = "BBG00R4FG9L2"
    prepared = _prepare(tmp_path, (query,), exact_calendar)

    def post(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        del url, body, headers
        response = [
            {
                "data": [
                    _mapping_row(
                        query,
                        market="EP",
                        share="BBG001T49NZ9",
                        figi="BBG00R4FG9M1",
                    )
                ]
            }
        ]
        return HttpResult(200, {}, json.dumps(response).encode())

    _execute_complete(tmp_path, prepared, post=post)
    row = classify_market_consistency_run(tmp_path, run_id=prepared.run_id)[0]
    assert row.classification == "non_us_composite"
    assert row.relationship_seed_status == "matched"
    assert row.self_row_count == 0
    assert row.projection_reason_codes == ("frozen_tnxp_unique_relation_exception",)
    candidate = materialize_market_classification_candidate(
        tmp_path,
        run_id=prepared.run_id,
        materialized_at_utc="2026-07-19T16:03:00+00:00",
        materialized_by="test_tnxp_exception_classifier",
    )
    qa = json.loads((tmp_path / candidate.qa_path).read_text())
    hierarchy = {item["check_id"]: item for item in qa["results"]}[
        "resolved_composite_hierarchy_invalid_rows"
    ]
    assert qa["critical_failure_count"] == 0
    assert hierarchy["numerator"] == 0
    assert hierarchy["status"] == "passed"


def test_non_tnxp_no_self_relation_stays_unresolved(tmp_path: Path, exact_calendar) -> None:
    query = "BBG000000050"
    prepared = _prepare(tmp_path, (query,), exact_calendar)

    def post(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        del url, body, headers
        response = [
            {
                "data": [
                    _mapping_row(
                        query,
                        market="GR",
                        share="BBG000000150",
                        figi="BBG000000950",
                    )
                ]
            }
        ]
        return HttpResult(200, {}, json.dumps(response).encode())

    _execute_complete(tmp_path, prepared, post=post)
    row = classify_market_consistency_run(tmp_path, run_id=prepared.run_id)[0]
    assert row.classification == "unresolved_invalid_projection"
    assert row.projection_reason_codes == ("no_unique_exact_self_row",)


def test_authenticated_mode_secret_echo_and_persistence_are_fail_closed(
    tmp_path: Path, exact_calendar
) -> None:
    prepared = _prepare(tmp_path, _figis(1), exact_calendar, authenticated=True)
    with pytest.raises(IdentityMarketConsistencyError, match="authentication mode"):
        execute_market_consistency_run(tmp_path, run_id=prepared.run_id)

    key = "private-openfigi-key"

    def echo(url: str, body: bytes, headers: dict[str, str]) -> HttpResult:
        del url, body
        assert headers["X-OPENFIGI-APIKEY"] == key
        return HttpResult(200, {"Date": key}, key.encode())

    with pytest.raises(IdentityMarketConsistencyError, match="echoed the exact API key"):
        _execute_complete(tmp_path, prepared, post=echo, api_key=key)
    assert not list(tmp_path.rglob("attempt.json"))
    assert key.encode() not in b"".join(
        path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()
    )


def test_urllib_adapter_disables_redirects(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[object] = []

    class Opener:
        def open(self, request, timeout):
            observed.extend([request.full_url, timeout])
            raise urllib.error.HTTPError(
                request.full_url,
                302,
                "Found",
                {"Location": "https://evil.example/steal"},
                io.BytesIO(b"redirect refused"),
            )

    def build_opener(handler):
        assert isinstance(handler, market_module._NoRedirectHandler)
        return Opener()

    monkeypatch.setattr(market_module.urllib.request, "build_opener", build_opener)
    result = market_module._urllib_post(
        market_module.OPENFIGI_MAPPING_ENDPOINT,
        b"[]",
        {"X-OPENFIGI-APIKEY": "secret"},
    )
    assert result.status == 302
    assert observed == [market_module.OPENFIGI_MAPPING_ENDPOINT, 60]


def test_nonblocking_run_lock_rejects_concurrent_capture(tmp_path: Path, exact_calendar) -> None:
    prepared = _prepare(tmp_path, _figis(1), exact_calendar)
    lock = tmp_path / f"tmp/s7-openfigi-market-consistency/run_id={prepared.run_id}.lock"
    lock.parent.mkdir(parents=True)
    with lock.open("w") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(IdentityMarketConsistencyError, match="nonblocking run lock"):
            execute_market_consistency_run(tmp_path, run_id=prepared.run_id)


def test_attempt_tamper_fails_offline_replay(tmp_path: Path, exact_calendar) -> None:
    prepared = _prepare(tmp_path, _figis(1), exact_calendar)
    _execute_complete(tmp_path, prepared)
    attempt = next(tmp_path.rglob("attempt.json"))
    attempt.chmod(0o644)
    document = json.loads(attempt.read_text())
    document["response_body_base64"] = "W10="
    attempt.write_text(json.dumps(document, separators=(",", ":"), sort_keys=True))

    with pytest.raises(IdentityMarketConsistencyError, match=r"attempt ID|artifact receipt"):
        classify_market_consistency_run(tmp_path, run_id=prepared.run_id)


def test_offline_replay_rechecks_inventory_even_if_outer_chain_is_resigned(
    tmp_path: Path, exact_calendar
) -> None:
    prepared = _prepare(tmp_path, _figis(1), exact_calendar)
    result, _ = _execute_complete(tmp_path, prepared)
    assert result.final_manifest_path
    request = json.loads((tmp_path / prepared.request_manifest_path).read_text())
    inventory = tmp_path / request["inventory_binding"]["data"]["path"]
    inventory.chmod(0o644)
    raw = bytearray(inventory.read_bytes())
    raw[-1] ^= 1
    inventory.write_bytes(bytes(raw))

    # Re-signing the outer final document cannot repair the request's approved DATA pin.
    final_path = tmp_path / result.final_manifest_path
    final = json.loads(final_path.read_text())
    payload = {key: value for key, value in final.items() if key != "manifest_id"}
    final["manifest_id"] = stable_digest(payload)
    final_path.chmod(0o644)
    final_path.write_text(json.dumps(final, separators=(",", ":"), sort_keys=True))
    with pytest.raises(IdentityMarketConsistencyError, match=r"fixture inventory|inventory DATA"):
        classify_market_consistency_run(tmp_path, run_id=prepared.run_id)


def test_materialization_pit_and_candidate_replay_detect_tamper(
    tmp_path: Path, exact_calendar
) -> None:
    prepared = _prepare(tmp_path, _figis(1), exact_calendar)
    _execute_complete(tmp_path, prepared)
    with pytest.raises(IdentityMarketConsistencyError, match="cannot precede"):
        materialize_market_classification_candidate(
            tmp_path,
            run_id=prepared.run_id,
            materialized_at_utc="2026-07-19T15:59:00+00:00",
            materialized_by="test",
        )
    with pytest.raises(IdentityMarketConsistencyError, match="calendar-derived"):
        materialize_market_classification_candidate(
            tmp_path,
            run_id=prepared.run_id,
            materialized_at_utc="2026-07-19T16:03:00+00:00",
            materialized_by="test",
            source_available_session="2026-07-21",
        )
    candidate = materialize_market_classification_candidate(
        tmp_path,
        run_id=prepared.run_id,
        materialized_at_utc="2026-07-19T16:03:00+00:00",
        materialized_by="test",
    )
    data = tmp_path / candidate.data_path
    data.chmod(0o644)
    data.write_bytes(data.read_bytes() + b"tamper")
    with pytest.raises(IdentityMarketConsistencyError, match="artifact receipt"):
        materialize_market_classification_candidate(
            tmp_path,
            run_id=prepared.run_id,
            materialized_at_utc="2026-07-19T17:00:00+00:00",
            materialized_by="test",
        )


def test_official_candidate_verifier_rejects_self_consistent_resigned_all_us_data(
    tmp_path: Path, exact_calendar
) -> None:
    prepared = _prepare(tmp_path, _figis(2), exact_calendar)
    _execute_complete(tmp_path, prepared)
    candidate = materialize_market_classification_candidate(
        tmp_path,
        run_id=prepared.run_id,
        materialized_at_utc="2026-07-19T16:03:00+00:00",
        materialized_by="test_market_classifier",
    )
    original = json.loads((tmp_path / candidate.manifest_path).read_text())
    rows = pq.read_table(tmp_path / candidate.data_path).to_pylist()
    assert any(row["classification"] == "non_us_composite" for row in rows)
    for row in rows:
        row["classification"] = "us_composite"
        row["market_codes"] = ["US"]
    basis = dict(original["candidate_basis"])
    basis["classification_row_digest"] = stable_digest(rows)
    forged_id = stable_digest(basis)
    prefix = (
        f"manifests/silver/identity/openfigi-market-consistency-candidates/candidate_id={forged_id}"
    )
    data_relative = f"{prefix}/data/classification.parquet"
    data_path = tmp_path / data_relative
    data_path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), data_path, compression="zstd")
    payload = {
        **original,
        "candidate_basis": basis,
        "candidate_id": forged_id,
        "classification_counts": {"us_composite": len(rows)},
        "classification_row_counts": {
            "us_composite": sum(int(row["provider_observation_row_count"]) for row in rows)
        },
        "data": {
            "bytes": data_path.stat().st_size,
            "path": data_relative,
            "row_count": len(rows),
            "sha256": sha256_file(data_path),
        },
    }
    payload.pop("manifest_id", None)
    forged = {**payload, "manifest_id": stable_digest(payload)}
    manifest_relative = f"{prefix}/manifest.json"
    manifest_path = tmp_path / manifest_relative
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(forged, separators=(",", ":"), sort_keys=True))

    with pytest.raises(IdentityMarketConsistencyError, match="basis replay differs"):
        verify_market_classification_candidate(
            tmp_path,
            candidate_path=manifest_relative,
            candidate_id=forged_id,
            candidate_sha256=sha256_file(manifest_path),
            require_production_approval=False,
        )


def test_fixed_evidence_replays_18_relationship_keys_and_tnxp_exception() -> None:
    binding = market_module._load_external_evidence_binding()
    seeds = market_module._relationship_seeds()
    assert binding["cross_market"]["relationship_seed_count"] == 18
    assert len(seeds) == 18
    assert seeds["BBG00R4FG9L2"] == {
        "expected_composite_figi": "BBG00R4FG9L2",
        "expected_market_code": "EP",
        "expected_share_class_figi": "BBG001T49NZ9",
        "role": "foreign_composite",
        "ticker": "TNXP",
    }
    assert market_module._FROZEN_NO_SELF_RELATION_EXCEPTIONS == {
        "BBG00R4FG9L2": {
            "figi": "BBG00R4FG9M1",
            "compositeFIGI": "BBG00R4FG9L2",
            "exchCode": "EP",
            "shareClassFIGI": "BBG001T49NZ9",
        }
    }


def test_v3_recovery_slot_is_distinct_and_preserves_capture_versions() -> None:
    legacy_slot_id = "f39167969acee0a41e0069fcd6531c00b27469bd2265deef614a4e076aa03455"
    legacy_basis = {
        "approval_slot_version": "s7_gate_b_standing_approval_slot_v2",
        "authorized_actions": list(market_module._AUTHORIZED_ACTIONS),
        "continuing_authorization_sha256": S7_CONTINUING_AUTHORIZATION_SHA256,
        "production_data_root": "/mnt/HC_Volume_106309665/american_stocks",
        "reaffirmation_sha256": market_module.S7_REAFFIRMATION_SHA256,
    }
    predecessor = {
        "approval_slot_id": legacy_slot_id,
        "capture_run_id": ("c9d4ef9973878126036e0f4d5e398dd160424e09ad8a6a7e99a263c31f0d6584"),
        "disposition": (
            "capture_complete_not_consumed_due_to_candidate_frame_schema_inference_failure"
        ),
        "runtime_commit": "609ac20fe13f63e7ceb76cf738f4d6b55b78b466",
    }

    assert stable_digest(legacy_basis) == legacy_slot_id
    assert market_module.DIRECT_APPROVAL_SLOT_VERSION == (
        "s7_gate_b_standing_approval_slot_v3_schema_inference_recovery"
    )
    assert legacy_slot_id != market_module.DIRECT_APPROVAL_SLOT_ID
    assert predecessor == market_module._GATE_B_RECOVERY_PREDECESSOR
    assert market_module._DIRECT_APPROVAL_SLOT_BASIS["recovery_predecessor"] == predecessor
    assert market_module.MARKET_CONSISTENCY_RUN_VERSION == (
        "s7_openfigi_market_consistency_capture_v3"
    )
    assert market_module.MARKET_CLASSIFICATION_VERSION == (
        "s7_openfigi_composite_market_classification_v3"
    )


def test_direct_approval_receipt_is_replayable_and_literal_pinned(
    tmp_path: Path, exact_calendar, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_xnys_calendar_artifact(tmp_path, exact_calendar)
    figis = tuple(market_module._relationship_seeds())
    path, sha = _inventory(tmp_path, figis)
    inventory_path = tmp_path / path
    fixture_binding = {
        "candidate": {
            "bytes": 1,
            "candidate_id": "1" * 64,
            "path": "fixture-candidate.json",
            "sha256": "2" * 64,
        },
        "completion": {
            "bytes": 1,
            "completion_id": "3" * 64,
            "path": "fixture-completion.json",
            "sha256": "4" * 64,
        },
        "data": {
            "bytes": inventory_path.stat().st_size,
            "path": path,
            "row_count": 18,
            "sha256": sha,
        },
        "mode": "production",
    }
    monkeypatch.setattr(market_module, "PRODUCTION_DATA_ROOT", tmp_path.resolve())
    monkeypatch.setattr(
        market_module,
        "_production_inventory_manifest_binding",
        lambda _root: fixture_binding,
    )

    def verify_after_approval(root: Path) -> dict[str, object]:
        assert (root / market_module._direct_approval_path()).is_file()
        return fixture_binding

    monkeypatch.setattr(
        market_module,
        "_verify_production_inventory_binding",
        verify_after_approval,
    )
    runtime_binding = _fake_runtime_binding()
    monkeypatch.setattr(
        market_module,
        "_repository_runtime_binding",
        lambda: runtime_binding,
    )
    result = prepare_approved_market_consistency_run(
        tmp_path,
        authorization_text=S7_CONTINUING_AUTHORIZATION_TEXT,
        reaffirmation_text=market_module.S7_REAFFIRMATION_TEXT,
        approved_by="test_approver",
        prepared_by="test_preparer",
        authenticated=True,
        now=_Clock(datetime(2026, 7, 20, 2, 0, tzinfo=UTC)),
    )
    request = json.loads((tmp_path / result.request_manifest_path).read_text())
    receipt = request["direct_approval"]
    approval = json.loads((tmp_path / receipt["path"]).read_text())
    assert approval["continuing_authorization"] == {
        "literal_text": S7_CONTINUING_AUTHORIZATION_TEXT,
        "literal_text_sha256": S7_CONTINUING_AUTHORIZATION_SHA256,
    }
    assert approval["approval_reaffirmation"] == {
        "literal_text": market_module.S7_REAFFIRMATION_TEXT,
        "literal_text_sha256": market_module.S7_REAFFIRMATION_SHA256,
    }
    assert approval["approved_at_utc"] == "2026-07-20T02:00:00+00:00"
    assert approval["approval_availability"]["source_available_session"] == "2026-07-20"
    assert (
        hashlib.sha256((tmp_path / receipt["path"]).read_bytes()).hexdigest() == receipt["sha256"]
    )
    assert approval["false_capabilities"] == market_module._FALSE_CAPABILITIES
    assert approval["approval_slot_id"] == market_module.DIRECT_APPROVAL_SLOT_ID
    assert approval["recovery_predecessor"] == market_module._GATE_B_RECOVERY_PREDECESSOR
    assert receipt["path"] == market_module._direct_approval_path()
    legacy_slot = (
        tmp_path
        / "manifests/silver/identity/openfigi-market-consistency-direct-approvals"
        / "slot_id=f39167969acee0a41e0069fcd6531c00b27469bd2265deef614a4e076aa03455"
        / "manifest.json"
    )
    assert not legacy_slot.exists()
    assert approval["runtime_binding"] == runtime_binding
    assert request["runtime_binding"] == runtime_binding
    assert approval["resource_caps"]["max_total_attempts"] == 8
    assert approval["resource_caps"]["max_cumulative_response_bytes"] == (
        market_module.MAX_CUMULATIVE_RESPONSE_BYTES
    )
    assert approval["resource_caps"]["max_wall_clock_seconds"] == (
        market_module.MAX_CAPTURE_WALL_CLOCK_SECONDS
    )
    assert approval["resource_caps"]["disk_free_hard_floor_bytes"] == (
        market_module.PRODUCTION_DISK_FREE_HARD_FLOOR_BYTES
    )
    assert stable_digest(market_module._direct_approval_scope(approval)) == approval["approval_id"]
    assert result.run_id != market_module._GATE_B_RECOVERY_PREDECESSOR["capture_run_id"]

    replay = prepare_approved_market_consistency_run(
        tmp_path,
        authorization_text=S7_CONTINUING_AUTHORIZATION_TEXT,
        reaffirmation_text=market_module.S7_REAFFIRMATION_TEXT,
        approved_by="test_approver",
        prepared_by="test_preparer",
        authenticated=True,
        now=_Clock(datetime(2026, 7, 21, 2, 0, tzinfo=UTC)),
    )
    assert replay.run_id == result.run_id
    assert replay.request_manifest_path == result.request_manifest_path
    replay_request = json.loads((tmp_path / replay.request_manifest_path).read_text())
    assert replay_request["direct_approval"] == receipt

    # Simulate a process death after the immutable approval was written but before the
    # deterministic request became durable.  Recovery must rebuild the same request ID.
    (tmp_path / replay.request_manifest_path).unlink()
    recovered = prepare_approved_market_consistency_run(
        tmp_path,
        authorization_text=S7_CONTINUING_AUTHORIZATION_TEXT,
        reaffirmation_text=market_module.S7_REAFFIRMATION_TEXT,
        approved_by="test_approver",
        prepared_by="test_preparer",
        authenticated=True,
        now=_Clock(datetime(2026, 7, 21, 3, 0, tzinfo=UTC)),
    )
    assert recovered.run_id == result.run_id
    assert json.loads((tmp_path / recovered.request_manifest_path).read_text()) == request

    with pytest.raises(IdentityMarketConsistencyError, match="slot actors differ"):
        prepare_approved_market_consistency_run(
            tmp_path,
            authorization_text=S7_CONTINUING_AUTHORIZATION_TEXT,
            reaffirmation_text=market_module.S7_REAFFIRMATION_TEXT,
            approved_by="different_actor",
            prepared_by="test_preparer",
            authenticated=True,
            now=_Clock(datetime(2026, 7, 22, 2, 0, tzinfo=UTC)),
        )

    with pytest.raises(IdentityMarketConsistencyError, match="fixed-slot binding differs"):
        prepare_approved_market_consistency_run(
            tmp_path,
            authorization_text=S7_CONTINUING_AUTHORIZATION_TEXT,
            reaffirmation_text=market_module.S7_REAFFIRMATION_TEXT,
            approved_by="test_approver",
            prepared_by="test_preparer",
            authenticated=False,
            now=_Clock(datetime(2026, 7, 22, 3, 0, tzinfo=UTC)),
        )

    drifted_predecessor = {
        **market_module._GATE_B_RECOVERY_PREDECESSOR,
        "disposition": "tampered_recovery_disposition",
    }
    monkeypatch.setattr(
        market_module,
        "_GATE_B_RECOVERY_PREDECESSOR",
        drifted_predecessor,
    )
    with pytest.raises(IdentityMarketConsistencyError, match="fixed-slot binding differs"):
        prepare_approved_market_consistency_run(
            tmp_path,
            authorization_text=S7_CONTINUING_AUTHORIZATION_TEXT,
            reaffirmation_text=market_module.S7_REAFFIRMATION_TEXT,
            approved_by="test_approver",
            prepared_by="test_preparer",
            authenticated=True,
            now=_Clock(datetime(2026, 7, 22, 4, 0, tzinfo=UTC)),
        )


def test_production_runtime_source_drift_fails_before_network(
    tmp_path: Path, exact_calendar, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_xnys_calendar_artifact(tmp_path, exact_calendar)
    figis = tuple(market_module._relationship_seeds())
    path, sha = _inventory(tmp_path, figis)
    inventory_path = tmp_path / path
    fixture_binding = {
        "candidate": {
            "bytes": 1,
            "candidate_id": "1" * 64,
            "path": "fixture-candidate.json",
            "sha256": "2" * 64,
        },
        "completion": {
            "bytes": 1,
            "completion_id": "3" * 64,
            "path": "fixture-completion.json",
            "sha256": "4" * 64,
        },
        "data": {
            "bytes": inventory_path.stat().st_size,
            "path": path,
            "row_count": len(figis),
            "sha256": sha,
        },
        "mode": "production",
    }
    monkeypatch.setattr(market_module, "PRODUCTION_DATA_ROOT", tmp_path.resolve())
    monkeypatch.setattr(
        market_module,
        "_production_inventory_manifest_binding",
        lambda _root: fixture_binding,
    )
    monkeypatch.setattr(
        market_module,
        "_verify_production_inventory_binding",
        lambda _root: fixture_binding,
    )
    original = _fake_runtime_binding("a")
    monkeypatch.setattr(market_module, "_repository_runtime_binding", lambda: original)
    result = prepare_approved_market_consistency_run(
        tmp_path,
        authorization_text=S7_CONTINUING_AUTHORIZATION_TEXT,
        reaffirmation_text=market_module.S7_REAFFIRMATION_TEXT,
        approved_by="test_approver",
        prepared_by="test_preparer",
        authenticated=True,
        now=_Clock(datetime(2026, 7, 20, 2, 0, tzinfo=UTC)),
    )

    drifted = _fake_runtime_binding("b")
    monkeypatch.setattr(market_module, "_repository_runtime_binding", lambda: drifted)
    with pytest.raises(IdentityMarketConsistencyError, match="fixed-slot binding differs"):
        prepare_approved_market_consistency_run(
            tmp_path,
            authorization_text=S7_CONTINUING_AUTHORIZATION_TEXT,
            reaffirmation_text=market_module.S7_REAFFIRMATION_TEXT,
            approved_by="test_approver",
            prepared_by="test_preparer",
            authenticated=True,
            now=_Clock(datetime(2026, 7, 20, 3, 0, tzinfo=UTC)),
        )
    with pytest.raises(IdentityMarketConsistencyError, match="runtime source binding differs"):
        execute_market_consistency_run(
            tmp_path,
            run_id=result.run_id,
            api_key="test-key",
            http_post=lambda *_args: pytest.fail("runtime drift reached network"),
            require_production_approval=True,
        )


def test_offline_replay_succeeds_idempotently_without_network_or_capture_entrypoints(
    tmp_path: Path, exact_calendar, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared, _completed, runtime, frozen_reads = _install_offline_replay_fixture(
        tmp_path,
        exact_calendar,
        monkeypatch,
    )

    def forbidden_entrypoint(*_args, **_kwargs):
        pytest.fail("offline replay invoked a network/capture entrypoint")

    monkeypatch.setattr(market_module, "_urllib_post", forbidden_entrypoint)
    monkeypatch.setattr(
        market_module,
        "prepare_market_consistency_run",
        forbidden_entrypoint,
    )
    monkeypatch.setattr(
        market_module,
        "prepare_approved_market_consistency_run",
        forbidden_entrypoint,
    )
    monkeypatch.setattr(
        market_module,
        "execute_market_consistency_run",
        forbidden_entrypoint,
    )
    clock = _Clock(datetime(2026, 7, 20, 2, 0, tzinfo=UTC))
    first = market_module.execute_approved_market_classification_replay(
        tmp_path,
        approved_by="test_approver",
        prepared_by="test_preparer",
        materialized_by="test_materializer",
        now=clock,
    )
    assert first.idempotent is False
    assert first.candidate.idempotent is True
    assert first.candidate.composite_count == 3
    assert first.replay_id
    assert first.approval_id
    assert first.completion_id
    assert frozen_reads == [tmp_path.resolve(), tmp_path.resolve()]

    completion = json.loads((tmp_path / first.completion_path).read_text())
    assert completion["network_request_count"] == 0
    assert completion["source_mutation"] is False
    assert completion["capabilities"] == market_module._FALSE_CAPABILITIES
    candidate = json.loads((tmp_path / first.candidate.manifest_path).read_text())
    assert candidate["replay_id"] == first.replay_id
    assert candidate["offline_replay_approval"]["approval_id"] == first.approval_id
    assert candidate["candidate_basis"]["source_run_id"] == prepared.run_id
    assert candidate["candidate_basis"]["offline_replay"] == {
        "approval_id": first.approval_id,
        "approval_sha256": candidate["offline_replay_approval"]["sha256"],
        "classifier_algorithm_digest": market_module.CLASSIFIER_ALGORITHM_DIGEST,
        "classifier_qa_digest": market_module.CLASSIFIER_QA_DIGEST,
        "replay_id": first.replay_id,
        "source_capture_binding_digest": stable_digest(
            market_module._production_replay_source_binding()
        ),
        "transform_runtime_binding_digest": stable_digest(runtime),
    }

    second = market_module.execute_approved_market_classification_replay(
        tmp_path,
        approved_by="test_approver",
        prepared_by="test_preparer",
        materialized_by="ignored_on_idempotent_replay",
        now=_Clock(datetime(2026, 7, 21, 2, 0, tzinfo=UTC)),
    )
    assert second.idempotent is True
    assert second.replay_id == first.replay_id
    assert second.completion_id == first.completion_id
    assert second.candidate.candidate_id == first.candidate.candidate_id
    assert frozen_reads == [tmp_path.resolve()] * 3


def test_offline_replay_transform_runtime_drift_fails_before_frozen_source_read(
    tmp_path: Path, exact_calendar, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepared, _completed, _runtime, frozen_reads = _install_offline_replay_fixture(
        tmp_path,
        exact_calendar,
        monkeypatch,
    )
    market_module.execute_approved_market_classification_replay(
        tmp_path,
        approved_by="test_approver",
        prepared_by="test_preparer",
        materialized_by="test_materializer",
        now=_Clock(datetime(2026, 7, 20, 2, 0, tzinfo=UTC)),
    )
    frozen_reads.clear()
    monkeypatch.setattr(
        market_module,
        "_repository_runtime_binding",
        lambda: _fake_runtime_binding("b"),
    )

    with pytest.raises(IdentityMarketConsistencyError, match="fixed scope differs"):
        market_module.execute_approved_market_classification_replay(
            tmp_path,
            approved_by="test_approver",
            prepared_by="test_preparer",
            materialized_by="test_materializer",
            now=_Clock(datetime(2026, 7, 21, 2, 0, tzinfo=UTC)),
        )
    assert frozen_reads == []


@pytest.mark.parametrize("tamper_target", ["source", "candidate", "approval"])
def test_offline_replay_source_candidate_and_approval_tamper_fail_closed(
    tmp_path: Path,
    exact_calendar,
    monkeypatch: pytest.MonkeyPatch,
    tamper_target: str,
) -> None:
    _prepared, completed, _runtime, _frozen_reads = _install_offline_replay_fixture(
        tmp_path,
        exact_calendar,
        monkeypatch,
    )
    replay = market_module.execute_approved_market_classification_replay(
        tmp_path,
        approved_by="test_approver",
        prepared_by="test_preparer",
        materialized_by="test_materializer",
        now=_Clock(datetime(2026, 7, 20, 2, 0, tzinfo=UTC)),
    )
    if tamper_target == "source":
        assert completed.final_manifest_path is not None
        path = tmp_path / completed.final_manifest_path
        document = json.loads(path.read_text())
        document["tampered_after_replay"] = True
    elif tamper_target == "candidate":
        path = tmp_path / replay.candidate.manifest_path
        document = json.loads(path.read_text())
        document["created_by"] = "tampered_materializer"
    else:
        path = tmp_path / market_module._offline_replay_approval_path()
        document = json.loads(path.read_text())
        document["approved_by"] = "tampered_approver"
    path.chmod(0o644)
    path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")))

    with pytest.raises(IdentityMarketConsistencyError):
        market_module.execute_approved_market_classification_replay(
            tmp_path,
            approved_by="test_approver",
            prepared_by="test_preparer",
            materialized_by="test_materializer",
            now=_Clock(datetime(2026, 7, 21, 2, 0, tzinfo=UTC)),
        )


@pytest.mark.parametrize(
    "tamper_target",
    ["intent_as_candidate_qa", "transform_runtime_digest", "candidate_qa_as_intent"],
)
def test_offline_replay_resigned_completion_receipt_swaps_fail_closed(
    tmp_path: Path,
    exact_calendar,
    monkeypatch: pytest.MonkeyPatch,
    tamper_target: str,
) -> None:
    _install_offline_replay_fixture(tmp_path, exact_calendar, monkeypatch)
    replay = market_module.execute_approved_market_classification_replay(
        tmp_path,
        approved_by="test_approver",
        prepared_by="test_preparer",
        materialized_by="test_materializer",
        now=_Clock(datetime(2026, 7, 20, 2, 0, tzinfo=UTC)),
    )
    completion_path = tmp_path / replay.completion_path
    completion = json.loads(completion_path.read_text())
    if tamper_target == "intent_as_candidate_qa":
        completion["intent"] = completion["candidate_qa"]
    elif tamper_target == "transform_runtime_digest":
        completion["transform_runtime_binding_digest"] = "f" * 64
    else:
        completion["candidate_qa"] = completion["intent"]
    payload = dict(completion)
    payload.pop("completion_id")
    completion["completion_id"] = stable_digest(payload)
    completion_path.chmod(0o644)
    completion_path.write_text(json.dumps(completion, sort_keys=True, separators=(",", ":")))

    with pytest.raises(IdentityMarketConsistencyError):
        market_module.execute_approved_market_classification_replay(
            tmp_path,
            approved_by="test_approver",
            prepared_by="test_preparer",
            materialized_by="test_materializer",
            now=_Clock(datetime(2026, 7, 21, 2, 0, tzinfo=UTC)),
        )


def test_offline_replay_resigned_completion_cannot_predate_candidate(
    tmp_path: Path, exact_calendar, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_offline_replay_fixture(tmp_path, exact_calendar, monkeypatch)
    replay_times = iter(
        (
            datetime(2026, 7, 20, 2, 0, tzinfo=UTC),
            datetime(2026, 7, 20, 2, 5, tzinfo=UTC),
            datetime(2026, 7, 20, 2, 10, tzinfo=UTC),
        )
    )
    replay = market_module.execute_approved_market_classification_replay(
        tmp_path,
        approved_by="test_approver",
        prepared_by="test_preparer",
        materialized_by="test_materializer",
        now=lambda: next(replay_times),
    )
    completion_path = tmp_path / replay.completion_path
    completion = json.loads(completion_path.read_text())
    candidate = json.loads((tmp_path / replay.candidate.manifest_path).read_text())
    approval_path = tmp_path / market_module._offline_replay_approval_path()
    approval = json.loads(approval_path.read_text())
    completion["completed_at_utc"] = "2026-07-20T02:03:00+00:00"
    assert approval["approved_at_utc"] < completion["completed_at_utc"]
    assert completion["completed_at_utc"] < candidate["created_at_utc"]
    payload = dict(completion)
    payload.pop("completion_id")
    completion["completion_id"] = stable_digest(payload)
    completion_path.chmod(0o644)
    completion_path.write_text(json.dumps(completion, sort_keys=True, separators=(",", ":")))

    with pytest.raises(IdentityMarketConsistencyError):
        market_module.execute_approved_market_classification_replay(
            tmp_path,
            approved_by="test_approver",
            prepared_by="test_preparer",
            materialized_by="test_materializer",
            now=_Clock(datetime(2026, 7, 21, 2, 0, tzinfo=UTC)),
        )


def test_offline_replay_resigned_candidate_cannot_predate_approval(
    tmp_path: Path, exact_calendar, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepared, completed, _runtime, _frozen_reads = _install_offline_replay_fixture(
        tmp_path,
        exact_calendar,
        monkeypatch,
    )
    replay_times = iter(
        (
            datetime(2026, 7, 20, 2, 0, tzinfo=UTC),
            datetime(2026, 7, 20, 2, 5, tzinfo=UTC),
            datetime(2026, 7, 20, 2, 10, tzinfo=UTC),
        )
    )
    replay = market_module.execute_approved_market_classification_replay(
        tmp_path,
        approved_by="test_approver",
        prepared_by="test_preparer",
        materialized_by="test_materializer",
        now=lambda: next(replay_times),
    )
    assert completed.final_manifest_path is not None
    capture = json.loads((tmp_path / completed.final_manifest_path).read_text())
    approval_path = tmp_path / market_module._offline_replay_approval_path()
    approval = json.loads(approval_path.read_text())
    candidate_path = tmp_path / replay.candidate.manifest_path
    candidate = json.loads(candidate_path.read_text())
    candidate["created_at_utc"] = "2026-07-20T01:00:00+00:00"
    assert capture["latest_response_received_at_utc"] < candidate["created_at_utc"]
    assert candidate["created_at_utc"] < approval["approved_at_utc"]
    candidate_payload = dict(candidate)
    candidate_payload.pop("manifest_id")
    candidate["manifest_id"] = stable_digest(candidate_payload)
    candidate_path.chmod(0o644)
    candidate_path.write_text(json.dumps(candidate, sort_keys=True, separators=(",", ":")))

    completion_path = tmp_path / replay.completion_path
    completion = json.loads(completion_path.read_text())
    completion["candidate"] = {
        "bytes": candidate_path.stat().st_size,
        "candidate_id": replay.candidate.candidate_id,
        "path": replay.candidate.manifest_path,
        "sha256": sha256_file(candidate_path),
    }
    completion_payload = dict(completion)
    completion_payload.pop("completion_id")
    completion["completion_id"] = stable_digest(completion_payload)
    completion_path.chmod(0o644)
    completion_path.write_text(json.dumps(completion, sort_keys=True, separators=(",", ":")))

    with pytest.raises(IdentityMarketConsistencyError):
        market_module.execute_approved_market_classification_replay(
            tmp_path,
            approved_by="test_approver",
            prepared_by="test_preparer",
            materialized_by="test_materializer",
            now=_Clock(datetime(2026, 7, 21, 2, 0, tzinfo=UTC)),
        )


def test_ordinary_candidate_verification_never_falls_back_to_offline_replay(
    tmp_path: Path, exact_calendar, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = _prepare(tmp_path, _figis(3), exact_calendar)
    _execute_complete(tmp_path, prepared)
    candidate = materialize_market_classification_candidate(
        tmp_path,
        run_id=prepared.run_id,
        materialized_at_utc="2026-07-19T16:05:00+00:00",
        materialized_by="test_ordinary_materializer",
    )

    def forbidden_frozen_replay(_root: Path):
        pytest.fail("ordinary candidate verification entered the offline replay verifier")

    monkeypatch.setattr(
        market_module,
        "_verify_frozen_replay_capture",
        forbidden_frozen_replay,
    )
    request_path = tmp_path / prepared.request_manifest_path
    request = json.loads(request_path.read_text())
    request["prepared_by"] = "tampered_ordinary_preparer"
    request_path.chmod(0o644)
    request_path.write_text(json.dumps(request, sort_keys=True, separators=(",", ":")))

    with pytest.raises(IdentityMarketConsistencyError, match="ID recomputation failed"):
        verify_market_classification_candidate(
            tmp_path,
            candidate_path=candidate.manifest_path,
            candidate_id=candidate.candidate_id,
            candidate_sha256=sha256_file(tmp_path / candidate.manifest_path),
            require_production_approval=False,
        )


def test_production_cli_has_fixed_inventory_key_and_availability_surfaces() -> None:
    parser = build_parser()
    prepare = parser.parse_args(
        [
            "prepare",
            "--data-root",
            "/tmp/data",
            "--authorization-text",
            S7_CONTINUING_AUTHORIZATION_TEXT,
            "--reaffirmation-text",
            market_module.S7_REAFFIRMATION_TEXT,
            "--approved-by",
            "reviewer",
            "--prepared-by",
            "builder",
        ]
    )
    assert not hasattr(prepare, "inventory_data_path")
    assert not hasattr(prepare, "authenticated")
    assert not hasattr(prepare, "approved_at_utc")
    assert not hasattr(prepare, "prepared_at_utc")
    run = parser.parse_args(["run", "--data-root", "/tmp/data", "--run-id", "0" * 64])
    assert not hasattr(run, "api_key_env")
    classify = parser.parse_args(
        [
            "classify",
            "--data-root",
            "/tmp/data",
            "--run-id",
            "0" * 64,
            "--materialize",
            "--materialized-by",
            "builder",
        ]
    )
    assert not hasattr(classify, "source_available_session")
    assert not hasattr(classify, "materialized_at_utc")
    replay = parser.parse_args(
        [
            "replay-classify",
            "--data-root",
            "/tmp/data",
            "--approved-by",
            "reviewer",
            "--prepared-by",
            "builder",
            "--materialized-by",
            "materializer",
        ]
    )
    assert set(vars(replay)) == {
        "approved_by",
        "command",
        "data_root",
        "materialized_by",
        "prepared_by",
    }
    for forbidden_flag in (
        "--api-key-env",
        "--run-id",
        "--source-available-session",
        "--source-data-root",
    ):
        with pytest.raises(SystemExit):
            parser.parse_args(
                [
                    "replay-classify",
                    "--data-root",
                    "/tmp/data",
                    "--approved-by",
                    "reviewer",
                    "--prepared-by",
                    "builder",
                    "--materialized-by",
                    "materializer",
                    forbidden_flag,
                    "forbidden",
                ]
            )


def test_production_preparation_rejects_a_second_data_root(tmp_path: Path) -> None:
    with pytest.raises(IdentityMarketConsistencyError, match="canonical data root"):
        prepare_approved_market_consistency_run(
            tmp_path,
            authorization_text=S7_CONTINUING_AUTHORIZATION_TEXT,
            reaffirmation_text=market_module.S7_REAFFIRMATION_TEXT,
            approved_by="test_approver",
            prepared_by="test_preparer",
            authenticated=False,
        )
