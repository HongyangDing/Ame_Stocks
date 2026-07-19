from __future__ import annotations

import hashlib
import json
from pathlib import Path

from ame_stocks_api.artifacts import stable_digest

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_ROOT = ROOT / "docs/silver/evidence/s7-exact-groups"
MANIFEST_PATH = EVIDENCE_ROOT / "identity-exact-group-external-evidence-manifest.candidate.json"


def _load(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def test_exact_group_evidence_candidate_is_content_addressed_and_non_executable() -> None:
    manifest = _load(MANIFEST_PATH)
    assert isinstance(manifest, dict)
    payload = {key: value for key, value in manifest.items() if key != "manifest_id"}

    assert stable_digest(payload) == manifest["manifest_id"]
    assert manifest["manifest_id"] == (
        "30e3cd9f009c995ce594fd19344ce551ea39133a9c60caea661a7c7211743fdd"
    )
    assert hashlib.sha256(MANIFEST_PATH.read_bytes()).hexdigest() == (
        "e1a6d365fb2d12f913461576f51003ecabf1bb91a74e546cc717660578f0b17b"
    )
    assert manifest["manifest_status"] == "candidate_not_approved"
    assert manifest["manifest_type"] == "identity_exact_group_external_evidence"
    assert manifest["availability"] == {
        "available_session": "2026-07-20",
        "rule": "first_xnys_open_strictly_after_latest_actual_capture_v1",
    }
    assert not any(manifest["non_executable_capabilities"].values())
    assert all(
        case["candidate_adjudication_status"] == "not_planned_not_approved"
        for case in manifest["cases"]
    )


def test_every_exact_group_artifact_replays_and_headers_are_safe() -> None:
    manifest = _load(MANIFEST_PATH)
    assert isinstance(manifest, dict)
    assert len(manifest["artifacts"]) == 27

    for receipt in manifest["artifacts"]:
        path = ROOT / receipt["path"]
        raw = path.read_bytes()
        assert len(raw) == receipt["bytes"]
        assert hashlib.sha256(raw).hexdigest() == receipt["sha256"]
        assert receipt["source_url"].startswith("https://")
        assert receipt["source_available_session"] in {"2026-07-17", "2026-07-20"}
        assert receipt["captured_at_utc"] >= "2026-07-16T00:00:00Z"
        if path.name.endswith(".headers.txt"):
            lowered = raw.lower()
            assert b"authorization:" not in lowered
            assert b"x-openfigi-apikey:" not in lowered
            assert b"set-cookie:" not in lowered
            assert b"cookie:" not in lowered


def test_openfigi_exact_requests_and_assertions_replay() -> None:
    manifest = _load(MANIFEST_PATH)
    assert isinstance(manifest, dict)

    for request_path in EVIDENCE_ROOT.glob("openfigi-*.request.json"):
        jobs = _load(request_path)
        assert isinstance(jobs, list)
        assert jobs
        assert all(job["includeUnlistedEquities"] is True for job in jobs)
        assert all(job["marketSecDes"] == "Equity" for job in jobs)
        if "ticker" in request_path.name or "share-class" in request_path.name:
            assert all(job["exchCode"] == "US" for job in jobs)
        if "composite" in request_path.name:
            assert all(job["idType"] == "COMPOSITE_ID_BB_GLOBAL" for job in jobs)

    for case in manifest["cases"]:
        for assertion in case["openfigi_assertions"]:
            request = _load(ROOT / assertion["request_path"])
            response = _load(ROOT / assertion["response_path"])
            index = assertion["request_job_index"]
            assert isinstance(request, list)
            assert isinstance(response, list)
            assert index < len(request) == len(response)

            result = response[index]
            if "expected_warning" in assertion:
                assert request[index]["idValue"] == assertion["queried_share_class_figi"]
                assert result == {"warning": assertion["expected_warning"]}
                continue

            expected = assertion["expected"]
            assert any(
                all(row.get(key) == value for key, value in expected.items())
                for row in result["data"]
            )


def test_official_event_bytes_support_only_the_stated_case_facts() -> None:
    sor_announcement = (EVIDENCE_ROOT / "sor-2024-11-19-8k.html").read_bytes()
    sor_confirmation = (EVIDENCE_ROOT / "sor-2025-03-07-ncsr.html").read_bytes()
    xzo_filing = (EVIDENCE_ROOT / "xzo-2025-11-06-8k.html").read_bytes()
    anabv_notice = (EVIDENCE_ROOT / "anabv-2026-04-02-first-tracks-8k.html").read_bytes()
    anab_completion = (EVIDENCE_ROOT / "anab-2026-04-20-spin-completion-8k.html").read_bytes()

    assert b"after the close of business on December" in sor_announcement
    assert b"Effective January 1, 2025" in sor_confirmation
    assert b"continues to trade on the NYSE under the SOR ticker" in sor_confirmation
    assert b"commenced trading on the New York Stock Exchange on November" in xzo_filing
    assert b"ANABV" in anabv_notice
    assert b"without an entitlement" in anabv_notice
    assert b"On April&#160;20, 2026, the" in anab_completion
    assert b"Spin-Off</span> was completed in accordance" in anab_completion


def test_external_time_axes_are_not_backdated() -> None:
    manifest = _load(MANIFEST_PATH)
    assert isinstance(manifest, dict)

    for receipt in manifest["artifacts"]:
        if receipt["source_available_session"] == "2026-07-20":
            assert receipt["captured_at_utc"].startswith("2026-07-19T")

    assert manifest["openfigi_capture"]["response_name_ticker_historical_truth"] is False
    assert any("not a point-in-time archive" in item for item in manifest["limitations"])
    assert manifest["case_scope_binding"]["source_row_interval_finalized"] is False
