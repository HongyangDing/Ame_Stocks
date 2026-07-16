from __future__ import annotations

import hashlib
import json
from pathlib import Path

from ame_stocks_api.artifacts import stable_digest

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_ROOT = ROOT / "docs/silver/evidence/s7-cross-market"
MANIFEST_PATH = (
    EVIDENCE_ROOT / "identity-cross-market-external-evidence-manifest.candidate.json"
)


def _load(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def test_external_evidence_candidate_is_content_addressed_and_non_executable() -> None:
    manifest = _load(MANIFEST_PATH)
    assert isinstance(manifest, dict)
    payload = {key: value for key, value in manifest.items() if key != "manifest_id"}

    assert stable_digest(payload) == manifest["manifest_id"]
    assert manifest["manifest_id"] == (
        "2ae779168e3e56887a5b0ae557bb928b6006c1b96392fe1606c201e1649ff848"
    )
    assert hashlib.sha256(MANIFEST_PATH.read_bytes()).hexdigest() == (
        "9544537ac7e6817c1b8f946c9ae2d5afb65399b1b553c3fe233a298614b375ab"
    )
    assert manifest["manifest_status"] == "candidate_not_approved"
    assert manifest["manifest_type"] == "identity_cross_market_external_evidence"
    assert manifest["detector_preview_binding"] == {
        "completion_id": "7a1e2386e18428aecf50a9ce322eaaf6b3035307b4a704939584288f131c6b9d",
        "identity_case_count": 19,
        "preview_id": "306543f5fc1d30f868482392aaafdc781daf9f36f30d3f12504024c10f865c70",
        "preview_rewritten": False,
        "suspected_row_count": 89,
    }
    assert manifest["review_summary"] == {
        "confirmed_non_us_observation_rows": 79,
        "correct_us_inverse_observation_rows": 10,
        "group_count": 9,
        "identity_case_count": 19,
    }
    assert all(
        group["future_adjudication_status"] == "not_planned_not_approved"
        for group in manifest["groups"]
    )
    assert all(
        group["related_case_binding"]["exact_case_ids_deferred"] is True
        for group in manifest["groups"]
    )


def test_every_frozen_external_artifact_replays_by_path_size_and_sha256() -> None:
    manifest = _load(MANIFEST_PATH)
    assert isinstance(manifest, dict)
    receipts = manifest["artifacts"]
    assert len(receipts) == 25

    for receipt in receipts:
        path = ROOT / receipt["path"]
        raw = path.read_bytes()
        assert len(raw) == receipt["bytes"]
        assert hashlib.sha256(raw).hexdigest() == receipt["sha256"]
        assert receipt["source_url"].startswith("https://")
        assert receipt["source_available_session"] == "2026-07-17"
        assert "source_published_at_utc" in receipt
        assert receipt["publication_precision"] in {
            "second",
            "date",
            "month",
            "unknown",
        }
        if path.name.endswith(".headers.txt"):
            lowered = raw.lower()
            assert b"authorization:" not in lowered
            assert b"x-openfigi-apikey:" not in lowered
            assert b"set-cookie:" not in lowered


def test_openfigi_requests_and_exact_relationship_claims_replay() -> None:
    manifest = _load(MANIFEST_PATH)
    assert isinstance(manifest, dict)

    for request_path in EVIDENCE_ROOT.glob("openfigi-*.request.json"):
        jobs = _load(request_path)
        assert isinstance(jobs, list)
        assert jobs
        assert all(job["includeUnlistedEquities"] is True for job in jobs)
        assert all(job["marketSecDes"] == "Equity" for job in jobs)

    for claim in manifest["mapping_assertions"]:
        for side in ("canonical_composite", "foreign_composite"):
            relation = claim[side]
            request = _load(ROOT / relation["request_path"])
            response = _load(ROOT / relation["response_path"])
            assert request[relation["request_job_index"]] == {
                "idType": "COMPOSITE_ID_BB_GLOBAL",
                "idValue": relation["expected_composite_figi"],
                "includeUnlistedEquities": True,
                "marketSecDes": "Equity",
            }
            rows = response[relation["request_job_index"]]["data"]
            exact = [
                row
                for row in rows
                if row.get("compositeFIGI") == relation["expected_composite_figi"]
                and row.get("shareClassFIGI") == relation["expected_share_class_figi"]
                and row.get("exchCode") == relation["expected_market_code"]
            ]
            assert exact
            conflicting_share_classes = {
                row["shareClassFIGI"]
                for row in rows
                if row.get("compositeFIGI") == relation["expected_composite_figi"]
                and row.get("shareClassFIGI") is not None
            }
            assert conflicting_share_classes == {relation["expected_share_class_figi"]}

        reverse = claim["share_class_reverse_projection"]
        reverse_request = _load(ROOT / reverse["request_path"])
        reverse_response = _load(ROOT / reverse["response_path"])
        job_index = reverse["request_job_index"]
        assert reverse_request[job_index]["idType"] == (
            "ID_BB_GLOBAL_SHARE_CLASS_LEVEL"
        )
        assert {
            row.get("compositeFIGI") for row in reverse_response[job_index]["data"]
        }.issuperset(reverse["expected_composite_figis"])


def test_tnxp_unlisted_relation_does_not_require_a_composite_self_row() -> None:
    manifest = _load(MANIFEST_PATH)
    assert isinstance(manifest, dict)
    claim = next(item for item in manifest["mapping_assertions"] if item["ticker"] == "TNXP")
    foreign = claim["foreign_composite"]
    response = _load(ROOT / foreign["response_path"])
    rows = response[foreign["request_job_index"]]["data"]

    assert len(rows) == 1
    assert rows[0]["figi"] == "BBG00R4FG9M1"
    assert rows[0]["compositeFIGI"] == "BBG00R4FG9L2"
    assert rows[0]["shareClassFIGI"] == "BBG001T49NZ9"
    assert rows[0]["figi"] != rows[0]["compositeFIGI"]


def test_company_action_evidence_preserves_publication_precision_and_timing() -> None:
    manifest = _load(MANIFEST_PATH)
    assert isinstance(manifest, dict)
    by_ticker = {
        item["ticker"]: item for item in manifest["company_action_timing_assertions"]
    }

    assert by_ticker["AZPN"]["source_published_at_utc"] == "2022-05-16T21:34:48Z"
    assert by_ticker["CR"]["source_published_at_utc"] == "2022-02-28T18:57:39Z"
    assert by_ticker["FLOW"]["publication_precision"] == "date"
    assert "source_published_at_utc" not in by_ticker["FLOW"]
    assert by_ticker["TBLT"]["source_published_at_utc"] == "2022-04-25T17:11:41Z"
    assert by_ticker["TNXP"]["source_published_at_utc"] == "2022-05-16T13:00:40Z"

    assert b"<ACCEPTANCE-DATETIME>20220516213448" in (
        EVIDENCE_ROOT / "azpn-2022-05-16-sec-submission.txt"
    ).read_bytes()
    assert b"<ACCEPTANCE-DATETIME>20220228185739" in (
        EVIDENCE_ROOT / "cr-2022-02-28-sec-submission.txt"
    ).read_bytes()
    assert b"successful closing" in (
        EVIDENCE_ROOT / "flow-2022-04-05-issuer.html"
    ).read_bytes()
