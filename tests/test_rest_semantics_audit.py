import gzip
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

from ame_stocks_api.artifacts import write_json_atomic
from ame_stocks_api.audit.rest_semantics import RestSemanticAuditor
from ame_stocks_api.cli.rest_semantics_audit import main as semantic_audit_main
from ame_stocks_api.downloads import build_download_plan
from ame_stocks_core import ProviderDataset, ProviderRequest

SESSION = date(2026, 6, 30)


def _formal_request(dataset: ProviderDataset) -> ProviderRequest:
    start = {
        ProviderDataset.SPLITS: date(2003, 9, 10),
        ProviderDataset.DIVIDENDS: date(2003, 9, 10),
        ProviderDataset.NEWS: date(2016, 6, 22),
    }.get(dataset, SESSION)
    if dataset in {
        ProviderDataset.CONDITION_CODES,
        ProviderDataset.DISCLOSURE_TAXONOMY,
        ProviderDataset.RISK_TAXONOMY,
    }:
        start = SESSION
    requests = build_download_plan(dataset=dataset, start=start, end=SESSION).requests
    assert len(requests) == 1
    return requests[0]


def _write_request(
    root: Path,
    request: ProviderRequest,
    rows: list[dict[str, object]],
) -> Path:
    raw = json.dumps({"results": rows, "status": "OK"}, sort_keys=True).encode()
    compressed = gzip.compress(raw, mtime=0)
    relative = (
        f"bronze/massive/{request.dataset.value}/request_id={request.request_id}/"
        "page-00000.json.gz"
    )
    artifact_path = root / relative
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(compressed)
    manifest_path = (
        root
        / "manifests"
        / "massive"
        / request.dataset.value
        / f"{request.request_id}.json"
    )
    write_json_atomic(
        manifest_path,
        {
            "artifacts": [
                {
                    "compressed_bytes": len(compressed),
                    "is_last": True,
                    "next_continuation": None,
                    "path": relative,
                    "raw_bytes": len(raw),
                    "raw_sha256": hashlib.sha256(raw).hexdigest(),
                    "record_count": len(rows),
                    "sequence": 0,
                    "stored_sha256": hashlib.sha256(compressed).hexdigest(),
                }
            ],
            "checkpoint": None,
            "dataset": request.dataset.value,
            "manifest_schema_version": 1,
            "provider": "massive",
            "provider_contract_version": "1.1",
            "provider_version": "fixture",
            "request": request.canonical_dict(),
            "request_id": request.request_id,
            "status": "complete",
        },
    )
    return manifest_path


def _audit(root: Path, *datasets: ProviderDataset) -> dict[str, object]:
    return RestSemanticAuditor(
        root,
        start=SESSION,
        end=SESSION,
        datasets=tuple(datasets),
        max_examples=5,
    ).run()


def test_non_authoritative_disclosure_pilot_is_never_opened(tmp_path: Path) -> None:
    taxonomy = {
        "primary_category": "leadership",
        "secondary_category": "executives",
        "tertiary_category": "appointment",
    }
    _write_request(tmp_path, _formal_request(ProviderDataset.DISCLOSURE_TAXONOMY), [taxonomy])
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.EIGHT_K_DISCLOSURES),
        [{"accession_number": "0001-26-000001", **taxonomy}],
    )
    pilot = ProviderRequest(
        dataset=ProviderDataset.EIGHT_K_DISCLOSURES,
        start=SESSION - timedelta(days=1),
        end=SESSION - timedelta(days=1),
    )
    pilot_path = (
        tmp_path
        / "manifests"
        / "massive"
        / pilot.dataset.value
        / f"{pilot.request_id}.json"
    )
    pilot_path.parent.mkdir(parents=True, exist_ok=True)
    pilot_path.write_text("this pilot is deliberately invalid JSON", encoding="utf-8")

    report = _audit(
        tmp_path,
        ProviderDataset.DISCLOSURE_TAXONOMY,
        ProviderDataset.EIGHT_K_DISCLOSURES,
    )

    assert report["status"] == "passed"
    assert report["summary"]["ignored_non_authoritative_manifests"] == 1
    metrics = {item["dataset"]: item for item in report["datasets"]}
    assert metrics["eight_k_disclosures"]["rows"] == 1
    assert report["taxonomy_coverage"]["disclosure"]["status"] == "matched"
    assert list((tmp_path / "tmp" / "rest_semantics_audit").iterdir()) == []


def test_exact_and_conflicting_candidate_keys_fail(tmp_path: Path, capsys) -> None:
    request = _formal_request(ProviderDataset.SPLITS)
    first = {
        "execution_date": "2026-06-30",
        "id": "split-1",
        "split_from": 1,
        "split_to": 2,
        "ticker": "AAA",
    }
    conflicting = {**first, "split_to": 3}
    _write_request(tmp_path, request, [first, first, conflicting])

    report = _audit(tmp_path, ProviderDataset.SPLITS)

    assert report["status"] == "failed"
    uniqueness = report["uniqueness"]["splits"]
    assert uniqueness["exact_duplicate_excess_rows"] == 1
    assert uniqueness["conflicting_keys"] == 1
    assert report["gates"]["semantic_corruption"] == "failed"
    assert (
        semantic_audit_main(
            [
                "--data-root",
                str(tmp_path),
                "--start",
                SESSION.isoformat(),
                "--end",
                SESSION.isoformat(),
                "--dataset",
                "splits",
            ]
        )
        == 1
    )
    assert json.loads(capsys.readouterr().out)["status"] == "failed"


def test_condition_identity_expands_data_types_before_uniqueness(tmp_path: Path) -> None:
    request = _formal_request(ProviderDataset.CONDITION_CODES)
    _write_request(
        tmp_path,
        request,
        [
            {
                "asset_class": "stocks",
                "data_types": ["trade", "quote"],
                "id": 1,
                "name": "first",
            },
            {
                "asset_class": "stocks",
                "data_types": ["trade"],
                "id": 1,
                "name": "conflicting trade definition",
            },
        ],
    )

    report = _audit(tmp_path, ProviderDataset.CONDITION_CODES)

    assert report["status"] == "passed_with_differences"
    assert report["uniqueness"]["condition_codes"]["distinct_candidate_keys"] == 2
    assert report["uniqueness"]["condition_codes"]["conflicting_keys"] == 1
    assert report["gates"]["candidate_key_consistency"] == "different"


def test_edgar_combined_filing_uses_accession_and_cik_candidate_key(
    tmp_path: Path,
) -> None:
    request = _formal_request(ProviderDataset.EDGAR_INDEX)
    first = {
        "accession_number": "0001-26-000001",
        "cik": "0000000001",
        "filing_date": "2026-06-30",
    }
    second_registrant = {**first, "cik": "0000000002"}
    _write_request(tmp_path, request, [first, second_registrant, first])

    report = _audit(tmp_path, ProviderDataset.EDGAR_INDEX)

    assert report["status"] == "passed_with_differences"
    uniqueness = report["uniqueness"]["edgar_index"]
    assert uniqueness["distinct_candidate_keys"] == 2
    assert uniqueness["conflicting_keys"] == 0
    assert uniqueness["exact_duplicate_excess_rows"] == 1
    assert report["summary"]["difference_code_counts"]["provider_exact_duplicate_rows"] == 1


def test_empty_reference_snapshot_fails(tmp_path: Path) -> None:
    _write_request(tmp_path, _formal_request(ProviderDataset.CONDITION_CODES), [])

    report = _audit(tmp_path, ProviderDataset.CONDITION_CODES)

    assert report["status"] == "failed"
    assert report["summary"]["corruption_code_counts"]["empty_reference_snapshot"] == 1


def test_taxonomy_usage_path_must_exist_in_definition(tmp_path: Path) -> None:
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.RISK_TAXONOMY),
        [
            {
                "primary_category": "operations",
                "secondary_category": "supply_chain",
                "tertiary_category": "shortage",
            }
        ],
    )
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.RISK_FACTORS),
        [
            {
                "filing_date": "2026-06-30",
                "primary_category": "operations",
                "secondary_category": "supply_chain",
                "tertiary_category": "supplier_failure",
            }
        ],
    )

    report = _audit(
        tmp_path,
        ProviderDataset.RISK_TAXONOMY,
        ProviderDataset.RISK_FACTORS,
    )

    assert report["status"] == "failed"
    coverage = report["taxonomy_coverage"]["risk"]
    assert coverage["status"] == "failed"
    assert coverage["undecodable_usage_rows"] == 1
    assert report["summary"]["corruption_code_counts"]["taxonomy_path_not_decodable"] == 1


def test_accession_coverage_is_a_non_failing_difference(tmp_path: Path, capsys) -> None:
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.EDGAR_INDEX),
        [
            {
                "accession_number": "0001-26-000001",
                "cik": "0000000001",
                "filing_date": "2026-06-30",
            }
        ],
    )
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.FORM_3),
        [{"accession_number": "0002-26-000002", "filing_date": "2026-06-30"}],
    )

    report = _audit(tmp_path, ProviderDataset.EDGAR_INDEX, ProviderDataset.FORM_3)

    assert report["status"] == "passed_with_differences"
    assert report["gates"]["semantic_corruption"] == "passed"
    assert report["gates"]["accession_coverage"] == "different"
    assert report["accession_coverage"]["datasets"]["form_3"]["missing_edgar_rows"] == 1
    assert (
        semantic_audit_main(
            [
                "--data-root",
                str(tmp_path),
                "--start",
                SESSION.isoformat(),
                "--end",
                SESSION.isoformat(),
                "--dataset",
                "edgar_index",
                "--dataset",
                "form_3",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "passed_with_differences"
