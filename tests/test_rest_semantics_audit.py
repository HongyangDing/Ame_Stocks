import gzip
import hashlib
import json
from datetime import UTC, date, datetime, timedelta
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
    raw = json.dumps(
        {"request_id": "fixture", "results": rows, "status": "OK"},
        sort_keys=True,
    ).encode()
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


def _write_legacy_financial_history(
    root: Path,
    first_request_rows: list[dict[str, object]],
) -> None:
    requests = build_download_plan(
        dataset=ProviderDataset.LEGACY_FINANCIALS,
        start=date(2009, 3, 29),
        end=SESSION,
    ).requests
    for index, request in enumerate(requests):
        _write_request(root, request, first_request_rows if index == 0 else [])


def test_non_authoritative_disclosure_pilot_is_never_opened(tmp_path: Path) -> None:
    taxonomy = {
        "primary_category": "leadership",
        "secondary_category": "executives",
        "tertiary_category": "appointment",
        "taxonomy": "1.0",
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
                "taxonomy": "1.0",
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
    output = tmp_path / "rest-semantics-audit.json"

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
                "--output",
                str(output),
            ]
        )
        == 0
    )
    printed = json.loads(capsys.readouterr().out)
    stored = json.loads(output.read_text(encoding="utf-8"))
    assert printed["status"] == "passed_with_differences"
    assert stored == printed
    assert stored["report_path"] == str(output.resolve())


def test_missing_detail_accession_fails_semantics_and_coverage_gate(tmp_path: Path) -> None:
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
        [{"filing_date": "2026-06-30"}],
    )

    report = _audit(tmp_path, ProviderDataset.EDGAR_INDEX, ProviderDataset.FORM_3)

    assert report["status"] == "failed"
    assert report["gates"]["semantic_corruption"] == "failed"
    assert report["gates"]["accession_coverage"] == "failed"
    assert report["summary"]["corruption_code_counts"] == {
        "accession_number_missing": 1
    }
    assert report["accession_coverage"]["rows_without_accession"] == 1
    assert report["accession_coverage"]["datasets"]["form_3"][
        "rows_without_accession"
    ] == 1


def test_exact_detail_rows_are_profiled_across_the_authoritative_dataset(
    tmp_path: Path,
) -> None:
    accession = "0001-26-000001"
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.EDGAR_INDEX),
        [
            {
                "accession_number": accession,
                "cik": "0000000001",
                "filing_date": "2026-06-30",
            }
        ],
    )
    detail = {"accession_number": accession, "filing_date": "2026-06-30"}
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.FORM_3),
        [detail, detail],
    )

    report = _audit(tmp_path, ProviderDataset.EDGAR_INDEX, ProviderDataset.FORM_3)

    assert report["status"] == "passed_with_differences"
    assert report["uniqueness"]["form_3"]["exact_duplicate_excess_rows"] == 1
    assert report["summary"]["difference_code_counts"][
        "provider_exact_duplicate_rows"
    ] == 1


def test_detail_filing_date_must_match_edgar_for_the_same_accession(
    tmp_path: Path,
) -> None:
    accession = "0001-26-000001"
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.EDGAR_INDEX),
        [
            {
                "accession_number": accession,
                "cik": "0000000001",
                "filing_date": "2026-06-30",
            }
        ],
    )
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.FORM_3),
        [{"accession_number": accession, "filing_date": "2026-01-01"}],
    )

    report = _audit(tmp_path, ProviderDataset.EDGAR_INDEX, ProviderDataset.FORM_3)

    assert report["status"] == "failed"
    assert report["gates"]["accession_coverage"] == "failed"
    assert report["summary"]["corruption_code_counts"][
        "accession_filing_date_mismatch"
    ] == 1


def test_form_13f_identity_accepts_any_matching_edgar_pair(tmp_path: Path) -> None:
    accession = "0000000001-26-000001"
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.EDGAR_INDEX),
        [
            {
                "accession_number": accession,
                "cik": "0000000001",
                "filing_date": "2026-06-30",
                "form_type": "13F-HR",
            },
            {
                "accession_number": accession,
                "cik": "0000000002",
                "filing_date": "2026-06-30",
                "form_type": "13F-HR",
            },
        ],
    )
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.FORM_13F),
        [
            {
                "accession_number": accession,
                "filer_cik": "0000000002",
                "filing_date": "2026-06-30",
                "form_type": "13F-HR",
            }
        ],
    )

    report = _audit(tmp_path, ProviderDataset.EDGAR_INDEX, ProviderDataset.FORM_13F)

    assert report["status"] == "passed"
    assert report["gates"]["accession_coverage"] == "matched"
    assert report["accession_coverage"]["identity_mismatch_rows"] == 0


def test_form_13f_identity_mismatch_fails_accession_gate(tmp_path: Path) -> None:
    accession = "0000000001-26-000001"
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.EDGAR_INDEX),
        [
            {
                "accession_number": accession,
                "cik": "0000000001",
                "filing_date": "2026-06-30",
                "form_type": "13F-HR",
            }
        ],
    )
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.FORM_13F),
        [
            {
                "accession_number": accession,
                "filer_cik": "0000000002",
                "filing_date": "2026-06-30",
                "form_type": "13F-NT",
            }
        ],
    )

    report = _audit(tmp_path, ProviderDataset.EDGAR_INDEX, ProviderDataset.FORM_13F)

    assert report["status"] == "failed"
    assert report["gates"]["accession_coverage"] == "failed"
    assert report["summary"]["corruption_code_counts"] == {
        "accession_identity_mismatch": 1
    }
    details = report["accession_coverage"]["datasets"]["form_13f"]
    assert details["identity_mismatch_rows"] == 1


def test_form_13f_identity_requires_one_edgar_row_to_match_all_fields(
    tmp_path: Path,
) -> None:
    accession = "0000000001-26-000001"
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.EDGAR_INDEX),
        [
            {
                "accession_number": accession,
                "cik": "0000000001",
                "filing_date": "2026-06-30",
                "form_type": "13F-HR",
            },
            {
                "accession_number": accession,
                "cik": "0000000002",
                "filing_date": "2026-01-01",
                "form_type": "13F-NT",
            },
        ],
    )
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.FORM_13F),
        [
            {
                "accession_number": accession,
                "filer_cik": "0000000001",
                "filing_date": "2026-01-01",
                "form_type": "13F-HR",
            }
        ],
    )

    report = _audit(tmp_path, ProviderDataset.EDGAR_INDEX, ProviderDataset.FORM_13F)

    assert report["status"] == "failed"
    assert report["gates"]["accession_coverage"] == "failed"
    assert report["summary"]["corruption_code_counts"] == {
        "accession_identity_mismatch": 1
    }


def test_form_13f_pure_filing_date_mismatch_is_not_double_counted(
    tmp_path: Path,
) -> None:
    accession = "0000000001-26-000001"
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.EDGAR_INDEX),
        [
            {
                "accession_number": accession,
                "cik": "0000000001",
                "filing_date": "2026-06-30",
                "form_type": "13F-HR",
            }
        ],
    )
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.FORM_13F),
        [
            {
                "accession_number": accession,
                "filer_cik": "0000000001",
                "filing_date": "2026-01-01",
                "form_type": "13F-HR",
            }
        ],
    )

    report = _audit(tmp_path, ProviderDataset.EDGAR_INDEX, ProviderDataset.FORM_13F)

    assert report["status"] == "failed"
    assert report["summary"]["corruption_code_counts"] == {
        "accession_filing_date_mismatch": 1
    }
    assert report["accession_coverage"]["identity_mismatch_rows"] == 0


def test_taxonomy_snapshot_must_have_one_explicit_version(tmp_path: Path) -> None:
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.RISK_TAXONOMY),
        [
            {
                "primary_category": "operations",
                "secondary_category": "supply_chain",
                "tertiary_category": "shortage",
                "taxonomy": "1.0",
            },
            {
                "primary_category": "finance",
                "secondary_category": "capital",
                "tertiary_category": "liquidity",
                "taxonomy": "2.0",
            },
        ],
    )

    report = _audit(tmp_path, ProviderDataset.RISK_TAXONOMY)

    assert report["status"] == "failed"
    assert report["summary"]["corruption_code_counts"][
        "taxonomy_version_ambiguous"
    ] == 2


def test_missing_candidate_key_fails_candidate_key_gate(tmp_path: Path) -> None:
    _write_request(
        tmp_path,
        _formal_request(ProviderDataset.FLOAT),
        [{"effective_date": "2026-06-30", "free_float": 100}],
    )

    report = _audit(tmp_path, ProviderDataset.FLOAT)

    assert report["status"] == "failed"
    assert report["gates"]["candidate_key_consistency"] == "failed"
    assert report["summary"]["corruption_code_counts"]["missing_candidate_key"] == 1


def test_semantic_response_envelope_requires_status_and_request_id(tmp_path: Path) -> None:
    manifest_path = _write_request(
        tmp_path,
        _formal_request(ProviderDataset.SPLITS),
        [{"id": "split-1"}],
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifact_path = tmp_path / manifest["artifacts"][0]["path"]
    artifact_path.write_bytes(gzip.compress(b'{"results": [{"id": "split-1"}]}', mtime=0))

    report = _audit(tmp_path, ProviderDataset.SPLITS)

    assert report["status"] == "failed"
    assert report["summary"]["corruption_code_counts"]["response_envelope_invalid"] == 1


def test_grouped_daily_rows_use_ticker_timestamp_candidate_key_and_value_contract(
    tmp_path: Path,
) -> None:
    request = _formal_request(ProviderDataset.DAILY_BARS)
    timestamp = int(datetime(2026, 6, 30, tzinfo=UTC).timestamp() * 1000)
    _write_request(
        tmp_path,
        request,
        [
            {
                "T": "AAPL",
                "t": timestamp,
                "o": 200.0,
                "h": 205.0,
                "l": 198.0,
                "c": 203.0,
                "v": 1_000_000,
                "vw": 202.5,
            }
        ],
    )

    report = _audit(tmp_path, ProviderDataset.DAILY_BARS)

    assert report["status"] == "passed"
    assert report["uniqueness"]["daily_bars"]["distinct_candidate_keys"] == 1
    assert report["dataset_windows"]["daily_bars"]["start"] == "2026-06-30"


def test_grouped_daily_rows_reject_missing_vwap_even_when_candidate_key_is_present(
    tmp_path: Path,
) -> None:
    request = _formal_request(ProviderDataset.DAILY_BARS)
    timestamp = int(datetime(2026, 6, 30, tzinfo=UTC).timestamp() * 1000)
    _write_request(
        tmp_path,
        request,
        [
            {
                "T": "AAPL",
                "t": timestamp,
                "o": 200.0,
                "h": 205.0,
                "l": 198.0,
                "c": 203.0,
                "v": 1_000_000,
            }
        ],
    )

    report = _audit(tmp_path, ProviderDataset.DAILY_BARS)

    assert report["status"] == "failed"
    assert report["summary"]["corruption_code_counts"]["row_contract_invalid"] == 1


def test_legacy_financial_candidate_key_includes_end_date(tmp_path: Path) -> None:
    common = {
        "cik": "0000320193",
        "filing_date": "2009-04-01",
        "timeframe": "annual",
    }
    _write_legacy_financial_history(
        tmp_path,
        [
            {
                **common,
                "end_date": "2008-12-31",
                "financials": {"income_statement": {"revenues": {"value": 1}}},
            },
            {
                **common,
                "end_date": "2009-03-31",
                "financials": {"income_statement": {"revenues": {"value": 2}}},
            },
        ],
    )

    report = _audit(tmp_path, ProviderDataset.LEGACY_FINANCIALS)

    assert report["status"] == "passed"
    assert report["uniqueness"]["legacy_financials"] == {
        "conflict_examples": [],
        "conflicting_keys": 0,
        "distinct_candidate_keys": 2,
        "duplicate_examples": [],
        "exact_duplicate_excess_rows": 0,
    }


def test_legacy_financial_same_period_conflict_and_missing_end_date_fail(
    tmp_path: Path,
) -> None:
    common = {
        "cik": "0000320193",
        "end_date": "2008-12-31",
        "filing_date": "2009-04-01",
        "timeframe": "annual",
    }
    _write_legacy_financial_history(
        tmp_path,
        [
            {**common, "financials": {"income_statement": {"revenues": {"value": 1}}}},
            {**common, "financials": {"income_statement": {"revenues": {"value": 2}}}},
            {
                "cik": "0000789019",
                "filing_date": "2009-04-02",
                "financials": {"income_statement": {"revenues": {"value": 3}}},
                "timeframe": "annual",
            },
        ],
    )

    report = _audit(tmp_path, ProviderDataset.LEGACY_FINANCIALS)

    assert report["status"] == "failed"
    assert report["uniqueness"]["legacy_financials"]["conflicting_keys"] == 1
    assert report["summary"]["corruption_code_counts"] == {
        "conflicting_candidate_keys": 1,
        "missing_candidate_key": 1,
        "row_contract_invalid": 1,
    }
