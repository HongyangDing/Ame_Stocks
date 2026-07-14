from __future__ import annotations

import json
from pathlib import Path

from ame_stocks_api.silver.contracts import QASeverity, QAStatus, TableContract
from ame_stocks_api.silver.ticker_overview_contract import (
    TICKER_OVERVIEW_SAFE_CONTRACT,
    TICKER_OVERVIEW_SAFE_CONTRACT_ID,
)

_ROOT = Path(__file__).resolve().parents[1]
_CANDIDATE = _ROOT / "docs/silver/contracts/identity/ticker_overview_safe.schema-v1.candidate.json"
_RESOURCE = (
    _ROOT / "backend/ame_stocks_api/silver/schema_resources/ticker_overview_safe.schema-v1.json"
)


def test_s6_candidate_is_valid_packaged_and_frozen() -> None:
    candidate = TableContract.from_dict(json.loads(_CANDIDATE.read_text(encoding="utf-8")))

    assert candidate == TICKER_OVERVIEW_SAFE_CONTRACT
    assert candidate.contract_id == TICKER_OVERVIEW_SAFE_CONTRACT_ID
    assert candidate.contract_id == (
        "f4e873e6595fee0a66362a0d39b3f7c36176b95354ecad93453613f7ac84ca3c"
    )
    assert candidate.schema_digest == (
        "228404866f33e709fc75e2b50f1ce022602e1b833b84f63315e009a3e07a8643"
    )
    assert _CANDIDATE.read_bytes() == _RESOURCE.read_bytes()
    assert (candidate.domain, candidate.table, candidate.schema_version) == (
        "identity",
        "ticker_overview_safe",
        1,
    )
    assert candidate.source_datasets == ("ticker_overview",)


def test_s6_contract_freezes_lifecycle_grain_temporal_boundary_and_direct_lineage() -> None:
    contract = TICKER_OVERVIEW_SAFE_CONTRACT
    columns = {item.name: item for item in contract.columns}

    assert len(columns) == 47
    assert contract.primary_key == ("lifecycle_id",)
    assert contract.partition_by == ("source_capture_date",)
    assert contract.sort_by == (
        "source_capture_date",
        "query_date",
        "query_ticker",
        "lifecycle_id",
    )
    assert "server-side" in columns["query_date"].description
    assert "Always false" in columns["backtest_identity_eligible"].description
    assert (
        "retrospective_historical_query_without_archived_vintage_v1"
        in columns["source_availability_quality"].description
    )
    assert {
        "source_manifest_path",
        "source_manifest_sha256",
        "source_artifact_path",
        "source_artifact_sha256",
        "source_artifact_raw_sha256",
        "source_result_hash",
        "source_pointer",
    }.issubset(columns)
    assert not {
        "market_cap",
        "share_class_shares_outstanding",
        "weighted_shares_outstanding",
    }.intersection(columns)


def test_s6_contract_freezes_critical_high_and_medium_policy() -> None:
    rules = {item.check_id: item for item in TICKER_OVERVIEW_SAFE_CONTRACT.qa_rules}

    assert len(rules) == 26
    assert rules["unresolved_identity_rows"].severity is QASeverity.HIGH
    assert rules["unresolved_identity_rows"].failure_status is QAStatus.WARNING
    assert rules["sic_code_missing_rows"].severity is QASeverity.MEDIUM
    assert rules["list_date_missing_rows"].severity is QASeverity.MEDIUM
    assert rules["retrospective_query_without_archived_vintage_rows"].severity is (
        QASeverity.MEDIUM
    )
    assert all(type(item.limit) is float and item.limit == 0.0 for item in rules.values())
    assert all(
        item.failure_status is QAStatus.FAILED
        for key, item in rules.items()
        if key
        not in {
            "unresolved_identity_rows",
            "sic_code_missing_rows",
            "list_date_missing_rows",
            "retrospective_query_without_archived_vintage_rows",
        }
    )
