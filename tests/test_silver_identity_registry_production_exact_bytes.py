from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ame_stocks_api.silver.identity_market_sequence as gate_c_module
import ame_stocks_api.silver.identity_registry_production as production
from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.identity_market_sequence import (
    REVIEWED_EVIDENCE_SCHEMA,
    REVIEWED_FOREIGN_ROW_COUNT,
    S7MarketSequenceResourceCaps,
)
from ame_stocks_api.silver.identity_registry_workflow import StoredControlDocument


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )


def _write(root: Path, relative: str, content: bytes) -> dict[str, object]:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return {
        "bytes": len(content),
        "path": relative,
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def _reviewed_rows(ticker: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for ordinal in range(REVIEWED_FOREIGN_ROW_COUNT):
        session = date(2022, 1, 1) + timedelta(days=ordinal)
        source_id = stable_digest({"ordinal": ordinal, "source": "reviewed", "ticker": ticker})
        case_id = stable_digest({"case": ticker})
        row: dict[str, object] = {}
        for field in REVIEWED_EVIDENCE_SCHEMA:
            if pa.types.is_string(field.type):
                row[field.name] = "fixture"
            elif pa.types.is_boolean(field.type):
                row[field.name] = False
            elif pa.types.is_int64(field.type):
                row[field.name] = ordinal
            elif pa.types.is_date32(field.type):
                row[field.name] = session
            elif pa.types.is_list(field.type):
                row[field.name] = []
            else:  # pragma: no cover - the reviewed evidence schema is fixed
                raise AssertionError(field.type)
        row.update(
            {
                "provider": "massive",
                "market": "stocks",
                "locale": "us",
                "ticker": ticker,
                "session_date": session,
                "observed_composite_figi": "BBG000000001",
                "observed_share_class_figi": "BBG000000002",
                "primary_exchange_mic": "XNAS",
                "selected_source_record_id": source_id,
                "related_identity_case_ids": [case_id],
                "related_case_resolution_roles": ["contaminated_middle_episode"],
                "related_case_bindings_json": json.dumps(
                    {
                        "related_cases": [
                            {
                                "identity_case_id": case_id,
                                "identity_case_resolution_role": ("contaminated_middle_episode"),
                            }
                        ]
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            }
        )
        rows.append(row)
    return rows


def _gate_c_fixture(root: Path) -> tuple[StoredControlDocument, Path, Path, SimpleNamespace]:
    reviewed_relative = "gate-c/evidence/reviewed.parquet"
    reviewed_path = root / reviewed_relative
    reviewed_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(_reviewed_rows("AZPN"), schema=REVIEWED_EVIDENCE_SCHEMA),
        reviewed_path,
        compression="zstd",
    )
    reviewed_content = reviewed_path.read_bytes()
    reviewed_ref = {
        "bytes": len(reviewed_content),
        "path": reviewed_relative,
        "row_count": REVIEWED_FOREIGN_ROW_COUNT,
        "schema_digest": stable_digest(str(REVIEWED_EVIDENCE_SCHEMA)),
        "sha256": hashlib.sha256(reviewed_content).hexdigest(),
    }

    candidate_id = "a" * 64
    manifest_id = "b" * 64
    candidate_relative = "gate-c/candidate.json"
    candidate_document = {
        "availability": {"candidate_available_session": "2026-07-20"},
        "candidate_id": candidate_id,
        "manifest_id": manifest_id,
        "outputs": {"reviewed_foreign_source_evidence": reviewed_ref},
        "registry_loader_source_refs": {
            "detector_preview": {
                "preview_artifact_id": "c" * 64,
                "sha256": "d" * 64,
            },
            "reviewed_external_evidence": {
                "manifest_id": "e" * 64,
                "sha256": "f" * 64,
            },
        },
    }
    candidate_content = _canonical(candidate_document)
    candidate_ref = {
        **_write(root, candidate_relative, candidate_content),
        "candidate_id": candidate_id,
        "manifest_id": manifest_id,
        "state": "awaiting_review",
    }

    plan_id = "1" * 64
    plan_relative = "gate-c/plan.json"
    plan_document = {
        "plan_id": plan_id,
        "resource_caps": S7MarketSequenceResourceCaps().to_dict(),
    }
    plan_content = _canonical(plan_document)
    plan_receipt = _write(root, plan_relative, plan_content)
    completion_id = "2" * 64
    completion_relative = "gate-c/completion.json"
    completion_document = {
        "authorization": {
            "authorization_id": "3" * 64,
            "path": "gate-c/authorization.json",
            "sha256": "4" * 64,
        },
        "candidate": candidate_ref,
        "completion_id": completion_id,
        "plan": {
            "path": plan_relative,
            "plan_id": plan_id,
            "sha256": plan_receipt["sha256"],
        },
    }
    completion_content = _canonical(completion_document)
    _write(root, completion_relative, completion_content)
    completion_ref = StoredControlDocument(
        object_id=completion_id,
        path=completion_relative,
        sha256=hashlib.sha256(completion_content).hexdigest(),
        bytes=len(completion_content),
    )
    result = SimpleNamespace(
        candidate_id=candidate_id,
        completion_id=completion_id,
        completion_sha256=completion_ref.sha256,
        manifest_path=candidate_relative,
        reviewed_evidence_path=reviewed_relative,
        reviewed_foreign_row_count=REVIEWED_FOREIGN_ROW_COUNT,
    )
    return completion_ref, root / candidate_relative, reviewed_path, result


@pytest.mark.parametrize("replaced_artifact", ["candidate", "reviewed_evidence"])
def test_gate_c_loader_fails_closed_if_path_changes_after_upstream_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replaced_artifact: str,
) -> None:
    completion_ref, candidate_path, reviewed_path, result = _gate_c_fixture(tmp_path)

    def verified_then_replaced(*_args: object, **_kwargs: object) -> SimpleNamespace:
        if replaced_artifact == "candidate":
            document = json.loads(candidate_path.read_bytes())
            document["availability"]["candidate_available_session"] = "2026-07-21"
            candidate_path.write_bytes(_canonical(document))
        else:
            pq.write_table(
                pa.Table.from_pylist(_reviewed_rows("CR"), schema=REVIEWED_EVIDENCE_SCHEMA),
                reviewed_path,
                compression="snappy",
            )
        return result

    monkeypatch.setattr(
        gate_c_module,
        "_candidate_from_completion",
        verified_then_replaced,
    )

    expected_label = (
        "Gate C candidate" if replaced_artifact == "candidate" else "Gate C reviewed evidence"
    )
    with pytest.raises(production.IdentityRegistryProductionError, match=expected_label):
        production._load_gate_c_source(tmp_path, completion_ref)
