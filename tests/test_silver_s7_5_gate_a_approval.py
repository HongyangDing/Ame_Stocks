from __future__ import annotations

import hashlib
import json
from pathlib import Path

from ame_stocks_api.artifacts import stable_digest

REPO_ROOT = Path(__file__).resolve().parents[1]
APPROVAL_EVENT_ID = "4e04a6d7865c940740f214967247eea6af0ed3d5c4b4ca5b3f95b4023460722d"
APPROVAL_PATH = (
    REPO_ROOT
    / "docs/silver/decisions/s7_5/gate_a"
    / f"approval_event_id={APPROVAL_EVENT_ID}/manifest.json"
)
CANDIDATE_PATH = (
    REPO_ROOT / "docs/silver/contracts/control/s7_5_incremental_contract_bundle-v1.candidate.json"
)


def test_gate_a_approval_binds_exact_candidate_and_keeps_runtime_disabled() -> None:
    approval = json.loads(APPROVAL_PATH.read_text(encoding="utf-8"))
    candidate_bytes = CANDIDATE_PATH.read_bytes()
    candidate = json.loads(candidate_bytes)

    logical = dict(approval)
    assert logical.pop("approval_event_id") == APPROVAL_EVENT_ID
    assert stable_digest(logical) == APPROVAL_EVENT_ID
    assert APPROVAL_PATH.parent.name == f"approval_event_id={APPROVAL_EVENT_ID}"

    approved_candidate = approval["approved_candidate"]
    assert approved_candidate == {
        "candidate_sha256": hashlib.sha256(candidate_bytes).hexdigest(),
        "contract_id": candidate["contract_id"],
        "path": "docs/silver/contracts/control/s7_5_incremental_contract_bundle-v1.candidate.json",
    }
    assert approval["approval_literal"] == "批准Gate A"
    assert set(approval["runtime_capabilities_remain_false"].values()) == {False}
    assert "production_or_remote_parquet_content_read" in approval["denied_scope"]
    assert "i2_local_s4_single_session_incremental_implementation" in approval["authorized_scope"]
