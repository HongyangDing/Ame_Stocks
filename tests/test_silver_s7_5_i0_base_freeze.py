from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path, PurePosixPath

import pytest

from ame_stocks_api.artifacts import stable_digest

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "docs/silver/contracts/control/s7_5_i0_base_freeze.schema-v1.json"
FREEZE_ID = "da74c44f426310bcc6519c11751bc87352884c1be88caedb73d817bcf3a62f79"
MANIFEST_PATH = (
    ROOT / "docs/silver/decisions/s7_5/i0" / f"base_freeze_id={FREEZE_ID}" / "manifest.json"
)
HEX_64 = re.compile(r"^[0-9a-f]{64}$")


def _load(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _assert_safe_relative(path: str) -> None:
    value = PurePosixPath(path)
    assert not value.is_absolute()
    assert value.as_posix() == path
    assert ".." not in value.parts


def _all_data_file_pins(payload: dict[str, object]) -> list[dict[str, object]]:
    release_set = payload["release_set"]
    source = payload["source_snapshot"]
    completion = payload["full_completion"]
    quality = payload["quality"]
    performance = payload["performance_baseline"]
    assert isinstance(release_set, dict)
    assert isinstance(source, dict)
    assert isinstance(completion, dict)
    assert isinstance(quality, dict)
    assert isinstance(performance, dict)
    s4_release = source["s4_release_set"]
    profile = performance["profile_completion"]
    assert isinstance(s4_release, dict)
    assert isinstance(profile, dict)
    pins = [
        release_set["manifest"],
        source["source_binding_manifest"],
        s4_release["manifest"],
        completion["manifest"],
        completion["candidate_manifest"],
        quality["qa"],
        profile["manifest"],
    ]
    pins.extend(member["manifest"] for member in payload["members"])
    pins.extend(registry["manifest"] for registry in payload["registry_bundle"]["registry_pins"])
    assert all(isinstance(pin, dict) for pin in pins)
    return pins


def test_i0_freeze_is_content_addressed_closed_and_read_only() -> None:
    schema = _load(SCHEMA_PATH)
    manifest = _load(MANIFEST_PATH)

    assert schema["additionalProperties"] is False
    assert schema["properties"]["artifact_type"]["const"] == "s7_5_i0_base_freeze"
    assert set(manifest) == {
        "artifact_type",
        "schema_version",
        "base_freeze_id",
        "logical_payload",
        "captured_at_utc",
        "captured_by",
    }
    assert manifest["artifact_type"] == "s7_5_i0_base_freeze"
    assert manifest["schema_version"] == 1
    assert manifest["captured_by"] == "ame_stocks_s7_5_i0_read_only_capture_v1"
    payload = manifest["logical_payload"]
    assert manifest["base_freeze_id"] == FREEZE_ID == stable_digest(payload)
    assert MANIFEST_PATH.parent.name == f"base_freeze_id={FREEZE_ID}"

    assert payload["base_role"] == "immutable_equivalence_oracle"
    assert payload["resolved_view"] == "latest_reviewed_research_at_source_cutoff"
    assert payload["next_gate"] == "i1_contract_design"
    assert payload["capabilities"] == {
        "base_mutation": False,
        "incremental_execution": False,
        "parquet_content_read": False,
        "publish": False,
        "registry_mutation": False,
        "s7_execution": False,
        "s8_execution": False,
    }


def test_i0_freeze_pins_the_published_chain_and_semantic_oracles() -> None:
    payload = _load(MANIFEST_PATH)["logical_payload"]
    release = payload["release_set"]
    source = payload["source_snapshot"]
    completion = payload["full_completion"]
    quality = payload["quality"]

    assert release == {
        "available_session": "2026-08-03",
        "manifest": {
            "bytes": 4668,
            "path": (
                "manifests/silver/identity/s7-four-table-release-sets/"
                "release_set_id=5ce4ad18b44d86fe70fd25c50d1023fb1aa39f25f50fa2f93a0a1c4452eb811e/"
                "manifest.json"
            ),
            "sha256": "3690046fc32801dc23e85d4713d90b476b188988ec2426bd1d1c13fcdd9f1c0b",
        },
        "published_at_utc": "2026-08-02T00:58:43.178962+00:00",
        "release_set_id": "5ce4ad18b44d86fe70fd25c50d1023fb1aa39f25f50fa2f93a0a1c4452eb811e",
        "release_set_version": 1,
        "source_binding_id": "9b895f73f7987a92b4208ba22e957e0f5931d5a11b2622642e80f5a0109d97a9",
        "source_cutoff_session": "2026-07-29",
        "state": "published",
        "visibility_rule": "all_four_members_visible_only_through_this_exact_marker_v1",
    }
    assert source["session_count"] == 2513
    assert source["membership_row_count"] == 69_376_329
    assert source["source_cutoff_session"] == release["source_cutoff_session"]
    assert source["source_binding_id"] == release["source_binding_id"]
    assert completion["complete"] is True
    assert quality["critical_failure_count"] == 0
    assert quality["source_membership_omission_or_duplication_rows"] == 0
    assert quality["missing_eligible_alias_rows"] == 0
    assert quality["multi_registry_composite_override_collision_rows"] == 0
    assert quality["unapproved_canonical_override_rows"] == 0
    assert quality["unknown_or_unapproved_foreign_identity_eligible_rows"] == 0
    assert quality["identity_quality_forced_liquidation_rows"] == 0

    members = payload["members"]
    assert [row["table_name"] for row in members] == [
        "asset_master",
        "ticker_alias",
        "issuer_master",
        "universe_daily",
    ]
    assert [row["row_count"] for row in members] == [14_865, 33_081, 14_955, 69_376_329]
    assert sum(row["output_bytes"] for row in members) == 8_689_015_118
    assert members[-1]["partition_count"] == 2513
    assert members[-1]["first_partition_session"] == "2016-07-11"
    assert members[-1]["last_partition_session"] == "2026-07-09"

    registry_pins = payload["registry_bundle"]["registry_pins"]
    assert [row["registry_name"] for row in registry_pins] == [
        "identity_adjudication",
        "identity_cross_market_adjudication",
        "provider_composite_override",
        "share_class_adjudication",
        "asset_transition",
    ]
    source_projection = [
        {
            "manifest_bytes": row["manifest"]["bytes"],
            "manifest_path": row["manifest"]["path"],
            "manifest_sha256": row["manifest"]["sha256"],
            "registry_name": row["registry_name"],
            "release_available_session": row["release_available_session"],
            "release_id": row["release_id"],
        }
        for row in registry_pins
    ]
    assert stable_digest(source_projection) == payload["registry_bundle"]["registry_pins_digest"]


def test_i0_repository_pins_fixtures_and_measurements_are_frozen() -> None:
    payload = _load(MANIFEST_PATH)["logical_payload"]
    for pin in payload["document_pins"]:
        _assert_safe_relative(pin["path"])
        data = (ROOT / pin["path"]).read_bytes()
        assert len(data) == pin["bytes"]
        assert hashlib.sha256(data).hexdigest() == pin["sha256"]

    fixture_types = {row["case_type"] for row in payload["fixture_index"]}
    assert fixture_types == {
        "normal_session",
        "half_day",
        "new_ticker",
        "ticker_gap",
        "identity_collision",
        "historical_correction",
    }
    performance = payload["performance_baseline"]
    assert performance["stage_seconds"]["full_run"] == 78_501.446429
    assert performance["stage_seconds"]["publish_verify"] == 22_159.649902
    assert performance["full_resource_contract"]["peak_rss_bytes"] is None
    assert (
        performance["full_resource_contract"]["peak_rss_measurement_status"]
        == "not_recorded_by_v1_completion"
    )

    serialized = json.dumps(payload, sort_keys=True).lower()
    assert "massive_api_key" not in serialized
    assert "5tra" not in serialized
    for pin in _all_data_file_pins(payload):
        _assert_safe_relative(pin["path"])
        assert pin["bytes"] > 0
        assert HEX_64.fullmatch(pin["sha256"])


@pytest.mark.skipif(
    not os.environ.get("AME_STOCKS_DATA_ROOT"),
    reason="set AME_STOCKS_DATA_ROOT for the remote manifest-only pin check",
)
def test_i0_remote_manifest_pins_and_cross_links_are_still_exact() -> None:
    data_root = Path(os.environ["AME_STOCKS_DATA_ROOT"]).resolve()
    payload = _load(MANIFEST_PATH)["logical_payload"]
    for pin in _all_data_file_pins(payload):
        path = data_root / pin["path"]
        assert path.is_file()
        assert path.stat().st_size == pin["bytes"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == pin["sha256"]

    release = _load(data_root / payload["release_set"]["manifest"]["path"])
    source = _load(data_root / payload["source_snapshot"]["source_binding_manifest"]["path"])
    completion = _load(data_root / payload["full_completion"]["manifest"]["path"])
    qa = _load(data_root / payload["quality"]["qa"]["path"])
    expected_binding = payload["release_set"]["source_binding_id"]
    assert release["state"] == "published"
    assert release["release_set_id"] == payload["release_set"]["release_set_id"]
    assert release["source_binding_id"] == expected_binding
    assert source["source_binding_id"] == expected_binding
    assert completion["source_binding_id"] == expected_binding
    assert completion["complete"] is True
    assert qa["critical_failure_count"] == 0
    assert source["session_count"] == qa["session_count"] == 2513
    assert source["row_count"] == qa["source_membership_rows"] == 69_376_329
    assert {member["table_name"]: member["release_id"] for member in release["members"]} == {
        member["table_name"]: member["release_id"] for member in payload["members"]
    }
