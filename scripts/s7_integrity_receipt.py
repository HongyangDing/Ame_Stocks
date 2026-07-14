"""Reproduce the read-only S7 release and control-file integrity receipt."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, sha256_file, stable_digest
from ame_stocks_api.silver.asset_release_set import (
    require_asset_release_set_membership,
)
from ame_stocks_api.silver.contracts import (
    ArtifactRef,
    ArtifactRole,
    SourceInventory,
    SourceInventoryItem,
)
from ame_stocks_api.silver.reader import (
    PublishedAssetEvidenceReader,
    PublishedSilverReader,
)
from ame_stocks_api.silver.store import SilverStore
from ame_stocks_api.silver.ticker_overview_source import (
    load_ticker_overview_coverage_receipt,
    ticker_overview_lifecycle_plan_bytes,
)

DATA_ROOT = Path("/mnt/HC_Volume_106309665/american_stocks")
S4_RELEASE_SET_ID = (
    "f81c7ee28939db3350fce809326723e911b6d486c6db166d2575fcc92cb2101d"
)
S4_RELEASE_SET_SHA256 = (
    "937eaf4ed502fb2786dafb0dce9ec613bcaccb2cd488812cc5900118238d6c13"
)
ARTIFACT_REFS_DIGEST_VERSION = "s7_release_output_groups_v1"
RELEASE_BUNDLE_DIGEST_VERSION = "s7_six_release_receipts_v1"
RELEASES = {
    "asset_observation_daily": {
        "kind": "protected_s4",
        "release_id": "26819530e50cb92cbe0ec833d4b731b959c8bd2463ee2197255c02994241d44c",
        "release_sha256": "f5fb26e75f44382caddf980e8fdf88a77903465b55bfd367f8d9029852848084",
    },
    "asset_observation_version": {
        "kind": "protected_s4",
        "release_id": "b422fd05df859b33587b8ece80d078247dd972d01d272710ef49c3529b0e54be",
        "release_sha256": "0ea30b7cf2338e6067b82eff455a3973c59fcc20b433a0de9ba486ec9d8deaf3",
    },
    "universe_source_daily": {
        "kind": "protected_s4",
        "release_id": "c7e0d9a75857cbca130ba8873a737411ccb2f11d3e711ee0c0b0d9d0e2f5c614",
        "release_sha256": "6b2c6ca1b612c4c38ddc8e359c1402c177a4f19b0295604d42b78bcd5804596d",
    },
    "ticker_event_request_status": {
        "kind": "published_silver",
        "release_id": "afc63db6850fb50295daa8e6e499c52fe1c16b8290b7932b08aea67531ff98eb",
        "release_sha256": "29a8c5dbe1de1fbdc819a8e8a08f998967cde2ea19c3bb56e94b34bdea9fdb11",
    },
    "ticker_change_event": {
        "kind": "published_silver",
        "release_id": "18a7eb3dd6805b94151f5b6ce0167c19dbeb328f45bec7c2f806dac42b8a6350",
        "release_sha256": "34cff4cdacbdace305f5ee541c101112a5a7f7fb4e572a3c2405509cf178ba50",
    },
    "ticker_overview_safe": {
        "kind": "published_silver",
        "release_id": "8715f90d0e01f990e9738b9266edfeb2830a76d59a00ae4fb7490d9f077092a5",
        "release_sha256": "a830ad88706393db8b28534379538149aa676e254ca87fd9cbb046ce4d2b51fe",
    },
}
S6_BUILD_ID = "f9e66da7f8aa86f9a2eacff4ee745874776f52d62182d3554d99c7f9b5b90ec0"
S6_BUILD_MANIFEST_SHA256 = (
    "b616b32bac23124d367dc7e5493130c101f76f222d179b6049c5ac813e1390e0"
)
S6_OVERVIEW_INVENTORY_ID = (
    "5503057d5e575e3827bf53599ee342f7ad6d2d8328cf20a127b08ec5c1fc8c03"
)
S6_OVERVIEW_INVENTORY_SHA256 = (
    "822eeaa395e327f11c2b59472619e6b13425ccbf0a16eee5219c664fa50f62e7"
)
S6_LIFECYCLE_INVENTORY_ID = (
    "b566cd78a7d65d9d986edbb3d538b567b03dd1b6efe898b3df994c35f5668076"
)
S6_LIFECYCLE_INVENTORY_SHA256 = (
    "321dfe2c548609b23a2defa9bb2792c4aa5d2943adc4c96be2e0eaecab5d965a"
)
S6_COVERAGE_RECEIPT_PATH = (
    "manifests/silver/source-coverage/ticker_overview/"
    "coverage-01b34fb0f08df51d67ef5124154a2e9026ed5a3621ec060f298440a0ac608a6b.json"
)
S6_COVERAGE_RECEIPT_SHA256 = (
    "b771d67e3c0d6139a31766c2b2ffb431292d1d896a4e593a7c100fcaec552ae7"
)
LIFECYCLE_PATH = DATA_ROOT / (
    "manifests/plans/ticker_overview/"
    "lifecycles-2016-07-11_2026-07-09.jsonl"
)
S6_QUARANTINE_PATH = DATA_ROOT / (
    "silver/schema=v1/identity/ticker_overview_safe/"
    f"build_id={S6_BUILD_ID}/quarantine/quarantine-record.parquet"
)


def artifact_refs_digest(receipts: Sequence[Mapping[str, object]]) -> str:
    """Commit to all release outputs through their exact per-release output digests."""

    groups = [
        {
            "artifact_count": receipt["artifact_count"],
            "outputs_digest": receipt["outputs_digest"],
            "table": receipt["table"],
        }
        for receipt in receipts
    ]
    return stable_digest(
        {
            "artifact_refs_digest_version": ARTIFACT_REFS_DIGEST_VERSION,
            "release_output_groups": groups,
        }
    )


def release_bundle_digest(receipts: Sequence[Mapping[str, object]]) -> str:
    """Commit to the complete, ordered six-release receipt payload."""

    return stable_digest(
        {
            "release_bundle_digest_version": RELEASE_BUNDLE_DIGEST_VERSION,
            "release_receipts": list(receipts),
        }
    )


def _load_inventory(
    relative_path: str,
    *,
    expected_sha256: str,
    expected_inventory_id: str,
) -> tuple[SourceInventory, dict[str, object]]:
    path = safe_relative_path(DATA_ROOT, relative_path)
    content = path.read_bytes()
    assert sha256_file(path) == expected_sha256
    inventory = SourceInventory.from_dict(json.loads(content))
    assert inventory.inventory_id == expected_inventory_id
    assert path.name == f"inventory-{inventory.inventory_id}.json"
    return inventory, {
        "bytes": len(content),
        "inventory_id": inventory.inventory_id,
        "path": relative_path,
        "sha256": expected_sha256,
    }


def _source_item(source: ArtifactRef) -> SourceInventoryItem:
    return SourceInventoryItem(
        path=source.path,
        sha256=source.sha256,
        bytes=source.bytes,
        row_count=int(source.row_count or 0),
        media_type=source.media_type,
        table=source.table,
        schema_digest=source.schema_digest,
    )


def verify_s4_release_set() -> dict[str, object]:
    expected_members = {
        table: (str(spec["release_id"]), str(spec["release_sha256"]))
        for table, spec in RELEASES.items()
        if spec["kind"] == "protected_s4"
    }
    first_release_id = next(iter(expected_members.values()))[0]
    release_set = require_asset_release_set_membership(DATA_ROOT, first_release_id)
    assert release_set.release_set_id == S4_RELEASE_SET_ID
    actual_members = {
        member.table: (member.release_id, member.release_sha256)
        for member in release_set.members
    }
    assert actual_members == expected_members
    marker_path = DATA_ROOT / (
        "manifests/silver/release-sets/assets/"
        f"release_set_id={release_set.release_set_id}/manifest.json"
    )
    assert sha256_file(marker_path) == S4_RELEASE_SET_SHA256
    return {
        "bytes": marker_path.stat().st_size,
        "member_release_ids_by_table": {
            table: release_id
            for table, (release_id, _release_sha256) in actual_members.items()
        },
        "path": str(marker_path.relative_to(DATA_ROOT)),
        "release_set_id": release_set.release_set_id,
        "sha256": S4_RELEASE_SET_SHA256,
    }


def release_receipts() -> list[dict[str, object]]:
    store = SilverStore(DATA_ROOT)
    s4_reader = PublishedAssetEvidenceReader(DATA_ROOT)
    silver_reader = PublishedSilverReader(DATA_ROOT)
    receipts: list[dict[str, object]] = []
    for table, expected in RELEASES.items():
        release_id = str(expected["release_id"])
        if expected["kind"] == "protected_s4":
            published = s4_reader.inspect(release_id)
            assert published.backtest_identity_eligible is False
            assert published.publication_scope == "identity_evidence_pending_s7"
        else:
            published = silver_reader.inspect(release_id)
        release, stored = store.load_release(release_id)
        assert stored.sha256 == expected["release_sha256"]
        assert release.table == table
        outputs = tuple(sorted(release.outputs, key=lambda output: output.path))
        receipts.append(
            {
                "artifact_count": len(outputs),
                "build_id": release.build_id,
                "contract_id": published.contract.contract_id,
                "outputs_digest": stable_digest(
                    [output.to_dict() for output in outputs]
                ),
                "release_id": release.release_id,
                "release_manifest_sha256": stored.sha256,
                "row_count": sum(int(output.row_count or 0) for output in outputs),
                "schema_digest": published.contract.schema_digest,
                "stored_bytes": sum(output.bytes for output in outputs),
                "table": table,
            }
        )
    return receipts


def s6_control_receipts() -> dict[str, dict[str, object]]:
    store = SilverStore(DATA_ROOT)
    published = PublishedSilverReader(DATA_ROOT).inspect(
        str(RELEASES["ticker_overview_safe"]["release_id"])
    )
    release = published.release
    build = published.build
    assert release.build_id == build.build_id == S6_BUILD_ID
    assert release.build_manifest_sha256 == S6_BUILD_MANIFEST_SHA256
    assert release.release_id == RELEASES["ticker_overview_safe"]["release_id"]

    parameters = dict(build.intent.parameters)
    assert parameters["overview_inventory_id"] == S6_OVERVIEW_INVENTORY_ID
    assert parameters["lifecycle_inventory_id"] == S6_LIFECYCLE_INVENTORY_ID
    assert parameters["coverage_receipt_path"] == S6_COVERAGE_RECEIPT_PATH
    assert parameters["coverage_receipt_sha256"] == S6_COVERAGE_RECEIPT_SHA256

    lineage = {
        (source.lineage_manifest_path, source.lineage_manifest_sha256)
        for source in build.intent.inputs
    }
    overview_inventory_path = (
        "manifests/silver/source-inventories/ticker_overview/"
        f"inventory-{S6_OVERVIEW_INVENTORY_ID}.json"
    )
    assert lineage == {(overview_inventory_path, S6_OVERVIEW_INVENTORY_SHA256)}
    overview_inventory, overview_receipt = _load_inventory(
        overview_inventory_path,
        expected_sha256=S6_OVERVIEW_INVENTORY_SHA256,
        expected_inventory_id=S6_OVERVIEW_INVENTORY_ID,
    )
    assert tuple(sorted(overview_inventory.artifacts, key=lambda item: item.path)) == tuple(
        sorted((_source_item(source) for source in build.intent.inputs), key=lambda item: item.path)
    )
    store.verify_source_artifacts(tuple(build.intent.inputs), published.contract)

    lifecycle_inventory_path = (
        "manifests/silver/source-inventories/ticker_overview/"
        f"inventory-{S6_LIFECYCLE_INVENTORY_ID}.json"
    )
    lifecycle_inventory, lifecycle_inventory_receipt = _load_inventory(
        lifecycle_inventory_path,
        expected_sha256=S6_LIFECYCLE_INVENTORY_SHA256,
        expected_inventory_id=S6_LIFECYCLE_INVENTORY_ID,
    )
    assert overview_inventory.git_commit == lifecycle_inventory.git_commit
    assert len(lifecycle_inventory.upstream_manifests) == 1
    coverage_ref = lifecycle_inventory.upstream_manifests[0]
    assert (coverage_ref.path, coverage_ref.sha256) == (
        S6_COVERAGE_RECEIPT_PATH,
        S6_COVERAGE_RECEIPT_SHA256,
    )
    assert coverage_ref in overview_inventory.upstream_manifests
    assert len(lifecycle_inventory.artifacts) == 1
    lifecycle_item = lifecycle_inventory.artifacts[0]
    lifecycle_source = ArtifactRef(
        path=lifecycle_item.path,
        sha256=lifecycle_item.sha256,
        bytes=lifecycle_item.bytes,
        row_count=lifecycle_item.row_count,
        media_type=lifecycle_item.media_type,
        role=ArtifactRole.SOURCE,
        source_dataset=lifecycle_inventory.source_dataset,
        source_layer=lifecycle_inventory.source_layer,
        lineage_manifest_path=lifecycle_inventory_path,
        lineage_manifest_sha256=S6_LIFECYCLE_INVENTORY_SHA256,
        table=lifecycle_item.table,
        schema_digest=lifecycle_item.schema_digest,
    )
    store.verify_source_artifacts((lifecycle_source,), published.contract)
    coverage = load_ticker_overview_coverage_receipt(
        DATA_ROOT,
        coverage_receipt_path=coverage_ref.path,
        coverage_receipt_sha256=coverage_ref.sha256,
    )
    coverage_plan = dict(coverage["lifecycle_plan"])
    assert coverage_plan == {
        "bytes": lifecycle_item.bytes,
        "media_type": lifecycle_item.media_type,
        "path": lifecycle_item.path,
        "row_count": lifecycle_item.row_count,
        "sha256": lifecycle_item.sha256,
    }
    plan_content = ticker_overview_lifecycle_plan_bytes(
        DATA_ROOT,
        coverage_receipt_path=coverage_ref.path,
        coverage_receipt_sha256=coverage_ref.sha256,
    )
    assert plan_content == LIFECYCLE_PATH.read_bytes()
    lifecycle_rows = [json.loads(line) for line in plan_content.splitlines()]
    assert len(lifecycle_rows) == 30_739
    assert len({row["lifecycle_id"] for row in lifecycle_rows}) == len(lifecycle_rows)

    quarantine_outputs = [
        output for output in build.outputs if output.role is ArtifactRole.QUARANTINE
    ]
    assert len(quarantine_outputs) == 1
    quarantine_output = quarantine_outputs[0]
    assert safe_relative_path(DATA_ROOT, quarantine_output.path) == S6_QUARANTINE_PATH
    quarantine_path = store.verify_artifact(quarantine_output, contract=None)
    quarantine_rows = pq.read_table(quarantine_path).to_pylist()
    assert len(quarantine_rows) == 169
    assert len({row["source_record_id"] for row in quarantine_rows}) == len(
        quarantine_rows
    )

    common_binding = {
        "bound_build_id": build.build_id,
        "bound_build_manifest_sha256": release.build_manifest_sha256,
        "bound_release_id": release.release_id,
        "bound_release_manifest_sha256": str(
            RELEASES["ticker_overview_safe"]["release_sha256"]
        ),
    }
    return {
        "s6_pending_quarantine": {
            **common_binding,
            "bytes": quarantine_output.bytes,
            "path": quarantine_output.path,
            "row_count": int(quarantine_output.row_count or 0),
            "sha256": quarantine_output.sha256,
            "source_issue_set_digest": stable_digest(
                sorted(
                    (
                        row["source_record_id"],
                        row["issue_code"],
                        row["severity"],
                        row["review_status"],
                    )
                    for row in quarantine_rows
                )
            ),
        },
        "ticker_overview_lifecycle_plan": {
            **common_binding,
            "bound_coverage_receipt_path": coverage_ref.path,
            "bound_coverage_receipt_sha256": coverage_ref.sha256,
            "bound_lifecycle_inventory": lifecycle_inventory_receipt,
            "bound_overview_inventory": overview_receipt,
            "bytes": len(plan_content),
            "identity_set_digest": stable_digest(
                sorted(
                    (
                        row["lifecycle_id"],
                        row["ticker"],
                        row["query_date"],
                        row["identity_type"],
                        row["identity_value"],
                    )
                    for row in lifecycle_rows
                )
            ),
            "path": str(LIFECYCLE_PATH.relative_to(DATA_ROOT)),
            "row_count": len(lifecycle_rows),
            "sha256": sha256_file(LIFECYCLE_PATH),
        },
    }


def build_receipt() -> dict[str, object]:
    s4_release_set = verify_s4_release_set()
    receipts = release_receipts()
    return {
        "bundle_integrity": {
            "artifact_refs_digest": artifact_refs_digest(receipts),
            "artifact_refs_digest_version": ARTIFACT_REFS_DIGEST_VERSION,
            "release_bundle_digest": release_bundle_digest(receipts),
            "release_bundle_digest_version": RELEASE_BUNDLE_DIGEST_VERSION,
            "verified_artifact_count": sum(
                int(receipt["artifact_count"]) for receipt in receipts
            ),
        },
        "control_files": s6_control_receipts(),
        "release_receipts": receipts,
        "s4_release_set": s4_release_set,
        "write_boundary": {
            "bronze_or_published_silver_modified": False,
            "file_system_outputs": 0,
        },
    }


if __name__ == "__main__":
    print(json.dumps(build_receipt(), indent=2, sort_keys=True))
