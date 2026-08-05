from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    ASSET_COUNTER_NAMES,
    IDENTITY_REGISTRY_ORDER,
    ISSUER_COUNTER_NAMES,
    LEGACY_S7_V1_RELEASE_SET_ID,
    NATIVE_V2_FIXTURE_RELEASE_FAMILY,
    NATIVE_V2_RELEASE_FAMILY,
    AggregateCount,
    AssetAggregateState,
    ExactPinReadCache,
    I3CheckpointError,
    I3CheckpointState,
    IdentityPolicyBundle,
    IdentityRegistryReleasePin,
    IssuerAggregateState,
    NativeV2OutputArtifact,
    NativeV2ParentReleasePin,
    NativeV2ReleaseManifest,
    OpenAliasState,
    ResolvedPartitionState,
    S4TerminalPartitionPin,
    TerminalRowVersionState,
    UnresolvedSubjectState,
    i3_resolved_state_digest,
    load_i3_checkpoint_exact,
    load_native_v2_parent_release_exact,
)
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_CONTRACTS,
    I3_V2_SCHEMA_BUNDLE_DIGEST,
    I3_V2_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_identity import (
    AliasResolutionDisposition,
    AliasResolutionMethod,
    AliasResolutionStatus,
    AliasResolutionVersion,
    AliasSegmentIdentity,
    ShareClassResolutionMethod,
    canonical_asset_id,
    canonical_issuer_id,
    canonical_share_class_id,
)

LAST_SESSION = date(2026, 7, 10)
POLICY_CUTOFF = date(2026, 7, 29)
POLICY_AVAILABLE = date(2026, 8, 3)
CHECKPOINT_AVAILABLE = date(2026, 8, 5)


def _digest(label: str) -> str:
    return stable_digest({"fixture": label})


def _pin(label: str) -> ArtifactPin:
    content = f"{label}\n".encode()
    return ArtifactPin(
        path=f"fixtures/i3/{label}.json",
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _policy_bundle() -> IdentityPolicyBundle:
    return IdentityPolicyBundle(
        tuple(
            IdentityRegistryReleasePin(
                registry_kind=kind,
                release_id=_digest(f"{kind.value}-release"),
                artifact=_pin(f"registry-{kind.value}"),
                decision_cutoff_session=POLICY_CUTOFF,
                release_available_session=date(2026, 8, 2),
            )
            for kind in IDENTITY_REGISTRY_ORDER
        ),
        bundle_available_session=POLICY_AVAILABLE,
    )


def _segment() -> AliasSegmentIdentity:
    return AliasSegmentIdentity(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker="AAPL",
        observed_composite_figi="BBG000B9XRY4",
        observed_share_class_figi="BBG001S5N8V8",
        observed_cik_normalized="0000320193",
        valid_from_session=LAST_SESSION,
        segment_origin_source_record_id=_digest("aapl-origin"),
    )


def _resolution(
    segment: AliasSegmentIdentity, policy: IdentityPolicyBundle
) -> AliasResolutionVersion:
    return AliasResolutionVersion.for_segment(
        segment,
        canonical_asset_id=canonical_asset_id("BBG000B9XRY4"),
        canonical_composite_figi="BBG000B9XRY4",
        canonical_share_class_id=canonical_share_class_id("BBG001S5N8V8"),
        canonical_share_class_figi="BBG001S5N8V8",
        canonical_issuer_id=canonical_issuer_id("0000320193"),
        canonical_cik_normalized="0000320193",
        resolution_method=AliasResolutionMethod.DIRECT_OBSERVED,
        resolution_status=AliasResolutionStatus.RESOLVED,
        disposition=AliasResolutionDisposition.OBSERVED_CONSISTENT,
        decision_lineage_ids=(),
        share_class_resolution_method=ShareClassResolutionMethod.DIRECT_OBSERVED,
        share_class_decision_lineage_ids=(),
        identity_policy_bundle_id=policy.identity_policy_bundle_id,
        identity_cutoff_session=policy.policy_cutoff_session,
        resolution_available_session=POLICY_AVAILABLE,
        evidence_cutoff_session=POLICY_CUTOFF,
        evidence_available_session=POLICY_CUTOFF,
        valid_through_session=LAST_SESSION,
        source_record_set_digest=_digest("aapl-source-records"),
        predecessor_alias_resolution_version_id=None,
        is_tombstone=False,
        tombstone_reason_code=None,
    )


def _counters(names: tuple[str, ...], **nonzero: int) -> tuple[AggregateCount, ...]:
    return tuple(AggregateCount(name=name, value=nonzero.get(name, 0)) for name in names)


def _checkpoint() -> I3CheckpointState:
    policy = _policy_bundle()
    segment = _segment()
    resolution = _resolution(segment, policy)
    asset_id = canonical_asset_id("BBG000B9XRY4")
    issuer_id = canonical_issuer_id("0000320193")
    asset_version_id = _digest("asset-row-version")
    issuer_version_id = _digest("issuer-row-version")
    asset = AssetAggregateState(
        asset_id=asset_id,
        canonical_composite_figi="BBG000B9XRY4",
        canonical_share_class_figi="BBG001S5N8V8",
        canonical_share_class_figis=("BBG001S5N8V8",),
        terminal_row_version_id=asset_version_id,
        first_direct_observed_session=LAST_SESSION,
        last_direct_observed_session=LAST_SESSION,
        first_canonical_membership_session=LAST_SESSION,
        last_canonical_membership_session=LAST_SESSION,
        observed_tickers=("AAPL",),
        observed_composite_figis=("BBG000B9XRY4",),
        observed_share_class_figis=("BBG001S5N8V8",),
        observed_issuer_ids=(issuer_id,),
        identity_adjudication_ids=(),
        genuine_transition_identity_adjudication_ids=(),
        provider_contamination_identity_adjudication_ids=(),
        cross_market_adjudication_ids=(),
        provider_composite_override_ids=(),
        share_class_adjudication_ids=(),
        asset_transition_ids=(),
        predecessor_asset_ids=(),
        successor_asset_ids=(),
        counters=_counters(
            ASSET_COUNTER_NAMES,
            candidate_evidence_row_count=1,
            direct_observed_evidence_row_count=1,
            strong_evidence_row_count=1,
        ),
        source_record_set_digest=_digest("asset-source-records"),
        identity_evidence_available_session=POLICY_CUTOFF,
        state_available_session=CHECKPOINT_AVAILABLE,
    )
    issuer = IssuerAggregateState(
        issuer_id=issuer_id,
        cik_normalized="0000320193",
        terminal_row_version_id=issuer_version_id,
        first_observed_session=LAST_SESSION,
        last_observed_session=LAST_SESSION,
        observed_asset_ids=(asset_id,),
        observed_tickers=("AAPL",),
        reference_names=("Apple Inc.",),
        sic_codes=("3571",),
        counters=_counters(ISSUER_COUNTER_NAMES, source_evidence_row_count=1),
        source_record_set_digest=_digest("issuer-source-records"),
        reference_available_session=LAST_SESSION,
        state_available_session=CHECKPOINT_AVAILABLE,
    )
    terminal_rows = (
        TerminalRowVersionState(
            table_name="asset_master",
            stable_row_key=asset_id,
            row_version_id=asset_version_id,
            predecessor_row_version_id=None,
            row_payload_digest=_digest("asset-row-payload"),
            index_artifact=_pin("asset-row-index"),
            availability_session=CHECKPOINT_AVAILABLE,
        ),
        TerminalRowVersionState(
            table_name="issuer_master",
            stable_row_key=issuer_id,
            row_version_id=issuer_version_id,
            predecessor_row_version_id=None,
            row_payload_digest=_digest("issuer-row-payload"),
            index_artifact=_pin("issuer-row-index"),
            availability_session=CHECKPOINT_AVAILABLE,
        ),
        TerminalRowVersionState(
            table_name="ticker_alias",
            stable_row_key=segment.alias_segment_id,
            row_version_id=resolution.alias_resolution_version_id,
            predecessor_row_version_id=None,
            row_payload_digest=_digest("alias-row-payload"),
            index_artifact=_pin("alias-row-index"),
            availability_session=CHECKPOINT_AVAILABLE,
        ),
    )
    s4_terminal_pins = tuple(
        S4TerminalPartitionPin(
            table_name=table_name,
            session_date=LAST_SESSION,
            partition_receipt_id=_digest(f"{table_name}-receipt"),
            artifact=_pin(f"s4-{table_name}"),
            availability_session=CHECKPOINT_AVAILABLE,
        )
        for table_name in (
            "asset_observation_daily",
            "asset_observation_version",
            "universe_source_daily",
        )
    )
    policy_artifact = policy.exact_pin(path="fixtures/i3/identity-policy-bundle.json")
    open_aliases = (OpenAliasState(segment=segment, resolution=resolution),)
    unresolved = (
        UnresolvedSubjectState(
            subject_kind="ticker_identity",
            subject_key="ZZZZ",
            first_observed_session=LAST_SESSION,
            last_observed_session=LAST_SESSION,
            reason_codes=("pending_review",),
            source_record_set_digest=_digest("unresolved-records"),
            state_available_session=CHECKPOINT_AVAILABLE,
        ),
    )
    resolved_partitions = (
        ResolvedPartitionState(
            session_date=LAST_SESSION,
            partition_receipt_id=_digest("universe-partition-receipt"),
            artifact=_pin("universe-partition"),
            row_count=2,
            availability_session=CHECKPOINT_AVAILABLE,
        ),
    )
    state_digest = i3_resolved_state_digest(
        last_session=LAST_SESSION,
        source_cutoff_session=LAST_SESSION,
        availability_cutoff_session=CHECKPOINT_AVAILABLE,
        s4_terminal_pins=s4_terminal_pins,
        calendar_digest=_digest("calendar"),
        schema_digest=I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=_digest("transform"),
        identity_policy_bundle=policy,
        identity_policy_bundle_artifact=policy_artifact,
        open_aliases=open_aliases,
        asset_aggregates=(asset,),
        issuer_aggregates=(issuer,),
        unresolved_subjects=unresolved,
        resolved_partition_map=resolved_partitions,
        terminal_row_versions=terminal_rows,
    )
    parent_manifest = _parent_manifest(policy, resolved_state_digest=state_digest)
    return I3CheckpointState(
        parent_release=NativeV2ParentReleasePin.from_manifest(
            parent_manifest,
            path="fixtures/i3/native-v2-parent-manifest.json",
        ),
        last_session=LAST_SESSION,
        source_cutoff_session=LAST_SESSION,
        availability_cutoff_session=CHECKPOINT_AVAILABLE,
        s4_terminal_pins=s4_terminal_pins,
        calendar_digest=_digest("calendar"),
        schema_digest=I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=_digest("transform"),
        identity_policy_bundle=policy,
        identity_policy_bundle_artifact=policy_artifact,
        open_aliases=open_aliases,
        asset_aggregates=(asset,),
        issuer_aggregates=(issuer,),
        unresolved_subjects=unresolved,
        resolved_partition_map=resolved_partitions,
        terminal_row_versions=terminal_rows,
    )


def _pin_bytes(path: str, content: bytes) -> ArtifactPin:
    return ArtifactPin(path=path, sha256=hashlib.sha256(content).hexdigest(), bytes=len(content))


def _parent_manifest(
    policy: IdentityPolicyBundle | None = None,
    *,
    resolved_state_digest: str | None = None,
) -> NativeV2ReleaseManifest:
    output_artifacts = tuple(
        NativeV2OutputArtifact(
            table_name=table_name,
            session_date=LAST_SESSION,
            row_count=1,
            contract_id=I3_V2_CONTRACTS[table_name].contract_id,
            schema_digest=I3_V2_CONTRACTS[table_name].schema_digest,
            artifact=_pin(f"native-v2-{table_name}"),
        )
        for table_name in I3_V2_TABLE_ORDER
    )
    return NativeV2ReleaseManifest(
        release_family=NATIVE_V2_FIXTURE_RELEASE_FAMILY,
        terminal_session=LAST_SESSION,
        release_available_session=CHECKPOINT_AVAILABLE,
        native_v2_migration_id=_digest("native-v2-migration"),
        identity_policy_bundle_id=(policy or _policy_bundle()).identity_policy_bundle_id,
        transform_semantics_digest=_digest("transform"),
        resolved_state_digest=resolved_state_digest or _digest("standalone-resolved-state"),
        output_artifacts=output_artifacts,
    )


def _parent_at(checkpoint: I3CheckpointState, available_session: date) -> NativeV2ParentReleasePin:
    manifest = replace(
        _parent_manifest(
            checkpoint.identity_policy_bundle,
            resolved_state_digest=checkpoint.resolved_state_digest,
        ),
        release_available_session=available_session,
    )
    return NativeV2ParentReleasePin.from_manifest(
        manifest,
        path=checkpoint.parent_release.manifest.path,
    )


def test_checkpoint_deterministic_roundtrip_and_exact_loader() -> None:
    checkpoint = _checkpoint()
    content = checkpoint.canonical_bytes()
    pin = checkpoint.exact_pin(path="manifests/silver/i3/checkpoint.json")

    assert I3CheckpointState.from_dict(checkpoint.to_dict()) == checkpoint
    assert checkpoint.canonical_bytes() == content
    assert (
        checkpoint.checkpoint_id == I3CheckpointState.from_dict(checkpoint.to_dict()).checkpoint_id
    )
    manifest = _parent_manifest(
        checkpoint.identity_policy_bundle,
        resolved_state_digest=checkpoint.resolved_state_digest,
    )
    artifacts = {
        pin.path: content,
        checkpoint.parent_release.manifest.path: manifest.canonical_bytes(),
        checkpoint.identity_policy_bundle_artifact.path: (
            checkpoint.identity_policy_bundle.canonical_bytes()
        ),
    }
    assert (
        load_i3_checkpoint_exact(
            pin,
            artifacts.__getitem__,
            expected_release_family=NATIVE_V2_FIXTURE_RELEASE_FAMILY,
        )
        == checkpoint
    )
    assert (
        load_native_v2_parent_release_exact(
            checkpoint.parent_release,
            artifacts.__getitem__,
            expected_release_family=NATIVE_V2_FIXTURE_RELEASE_FAMILY,
        )
        == manifest
    )
    with pytest.raises(I3CheckpointError, match="production native-v2 authority"):
        load_i3_checkpoint_exact(
            pin,
            artifacts.__getitem__,
            expected_release_family="s7_5_native_v2",
        )


def test_native_v2_parent_reproduces_manifest_and_separates_fixture_family() -> None:
    manifest = _parent_manifest()
    parent = NativeV2ParentReleasePin.from_manifest(
        manifest,
        path="fixtures/i3/native-v2-release.json",
    )

    assert parent.release_family == NATIVE_V2_FIXTURE_RELEASE_FAMILY
    assert parent.release_id == manifest.release_id
    assert parent.output_content_digest == manifest.output_content_digest
    assert NativeV2ReleaseManifest.from_dict(manifest.to_dict()) == manifest

    with pytest.raises(I3CheckpointError, match="production native-v2 authority"):
        load_native_v2_parent_release_exact(
            parent,
            lambda path: manifest.canonical_bytes(),
            expected_release_family=NATIVE_V2_RELEASE_FAMILY,
        )

    with pytest.raises(I3CheckpointError, match="release ID does not reproduce"):
        replace(parent, release_id=_digest("forged-native-v2-release"))

    with pytest.raises(I3CheckpointError, match="exact four-table output set"):
        replace(manifest, output_artifacts=manifest.output_artifacts[:-1])

    with pytest.raises(I3CheckpointError, match="output session differs"):
        replace(
            manifest,
            output_artifacts=(
                replace(
                    manifest.output_artifacts[0],
                    session_date=date(2026, 7, 8),
                ),
                *manifest.output_artifacts[1:],
            ),
        )

    tampered = json.loads(manifest.canonical_bytes())
    tampered["output_artifacts"][0]["artifact"]["sha256"] = "0" * 64
    tampered_bytes = json.dumps(tampered, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    tampered_pin = _pin_bytes(parent.manifest.path, tampered_bytes)
    with pytest.raises(I3CheckpointError, match="output-content digest does not reproduce"):
        load_native_v2_parent_release_exact(
            replace(parent, manifest=tampered_pin),
            lambda path: tampered_bytes,
            expected_release_family=NATIVE_V2_FIXTURE_RELEASE_FAMILY,
        )

    shared_path = "fixtures/i3/shared-output-and-manifest.json"
    path_collision_manifest = replace(
        manifest,
        output_artifacts=(
            replace(
                manifest.output_artifacts[0],
                artifact=replace(manifest.output_artifacts[0].artifact, path=shared_path),
            ),
            *manifest.output_artifacts[1:],
        ),
    )
    path_collision_parent = NativeV2ParentReleasePin.from_manifest(
        path_collision_manifest,
        path=shared_path,
    )
    with pytest.raises(I3CheckpointError, match="cannot reuse the manifest path"):
        load_native_v2_parent_release_exact(
            path_collision_parent,
            lambda path: path_collision_manifest.canonical_bytes(),
            expected_release_family=NATIVE_V2_FIXTURE_RELEASE_FAMILY,
        )


def test_identity_policy_bundle_has_fixed_order_and_content_address() -> None:
    bundle = _policy_bundle()

    assert tuple(item.registry_kind for item in bundle.registry_releases) == IDENTITY_REGISTRY_ORDER
    assert bundle.decision_cutoff_session == POLICY_CUTOFF
    assert bundle.bundle_available_session == POLICY_AVAILABLE
    assert bundle.policy_cutoff_session == POLICY_AVAILABLE
    assert IdentityPolicyBundle.from_dict(bundle.to_dict()) == bundle
    assert bundle.identity_policy_bundle_id == _policy_bundle().identity_policy_bundle_id
    assert (
        bundle.exact_pin(path="manifests/silver/i3/policy.json").sha256
        == hashlib.sha256(bundle.canonical_bytes()).hexdigest()
    )

    with pytest.raises(I3CheckpointError, match="fixed five-registry order"):
        IdentityPolicyBundle(
            tuple(reversed(bundle.registry_releases)),
            bundle_available_session=bundle.bundle_available_session,
        )

    with pytest.raises(I3CheckpointError, match="availability precedes"):
        replace(
            bundle.registry_releases[0],
            release_available_session=date(2026, 7, 28),
        )

    with pytest.raises(I3CheckpointError, match="precedes a selected registry"):
        replace(bundle, bundle_available_session=date(2026, 8, 1))

    duplicate_release = replace(
        bundle.registry_releases[1],
        release_id=bundle.registry_releases[0].release_id,
    )
    with pytest.raises(I3CheckpointError, match="release IDs must be unique"):
        replace(
            bundle,
            registry_releases=(
                bundle.registry_releases[0],
                duplicate_release,
                *bundle.registry_releases[2:],
            ),
        )


def test_checkpoint_rejects_v1_parent_and_policy_after_checkpoint() -> None:
    checkpoint = _checkpoint()

    with pytest.raises(I3CheckpointError, match="policy bundle artifact does not reproduce"):
        replace(checkpoint, identity_policy_bundle_artifact=_pin("wrong-policy-wrapper"))

    with pytest.raises(I3CheckpointError, match="legacy S7 v1"):
        replace(checkpoint.parent_release, release_id=LEGACY_S7_V1_RELEASE_SET_ID)
    with pytest.raises(I3CheckpointError, match="native-v2 release family"):
        replace(checkpoint.parent_release, release_family="s7_v1")
    with pytest.raises(I3CheckpointError, match="native-v2 schema"):
        replace(checkpoint.parent_release, schema_version=1)
    with pytest.raises(I3CheckpointError, match="wrong immutable v1 oracle"):
        replace(
            checkpoint.parent_release,
            legacy_oracle_release_set_id=_digest("wrong-v1-oracle"),
        )

    late_policy = IdentityPolicyBundle(
        tuple(
            replace(item, release_available_session=date(2026, 8, 6))
            for item in checkpoint.identity_policy_bundle.registry_releases
        ),
        bundle_available_session=date(2026, 8, 6),
    )
    with pytest.raises(I3CheckpointError):
        replace(
            checkpoint,
            identity_policy_bundle=late_policy,
            identity_policy_bundle_artifact=late_policy.exact_pin(
                path=checkpoint.identity_policy_bundle_artifact.path
            ),
        )

    backdated_resolution = replace(
        checkpoint.open_aliases[0].resolution,
        segment=checkpoint.open_aliases[0].segment,
        resolution_available_session=POLICY_CUTOFF,
    )
    with pytest.raises(I3CheckpointError, match="precedes its policy bundle"):
        replace(
            checkpoint,
            open_aliases=(
                OpenAliasState(
                    segment=checkpoint.open_aliases[0].segment,
                    resolution=backdated_resolution,
                ),
            ),
        )

    with pytest.raises(I3CheckpointError, match="resolved partition availability precedes"):
        replace(
            checkpoint,
            resolved_partition_map=(
                replace(
                    checkpoint.resolved_partition_map[0],
                    availability_session=POLICY_CUTOFF,
                ),
            ),
        )


def test_checkpoint_rejects_parent_backdated_before_policy_floor() -> None:
    checkpoint = _checkpoint()
    backdated_parent = _parent_at(checkpoint, date(2026, 8, 2))

    with pytest.raises(I3CheckpointError, match="precedes its policy bundle"):
        replace(checkpoint, parent_release=backdated_parent, open_aliases=())


def test_checkpoint_rejects_parent_backdated_before_open_alias_knowledge() -> None:
    checkpoint = _checkpoint()

    with pytest.raises(I3CheckpointError, match="precedes open-alias identity evidence"):
        replace(
            checkpoint,
            parent_release=_parent_at(checkpoint, date(2026, 7, 28)),
        )

    with pytest.raises(I3CheckpointError, match="precedes an open-alias resolution"):
        replace(
            checkpoint,
            parent_release=_parent_at(checkpoint, date(2026, 8, 2)),
        )


def test_checkpoint_rejects_parent_backdated_before_every_referenced_state() -> None:
    checkpoint = _checkpoint()
    later = date(2026, 8, 6)

    with pytest.raises(I3CheckpointError, match="precedes an S4 terminal pin"):
        replace(
            checkpoint,
            availability_cutoff_session=later,
            s4_terminal_pins=(
                replace(checkpoint.s4_terminal_pins[0], availability_session=later),
                *checkpoint.s4_terminal_pins[1:],
            ),
        )

    with pytest.raises(I3CheckpointError, match="precedes a resolved partition"):
        replace(
            checkpoint,
            availability_cutoff_session=later,
            resolved_partition_map=(
                replace(checkpoint.resolved_partition_map[0], availability_session=later),
            ),
        )

    with pytest.raises(I3CheckpointError, match="precedes a terminal row version"):
        replace(
            checkpoint,
            availability_cutoff_session=later,
            terminal_row_versions=(
                replace(checkpoint.terminal_row_versions[0], availability_session=later),
                *checkpoint.terminal_row_versions[1:],
            ),
        )

    with pytest.raises(I3CheckpointError, match="precedes an asset aggregate"):
        replace(
            checkpoint,
            availability_cutoff_session=later,
            asset_aggregates=(
                replace(checkpoint.asset_aggregates[0], state_available_session=later),
            ),
        )

    with pytest.raises(I3CheckpointError, match="precedes an issuer aggregate"):
        replace(
            checkpoint,
            availability_cutoff_session=later,
            issuer_aggregates=(
                replace(checkpoint.issuer_aggregates[0], state_available_session=later),
            ),
        )

    with pytest.raises(I3CheckpointError, match="precedes an unresolved subject"):
        replace(
            checkpoint,
            availability_cutoff_session=later,
            unresolved_subjects=(
                replace(checkpoint.unresolved_subjects[0], state_available_session=later),
            ),
        )


def test_checkpoint_rejects_missing_duplicate_and_forked_maps() -> None:
    checkpoint = _checkpoint()

    with pytest.raises(I3CheckpointError, match="open alias is missing"):
        replace(checkpoint, terminal_row_versions=checkpoint.terminal_row_versions[:-1])

    with pytest.raises(I3CheckpointError, match="resolved partition map must be sorted and unique"):
        replace(
            checkpoint,
            resolved_partition_map=(
                checkpoint.resolved_partition_map[0],
                checkpoint.resolved_partition_map[0],
            ),
        )

    with pytest.raises(I3CheckpointError, match="resolved state differs"):
        replace(
            checkpoint,
            resolved_partition_map=(
                replace(
                    checkpoint.resolved_partition_map[0],
                    artifact=_pin("swapped-resolved-partition"),
                ),
            ),
        )


def test_asset_row_counters_are_independent_from_distinct_decision_counts() -> None:
    asset = _checkpoint().asset_aggregates[0]
    contamination_id = _digest("one-provider-contamination-decision")

    repeated = replace(
        asset,
        identity_adjudication_ids=(contamination_id,),
        provider_contamination_identity_adjudication_ids=(contamination_id,),
        counters=_counters(
            ASSET_COUNTER_NAMES,
            adjudicated_override_evidence_row_count=3,
            identity_adjudication_count=1,
            provider_contamination_adjudication_count=1,
            strong_evidence_row_count=3,
        ),
    )

    counts = {item.name: item.value for item in repeated.counters}
    assert counts["adjudicated_override_evidence_row_count"] == 3
    assert counts["identity_adjudication_count"] == 1
    assert counts["candidate_evidence_row_count"] == 0


def test_checkpoint_rejects_duplicate_receipts_and_future_aggregate_state() -> None:
    checkpoint = _checkpoint()
    duplicate_s4_receipt = replace(
        checkpoint.s4_terminal_pins[1],
        partition_receipt_id=checkpoint.s4_terminal_pins[0].partition_receipt_id,
    )
    with pytest.raises(I3CheckpointError, match="S4 terminal partition receipt IDs"):
        replace(
            checkpoint,
            s4_terminal_pins=(
                checkpoint.s4_terminal_pins[0],
                duplicate_s4_receipt,
                checkpoint.s4_terminal_pins[2],
            ),
        )

    with pytest.raises(I3CheckpointError, match="asset aggregate extends"):
        replace(
            checkpoint,
            asset_aggregates=(
                replace(
                    checkpoint.asset_aggregates[0],
                    last_canonical_membership_session=date(2026, 7, 13),
                ),
            ),
        )

    alias_terminal = checkpoint.terminal_row_versions[-1]
    fork = replace(alias_terminal, row_version_id=_digest("forked-alias-version"))
    with pytest.raises(
        I3CheckpointError,
        match="terminal row-version map must be sorted and unique",
    ):
        replace(
            checkpoint,
            terminal_row_versions=(
                *checkpoint.terminal_row_versions[:-1],
                alias_terminal,
                fork,
            ),
        )


def test_checkpoint_rejects_single_byte_tamper_before_parse() -> None:
    checkpoint = _checkpoint()
    content = checkpoint.canonical_bytes()
    pin = checkpoint.exact_pin(path="manifests/silver/i3/checkpoint.json")
    tampered = content[:-2] + (b"0" if content[-2:-1] != b"0" else b"1") + content[-1:]

    with pytest.raises(I3CheckpointError, match="SHA-256"):
        load_i3_checkpoint_exact(
            pin,
            lambda path: tampered,
            expected_release_family=NATIVE_V2_FIXTURE_RELEASE_FAMILY,
        )


@pytest.mark.parametrize(
    "content,error",
    [
        (b'{"x":1,"x":2}\n', "duplicate JSON key"),
        (b'{"x":NaN}\n', "non-finite JSON number"),
    ],
)
def test_loader_rejects_duplicate_keys_and_nonfinite_json(content: bytes, error: str) -> None:
    pin = _pin_bytes("manifests/silver/i3/invalid.json", content)

    with pytest.raises(I3CheckpointError, match=error):
        load_i3_checkpoint_exact(
            pin,
            lambda path: content,
            expected_release_family=NATIVE_V2_FIXTURE_RELEASE_FAMILY,
        )


def test_checkpoint_rejects_unknown_fields_and_noncanonical_bytes() -> None:
    checkpoint = _checkpoint()
    document = checkpoint.to_dict()
    document["unknown"] = True
    with pytest.raises(I3CheckpointError, match="fields differ"):
        I3CheckpointState.from_dict(document)

    noncanonical = (json.dumps(checkpoint.to_dict(), indent=2, sort_keys=True) + "\n").encode()
    pin = _pin_bytes("manifests/silver/i3/noncanonical.json", noncanonical)
    with pytest.raises(I3CheckpointError, match="canonical serialization"):
        load_i3_checkpoint_exact(
            pin,
            lambda path: noncanonical,
            expected_release_family=NATIVE_V2_FIXTURE_RELEASE_FAMILY,
        )


def test_exact_pin_cache_reads_once_and_rejects_same_path_different_pin() -> None:
    checkpoint = _checkpoint()
    content = checkpoint.canonical_bytes()
    pin = checkpoint.exact_pin(path="manifests/silver/i3/checkpoint.json")
    manifest_content = _parent_manifest(
        checkpoint.identity_policy_bundle,
        resolved_state_digest=checkpoint.resolved_state_digest,
    ).canonical_bytes()
    policy_content = checkpoint.identity_policy_bundle.canonical_bytes()
    reads: list[str] = []

    def reader(path: str) -> bytes:
        reads.append(path)
        return {
            pin.path: content,
            checkpoint.parent_release.manifest.path: manifest_content,
            checkpoint.identity_policy_bundle_artifact.path: policy_content,
        }[path]

    cache = ExactPinReadCache()
    assert (
        load_i3_checkpoint_exact(
            pin,
            reader,
            expected_release_family=NATIVE_V2_FIXTURE_RELEASE_FAMILY,
            cache=cache,
        )
        == checkpoint
    )
    assert (
        load_i3_checkpoint_exact(
            pin,
            reader,
            expected_release_family=NATIVE_V2_FIXTURE_RELEASE_FAMILY,
            cache=cache,
        )
        == checkpoint
    )
    assert reads == [
        pin.path,
        checkpoint.parent_release.manifest.path,
        checkpoint.identity_policy_bundle_artifact.path,
    ]

    conflicting_pin = ArtifactPin(path=pin.path, sha256=_digest("different"), bytes=pin.bytes)
    with pytest.raises(I3CheckpointError, match="different exact pins"):
        cache.read(conflicting_pin, reader)
    assert reads == [
        pin.path,
        checkpoint.parent_release.manifest.path,
        checkpoint.identity_policy_bundle_artifact.path,
    ]


def test_checkpoint_ids_and_digests_are_recomputed_on_load() -> None:
    checkpoint = _checkpoint()
    for field in ("checkpoint_id", "resolved_content_digest", "rebuild_basis_digest"):
        document = checkpoint.to_dict()
        document[field] = _digest(f"tampered-{field}")
        with pytest.raises(I3CheckpointError, match="does not reproduce"):
            I3CheckpointState.from_dict(document)
