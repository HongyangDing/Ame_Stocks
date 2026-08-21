"""Typed, content-addressed state for the S7.5 I3 incremental checkpoint.

The checkpoint is a rebuildable cache, never publication authority.  This
module deliberately exposes no release capability and performs no filesystem
discovery.  Callers must supply one exact :class:`ArtifactPin` and a byte
reader; every byte is authenticated before strict JSON parsing.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Self

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.incremental_contract import ArtifactPin, IncrementalContractError
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_CONTRACTS,
    I3_V2_SCHEMA_BUNDLE_DIGEST,
    I3_V2_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_identity import (
    AliasResolutionVersion,
    AliasSegmentIdentity,
    canonical_asset_id,
    canonical_issuer_id,
)

I3_CHECKPOINT_NAMESPACE = "ame_stocks.silver.s7_5.i3_checkpoint"
I3_CHECKPOINT_RULE_VERSION = "s7_5_i3_checkpoint_state_v1"
I3_CHECKPOINT_ROLE = "rebuildable_cache_not_authority"
IDENTITY_POLICY_BUNDLE_NAMESPACE = "ame_stocks.identity.policy_bundle"
IDENTITY_POLICY_BUNDLE_RULE_VERSION = "s7_5_identity_policy_bundle_v1"
NATIVE_V2_RELEASE_FAMILY = "s7_5_native_v2"
NATIVE_V2_FIXTURE_RELEASE_FAMILY = "s7_5_native_v2_fixture"
NATIVE_V2_SCHEMA_VERSION = 2
NATIVE_V2_RELEASE_MANIFEST_ARTIFACT_TYPE = "s7_5_native_v2_release_manifest"
NATIVE_V2_RELEASE_ID_RULE_VERSION = "s7_5_native_v2_release_id_v1"
LEGACY_S7_V1_RELEASE_SET_ID = "5ce4ad18b44d86fe70fd25c50d1023fb1aa39f25f50fa2f93a0a1c4452eb811e"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_CIK = re.compile(r"^[0-9]{10}$")
_TOKEN = re.compile(r"^[a-z][a-z0-9_]*$")
_TICKER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.\-/]{0,31}$")


class I3CheckpointError(ValueError):
    """Raised when checkpoint state is incomplete, ambiguous, or unauthenticated."""


class IdentityRegistryKind(StrEnum):
    """The five mutually exclusive S7 identity registry responsibilities."""

    IDENTITY_ADJUDICATION = "identity_adjudication"
    IDENTITY_CROSS_MARKET_ADJUDICATION = "identity_cross_market_adjudication"
    PROVIDER_COMPOSITE_OVERRIDE = "provider_composite_override"
    SHARE_CLASS_ADJUDICATION = "share_class_adjudication"
    ASSET_TRANSITION = "asset_transition"


IDENTITY_REGISTRY_ORDER = tuple(IdentityRegistryKind)
S4_TERMINAL_TABLE_ORDER = (
    "asset_observation_daily",
    "asset_observation_version",
    "universe_source_daily",
)
TERMINAL_ROW_TABLES = frozenset({"asset_master", "issuer_master", "ticker_alias"})
ASSET_COUNTER_NAMES = (
    "adjudicated_override_evidence_row_count",
    "candidate_evidence_row_count",
    "conflict_evidence_row_count",
    "cross_market_adjudication_count",
    "cross_market_override_evidence_row_count",
    "direct_observed_evidence_row_count",
    "genuine_transition_adjudication_count",
    "identity_adjudication_count",
    "provider_composite_override_count",
    "provider_contamination_adjudication_count",
    "share_class_adjudication_count",
    "strong_evidence_row_count",
)
ISSUER_COUNTER_NAMES = (
    "excluded_contamination_evidence_row_count",
    "excluded_cross_market_contamination_evidence_row_count",
    "source_evidence_row_count",
)


@dataclass(frozen=True, slots=True)
class IdentityRegistryReleasePin:
    """One exact registry release selected by an identity-policy bundle."""

    registry_kind: IdentityRegistryKind
    release_id: str
    artifact: ArtifactPin
    decision_cutoff_session: date
    release_available_session: date

    def __post_init__(self) -> None:
        if not isinstance(self.registry_kind, IdentityRegistryKind):
            raise I3CheckpointError("identity registry kind is invalid")
        _digest(self.release_id, "identity registry release ID")
        _artifact(self.artifact, "identity registry artifact")
        _session(self.decision_cutoff_session, "registry decision cutoff")
        _session(self.release_available_session, "registry release availability")
        if self.release_available_session < self.decision_cutoff_session:
            raise I3CheckpointError("registry release availability precedes its decision cutoff")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "decision_cutoff_session": self.decision_cutoff_session.isoformat(),
            "registry_kind": self.registry_kind.value,
            "release_available_session": self.release_available_session.isoformat(),
            "release_id": self.release_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "artifact",
                "decision_cutoff_session",
                "registry_kind",
                "release_available_session",
                "release_id",
            },
            "identity registry release pin",
        )
        try:
            kind = IdentityRegistryKind(_text(item["registry_kind"], "identity registry kind"))
        except ValueError as exc:
            raise I3CheckpointError("identity registry kind is invalid") from exc
        return cls(
            registry_kind=kind,
            release_id=_text(item["release_id"], "identity registry release ID"),
            artifact=_artifact_from_dict(item["artifact"], "identity registry artifact"),
            decision_cutoff_session=_session_from_json(
                item["decision_cutoff_session"], "registry decision cutoff"
            ),
            release_available_session=_session_from_json(
                item["release_available_session"], "registry release availability"
            ),
        )


@dataclass(frozen=True, slots=True)
class IdentityPolicyBundle:
    """Atomic, fixed-order selection of all five identity registries."""

    registry_releases: tuple[IdentityRegistryReleasePin, ...]
    bundle_available_session: date

    def __post_init__(self) -> None:
        if type(self.registry_releases) is not tuple:
            raise I3CheckpointError("identity registry releases must be a tuple")
        if not all(isinstance(item, IdentityRegistryReleasePin) for item in self.registry_releases):
            raise I3CheckpointError("identity policy bundle contains an invalid registry pin")
        if tuple(item.registry_kind for item in self.registry_releases) != IDENTITY_REGISTRY_ORDER:
            raise I3CheckpointError(
                "identity registry releases must use the fixed five-registry order"
            )
        paths = [item.artifact.path for item in self.registry_releases]
        if len(paths) != len(set(paths)):
            raise I3CheckpointError("identity policy registry artifact paths must be unique")
        release_ids = [item.release_id for item in self.registry_releases]
        if len(release_ids) != len(set(release_ids)):
            raise I3CheckpointError("identity policy registry release IDs must be unique")
        _session(self.bundle_available_session, "identity policy bundle availability")
        if self.bundle_available_session < max(
            item.release_available_session for item in self.registry_releases
        ):
            raise I3CheckpointError(
                "identity policy bundle availability precedes a selected registry release"
            )

    @property
    def decision_cutoff_session(self) -> date:
        return max(item.decision_cutoff_session for item in self.registry_releases)

    @property
    def policy_available_session(self) -> date:
        return self.bundle_available_session

    @property
    def policy_cutoff_session(self) -> date:
        """Operational as-of cutoff; never precedes the wrapper's first availability."""

        return max(self.decision_cutoff_session, self.bundle_available_session)

    def identity_payload(self) -> dict[str, object]:
        return {
            "namespace": IDENTITY_POLICY_BUNDLE_NAMESPACE,
            "decision_cutoff_session": self.decision_cutoff_session.isoformat(),
            "policy_available_session": self.policy_available_session.isoformat(),
            "policy_cutoff_session": self.policy_cutoff_session.isoformat(),
            "registry_releases": [item.to_dict() for item in self.registry_releases],
            "rule_version": IDENTITY_POLICY_BUNDLE_RULE_VERSION,
        }

    @property
    def identity_policy_bundle_id(self) -> str:
        return stable_digest(self.identity_payload())

    def to_dict(self) -> dict[str, object]:
        return {
            "identity_policy_bundle_id": self.identity_policy_bundle_id,
            **self.identity_payload(),
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "decision_cutoff_session",
                "identity_policy_bundle_id",
                "namespace",
                "policy_available_session",
                "policy_cutoff_session",
                "registry_releases",
                "rule_version",
            },
            "identity policy bundle",
        )
        _literal(item["namespace"], IDENTITY_POLICY_BUNDLE_NAMESPACE, "policy namespace")
        _literal(
            item["rule_version"],
            IDENTITY_POLICY_BUNDLE_RULE_VERSION,
            "policy rule version",
        )
        releases = _array(item["registry_releases"], "identity registry releases")
        bundle = cls(
            tuple(IdentityRegistryReleasePin.from_dict(value) for value in releases),
            bundle_available_session=_session_from_json(
                item["policy_available_session"], "policy availability"
            ),
        )
        if _text(item["identity_policy_bundle_id"], "identity policy bundle ID") != (
            bundle.identity_policy_bundle_id
        ):
            raise I3CheckpointError("identity policy bundle ID does not reproduce")
        if _session_from_json(item["decision_cutoff_session"], "decision cutoff") != (
            bundle.decision_cutoff_session
        ):
            raise I3CheckpointError("identity policy decision cutoff does not reproduce")
        if _session_from_json(item["policy_cutoff_session"], "policy cutoff") != (
            bundle.policy_cutoff_session
        ):
            raise I3CheckpointError("identity policy cutoff does not reproduce")
        return bundle


@dataclass(frozen=True, slots=True)
class NativeV2OutputArtifact:
    """One typed four-table output projection bound by a native-v2 manifest."""

    table_name: str
    session_date: date
    row_count: int
    contract_id: str
    schema_digest: str
    artifact: ArtifactPin

    def __post_init__(self) -> None:
        if self.table_name not in I3_V2_TABLE_ORDER:
            raise I3CheckpointError("native-v2 output table role is invalid")
        _session(self.session_date, "native-v2 output session")
        _nonnegative_int(self.row_count, "native-v2 output row count")
        contract = I3_V2_CONTRACTS[self.table_name]
        if self.contract_id != contract.contract_id:
            raise I3CheckpointError("native-v2 output binds the wrong table contract")
        if self.schema_digest != contract.schema_digest:
            raise I3CheckpointError("native-v2 output binds the wrong table schema")
        _artifact(self.artifact, "native-v2 typed output artifact")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "contract_id": self.contract_id,
            "row_count": self.row_count,
            "schema_digest": self.schema_digest,
            "session_date": self.session_date.isoformat(),
            "table_name": self.table_name,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "artifact",
                "contract_id",
                "row_count",
                "schema_digest",
                "session_date",
                "table_name",
            },
            "native-v2 typed output",
        )
        return cls(
            table_name=_text(item["table_name"], "native-v2 output table"),
            session_date=_session_from_json(item["session_date"], "native-v2 output session"),
            row_count=_integer(item["row_count"], "native-v2 output row count"),
            contract_id=_text(item["contract_id"], "native-v2 output contract ID"),
            schema_digest=_text(item["schema_digest"], "native-v2 output schema digest"),
            artifact=_artifact_from_dict(item["artifact"], "native-v2 output artifact"),
        )


@dataclass(frozen=True, slots=True)
class NativeV2ReleaseManifest:
    """Closed logical manifest for either production or fixture native-v2 output.

    The release ID is derived without the manifest artifact pin, so the manifest
    bytes can include the release ID without a self-hash cycle.  A parent pin
    contains this complete logical projection and can therefore reject a forged
    release ID before any filesystem read; the exact loader additionally proves
    the immutable manifest bytes.
    """

    release_family: str
    terminal_session: date
    release_available_session: date
    native_v2_migration_id: str
    identity_policy_bundle_id: str
    transform_semantics_digest: str
    resolved_state_digest: str
    output_artifacts: tuple[NativeV2OutputArtifact, ...]
    parent_release_id: str | None = None
    source_checkpoint_id: str | None = None
    legacy_oracle_release_set_id: str = LEGACY_S7_V1_RELEASE_SET_ID
    schema_bundle_digest: str = I3_V2_SCHEMA_BUNDLE_DIGEST
    schema_version: int = NATIVE_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.release_family not in {
            NATIVE_V2_RELEASE_FAMILY,
            NATIVE_V2_FIXTURE_RELEASE_FAMILY,
        }:
            raise I3CheckpointError("native-v2 release family is invalid")
        _session(self.terminal_session, "native-v2 terminal session")
        _session(self.release_available_session, "native-v2 release availability")
        if self.release_available_session < self.terminal_session:
            raise I3CheckpointError("native-v2 release availability precedes terminal session")
        _digest(self.native_v2_migration_id, "native-v2 migration ID")
        _digest(self.identity_policy_bundle_id, "identity policy bundle ID")
        _digest(self.transform_semantics_digest, "native-v2 transform semantics digest")
        _digest(self.resolved_state_digest, "native-v2 resolved-state digest")
        _optional_digest(self.parent_release_id, "native-v2 parent release ID")
        _optional_digest(self.source_checkpoint_id, "native-v2 source checkpoint ID")
        if (self.parent_release_id is None) != (self.source_checkpoint_id is None):
            raise I3CheckpointError(
                "native-v2 parent release and source checkpoint must be jointly present"
            )
        if self.parent_release_id == LEGACY_S7_V1_RELEASE_SET_ID:
            raise I3CheckpointError("legacy S7 v1 release set cannot masquerade as a v2 parent")
        if self.legacy_oracle_release_set_id != LEGACY_S7_V1_RELEASE_SET_ID:
            raise I3CheckpointError("native-v2 release binds the wrong immutable v1 oracle")
        if self.schema_bundle_digest != I3_V2_SCHEMA_BUNDLE_DIGEST:
            raise I3CheckpointError("native-v2 release binds the wrong schema bundle")
        if self.schema_version != NATIVE_V2_SCHEMA_VERSION:
            raise I3CheckpointError("native-v2 release does not use the native-v2 schema")
        if type(self.output_artifacts) is not tuple or any(
            type(item) is not NativeV2OutputArtifact for item in self.output_artifacts
        ):
            raise I3CheckpointError("native-v2 release outputs must be a typed tuple")
        if tuple(item.table_name for item in self.output_artifacts) != I3_V2_TABLE_ORDER:
            raise I3CheckpointError("native-v2 release must bind the exact four-table output set")
        if any(item.session_date != self.terminal_session for item in self.output_artifacts):
            raise I3CheckpointError("native-v2 output session differs from terminal session")
        _unique_paths(
            tuple(item.artifact for item in self.output_artifacts),
            "native-v2 release output artifacts",
        )
        if self.parent_release_id == self.release_id:
            raise I3CheckpointError("native-v2 release cannot name itself as parent")

    @property
    def output_content_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.output_artifacts])

    def identity_payload(self) -> dict[str, object]:
        return {
            "identity_policy_bundle_id": self.identity_policy_bundle_id,
            "legacy_oracle_release_set_id": self.legacy_oracle_release_set_id,
            "native_v2_migration_id": self.native_v2_migration_id,
            "output_content_digest": self.output_content_digest,
            "parent_release_id": self.parent_release_id,
            "release_available_session": self.release_available_session.isoformat(),
            "release_family": self.release_family,
            "resolved_state_digest": self.resolved_state_digest,
            "schema_bundle_digest": self.schema_bundle_digest,
            "schema_version": self.schema_version,
            "source_checkpoint_id": self.source_checkpoint_id,
            "terminal_session": self.terminal_session.isoformat(),
            "transform_semantics_digest": self.transform_semantics_digest,
        }

    def logical_payload(self) -> dict[str, object]:
        return {
            **self.identity_payload(),
            "output_artifacts": [item.to_dict() for item in self.output_artifacts],
        }

    @property
    def release_id(self) -> str:
        return stable_digest(
            {
                "rule_version": NATIVE_V2_RELEASE_ID_RULE_VERSION,
                **self.identity_payload(),
            }
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact_type": NATIVE_V2_RELEASE_MANIFEST_ARTIFACT_TYPE,
            "release_id": self.release_id,
            "release_id_rule_version": NATIVE_V2_RELEASE_ID_RULE_VERSION,
            **self.logical_payload(),
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "artifact_type",
                "identity_policy_bundle_id",
                "legacy_oracle_release_set_id",
                "native_v2_migration_id",
                "output_artifacts",
                "output_content_digest",
                "parent_release_id",
                "release_available_session",
                "release_family",
                "release_id",
                "release_id_rule_version",
                "resolved_state_digest",
                "schema_bundle_digest",
                "schema_version",
                "source_checkpoint_id",
                "terminal_session",
                "transform_semantics_digest",
            },
            "native-v2 release manifest",
        )
        _literal(
            item["artifact_type"],
            NATIVE_V2_RELEASE_MANIFEST_ARTIFACT_TYPE,
            "native-v2 release artifact type",
        )
        _literal(
            item["release_id_rule_version"],
            NATIVE_V2_RELEASE_ID_RULE_VERSION,
            "native-v2 release ID rule version",
        )
        manifest = cls(
            release_family=_text(item["release_family"], "native-v2 release family"),
            terminal_session=_session_from_json(
                item["terminal_session"], "native-v2 terminal session"
            ),
            release_available_session=_session_from_json(
                item["release_available_session"], "native-v2 release availability"
            ),
            native_v2_migration_id=_text(item["native_v2_migration_id"], "native-v2 migration ID"),
            identity_policy_bundle_id=_text(
                item["identity_policy_bundle_id"], "identity policy bundle ID"
            ),
            transform_semantics_digest=_text(
                item["transform_semantics_digest"], "native-v2 transform semantics digest"
            ),
            resolved_state_digest=_text(
                item["resolved_state_digest"], "native-v2 resolved-state digest"
            ),
            output_artifacts=tuple(
                NativeV2OutputArtifact.from_dict(value)
                for value in _array(item["output_artifacts"], "native-v2 output artifacts")
            ),
            parent_release_id=_optional_text(
                item["parent_release_id"], "native-v2 parent release ID"
            ),
            source_checkpoint_id=_optional_text(
                item["source_checkpoint_id"], "native-v2 source checkpoint ID"
            ),
            legacy_oracle_release_set_id=_text(
                item["legacy_oracle_release_set_id"], "legacy v1 oracle release-set ID"
            ),
            schema_bundle_digest=_text(
                item["schema_bundle_digest"], "native-v2 schema bundle digest"
            ),
            schema_version=_integer(item["schema_version"], "native-v2 schema version"),
        )
        if _text(item["output_content_digest"], "native-v2 output-content digest") != (
            manifest.output_content_digest
        ):
            raise I3CheckpointError("native-v2 output-content digest does not reproduce")
        if _text(item["release_id"], "native-v2 release ID") != manifest.release_id:
            raise I3CheckpointError("native-v2 release ID does not reproduce")
        return manifest


@dataclass(frozen=True, slots=True)
class NativeV2ParentReleasePin:
    """Exact, reproducible logical projection of one native-v2 manifest."""

    release_id: str
    manifest: ArtifactPin
    release_available_session: date
    terminal_session: date
    native_v2_migration_id: str
    identity_policy_bundle_id: str
    transform_semantics_digest: str
    resolved_state_digest: str
    output_content_digest: str
    parent_release_id: str | None
    source_checkpoint_id: str | None
    legacy_oracle_release_set_id: str = LEGACY_S7_V1_RELEASE_SET_ID
    release_family: str = NATIVE_V2_RELEASE_FAMILY
    schema_bundle_digest: str = I3_V2_SCHEMA_BUNDLE_DIGEST
    schema_version: int = NATIVE_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _artifact(self.manifest, "native-v2 parent manifest")
        _digest(self.release_id, "native-v2 parent release ID")
        if self.release_id == LEGACY_S7_V1_RELEASE_SET_ID:
            raise I3CheckpointError("legacy S7 v1 release set cannot masquerade as a v2 parent")
        _session(self.terminal_session, "native-v2 parent terminal session")
        _session(self.release_available_session, "native-v2 parent availability")
        if self.release_available_session < self.terminal_session:
            raise I3CheckpointError("native-v2 parent availability precedes terminal session")
        _digest(self.native_v2_migration_id, "native-v2 migration ID")
        _digest(self.identity_policy_bundle_id, "identity policy bundle ID")
        _digest(self.transform_semantics_digest, "native-v2 transform semantics digest")
        _digest(self.resolved_state_digest, "native-v2 resolved-state digest")
        _digest(self.output_content_digest, "native-v2 output-content digest")
        _optional_digest(self.parent_release_id, "native-v2 parent release ID")
        _optional_digest(self.source_checkpoint_id, "native-v2 source checkpoint ID")
        if (self.parent_release_id is None) != (self.source_checkpoint_id is None):
            raise I3CheckpointError(
                "native-v2 parent release and source checkpoint must be jointly present"
            )
        if self.parent_release_id == LEGACY_S7_V1_RELEASE_SET_ID:
            raise I3CheckpointError("legacy S7 v1 release set cannot masquerade as a v2 parent")
        if self.legacy_oracle_release_set_id != LEGACY_S7_V1_RELEASE_SET_ID:
            raise I3CheckpointError("native-v2 parent binds the wrong immutable v1 oracle")
        if self.release_family not in {
            NATIVE_V2_RELEASE_FAMILY,
            NATIVE_V2_FIXTURE_RELEASE_FAMILY,
        }:
            raise I3CheckpointError("parent release is not in a native-v2 release family")
        if self.schema_bundle_digest != I3_V2_SCHEMA_BUNDLE_DIGEST:
            raise I3CheckpointError("native-v2 parent binds the wrong schema bundle")
        if self.schema_version != NATIVE_V2_SCHEMA_VERSION:
            raise I3CheckpointError("parent release does not use the native-v2 schema")
        expected_release_id = stable_digest(
            {
                "rule_version": NATIVE_V2_RELEASE_ID_RULE_VERSION,
                **self.identity_payload(),
            }
        )
        if self.release_id != expected_release_id:
            raise I3CheckpointError("native-v2 parent release ID does not reproduce")

    def identity_payload(self) -> dict[str, object]:
        return {
            "identity_policy_bundle_id": self.identity_policy_bundle_id,
            "legacy_oracle_release_set_id": self.legacy_oracle_release_set_id,
            "native_v2_migration_id": self.native_v2_migration_id,
            "output_content_digest": self.output_content_digest,
            "parent_release_id": self.parent_release_id,
            "release_available_session": self.release_available_session.isoformat(),
            "release_family": self.release_family,
            "resolved_state_digest": self.resolved_state_digest,
            "schema_bundle_digest": self.schema_bundle_digest,
            "schema_version": self.schema_version,
            "source_checkpoint_id": self.source_checkpoint_id,
            "terminal_session": self.terminal_session.isoformat(),
            "transform_semantics_digest": self.transform_semantics_digest,
        }

    @classmethod
    def from_manifest(cls, manifest: NativeV2ReleaseManifest, *, path: str) -> Self:
        if not isinstance(manifest, NativeV2ReleaseManifest):
            raise I3CheckpointError("native-v2 parent requires a typed manifest")
        return cls(
            release_id=manifest.release_id,
            manifest=manifest.exact_pin(path=path),
            release_available_session=manifest.release_available_session,
            terminal_session=manifest.terminal_session,
            native_v2_migration_id=manifest.native_v2_migration_id,
            identity_policy_bundle_id=manifest.identity_policy_bundle_id,
            transform_semantics_digest=manifest.transform_semantics_digest,
            resolved_state_digest=manifest.resolved_state_digest,
            output_content_digest=manifest.output_content_digest,
            parent_release_id=manifest.parent_release_id,
            source_checkpoint_id=manifest.source_checkpoint_id,
            legacy_oracle_release_set_id=manifest.legacy_oracle_release_set_id,
            release_family=manifest.release_family,
            schema_bundle_digest=manifest.schema_bundle_digest,
            schema_version=manifest.schema_version,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "manifest": self.manifest.to_dict(),
            "release_id": self.release_id,
            **self.identity_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        fields = {
            "identity_policy_bundle_id",
            "legacy_oracle_release_set_id",
            "manifest",
            "native_v2_migration_id",
            "output_content_digest",
            "parent_release_id",
            "release_available_session",
            "release_family",
            "release_id",
            "resolved_state_digest",
            "schema_bundle_digest",
            "schema_version",
            "source_checkpoint_id",
            "terminal_session",
            "transform_semantics_digest",
        }
        item = _closed_mapping(value, fields, "native-v2 parent release pin")
        return cls(
            release_id=_text(item["release_id"], "native-v2 parent release ID"),
            manifest=_artifact_from_dict(item["manifest"], "native-v2 parent manifest"),
            release_available_session=_session_from_json(
                item["release_available_session"], "native-v2 parent availability"
            ),
            terminal_session=_session_from_json(
                item["terminal_session"], "native-v2 parent terminal session"
            ),
            native_v2_migration_id=_text(item["native_v2_migration_id"], "native-v2 migration ID"),
            identity_policy_bundle_id=_text(
                item["identity_policy_bundle_id"], "identity policy bundle ID"
            ),
            transform_semantics_digest=_text(
                item["transform_semantics_digest"], "native-v2 transform semantics digest"
            ),
            resolved_state_digest=_text(
                item["resolved_state_digest"], "native-v2 resolved-state digest"
            ),
            output_content_digest=_text(
                item["output_content_digest"], "native-v2 output-content digest"
            ),
            parent_release_id=_optional_text(
                item["parent_release_id"], "native-v2 parent release ID"
            ),
            source_checkpoint_id=_optional_text(
                item["source_checkpoint_id"], "native-v2 source checkpoint ID"
            ),
            legacy_oracle_release_set_id=_text(
                item["legacy_oracle_release_set_id"], "legacy v1 oracle release-set ID"
            ),
            release_family=_text(item["release_family"], "release family"),
            schema_bundle_digest=_text(
                item["schema_bundle_digest"], "native-v2 schema bundle digest"
            ),
            schema_version=_integer(item["schema_version"], "native-v2 schema version"),
        )


@dataclass(frozen=True, slots=True)
class S4TerminalPartitionPin:
    """Exact terminal S4 partition needed to advance one checkpoint."""

    table_name: str
    session_date: date
    partition_receipt_id: str
    artifact: ArtifactPin
    availability_session: date

    def __post_init__(self) -> None:
        if self.table_name not in S4_TERMINAL_TABLE_ORDER:
            raise I3CheckpointError("S4 terminal table name is invalid")
        _session(self.session_date, "S4 terminal session")
        _digest(self.partition_receipt_id, "S4 terminal partition receipt ID")
        _artifact(self.artifact, "S4 terminal partition artifact")
        _session(self.availability_session, "S4 terminal partition availability")
        if self.availability_session < self.session_date:
            raise I3CheckpointError("S4 terminal availability precedes its session")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "availability_session": self.availability_session.isoformat(),
            "partition_receipt_id": self.partition_receipt_id,
            "session_date": self.session_date.isoformat(),
            "table_name": self.table_name,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "artifact",
                "availability_session",
                "partition_receipt_id",
                "session_date",
                "table_name",
            },
            "S4 terminal partition pin",
        )
        return cls(
            table_name=_text(item["table_name"], "S4 terminal table name"),
            session_date=_session_from_json(item["session_date"], "S4 terminal session"),
            partition_receipt_id=_text(
                item["partition_receipt_id"], "S4 terminal partition receipt ID"
            ),
            artifact=_artifact_from_dict(item["artifact"], "S4 terminal partition artifact"),
            availability_session=_session_from_json(
                item["availability_session"], "S4 terminal partition availability"
            ),
        )


@dataclass(frozen=True, slots=True)
class OpenAliasState:
    """Exact open alias segment and its current resolution version."""

    segment: AliasSegmentIdentity
    resolution: AliasResolutionVersion

    def __post_init__(self) -> None:
        if not isinstance(self.segment, AliasSegmentIdentity):
            raise I3CheckpointError("open alias segment is invalid")
        if not isinstance(self.resolution, AliasResolutionVersion):
            raise I3CheckpointError("open alias resolution is invalid")
        if self.resolution.alias_segment_id != self.segment.alias_segment_id:
            raise I3CheckpointError("open alias segment and resolution do not match")
        if self.resolution.is_tombstone:
            raise I3CheckpointError("tombstoned aliases cannot remain open")

    def to_dict(self) -> dict[str, object]:
        return {"resolution": self.resolution.to_dict(), "segment": self.segment.to_dict()}

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(value, {"resolution", "segment"}, "open alias state")
        segment = AliasSegmentIdentity.from_dict(item["segment"])
        return cls(
            segment=segment,
            resolution=AliasResolutionVersion.from_dict(item["resolution"], segment=segment),
        )


@dataclass(frozen=True, slots=True)
class AggregateCount:
    """One closed-name, nonnegative aggregate counter."""

    name: str
    value: int

    def __post_init__(self) -> None:
        _token(self.name, "aggregate counter name")
        _nonnegative_int(self.value, "aggregate counter value")

    def to_dict(self) -> dict[str, object]:
        return {"name": self.name, "value": self.value}

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(value, {"name", "value"}, "aggregate counter")
        return cls(
            name=_text(item["name"], "aggregate counter name"),
            value=_integer(item["value"], "aggregate counter value"),
        )


@dataclass(frozen=True, slots=True)
class AssetAggregateState:
    """Mutable summary needed to advance one asset without replaying history."""

    asset_id: str
    canonical_composite_figi: str
    canonical_share_class_figi: str | None
    canonical_share_class_figis: tuple[str, ...]
    terminal_row_version_id: str
    first_direct_observed_session: date | None
    last_direct_observed_session: date | None
    first_canonical_membership_session: date | None
    last_canonical_membership_session: date | None
    observed_tickers: tuple[str, ...]
    observed_composite_figis: tuple[str, ...]
    observed_share_class_figis: tuple[str, ...]
    observed_issuer_ids: tuple[str, ...]
    identity_adjudication_ids: tuple[str, ...]
    genuine_transition_identity_adjudication_ids: tuple[str, ...]
    provider_contamination_identity_adjudication_ids: tuple[str, ...]
    cross_market_adjudication_ids: tuple[str, ...]
    provider_composite_override_ids: tuple[str, ...]
    share_class_adjudication_ids: tuple[str, ...]
    asset_transition_ids: tuple[str, ...]
    predecessor_asset_ids: tuple[str, ...]
    successor_asset_ids: tuple[str, ...]
    counters: tuple[AggregateCount, ...]
    source_record_set_digest: str
    identity_evidence_available_session: date
    state_available_session: date

    def __post_init__(self) -> None:
        _digest(self.asset_id, "asset aggregate ID")
        _figi(self.canonical_composite_figi, "canonical Composite FIGI")
        if self.asset_id != canonical_asset_id(self.canonical_composite_figi):
            raise I3CheckpointError("asset aggregate ID does not reproduce from its Composite FIGI")
        _optional_figi(self.canonical_share_class_figi, "canonical Share Class FIGI")
        _sorted_unique_text(
            self.canonical_share_class_figis,
            "asset canonical Share Class FIGIs",
            validator=lambda value: _figi(value, "canonical Share Class FIGI"),
        )
        selected_share = (
            self.canonical_share_class_figis[0]
            if len(self.canonical_share_class_figis) == 1
            else None
        )
        if self.canonical_share_class_figi != selected_share:
            raise I3CheckpointError(
                "asset canonical Share Class summary differs from its complete set"
            )
        _digest(self.terminal_row_version_id, "asset terminal row-version ID")
        _optional_date_pair(
            self.first_direct_observed_session,
            self.last_direct_observed_session,
            "asset direct-observed interval",
        )
        _optional_date_pair(
            self.first_canonical_membership_session,
            self.last_canonical_membership_session,
            "asset canonical-membership interval",
        )
        if self.first_canonical_membership_session is None:
            raise I3CheckpointError("asset canonical-membership interval is required")
        _sorted_unique_text(self.observed_tickers, "asset observed tickers", validator=_ticker)
        _sorted_unique_text(
            self.observed_composite_figis,
            "asset observed Composite FIGIs",
            validator=lambda value: _figi(value, "observed Composite FIGI"),
        )
        _sorted_unique_text(
            self.observed_share_class_figis,
            "asset observed Share Class FIGIs",
            validator=lambda value: _figi(value, "observed Share Class FIGI"),
        )
        _sorted_unique_text(
            self.observed_issuer_ids,
            "asset observed issuer IDs",
            validator=lambda value: _digest(value, "observed issuer ID"),
        )
        for values, label in (
            (self.identity_adjudication_ids, "identity adjudication IDs"),
            (
                self.genuine_transition_identity_adjudication_ids,
                "genuine-transition identity adjudication IDs",
            ),
            (
                self.provider_contamination_identity_adjudication_ids,
                "provider-contamination identity adjudication IDs",
            ),
            (self.cross_market_adjudication_ids, "cross-market adjudication IDs"),
            (self.provider_composite_override_ids, "provider Composite override IDs"),
            (self.share_class_adjudication_ids, "Share Class adjudication IDs"),
            (self.asset_transition_ids, "asset transition IDs"),
        ):
            _sorted_unique_text(
                values,
                f"asset {label}",
                validator=lambda value, label=label: _digest(value, label),
            )
        _sorted_unique_text(
            self.predecessor_asset_ids,
            "predecessor asset IDs",
            validator=lambda value: _digest(value, "predecessor asset ID"),
        )
        _sorted_unique_text(
            self.successor_asset_ids,
            "successor asset IDs",
            validator=lambda value: _digest(value, "successor asset ID"),
        )
        _fixed_counters(self.counters, ASSET_COUNTER_NAMES, "asset aggregate counters")
        counter = {item.name: item.value for item in self.counters}
        genuine_ids = set(self.genuine_transition_identity_adjudication_ids)
        contamination_ids = set(self.provider_contamination_identity_adjudication_ids)
        if genuine_ids.intersection(contamination_ids):
            raise I3CheckpointError("identity adjudication dispositions overlap")
        if set(self.identity_adjudication_ids) != genuine_ids.union(contamination_ids):
            raise I3CheckpointError(
                "identity adjudication IDs differ from their complete disposition sets"
            )
        if any(
            value > counter["strong_evidence_row_count"]
            for value in (
                counter["direct_observed_evidence_row_count"],
                counter["adjudicated_override_evidence_row_count"],
                counter["cross_market_override_evidence_row_count"],
            )
        ):
            raise I3CheckpointError("asset S4 evidence subtype count exceeds strong rows")
        expected_distinct_counts = {
            "cross_market_adjudication_count": len(self.cross_market_adjudication_ids),
            "genuine_transition_adjudication_count": len(genuine_ids),
            "identity_adjudication_count": len(self.identity_adjudication_ids),
            "provider_composite_override_count": len(self.provider_composite_override_ids),
            "provider_contamination_adjudication_count": len(contamination_ids),
            "share_class_adjudication_count": len(self.share_class_adjudication_ids),
        }
        if any(counter[name] != value for name, value in expected_distinct_counts.items()):
            raise I3CheckpointError("asset distinct decision counts differ from complete sets")
        _digest(self.source_record_set_digest, "asset source-record-set digest")
        _session(self.identity_evidence_available_session, "asset evidence availability")
        _session(self.state_available_session, "asset aggregate availability")
        if self.identity_evidence_available_session > self.state_available_session:
            raise I3CheckpointError("asset state availability precedes identity evidence")
        if self.last_canonical_membership_session > self.state_available_session:
            raise I3CheckpointError("asset state availability precedes last membership")
        if (
            self.last_direct_observed_session is not None
            and self.last_direct_observed_session > self.last_canonical_membership_session
        ):
            raise I3CheckpointError("asset direct observation exceeds membership interval")

    def to_dict(self) -> dict[str, object]:
        return {
            "asset_id": self.asset_id,
            "canonical_composite_figi": self.canonical_composite_figi,
            "canonical_share_class_figi": self.canonical_share_class_figi,
            "canonical_share_class_figis": list(self.canonical_share_class_figis),
            "counters": [item.to_dict() for item in self.counters],
            "cross_market_adjudication_ids": list(self.cross_market_adjudication_ids),
            "first_canonical_membership_session": _date_json(
                self.first_canonical_membership_session
            ),
            "first_direct_observed_session": _date_json(self.first_direct_observed_session),
            "last_canonical_membership_session": _date_json(self.last_canonical_membership_session),
            "last_direct_observed_session": _date_json(self.last_direct_observed_session),
            "genuine_transition_identity_adjudication_ids": list(
                self.genuine_transition_identity_adjudication_ids
            ),
            "identity_adjudication_ids": list(self.identity_adjudication_ids),
            "identity_evidence_available_session": (
                self.identity_evidence_available_session.isoformat()
            ),
            "observed_composite_figis": list(self.observed_composite_figis),
            "observed_issuer_ids": list(self.observed_issuer_ids),
            "observed_share_class_figis": list(self.observed_share_class_figis),
            "observed_tickers": list(self.observed_tickers),
            "asset_transition_ids": list(self.asset_transition_ids),
            "predecessor_asset_ids": list(self.predecessor_asset_ids),
            "provider_composite_override_ids": list(self.provider_composite_override_ids),
            "provider_contamination_identity_adjudication_ids": list(
                self.provider_contamination_identity_adjudication_ids
            ),
            "share_class_adjudication_ids": list(self.share_class_adjudication_ids),
            "source_record_set_digest": self.source_record_set_digest,
            "state_available_session": self.state_available_session.isoformat(),
            "successor_asset_ids": list(self.successor_asset_ids),
            "terminal_row_version_id": self.terminal_row_version_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        fields = {
            "asset_id",
            "canonical_composite_figi",
            "canonical_share_class_figi",
            "canonical_share_class_figis",
            "counters",
            "cross_market_adjudication_ids",
            "first_canonical_membership_session",
            "first_direct_observed_session",
            "last_canonical_membership_session",
            "last_direct_observed_session",
            "genuine_transition_identity_adjudication_ids",
            "identity_adjudication_ids",
            "identity_evidence_available_session",
            "observed_composite_figis",
            "observed_issuer_ids",
            "observed_share_class_figis",
            "observed_tickers",
            "asset_transition_ids",
            "predecessor_asset_ids",
            "provider_composite_override_ids",
            "provider_contamination_identity_adjudication_ids",
            "share_class_adjudication_ids",
            "source_record_set_digest",
            "state_available_session",
            "successor_asset_ids",
            "terminal_row_version_id",
        }
        item = _closed_mapping(value, fields, "asset aggregate state")
        return cls(
            asset_id=_text(item["asset_id"], "asset aggregate ID"),
            canonical_composite_figi=_text(
                item["canonical_composite_figi"], "canonical Composite FIGI"
            ),
            canonical_share_class_figi=_optional_text(
                item["canonical_share_class_figi"], "canonical Share Class FIGI"
            ),
            canonical_share_class_figis=_text_tuple(
                item["canonical_share_class_figis"], "canonical Share Class FIGIs"
            ),
            terminal_row_version_id=_text(
                item["terminal_row_version_id"], "asset terminal row-version ID"
            ),
            first_direct_observed_session=_optional_session_from_json(
                item["first_direct_observed_session"], "first direct-observed session"
            ),
            last_direct_observed_session=_optional_session_from_json(
                item["last_direct_observed_session"], "last direct-observed session"
            ),
            first_canonical_membership_session=_optional_session_from_json(
                item["first_canonical_membership_session"], "first canonical-membership session"
            ),
            last_canonical_membership_session=_optional_session_from_json(
                item["last_canonical_membership_session"], "last canonical-membership session"
            ),
            observed_tickers=_text_tuple(item["observed_tickers"], "asset observed tickers"),
            observed_composite_figis=_text_tuple(
                item["observed_composite_figis"], "asset observed Composite FIGIs"
            ),
            observed_share_class_figis=_text_tuple(
                item["observed_share_class_figis"], "asset observed Share Class FIGIs"
            ),
            observed_issuer_ids=_text_tuple(
                item["observed_issuer_ids"], "asset observed issuer IDs"
            ),
            identity_adjudication_ids=_text_tuple(
                item["identity_adjudication_ids"], "identity adjudication IDs"
            ),
            genuine_transition_identity_adjudication_ids=_text_tuple(
                item["genuine_transition_identity_adjudication_ids"],
                "genuine-transition identity adjudication IDs",
            ),
            cross_market_adjudication_ids=_text_tuple(
                item["cross_market_adjudication_ids"], "cross-market adjudication IDs"
            ),
            provider_composite_override_ids=_text_tuple(
                item["provider_composite_override_ids"], "provider Composite override IDs"
            ),
            provider_contamination_identity_adjudication_ids=_text_tuple(
                item["provider_contamination_identity_adjudication_ids"],
                "provider-contamination identity adjudication IDs",
            ),
            share_class_adjudication_ids=_text_tuple(
                item["share_class_adjudication_ids"], "Share Class adjudication IDs"
            ),
            asset_transition_ids=_text_tuple(item["asset_transition_ids"], "asset transition IDs"),
            predecessor_asset_ids=_text_tuple(
                item["predecessor_asset_ids"], "predecessor asset IDs"
            ),
            successor_asset_ids=_text_tuple(item["successor_asset_ids"], "successor asset IDs"),
            counters=_counter_tuple(item["counters"], "asset aggregate counters"),
            source_record_set_digest=_text(
                item["source_record_set_digest"], "asset source-record-set digest"
            ),
            identity_evidence_available_session=_session_from_json(
                item["identity_evidence_available_session"], "asset evidence availability"
            ),
            state_available_session=_session_from_json(
                item["state_available_session"], "asset aggregate availability"
            ),
        )


@dataclass(frozen=True, slots=True)
class IssuerAggregateState:
    """Mutable summary needed to advance one issuer without replaying history."""

    issuer_id: str
    cik_normalized: str
    terminal_row_version_id: str
    first_observed_session: date
    last_observed_session: date
    observed_asset_ids: tuple[str, ...]
    observed_tickers: tuple[str, ...]
    reference_names: tuple[str, ...]
    sic_codes: tuple[str, ...]
    counters: tuple[AggregateCount, ...]
    source_record_set_digest: str
    reference_available_session: date
    state_available_session: date

    def __post_init__(self) -> None:
        _digest(self.issuer_id, "issuer aggregate ID")
        if not _CIK.fullmatch(self.cik_normalized):
            raise I3CheckpointError("issuer aggregate CIK is invalid")
        if self.issuer_id != canonical_issuer_id(self.cik_normalized):
            raise I3CheckpointError("issuer aggregate ID does not reproduce from its CIK")
        _digest(self.terminal_row_version_id, "issuer terminal row-version ID")
        _date_pair(self.first_observed_session, self.last_observed_session, "issuer interval")
        _sorted_unique_text(
            self.observed_asset_ids,
            "issuer observed asset IDs",
            validator=lambda value: _digest(value, "observed asset ID"),
        )
        _sorted_unique_text(self.observed_tickers, "issuer observed tickers", validator=_ticker)
        _sorted_unique_text(self.reference_names, "issuer reference names")
        _sorted_unique_text(self.sic_codes, "issuer SIC codes")
        _fixed_counters(self.counters, ISSUER_COUNTER_NAMES, "issuer aggregate counters")
        _digest(self.source_record_set_digest, "issuer source-record-set digest")
        _session(self.reference_available_session, "issuer reference availability")
        _session(self.state_available_session, "issuer aggregate availability")
        if self.reference_available_session > self.state_available_session:
            raise I3CheckpointError("issuer state availability precedes reference evidence")
        if self.last_observed_session > self.state_available_session:
            raise I3CheckpointError("issuer state availability precedes last observation")

    def to_dict(self) -> dict[str, object]:
        return {
            "cik_normalized": self.cik_normalized,
            "counters": [item.to_dict() for item in self.counters],
            "first_observed_session": self.first_observed_session.isoformat(),
            "issuer_id": self.issuer_id,
            "last_observed_session": self.last_observed_session.isoformat(),
            "observed_asset_ids": list(self.observed_asset_ids),
            "observed_tickers": list(self.observed_tickers),
            "reference_names": list(self.reference_names),
            "reference_available_session": self.reference_available_session.isoformat(),
            "sic_codes": list(self.sic_codes),
            "source_record_set_digest": self.source_record_set_digest,
            "state_available_session": self.state_available_session.isoformat(),
            "terminal_row_version_id": self.terminal_row_version_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "cik_normalized",
                "counters",
                "first_observed_session",
                "issuer_id",
                "last_observed_session",
                "observed_asset_ids",
                "observed_tickers",
                "reference_names",
                "reference_available_session",
                "sic_codes",
                "source_record_set_digest",
                "state_available_session",
                "terminal_row_version_id",
            },
            "issuer aggregate state",
        )
        return cls(
            issuer_id=_text(item["issuer_id"], "issuer aggregate ID"),
            cik_normalized=_text(item["cik_normalized"], "issuer aggregate CIK"),
            terminal_row_version_id=_text(
                item["terminal_row_version_id"], "issuer terminal row-version ID"
            ),
            first_observed_session=_session_from_json(
                item["first_observed_session"], "issuer first-observed session"
            ),
            last_observed_session=_session_from_json(
                item["last_observed_session"], "issuer last-observed session"
            ),
            observed_asset_ids=_text_tuple(item["observed_asset_ids"], "issuer observed asset IDs"),
            observed_tickers=_text_tuple(item["observed_tickers"], "issuer observed tickers"),
            reference_names=_text_tuple(item["reference_names"], "issuer reference names"),
            reference_available_session=_session_from_json(
                item["reference_available_session"], "issuer reference availability"
            ),
            sic_codes=_text_tuple(item["sic_codes"], "issuer SIC codes"),
            counters=_counter_tuple(item["counters"], "issuer aggregate counters"),
            source_record_set_digest=_text(
                item["source_record_set_digest"], "issuer source-record-set digest"
            ),
            state_available_session=_session_from_json(
                item["state_available_session"], "issuer aggregate availability"
            ),
        )


@dataclass(frozen=True, slots=True)
class UnresolvedSubjectState:
    """One fail-closed identity subject carried into the next review boundary."""

    subject_kind: str
    subject_key: str
    first_observed_session: date
    last_observed_session: date
    reason_codes: tuple[str, ...]
    source_record_set_digest: str
    state_available_session: date

    def __post_init__(self) -> None:
        _token(self.subject_kind, "unresolved subject kind")
        _nonempty_text(self.subject_key, "unresolved subject key")
        _date_pair(
            self.first_observed_session,
            self.last_observed_session,
            "unresolved subject interval",
        )
        _sorted_unique_text(
            self.reason_codes,
            "unresolved subject reason codes",
            validator=lambda value: _token(value, "unresolved reason code"),
            nonempty=True,
        )
        _digest(self.source_record_set_digest, "unresolved source-record-set digest")
        _session(self.state_available_session, "unresolved subject availability")
        if self.last_observed_session > self.state_available_session:
            raise I3CheckpointError("unresolved availability precedes last observation")

    @property
    def unresolved_subject_id(self) -> str:
        return stable_digest(
            {
                "first_observed_session": self.first_observed_session.isoformat(),
                "last_observed_session": self.last_observed_session.isoformat(),
                "reason_codes": list(self.reason_codes),
                "source_record_set_digest": self.source_record_set_digest,
                "state_available_session": self.state_available_session.isoformat(),
                "subject_key": self.subject_key,
                "subject_kind": self.subject_kind,
            }
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "first_observed_session": self.first_observed_session.isoformat(),
            "last_observed_session": self.last_observed_session.isoformat(),
            "reason_codes": list(self.reason_codes),
            "source_record_set_digest": self.source_record_set_digest,
            "state_available_session": self.state_available_session.isoformat(),
            "subject_key": self.subject_key,
            "subject_kind": self.subject_kind,
            "unresolved_subject_id": self.unresolved_subject_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "first_observed_session",
                "last_observed_session",
                "reason_codes",
                "source_record_set_digest",
                "state_available_session",
                "subject_key",
                "subject_kind",
                "unresolved_subject_id",
            },
            "unresolved subject state",
        )
        state = cls(
            subject_kind=_text(item["subject_kind"], "unresolved subject kind"),
            subject_key=_text(item["subject_key"], "unresolved subject key"),
            first_observed_session=_session_from_json(
                item["first_observed_session"], "unresolved first-observed session"
            ),
            last_observed_session=_session_from_json(
                item["last_observed_session"], "unresolved last-observed session"
            ),
            reason_codes=_text_tuple(item["reason_codes"], "unresolved reason codes"),
            source_record_set_digest=_text(
                item["source_record_set_digest"], "unresolved source-record-set digest"
            ),
            state_available_session=_session_from_json(
                item["state_available_session"], "unresolved subject availability"
            ),
        )
        if _text(item["unresolved_subject_id"], "unresolved subject ID") != (
            state.unresolved_subject_id
        ):
            raise I3CheckpointError("unresolved subject ID does not reproduce")
        return state


@dataclass(frozen=True, slots=True)
class ResolvedPartitionState:
    """One exact terminal universe partition in the resolved snapshot map."""

    session_date: date
    partition_receipt_id: str
    artifact: ArtifactPin
    row_count: int
    availability_session: date

    def __post_init__(self) -> None:
        _session(self.session_date, "resolved partition session")
        _digest(self.partition_receipt_id, "resolved partition receipt ID")
        _artifact(self.artifact, "resolved partition artifact")
        _nonnegative_int(self.row_count, "resolved partition row count")
        _session(self.availability_session, "resolved partition availability")
        if self.availability_session < self.session_date:
            raise I3CheckpointError("resolved partition availability precedes its session")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "availability_session": self.availability_session.isoformat(),
            "partition_receipt_id": self.partition_receipt_id,
            "row_count": self.row_count,
            "session_date": self.session_date.isoformat(),
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "artifact",
                "availability_session",
                "partition_receipt_id",
                "row_count",
                "session_date",
            },
            "resolved partition state",
        )
        return cls(
            session_date=_session_from_json(item["session_date"], "resolved partition session"),
            partition_receipt_id=_text(
                item["partition_receipt_id"], "resolved partition receipt ID"
            ),
            artifact=_artifact_from_dict(item["artifact"], "resolved partition artifact"),
            row_count=_integer(item["row_count"], "resolved partition row count"),
            availability_session=_session_from_json(
                item["availability_session"], "resolved partition availability"
            ),
        )


@dataclass(frozen=True, slots=True)
class TerminalRowVersionState:
    """Exact terminal row-version locator for a stable non-session row key."""

    table_name: str
    stable_row_key: str
    row_version_id: str
    predecessor_row_version_id: str | None
    row_payload_digest: str
    index_artifact: ArtifactPin
    availability_session: date

    def __post_init__(self) -> None:
        if self.table_name not in TERMINAL_ROW_TABLES:
            raise I3CheckpointError("terminal row table name is invalid")
        _digest(self.stable_row_key, "terminal stable row key")
        _digest(self.row_version_id, "terminal row-version ID")
        _optional_digest(self.predecessor_row_version_id, "terminal predecessor row-version ID")
        if self.predecessor_row_version_id == self.row_version_id:
            raise I3CheckpointError("terminal row version cannot name itself as predecessor")
        _digest(self.row_payload_digest, "terminal row payload digest")
        _artifact(self.index_artifact, "terminal row index artifact")
        _session(self.availability_session, "terminal row availability")

    @property
    def map_key(self) -> tuple[str, str]:
        return (self.table_name, self.stable_row_key)

    def to_dict(self) -> dict[str, object]:
        return {
            "availability_session": self.availability_session.isoformat(),
            "index_artifact": self.index_artifact.to_dict(),
            "predecessor_row_version_id": self.predecessor_row_version_id,
            "row_payload_digest": self.row_payload_digest,
            "row_version_id": self.row_version_id,
            "stable_row_key": self.stable_row_key,
            "table_name": self.table_name,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "availability_session",
                "index_artifact",
                "predecessor_row_version_id",
                "row_payload_digest",
                "row_version_id",
                "stable_row_key",
                "table_name",
            },
            "terminal row-version state",
        )
        return cls(
            table_name=_text(item["table_name"], "terminal row table name"),
            stable_row_key=_text(item["stable_row_key"], "terminal stable row key"),
            row_version_id=_text(item["row_version_id"], "terminal row-version ID"),
            predecessor_row_version_id=_optional_text(
                item["predecessor_row_version_id"], "terminal predecessor row-version ID"
            ),
            row_payload_digest=_text(item["row_payload_digest"], "terminal payload digest"),
            index_artifact=_artifact_from_dict(item["index_artifact"], "terminal index artifact"),
            availability_session=_session_from_json(
                item["availability_session"], "terminal row availability"
            ),
        )


def i3_resolved_state_digest(
    *,
    last_session: date,
    source_cutoff_session: date,
    availability_cutoff_session: date,
    s4_terminal_pins: tuple[S4TerminalPartitionPin, ...],
    calendar_digest: str,
    schema_digest: str,
    transform_semantics_digest: str,
    identity_policy_bundle: IdentityPolicyBundle,
    identity_policy_bundle_artifact: ArtifactPin,
    open_aliases: tuple[OpenAliasState, ...],
    asset_aggregates: tuple[AssetAggregateState, ...],
    issuer_aggregates: tuple[IssuerAggregateState, ...],
    unresolved_subjects: tuple[UnresolvedSubjectState, ...],
    resolved_partition_map: tuple[ResolvedPartitionState, ...],
    terminal_row_versions: tuple[TerminalRowVersionState, ...],
) -> str:
    """Digest the complete rebuildable state while deliberately excluding parent.

    Excluding the parent manifest avoids a self-hash cycle.  Including every
    other checkpoint state component prevents a valid parent manifest from
    being paired with a re-pinned but different resolved frontier.
    """

    return stable_digest(
        {
            "asset_aggregates": [item.to_dict() for item in asset_aggregates],
            "availability_cutoff_session": availability_cutoff_session.isoformat(),
            "calendar_digest": calendar_digest,
            "identity_policy_bundle": identity_policy_bundle.to_dict(),
            "identity_policy_bundle_artifact": identity_policy_bundle_artifact.to_dict(),
            "issuer_aggregates": [item.to_dict() for item in issuer_aggregates],
            "last_session": last_session.isoformat(),
            "open_aliases": [item.to_dict() for item in open_aliases],
            "resolved_partition_map": [item.to_dict() for item in resolved_partition_map],
            "rule_version": "s7_5_i3_resolved_state_digest_v1",
            "s4_terminal_pins": [item.to_dict() for item in s4_terminal_pins],
            "schema_digest": schema_digest,
            "source_cutoff_session": source_cutoff_session.isoformat(),
            "terminal_row_versions": [item.to_dict() for item in terminal_row_versions],
            "transform_semantics_digest": transform_semantics_digest,
            "unresolved_subjects": [item.to_dict() for item in unresolved_subjects],
        }
    )


@dataclass(frozen=True, slots=True)
class I3CheckpointState:
    """Complete rebuildable frontier for one native-v2 resolved snapshot."""

    parent_release: NativeV2ParentReleasePin
    last_session: date
    source_cutoff_session: date
    availability_cutoff_session: date
    s4_terminal_pins: tuple[S4TerminalPartitionPin, ...]
    calendar_digest: str
    schema_digest: str
    transform_semantics_digest: str
    identity_policy_bundle: IdentityPolicyBundle
    identity_policy_bundle_artifact: ArtifactPin
    open_aliases: tuple[OpenAliasState, ...]
    asset_aggregates: tuple[AssetAggregateState, ...]
    issuer_aggregates: tuple[IssuerAggregateState, ...]
    unresolved_subjects: tuple[UnresolvedSubjectState, ...]
    resolved_partition_map: tuple[ResolvedPartitionState, ...]
    terminal_row_versions: tuple[TerminalRowVersionState, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.parent_release, NativeV2ParentReleasePin):
            raise I3CheckpointError("checkpoint parent release is not a native-v2 pin")
        _session(self.last_session, "checkpoint last session")
        _session(self.source_cutoff_session, "checkpoint source cutoff")
        _session(self.availability_cutoff_session, "checkpoint availability cutoff")
        if not self.last_session <= self.source_cutoff_session <= self.availability_cutoff_session:
            raise I3CheckpointError("checkpoint last/source/availability sessions are not ordered")
        if self.parent_release.release_available_session > self.availability_cutoff_session:
            raise I3CheckpointError("checkpoint availability precedes parent release availability")
        if self.parent_release.terminal_session != self.last_session:
            raise I3CheckpointError("checkpoint parent does not terminate at last_session")
        if not isinstance(self.identity_policy_bundle, IdentityPolicyBundle):
            raise I3CheckpointError("checkpoint identity policy bundle is invalid")
        for item in self.open_aliases:
            if not isinstance(item, OpenAliasState):
                raise I3CheckpointError("open aliases contains an invalid item")
            if (
                item.resolution.evidence_available_session
                > self.parent_release.release_available_session
            ):
                raise I3CheckpointError(
                    "native-v2 parent availability precedes open-alias identity evidence"
                )
            if (
                item.resolution.resolution_available_session
                > self.parent_release.release_available_session
            ):
                raise I3CheckpointError(
                    "native-v2 parent availability precedes an open-alias resolution"
                )
        if self.parent_release.release_available_session < (
            self.identity_policy_bundle.bundle_available_session
        ):
            raise I3CheckpointError("native-v2 parent availability precedes its policy bundle")
        if type(self.s4_terminal_pins) is not tuple or not all(
            isinstance(item, S4TerminalPartitionPin) for item in self.s4_terminal_pins
        ):
            raise I3CheckpointError("checkpoint S4 terminal pins contain an invalid item")
        if tuple(item.table_name for item in self.s4_terminal_pins) != S4_TERMINAL_TABLE_ORDER:
            raise I3CheckpointError("checkpoint S4 terminal pins must use the fixed table order")
        if any(item.session_date != self.last_session for item in self.s4_terminal_pins):
            raise I3CheckpointError("checkpoint S4 terminal pins do not match last_session")
        if any(
            item.availability_session > self.availability_cutoff_session
            for item in self.s4_terminal_pins
        ):
            raise I3CheckpointError("S4 terminal pin availability exceeds checkpoint cutoff")
        if any(
            item.availability_session > self.parent_release.release_available_session
            for item in self.s4_terminal_pins
        ):
            raise I3CheckpointError("native-v2 parent availability precedes an S4 terminal pin")
        _unique_paths((item.artifact for item in self.s4_terminal_pins), "S4 terminal artifacts")
        s4_receipt_ids = [item.partition_receipt_id for item in self.s4_terminal_pins]
        if len(s4_receipt_ids) != len(set(s4_receipt_ids)):
            raise I3CheckpointError("S4 terminal partition receipt IDs must be unique")
        _digest(self.calendar_digest, "checkpoint calendar digest")
        _digest(self.schema_digest, "checkpoint schema digest")
        if self.schema_digest != I3_V2_SCHEMA_BUNDLE_DIGEST:
            raise I3CheckpointError("checkpoint does not bind the pinned native-v2 schema bundle")
        _digest(self.transform_semantics_digest, "checkpoint transform semantics digest")
        _artifact(self.identity_policy_bundle_artifact, "identity policy bundle artifact")
        if (
            self.identity_policy_bundle.exact_pin(path=self.identity_policy_bundle_artifact.path)
            != self.identity_policy_bundle_artifact
        ):
            raise I3CheckpointError("identity policy bundle artifact does not reproduce")
        if self.parent_release.identity_policy_bundle_id != (
            self.identity_policy_bundle.identity_policy_bundle_id
        ):
            raise I3CheckpointError("checkpoint parent binds a different identity policy bundle")
        if self.parent_release.transform_semantics_digest != self.transform_semantics_digest:
            raise I3CheckpointError("checkpoint parent binds different transform semantics")
        if self.identity_policy_bundle.policy_available_session > self.availability_cutoff_session:
            raise I3CheckpointError("identity policy availability exceeds checkpoint cutoff")
        _sorted_unique_objects(
            self.open_aliases,
            key=lambda item: item.segment.alias_segment_id,
            label="open aliases",
            expected_type=OpenAliasState,
        )
        open_tickers = [item.segment.ticker for item in self.open_aliases]
        if len(open_tickers) != len(set(open_tickers)):
            raise I3CheckpointError("checkpoint contains multiple open aliases for one ticker")
        _sorted_unique_objects(
            self.asset_aggregates,
            key=lambda item: item.asset_id,
            label="asset aggregates",
            expected_type=AssetAggregateState,
        )
        _sorted_unique_objects(
            self.issuer_aggregates,
            key=lambda item: item.issuer_id,
            label="issuer aggregates",
            expected_type=IssuerAggregateState,
        )
        _sorted_unique_objects(
            self.unresolved_subjects,
            key=lambda item: item.unresolved_subject_id,
            label="unresolved subjects",
            expected_type=UnresolvedSubjectState,
        )
        _sorted_unique_objects(
            self.resolved_partition_map,
            key=lambda item: item.session_date,
            label="resolved partition map",
            expected_type=ResolvedPartitionState,
            nonempty=True,
        )
        _sorted_unique_objects(
            self.terminal_row_versions,
            key=lambda item: item.map_key,
            label="terminal row-version map",
            expected_type=TerminalRowVersionState,
        )
        if self.resolved_partition_map[-1].session_date != self.last_session:
            raise I3CheckpointError("resolved partition map does not terminate at last_session")
        _unique_paths(
            (item.artifact for item in self.resolved_partition_map),
            "resolved partition artifacts",
        )
        partition_receipt_ids = [item.partition_receipt_id for item in self.resolved_partition_map]
        if len(partition_receipt_ids) != len(set(partition_receipt_ids)):
            raise I3CheckpointError("resolved partition receipt IDs must be unique")
        for item in self.resolved_partition_map:
            if item.session_date > self.last_session:
                raise I3CheckpointError("resolved partition is later than checkpoint last_session")
            if item.availability_session > self.availability_cutoff_session:
                raise I3CheckpointError("resolved partition availability exceeds checkpoint cutoff")
            if item.availability_session < self.identity_policy_bundle.bundle_available_session:
                raise I3CheckpointError("resolved partition availability precedes policy bundle")
            if item.availability_session > self.parent_release.release_available_session:
                raise I3CheckpointError(
                    "native-v2 parent availability precedes a resolved partition"
                )
        for item in self.terminal_row_versions:
            if item.availability_session > self.availability_cutoff_session:
                raise I3CheckpointError("terminal row availability exceeds checkpoint cutoff")
            if item.availability_session < self.identity_policy_bundle.bundle_available_session:
                raise I3CheckpointError("terminal row availability precedes policy bundle")
            if item.availability_session > self.parent_release.release_available_session:
                raise I3CheckpointError(
                    "native-v2 parent availability precedes a terminal row version"
                )
        terminal_ids = [item.row_version_id for item in self.terminal_row_versions]
        terminal_id_set = set(terminal_ids)
        if len(terminal_ids) != len(terminal_id_set):
            raise I3CheckpointError("terminal row-version IDs must be globally unique")
        if any(
            item.predecessor_row_version_id in terminal_id_set
            for item in self.terminal_row_versions
            if item.predecessor_row_version_id is not None
        ):
            raise I3CheckpointError(
                "terminal row map contains both a predecessor and its successor"
            )

        terminal = {item.map_key: item for item in self.terminal_row_versions}
        for item in self.open_aliases:
            version = item.resolution
            if version.identity_policy_bundle_id != (
                self.identity_policy_bundle.identity_policy_bundle_id
            ):
                raise I3CheckpointError("open alias uses a different identity policy bundle")
            if version.resolution_available_session < (
                self.identity_policy_bundle.bundle_available_session
            ):
                raise I3CheckpointError(
                    "open alias resolution availability precedes its policy bundle"
                )
            if version.identity_cutoff_session < (
                self.identity_policy_bundle.policy_cutoff_session
            ):
                raise I3CheckpointError("open alias identity cutoff precedes policy cutoff")
            if version.identity_cutoff_session > self.availability_cutoff_session:
                raise I3CheckpointError("open alias identity cutoff exceeds checkpoint cutoff")
            if version.valid_through_session != self.last_session:
                raise I3CheckpointError(
                    "open alias does not extend through checkpoint last_session"
                )
            if version.resolution_available_session > self.availability_cutoff_session:
                raise I3CheckpointError("open alias availability exceeds checkpoint cutoff")
            terminal_item = terminal.get(("ticker_alias", item.segment.alias_segment_id))
            if terminal_item is None or terminal_item.row_version_id != (
                version.alias_resolution_version_id
            ):
                raise I3CheckpointError("open alias is missing its exact terminal row version")
        asset_keys = {item.asset_id for item in self.asset_aggregates}
        issuer_keys = {item.issuer_id for item in self.issuer_aggregates}
        if {key for table, key in terminal if table == "asset_master"} != asset_keys:
            raise I3CheckpointError("asset aggregate and terminal row maps are incomplete")
        if {key for table, key in terminal if table == "issuer_master"} != issuer_keys:
            raise I3CheckpointError("issuer aggregate and terminal row maps are incomplete")
        for item in self.asset_aggregates:
            if item.state_available_session > self.availability_cutoff_session:
                raise I3CheckpointError("asset aggregate availability exceeds checkpoint cutoff")
            if item.state_available_session < self.identity_policy_bundle.bundle_available_session:
                raise I3CheckpointError("asset aggregate availability precedes policy bundle")
            if item.state_available_session > self.parent_release.release_available_session:
                raise I3CheckpointError("native-v2 parent availability precedes an asset aggregate")
            if (
                item.last_direct_observed_session is not None
                and item.last_direct_observed_session > self.last_session
            ) or (
                item.last_canonical_membership_session is not None
                and item.last_canonical_membership_session > self.last_session
            ):
                raise I3CheckpointError("asset aggregate extends beyond checkpoint last_session")
            if terminal[("asset_master", item.asset_id)].row_version_id != (
                item.terminal_row_version_id
            ):
                raise I3CheckpointError("asset aggregate terminal row version does not match")
        for item in self.issuer_aggregates:
            if item.state_available_session > self.availability_cutoff_session:
                raise I3CheckpointError("issuer aggregate availability exceeds checkpoint cutoff")
            if item.state_available_session < self.identity_policy_bundle.bundle_available_session:
                raise I3CheckpointError("issuer aggregate availability precedes policy bundle")
            if item.state_available_session > self.parent_release.release_available_session:
                raise I3CheckpointError(
                    "native-v2 parent availability precedes an issuer aggregate"
                )
            if item.last_observed_session > self.last_session:
                raise I3CheckpointError("issuer aggregate extends beyond checkpoint last_session")
            if terminal[("issuer_master", item.issuer_id)].row_version_id != (
                item.terminal_row_version_id
            ):
                raise I3CheckpointError("issuer aggregate terminal row version does not match")
        for item in self.unresolved_subjects:
            if item.last_observed_session > self.last_session:
                raise I3CheckpointError("unresolved subject extends beyond checkpoint last_session")
            if item.state_available_session > self.availability_cutoff_session:
                raise I3CheckpointError("unresolved subject availability exceeds checkpoint cutoff")
            if item.state_available_session < self.identity_policy_bundle.bundle_available_session:
                raise I3CheckpointError("unresolved subject availability precedes policy bundle")
            if item.state_available_session > self.parent_release.release_available_session:
                raise I3CheckpointError(
                    "native-v2 parent availability precedes an unresolved subject"
                )
        if self.parent_release.resolved_state_digest != self.resolved_state_digest:
            raise I3CheckpointError(
                "checkpoint resolved state differs from its native-v2 parent manifest"
            )

    @property
    def resolved_state_digest(self) -> str:
        return i3_resolved_state_digest(
            last_session=self.last_session,
            source_cutoff_session=self.source_cutoff_session,
            availability_cutoff_session=self.availability_cutoff_session,
            s4_terminal_pins=self.s4_terminal_pins,
            calendar_digest=self.calendar_digest,
            schema_digest=self.schema_digest,
            transform_semantics_digest=self.transform_semantics_digest,
            identity_policy_bundle=self.identity_policy_bundle,
            identity_policy_bundle_artifact=self.identity_policy_bundle_artifact,
            open_aliases=self.open_aliases,
            asset_aggregates=self.asset_aggregates,
            issuer_aggregates=self.issuer_aggregates,
            unresolved_subjects=self.unresolved_subjects,
            resolved_partition_map=self.resolved_partition_map,
            terminal_row_versions=self.terminal_row_versions,
        )

    @property
    def resolved_content_digest(self) -> str:
        return stable_digest(
            {
                "resolved_partition_map": [item.to_dict() for item in self.resolved_partition_map],
                "terminal_row_versions": [item.to_dict() for item in self.terminal_row_versions],
            }
        )

    @property
    def rebuild_basis_digest(self) -> str:
        return stable_digest(
            {
                "calendar_digest": self.calendar_digest,
                "identity_policy_bundle_id": (
                    self.identity_policy_bundle.identity_policy_bundle_id
                ),
                "identity_policy_bundle_artifact": (self.identity_policy_bundle_artifact.to_dict()),
                "parent_release": self.parent_release.to_dict(),
                "s4_terminal_pins": [item.to_dict() for item in self.s4_terminal_pins],
                "schema_digest": self.schema_digest,
                "transform_semantics_digest": self.transform_semantics_digest,
            }
        )

    def identity_payload(self) -> dict[str, object]:
        return {
            "asset_aggregates": [item.to_dict() for item in self.asset_aggregates],
            "availability_cutoff_session": self.availability_cutoff_session.isoformat(),
            "calendar_digest": self.calendar_digest,
            "identity_policy_bundle": self.identity_policy_bundle.to_dict(),
            "identity_policy_bundle_artifact": self.identity_policy_bundle_artifact.to_dict(),
            "issuer_aggregates": [item.to_dict() for item in self.issuer_aggregates],
            "last_session": self.last_session.isoformat(),
            "open_aliases": [item.to_dict() for item in self.open_aliases],
            "parent_release": self.parent_release.to_dict(),
            "rebuild_basis_digest": self.rebuild_basis_digest,
            "resolved_content_digest": self.resolved_content_digest,
            "resolved_state_digest": self.resolved_state_digest,
            "resolved_partition_map": [item.to_dict() for item in self.resolved_partition_map],
            "s4_terminal_pins": [item.to_dict() for item in self.s4_terminal_pins],
            "schema_digest": self.schema_digest,
            "source_cutoff_session": self.source_cutoff_session.isoformat(),
            "terminal_row_versions": [item.to_dict() for item in self.terminal_row_versions],
            "transform_semantics_digest": self.transform_semantics_digest,
            "unresolved_subjects": [item.to_dict() for item in self.unresolved_subjects],
        }

    @property
    def checkpoint_id(self) -> str:
        return stable_digest(
            {
                "namespace": I3_CHECKPOINT_NAMESPACE,
                "rule_version": I3_CHECKPOINT_RULE_VERSION,
                **self.identity_payload(),
            }
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "authoritative": False,
            "checkpoint_id": self.checkpoint_id,
            "checkpoint_role": I3_CHECKPOINT_ROLE,
            "namespace": I3_CHECKPOINT_NAMESPACE,
            "rule_version": I3_CHECKPOINT_RULE_VERSION,
            **self.identity_payload(),
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )

    @classmethod
    def from_dict(cls, value: object) -> Self:
        fields = {
            "asset_aggregates",
            "authoritative",
            "availability_cutoff_session",
            "calendar_digest",
            "checkpoint_id",
            "checkpoint_role",
            "identity_policy_bundle",
            "identity_policy_bundle_artifact",
            "issuer_aggregates",
            "last_session",
            "namespace",
            "open_aliases",
            "parent_release",
            "rebuild_basis_digest",
            "resolved_content_digest",
            "resolved_state_digest",
            "resolved_partition_map",
            "rule_version",
            "s4_terminal_pins",
            "schema_digest",
            "source_cutoff_session",
            "terminal_row_versions",
            "transform_semantics_digest",
            "unresolved_subjects",
        }
        item = _closed_mapping(value, fields, "I3 checkpoint state")
        _literal(item["namespace"], I3_CHECKPOINT_NAMESPACE, "checkpoint namespace")
        _literal(item["rule_version"], I3_CHECKPOINT_RULE_VERSION, "checkpoint rule version")
        _literal(item["checkpoint_role"], I3_CHECKPOINT_ROLE, "checkpoint role")
        if item["authoritative"] is not False:
            raise I3CheckpointError("checkpoint must remain explicitly non-authoritative")
        checkpoint = cls(
            parent_release=NativeV2ParentReleasePin.from_dict(item["parent_release"]),
            last_session=_session_from_json(item["last_session"], "checkpoint last session"),
            source_cutoff_session=_session_from_json(
                item["source_cutoff_session"], "checkpoint source cutoff"
            ),
            availability_cutoff_session=_session_from_json(
                item["availability_cutoff_session"], "checkpoint availability cutoff"
            ),
            s4_terminal_pins=tuple(
                S4TerminalPartitionPin.from_dict(value)
                for value in _array(item["s4_terminal_pins"], "S4 terminal pins")
            ),
            calendar_digest=_text(item["calendar_digest"], "checkpoint calendar digest"),
            schema_digest=_text(item["schema_digest"], "checkpoint schema digest"),
            transform_semantics_digest=_text(
                item["transform_semantics_digest"], "checkpoint transform semantics digest"
            ),
            identity_policy_bundle=IdentityPolicyBundle.from_dict(item["identity_policy_bundle"]),
            identity_policy_bundle_artifact=_artifact_from_dict(
                item["identity_policy_bundle_artifact"], "identity policy bundle artifact"
            ),
            open_aliases=tuple(
                OpenAliasState.from_dict(value)
                for value in _array(item["open_aliases"], "open aliases")
            ),
            asset_aggregates=tuple(
                AssetAggregateState.from_dict(value)
                for value in _array(item["asset_aggregates"], "asset aggregates")
            ),
            issuer_aggregates=tuple(
                IssuerAggregateState.from_dict(value)
                for value in _array(item["issuer_aggregates"], "issuer aggregates")
            ),
            unresolved_subjects=tuple(
                UnresolvedSubjectState.from_dict(value)
                for value in _array(item["unresolved_subjects"], "unresolved subjects")
            ),
            resolved_partition_map=tuple(
                ResolvedPartitionState.from_dict(value)
                for value in _array(item["resolved_partition_map"], "resolved partition map")
            ),
            terminal_row_versions=tuple(
                TerminalRowVersionState.from_dict(value)
                for value in _array(item["terminal_row_versions"], "terminal row versions")
            ),
        )
        if _text(item["checkpoint_id"], "checkpoint ID") != checkpoint.checkpoint_id:
            raise I3CheckpointError("checkpoint ID does not reproduce")
        if _text(item["resolved_content_digest"], "resolved-content digest") != (
            checkpoint.resolved_content_digest
        ):
            raise I3CheckpointError("checkpoint resolved-content digest does not reproduce")
        if _text(item["resolved_state_digest"], "resolved-state digest") != (
            checkpoint.resolved_state_digest
        ):
            raise I3CheckpointError("checkpoint resolved-state digest does not reproduce")
        if _text(item["rebuild_basis_digest"], "rebuild-basis digest") != (
            checkpoint.rebuild_basis_digest
        ):
            raise I3CheckpointError("checkpoint rebuild-basis digest does not reproduce")
        return checkpoint


def i3_checkpoint_storage_bytes(checkpoint: I3CheckpointState, *, path: str) -> bytes:
    """Serialize a checkpoint as legacy JSON or deterministic gzip JSON."""

    if not isinstance(checkpoint, I3CheckpointState):
        raise I3CheckpointError("checkpoint storage requires typed state")
    content = checkpoint.canonical_bytes()
    if path.endswith(".json.gz"):
        return gzip.compress(content, compresslevel=6, mtime=0)
    if path.endswith(".json"):
        return content
    raise I3CheckpointError("checkpoint storage path must end in .json or .json.gz")


def i3_checkpoint_storage_payload(content: bytes, *, path: str) -> bytes:
    """Return canonical JSON payload bytes from either supported storage form."""

    if type(content) is not bytes:
        raise I3CheckpointError("checkpoint storage content must be bytes")
    if path.endswith(".json.gz"):
        try:
            payload = gzip.decompress(content)
        except (OSError, EOFError) as exc:
            raise I3CheckpointError("checkpoint gzip payload is invalid") from exc
        if gzip.compress(payload, compresslevel=6, mtime=0) != content:
            raise I3CheckpointError("checkpoint gzip bytes are not deterministic canonical form")
        return payload
    if path.endswith(".json"):
        return content
    raise I3CheckpointError("checkpoint storage path must end in .json or .json.gz")


def i3_checkpoint_storage_pin(
    checkpoint: I3CheckpointState,
    *,
    path: str,
) -> ArtifactPin:
    content = i3_checkpoint_storage_bytes(checkpoint, path=path)
    return ArtifactPin(
        path=path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


class ExactPinReadCache:
    """Process-local exact-byte cache with fail-closed path identity."""

    def __init__(self) -> None:
        self._entries: dict[str, tuple[ArtifactPin, bytes]] = {}

    def read(self, pin: ArtifactPin, reader: Callable[[str], bytes]) -> bytes:
        _artifact(pin, "exact read pin")
        if not callable(reader):
            raise I3CheckpointError("exact artifact reader must be callable")
        existing = self._entries.get(pin.path)
        if existing is not None:
            existing_pin, content = existing
            if existing_pin != pin:
                raise I3CheckpointError("one artifact path was presented with different exact pins")
            return content
        content = reader(pin.path)
        if type(content) is not bytes:
            raise I3CheckpointError("exact artifact reader must return bytes")
        if len(content) != pin.bytes:
            raise I3CheckpointError("exact artifact byte count does not match its pin")
        if hashlib.sha256(content).hexdigest() != pin.sha256:
            raise I3CheckpointError("exact artifact SHA-256 does not match its pin")
        self._entries[pin.path] = (pin, content)
        return content


def load_i3_checkpoint_exact(
    pin: ArtifactPin,
    reader: Callable[[str], bytes],
    *,
    expected_release_family: str,
    cache: ExactPinReadCache | None = None,
) -> I3CheckpointState:
    """Authenticate, strictly parse, and canonically reproduce one checkpoint."""

    if expected_release_family == NATIVE_V2_RELEASE_FAMILY:
        raise I3CheckpointError("production native-v2 authority is not implemented")
    if expected_release_family != NATIVE_V2_FIXTURE_RELEASE_FAMILY:
        raise I3CheckpointError("expected native-v2 release family is invalid")
    exact_cache = cache or ExactPinReadCache()
    stored = exact_cache.read(pin, reader)
    content = i3_checkpoint_storage_payload(stored, path=pin.path)
    document = _strict_json_document(content)
    checkpoint = I3CheckpointState.from_dict(document)
    if checkpoint.canonical_bytes() != content:
        raise I3CheckpointError("checkpoint bytes are not the canonical serialization")
    if checkpoint.parent_release.release_family != expected_release_family:
        raise I3CheckpointError("checkpoint parent differs from the required release family")
    load_native_v2_parent_release_exact(
        checkpoint.parent_release,
        reader,
        expected_release_family=expected_release_family,
        cache=exact_cache,
    )
    policy_content = exact_cache.read(
        checkpoint.identity_policy_bundle_artifact,
        reader,
    )
    if policy_content != checkpoint.identity_policy_bundle.canonical_bytes():
        raise I3CheckpointError("identity policy bundle artifact bytes differ from checkpoint")
    return checkpoint


def load_native_v2_parent_release_exact(
    parent: NativeV2ParentReleasePin,
    reader: Callable[[str], bytes],
    *,
    expected_release_family: str,
    cache: ExactPinReadCache | None = None,
) -> NativeV2ReleaseManifest:
    """Authenticate parent manifest bytes and reconcile the compact projection."""

    if not isinstance(parent, NativeV2ParentReleasePin):
        raise I3CheckpointError("native-v2 parent exact loader requires a typed pin")
    if expected_release_family == NATIVE_V2_RELEASE_FAMILY:
        raise I3CheckpointError("production native-v2 authority is not implemented")
    if expected_release_family != NATIVE_V2_FIXTURE_RELEASE_FAMILY:
        raise I3CheckpointError("expected native-v2 release family is invalid")
    if parent.release_family != expected_release_family:
        raise I3CheckpointError("native-v2 parent differs from the required release family")
    content = (cache or ExactPinReadCache()).read(parent.manifest, reader)
    manifest = NativeV2ReleaseManifest.from_dict(_strict_json_document(content))
    if manifest.canonical_bytes() != content:
        raise I3CheckpointError("native-v2 release manifest is not canonical JSON")
    if any(item.artifact.path == parent.manifest.path for item in manifest.output_artifacts):
        raise I3CheckpointError("native-v2 output artifact cannot reuse the manifest path")
    reproduced = NativeV2ParentReleasePin.from_manifest(
        manifest,
        path=parent.manifest.path,
    )
    if reproduced != parent:
        raise I3CheckpointError("native-v2 parent projection differs from exact manifest")
    return manifest


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        + b"\n"
    )


def _strict_json_document(content: bytes) -> object:
    def pairs_hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise I3CheckpointError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise I3CheckpointError(f"non-finite JSON number is forbidden: {value}")

    try:
        return json.loads(
            content.decode("utf-8"),
            object_pairs_hook=pairs_hook,
            parse_constant=reject_constant,
        )
    except I3CheckpointError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise I3CheckpointError("checkpoint is not strict UTF-8 JSON") from exc


def _closed_mapping(
    value: object,
    expected_fields: set[str],
    label: str,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise I3CheckpointError(f"{label} must be an object")
    if set(value) != expected_fields:
        raise I3CheckpointError(f"{label} fields differ from the closed schema")
    if not all(isinstance(key, str) for key in value):
        raise I3CheckpointError(f"{label} keys must be strings")
    return value


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise I3CheckpointError(f"{label} must be an array")
    return value


def _text_tuple(value: object, label: str) -> tuple[str, ...]:
    return tuple(_text(item, label) for item in _array(value, label))


def _counter_tuple(value: object, label: str) -> tuple[AggregateCount, ...]:
    return tuple(AggregateCount.from_dict(item) for item in _array(value, label))


def _artifact_from_dict(value: object, label: str) -> ArtifactPin:
    item = _closed_mapping(value, {"bytes", "path", "sha256"}, label)
    try:
        return ArtifactPin(
            path=_text(item["path"], f"{label} path"),
            sha256=_text(item["sha256"], f"{label} SHA-256"),
            bytes=_integer(item["bytes"], f"{label} bytes"),
        )
    except (IncrementalContractError, ValueError) as exc:
        raise I3CheckpointError(f"{label} is invalid") from exc


def _artifact(value: object, label: str) -> None:
    if not isinstance(value, ArtifactPin):
        raise I3CheckpointError(f"{label} must be an ArtifactPin")


def _literal(value: object, expected: object, label: str) -> None:
    if value != expected or type(value) is not type(expected):
        raise I3CheckpointError(f"{label} is invalid")


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise I3CheckpointError(f"{label} must be a nonempty string")
    return value


def _optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _text(value, label)


def _nonempty_text(value: object, label: str) -> None:
    _text(value, label)


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise I3CheckpointError(f"{label} must be an integer")
    return value


def _nonnegative_int(value: object, label: str) -> None:
    if type(value) is not int or value < 0:
        raise I3CheckpointError(f"{label} must be a nonnegative integer")


def _digest(value: object, label: str) -> None:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise I3CheckpointError(f"{label} must be a lowercase SHA-256 digest")


def _optional_digest(value: object, label: str) -> None:
    if value is not None:
        _digest(value, label)


def _figi(value: object, label: str) -> None:
    if not isinstance(value, str) or not _FIGI.fullmatch(value):
        raise I3CheckpointError(f"{label} is invalid")


def _optional_figi(value: object, label: str) -> None:
    if value is not None:
        _figi(value, label)


def _token(value: object, label: str) -> None:
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise I3CheckpointError(f"{label} must be a lowercase token")


def _ticker(value: object, label: str = "ticker") -> None:
    if not isinstance(value, str) or not _TICKER.fullmatch(value):
        raise I3CheckpointError(f"{label} is invalid")


def _session(value: object, label: str) -> None:
    if type(value) is not date:
        raise I3CheckpointError(f"{label} must be a date")


def _session_from_json(value: object, label: str) -> date:
    text = _text(value, label)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise I3CheckpointError(f"{label} is invalid") from exc
    if parsed.isoformat() != text:
        raise I3CheckpointError(f"{label} is not canonical ISO date text")
    return parsed


def _optional_session_from_json(value: object, label: str) -> date | None:
    if value is None:
        return None
    return _session_from_json(value, label)


def _date_json(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _date_pair(first: date, last: date, label: str) -> None:
    _session(first, f"{label} first session")
    _session(last, f"{label} last session")
    if first > last:
        raise I3CheckpointError(f"{label} is reversed")


def _optional_date_pair(first: date | None, last: date | None, label: str) -> None:
    if (first is None) != (last is None):
        raise I3CheckpointError(f"{label} endpoints must be jointly present or absent")
    if first is not None and last is not None:
        _date_pair(first, last, label)


def _sorted_unique_text(
    values: tuple[str, ...],
    label: str,
    *,
    validator: Callable[[str], None] | None = None,
    nonempty: bool = False,
) -> None:
    if type(values) is not tuple:
        raise I3CheckpointError(f"{label} must be a tuple")
    if nonempty and not values:
        raise I3CheckpointError(f"{label} must not be empty")
    if list(values) != sorted(values) or len(values) != len(set(values)):
        raise I3CheckpointError(f"{label} must be sorted and unique")
    for value in values:
        _text(value, label)
        if validator is not None:
            validator(value)


def _fixed_counters(
    values: tuple[AggregateCount, ...], expected_names: Sequence[str], label: str
) -> None:
    if type(values) is not tuple or not all(isinstance(item, AggregateCount) for item in values):
        raise I3CheckpointError(f"{label} must be a tuple of AggregateCount")
    if tuple(item.name for item in values) != tuple(expected_names):
        raise I3CheckpointError(f"{label} must use the fixed complete counter order")


def _sorted_unique_objects(
    values: tuple[object, ...],
    *,
    key: Callable[[object], object],
    label: str,
    expected_type: type[object],
    nonempty: bool = False,
) -> None:
    if type(values) is not tuple:
        raise I3CheckpointError(f"{label} must be a tuple")
    if nonempty and not values:
        raise I3CheckpointError(f"{label} must not be empty")
    if not all(isinstance(item, expected_type) for item in values):
        raise I3CheckpointError(f"{label} contains an invalid item")
    keys = [key(item) for item in values]
    if keys != sorted(keys) or len(keys) != len(set(keys)):
        raise I3CheckpointError(f"{label} must be sorted and unique")


def _unique_paths(values: Sequence[ArtifactPin], label: str) -> None:
    paths = [item.path for item in values]
    if len(paths) != len(set(paths)):
        raise I3CheckpointError(f"{label} paths must be unique")


__all__ = [
    "ASSET_COUNTER_NAMES",
    "I3_CHECKPOINT_NAMESPACE",
    "I3_CHECKPOINT_ROLE",
    "I3_CHECKPOINT_RULE_VERSION",
    "IDENTITY_REGISTRY_ORDER",
    "ISSUER_COUNTER_NAMES",
    "LEGACY_S7_V1_RELEASE_SET_ID",
    "NATIVE_V2_FIXTURE_RELEASE_FAMILY",
    "NATIVE_V2_RELEASE_FAMILY",
    "NATIVE_V2_RELEASE_ID_RULE_VERSION",
    "NATIVE_V2_RELEASE_MANIFEST_ARTIFACT_TYPE",
    "NATIVE_V2_SCHEMA_VERSION",
    "AggregateCount",
    "AssetAggregateState",
    "ExactPinReadCache",
    "I3CheckpointError",
    "I3CheckpointState",
    "IdentityPolicyBundle",
    "IdentityRegistryKind",
    "IdentityRegistryReleasePin",
    "IssuerAggregateState",
    "NativeV2OutputArtifact",
    "NativeV2ParentReleasePin",
    "NativeV2ReleaseManifest",
    "OpenAliasState",
    "ResolvedPartitionState",
    "S4TerminalPartitionPin",
    "TerminalRowVersionState",
    "UnresolvedSubjectState",
    "i3_checkpoint_storage_bytes",
    "i3_checkpoint_storage_payload",
    "i3_checkpoint_storage_pin",
    "i3_resolved_state_digest",
    "load_i3_checkpoint_exact",
    "load_native_v2_parent_release_exact",
]
