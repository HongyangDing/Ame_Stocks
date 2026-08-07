"""Exact production input builder for the S7.5 I3 compact base RunSpec.

The operator supplies three already approved exact pins and explicit execution
semantics.  The builder follows only deterministic IDs embedded in the frozen
S7 source binding; it never scans directories, resolves ``latest``, consults a
clock, publishes a pointer, or creates a new approval.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Self

from ame_stocks_api.artifacts import safe_relative_path, stable_digest, write_bytes_immutable
from ame_stocks_api.silver import identity_materialization_streaming as streaming
from ame_stocks_api.silver.asset_incremental_contract import S4BaseFrontier
from ame_stocks_api.silver.asset_release_set import load_exact_asset_release_set_control
from ame_stocks_api.silver.calendar_artifact import load_xnys_calendar_artifact
from ame_stocks_api.silver.identity_materialization_publish import (
    load_published_s7_release_set,
)
from ame_stocks_api.silver.identity_registry_workflow import load_registry_release_set
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    LEGACY_S7_V1_RELEASE_SET_ID,
    IdentityPolicyBundle,
    IdentityRegistryKind,
    IdentityRegistryReleasePin,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I0_ORACLE_AVAILABLE_SESSION,
    S4_V1_RELEASE_SET_ID,
    I3ProductionCalendarPin,
    I3ProductionDependencyPin,
    I3ProductionDependencyRole,
    I3ProductionI2BaseFrontierPin,
    I3ProductionResourceCaps,
    I3ProductionRunKind,
    I3ProductionRunSpec,
    production_v2_contract_pins,
)
from ame_stocks_api.silver.incremental_i3_production_policy import (
    load_production_identity_policy_snapshot,
)
from ame_stocks_api.silver.incremental_i3_production_semantics import (
    I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST,
    production_native_v2_migration_id,
)

I3_PRODUCTION_BASE_CONFIG_RULE_VERSION = "s7_5_i3_production_base_config_v1"
_CONTROL_ROOT = "manifests/silver/identity/s7-5-native-v2-staging"


class I3ProductionInputError(RuntimeError):
    """Raised when exact approved inputs cannot produce one base RunSpec."""


@dataclass(frozen=True, slots=True)
class I3ProductionBaseRunConfig:
    s7_release_set_artifact: ArtifactPin
    s4_release_set_artifact: ArtifactPin
    i2_base_frontier_artifact: ArtifactPin
    run_available_session: date
    resource_caps: I3ProductionResourceCaps

    def __post_init__(self) -> None:
        for value, label in (
            (self.s7_release_set_artifact, "S7 release-set artifact"),
            (self.s4_release_set_artifact, "S4 release-set artifact"),
            (self.i2_base_frontier_artifact, "I2 base-frontier artifact"),
        ):
            if not isinstance(value, ArtifactPin):
                raise I3ProductionInputError(f"{label} is not an exact pin")
            _explicit_path(value.path, label)
        _native_date(self.run_available_session, "run availability")
        if not isinstance(self.resource_caps, I3ProductionResourceCaps):
            raise I3ProductionInputError("resource caps are invalid")

    @property
    def config_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_i3_production_base_config",
            "i2_base_frontier_artifact": self.i2_base_frontier_artifact.to_dict(),
            "resource_caps": self.resource_caps.to_dict(),
            "rule_version": I3_PRODUCTION_BASE_CONFIG_RULE_VERSION,
            "run_available_session": self.run_available_session.isoformat(),
            "s4_release_set_artifact": self.s4_release_set_artifact.to_dict(),
            "s7_release_set_artifact": self.s7_release_set_artifact.to_dict(),
        }

    def to_dict(self) -> dict[str, object]:
        return {"config_id": self.config_id, **self.logical_payload()}

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
                "config_id",
                "i2_base_frontier_artifact",
                "resource_caps",
                "rule_version",
                "run_available_session",
                "s4_release_set_artifact",
                "s7_release_set_artifact",
            },
            "production base config",
        )
        _literal(item["artifact_type"], "s7_5_i3_production_base_config", "config type")
        _literal(
            item["rule_version"],
            I3_PRODUCTION_BASE_CONFIG_RULE_VERSION,
            "config rule",
        )
        result = cls(
            s7_release_set_artifact=_artifact_from_dict(
                item["s7_release_set_artifact"], "S7 release-set artifact"
            ),
            s4_release_set_artifact=_artifact_from_dict(
                item["s4_release_set_artifact"], "S4 release-set artifact"
            ),
            i2_base_frontier_artifact=_artifact_from_dict(
                item["i2_base_frontier_artifact"], "I2 base-frontier artifact"
            ),
            run_available_session=_date_from_json(
                item["run_available_session"], "run availability"
            ),
            resource_caps=I3ProductionResourceCaps.from_dict(item["resource_caps"]),
        )
        if item["config_id"] != result.config_id:
            raise I3ProductionInputError("production base config ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class PreparedI3ProductionBaseRunSpec:
    run_spec: I3ProductionRunSpec
    run_spec_artifact: ArtifactPin
    identity_policy_bundle_artifact: ArtifactPin
    config_artifact: ArtifactPin


def store_i3_production_base_config(
    data_root: Path,
    config: I3ProductionBaseRunConfig,
) -> ArtifactPin:
    """Canonicalize and immutably store an operator's explicit exact inputs."""

    if not isinstance(config, I3ProductionBaseRunConfig):
        raise I3ProductionInputError("production base config is not typed")
    root = data_root.expanduser().resolve()
    relative = f"{_CONTROL_ROOT}/base-configs/config_id={config.config_id}/config.json"
    pin = _write_immutable(root, relative, config.canonical_bytes())
    if pin != config.exact_pin(path=relative):
        raise I3ProductionInputError("stored production base config bytes changed")
    return pin


def load_i3_production_base_config_exact(
    pin: ArtifactPin, *, data_root: Path
) -> I3ProductionBaseRunConfig:
    if _fixture_path(pin.path):
        raise I3ProductionInputError("fixture config cannot prepare production authority")
    content = _read_exact(data_root.expanduser().resolve(), pin)
    result = I3ProductionBaseRunConfig.from_dict(_strict_json(content))
    if result.canonical_bytes() != content:
        raise I3ProductionInputError("production base config is not canonical JSON")
    return result


def prepare_i3_production_base_run_spec(
    data_root: Path,
    config_pin: ArtifactPin,
) -> PreparedI3ProductionBaseRunSpec:
    """Exact-read approved S7/S4/frontier controls and store one immutable RunSpec."""

    root = data_root.expanduser().resolve()
    config = load_i3_production_base_config_exact(config_pin, data_root=root)
    i0, s4, policy, calendar, frontier = _load_exact_base_authorities(root, config)
    policy_path = (
        f"{_CONTROL_ROOT}/identity-policy-bundles/"
        f"identity_policy_bundle_id={policy.identity_policy_bundle_id}/bundle.json"
    )
    policy_pin = _write_immutable(root, policy_path, policy.canonical_bytes())
    if policy_pin != policy.exact_pin(path=policy_path):
        raise I3ProductionInputError("stored identity-policy bundle bytes changed")
    source_cutoff = max(policy.decision_cutoff_session, calendar.available_session)
    migration_id = production_native_v2_migration_id(
        i0_release_set_artifact=i0.artifact,
        s4_release_set_artifact=s4.artifact,
        identity_policy_bundle=policy,
        identity_policy_bundle_artifact=policy_pin,
        calendar_artifact=calendar.artifact,
        i2_base_frontier_artifact=config.i2_base_frontier_artifact,
    )
    run_spec = I3ProductionRunSpec(
        run_kind=I3ProductionRunKind.BASE,
        terminal_session=frontier.terminal_session,
        source_cutoff_session=source_cutoff,
        run_available_session=config.run_available_session,
        native_v2_migration_id=migration_id,
        transform_semantics_digest=I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST,
        i0_oracle=i0,
        s4_v1_source=s4,
        identity_policy_bundle=policy,
        identity_policy_bundle_artifact=policy_pin,
        calendar=calendar,
        v2_contracts=production_v2_contract_pins(),
        i2_receipts=(),
        i2_base_frontier=I3ProductionI2BaseFrontierPin(
            terminal_session=frontier.terminal_session,
            frontier_id=frontier.frontier_id,
            artifact=config.i2_base_frontier_artifact,
            frontier_available_session=frontier.release_available_session,
        ),
        resource_caps=config.resource_caps,
    )
    run_spec_path = f"{_CONTROL_ROOT}/run-specs/run_spec_id={run_spec.run_spec_id}/run-spec.json"
    run_spec_pin = _write_immutable(root, run_spec_path, run_spec.canonical_bytes())
    if run_spec_pin != run_spec.exact_pin(path=run_spec_path):
        raise I3ProductionInputError("stored production RunSpec bytes changed")
    return PreparedI3ProductionBaseRunSpec(
        run_spec=run_spec,
        run_spec_artifact=run_spec_pin,
        identity_policy_bundle_artifact=policy_pin,
        config_artifact=config_pin,
    )


def _load_exact_base_authorities(
    root: Path,
    config: I3ProductionBaseRunConfig,
) -> tuple[
    I3ProductionDependencyPin,
    I3ProductionDependencyPin,
    IdentityPolicyBundle,
    I3ProductionCalendarPin,
    S4BaseFrontier,
]:
    i0 = I3ProductionDependencyPin(
        role=I3ProductionDependencyRole.I0_V1_ORACLE,
        object_id=LEGACY_S7_V1_RELEASE_SET_ID,
        artifact=config.s7_release_set_artifact,
        available_session=I0_ORACLE_AVAILABLE_SESSION,
    )
    s7_content = _read_exact(root, config.s7_release_set_artifact)
    marker = load_published_s7_release_set(root, release_set_id=LEGACY_S7_V1_RELEASE_SET_ID)
    if _canonical_json_bytes(marker) != s7_content:
        raise I3ProductionInputError("published S7 marker differs from its supplied exact pin")
    source_binding_id = _digest(marker.get("source_binding_id"), "S7 source-binding ID")
    binding, _binding_pin = streaming._load_source_binding(root, source_binding_id)
    if binding.mode != "production":
        raise I3ProductionInputError("S7 source binding is not production")
    bound_s4 = ArtifactPin(
        path=binding.s4_release_set_manifest.path,
        sha256=binding.s4_release_set_manifest.sha256,
        bytes=binding.s4_release_set_manifest.bytes,
    )
    if binding.s4_release_set_id != S4_V1_RELEASE_SET_ID or bound_s4 != (
        config.s4_release_set_artifact
    ):
        raise I3ProductionInputError("S7 source binding selects another S4 release set")
    loaded_s4, loaded_s4_document = load_exact_asset_release_set_control(
        root,
        release_set_id=S4_V1_RELEASE_SET_ID,
        expected_sha256=config.s4_release_set_artifact.sha256,
        expected_bytes=config.s4_release_set_artifact.bytes,
    )
    if (
        loaded_s4.release_set_id != S4_V1_RELEASE_SET_ID
        or loaded_s4_document.path != config.s4_release_set_artifact.path
    ):
        raise I3ProductionInputError("exact S4 loader returned another release set")
    s4 = I3ProductionDependencyPin(
        role=I3ProductionDependencyRole.S4_V1_SOURCE,
        object_id=S4_V1_RELEASE_SET_ID,
        artifact=config.s4_release_set_artifact,
        available_session=binding.cutoff_session,
    )
    loaded_registries = load_registry_release_set(root, tuple(binding.registry_pins))
    policy = IdentityPolicyBundle(
        registry_releases=tuple(
            IdentityRegistryReleasePin(
                registry_kind=IdentityRegistryKind(item.registry_name),
                release_id=item.release_id,
                artifact=ArtifactPin(
                    path=item.manifest_path,
                    sha256=item.manifest_sha256,
                    bytes=item.manifest_bytes,
                ),
                decision_cutoff_session=item.release_available_session,
                release_available_session=item.release_available_session,
            )
            for item in binding.registry_pins
        ),
        bundle_available_session=max(
            item.release_available_session for item in binding.registry_pins
        ),
    )
    load_production_identity_policy_snapshot(loaded_registries, policy)
    loaded_calendar = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=binding.calendar_artifact_id,
        expected_sha256=binding.calendar_artifact_sha256,
    )
    calendar = I3ProductionCalendarPin(
        calendar_artifact_id=loaded_calendar.calendar_artifact_id,
        artifact=ArtifactPin(
            path=loaded_calendar.relative_path,
            sha256=loaded_calendar.sha256,
            bytes=len(loaded_calendar.content),
        ),
        available_session=binding.cutoff_session,
    )
    frontier_content = _read_exact(root, config.i2_base_frontier_artifact)
    frontier = S4BaseFrontier.from_dict(_strict_json(frontier_content))
    expected_frontier_path = _i2_base_frontier_relative(frontier.frontier_id)
    if (
        _canonical_json_bytes(frontier.to_dict()) != frontier_content
        or config.i2_base_frontier_artifact.path != expected_frontier_path
        or frontier.base_release_set_id != S4_V1_RELEASE_SET_ID
        or frontier.base_release_set_artifact != config.s4_release_set_artifact
        or frontier.calendar_artifact_id != calendar.calendar_artifact_id
        or frontier.terminal_session not in {item.session_date for item in loaded_calendar.sessions}
    ):
        raise I3ProductionInputError("I2 base frontier differs from exact S4/calendar inputs")
    return i0, s4, policy, calendar, frontier


def _write_immutable(root: Path, relative: str, content: bytes) -> ArtifactPin:
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, relative),
        content,
        temporary_directory=safe_relative_path(
            root, "tmp/silver-identity-s7-5-native-v2-staging-inputs"
        ),
    )
    return ArtifactPin(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
    )


def _i2_base_frontier_relative(frontier_id: str) -> str:
    _digest(frontier_id, "I2 base-frontier ID")
    return (
        "manifests/silver/incremental/s4/assets/base-frontiers/"
        f"frontier_id={frontier_id}/manifest.json"
    )


def _read_exact(root: Path, pin: ArtifactPin) -> bytes:
    path = safe_relative_path(root, pin.path)
    if not path.is_file() or path.is_symlink():
        raise I3ProductionInputError(f"exact input is missing: {pin.path}")
    content = path.read_bytes()
    if len(content) != pin.bytes or hashlib.sha256(content).hexdigest() != pin.sha256:
        raise I3ProductionInputError(f"exact input differs from pin: {pin.path}")
    return content


def _strict_json(content: bytes) -> object:
    def pairs(values: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in values:
            if key in result:
                raise I3ProductionInputError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        return json.loads(
            content.decode(),
            object_pairs_hook=pairs,
            parse_constant=lambda value: _raise_nonfinite_json(value),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise I3ProductionInputError("input is not strict UTF-8 JSON") from exc


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )


def _raise_nonfinite_json(value: str) -> object:
    raise I3ProductionInputError(f"non-finite JSON number is forbidden: {value}")


def _closed_mapping(value: object, fields: set[str], label: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != fields:
        raise I3ProductionInputError(f"{label} fields differ")
    return value


def _artifact_from_dict(value: object, label: str) -> ArtifactPin:
    item = _closed_mapping(value, {"bytes", "path", "sha256"}, label)
    try:
        return ArtifactPin(
            path=_text(item["path"], f"{label} path"),
            sha256=_text(item["sha256"], f"{label} SHA-256"),
            bytes=_integer(item["bytes"], f"{label} bytes"),
        )
    except ValueError as exc:
        raise I3ProductionInputError(f"{label} is invalid") from exc


def _explicit_path(value: str, label: str) -> None:
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or ".." in path.parts
        or any("latest" in part.lower() for part in path.parts)
        or any(item in value for item in "*?[]{}")
    ):
        raise I3ProductionInputError(f"{label} must be explicit and non-latest")


def _fixture_path(value: str) -> bool:
    return value.startswith("fixtures/") or "/fixtures/" in f"/{value}"


def _native_date(value: object, label: str) -> date:
    if type(value) is not date:
        raise I3ProductionInputError(f"{label} must be a date")
    return value


def _date_from_json(value: object, label: str) -> date:
    text = _text(value, label)
    try:
        result = date.fromisoformat(text)
    except ValueError as exc:
        raise I3ProductionInputError(f"{label} is not an ISO date") from exc
    if result.isoformat() != text:
        raise I3ProductionInputError(f"{label} is not canonical")
    return result


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise I3ProductionInputError(f"{label} must be a lowercase SHA-256")
    return text


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise I3ProductionInputError(f"{label} must be nonempty text")
    return value


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise I3ProductionInputError(f"{label} must be an integer")
    return value


def _literal(value: object, expected: object, label: str) -> None:
    if value != expected:
        raise I3ProductionInputError(f"{label} changed")


__all__ = [
    "I3_PRODUCTION_BASE_CONFIG_RULE_VERSION",
    "I3ProductionBaseRunConfig",
    "I3ProductionInputError",
    "PreparedI3ProductionBaseRunSpec",
    "load_i3_production_base_config_exact",
    "prepare_i3_production_base_run_spec",
    "store_i3_production_base_config",
]
