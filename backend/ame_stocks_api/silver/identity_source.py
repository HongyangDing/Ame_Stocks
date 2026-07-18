"""Exact-release, read-only source bundle for S7 identity resolution.

S7 is deliberately unable to discover a latest release.  The six pins below are the
user-reviewed S4/S5/S6 evidence bundle.  Opening the bundle replays every existing Silver
publication trust-chain check and then verifies table, release-manifest hash, build ID,
artifact count and row count against these immutable pins.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Final
from weakref import WeakSet

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, sha256_file
from ame_stocks_api.silver.contracts import (
    ArtifactRef,
    ArtifactRole,
    ReleaseManifest,
    SilverContractError,
)
from ame_stocks_api.silver.reader import (
    PublishedAssetEvidenceReader,
    PublishedRelease,
    PublishedSilverReader,
)
from ame_stocks_api.silver.store import SilverStore

if TYPE_CHECKING:
    from ame_stocks_api.silver.identity_bounce import IdentityObservation

S7_SIX_RELEASE_BINDING_ID: Final = (
    "49f3d20725f2609b43d6736df78993b2975c9f1b71947af93190dc0658366c64"
)
S7_S4_RELEASE_SET_ID: Final = "f81c7ee28939db3350fce809326723e911b6d486c6db166d2575fcc92cb2101d"
S7_S4_RELEASE_SET_MANIFEST_SHA256: Final = (
    "937eaf4ed502fb2786dafb0dce9ec613bcaccb2cd488812cc5900118238d6c13"
)

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SESSION_PARTITION = re.compile(r"(?:^|/)session_date=(\d{4}-\d{2}-\d{2})(?:/|$)")
_FULL_PHYSICAL_SCOPE: Final = "full_six_release_physical_v1"
_PREVIEW_PHYSICAL_SCOPE: Final = "approved_detector_preview_s4_daily_v1"
_DIRECTIONAL_PREVIEW_PHYSICAL_SCOPE: Final = (
    "approved_directional_raw_preview_s4_daily_v1"
)
_TEST_PHYSICAL_SCOPE: Final = "test_fixture_v1"


class IdentitySourceError(SilverContractError):
    """Raised before S7 work when its exact source bundle cannot be reproduced."""


_OFFICIAL_CAPABILITY_SEAL = object()
_TEST_CAPABILITY_SEAL = object()


@dataclass(frozen=True, slots=True, eq=False, weakref_slot=True)
class _IdentitySourceCapability:
    """Unserialized provenance capability attached to a single opened bundle."""

    official: bool
    data_root: Path | None
    artifact_memberships: Mapping[
        tuple[str, str],
        tuple[str, str, str, ArtifactRef],
    ]
    physical_scope: str
    authorized_sessions: tuple[date, ...]
    preview_control_binding: tuple[str, str, str, str] | None
    _seal: object = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.official) is not bool:
            raise IdentitySourceError("S7 source capability official marker must be a native bool")
        expected = _OFFICIAL_CAPABILITY_SEAL if self.official else _TEST_CAPABILITY_SEAL
        if self._seal is not expected:
            raise IdentitySourceError("S7 source capability was not issued by its factory")
        if self.official:
            if self.data_root is None or self.data_root != self.data_root.resolve():
                raise IdentitySourceError("official S7 source capability requires an exact root")
            memberships = dict(self.artifact_memberships)
            if not memberships:
                raise IdentitySourceError(
                    "official S7 source capability has no artifact membership"
                )
            for (table, path), value in memberships.items():
                release_id, manifest_path, manifest_sha256, ref = value
                if (
                    table not in S7_SOURCE_PINS
                    or not isinstance(ref, ArtifactRef)
                    or ref.path != path
                    or ref.table != table
                    or ref.role is not ArtifactRole.DATA
                    or release_id != S7_SOURCE_PINS[table].release_id
                    or manifest_path != f"manifests/silver/releases/release_id={release_id}.json"
                    or manifest_sha256 != S7_SOURCE_PINS[table].release_manifest_sha256
                ):
                    raise IdentitySourceError(
                        "official S7 source capability artifact membership is invalid"
                    )
            expected_counts = {table: pin.artifact_count for table, pin in S7_SOURCE_PINS.items()}
            actual_counts = {
                table: sum(key[0] == table for key in memberships) for table in S7_SOURCE_PINS
            }
            sessions = tuple(self.authorized_sessions)
            if self.physical_scope == _FULL_PHYSICAL_SCOPE:
                if (
                    actual_counts != expected_counts
                    or sessions
                    or self.preview_control_binding is not None
                ):
                    raise IdentitySourceError(
                        "full official S7 source capability membership is incomplete"
                    )
            elif self.physical_scope in {
                _PREVIEW_PHYSICAL_SCOPE,
                _DIRECTIONAL_PREVIEW_PHYSICAL_SCOPE,
            }:
                if (
                    not sessions
                    or tuple(sorted(set(sessions))) != sessions
                    or self.preview_control_binding is None
                    or len(self.preview_control_binding) != 4
                    or any(
                        not isinstance(item, str) or not _SHA256.fullmatch(item)
                        for item in self.preview_control_binding
                    )
                ):
                    raise IdentitySourceError(
                        "bounded preview source capability controls are invalid"
                    )
                expected_keys: set[tuple[str, date]] = {
                    (table, session)
                    for table in ("asset_observation_daily", "universe_source_daily")
                    for session in sessions
                }
                actual_keys: set[tuple[str, date]] = set()
                for table, path in memberships:
                    match = _SESSION_PARTITION.search(path)
                    if match is None:
                        raise IdentitySourceError(
                            "bounded preview capability contains a non-daily artifact"
                        )
                    actual_keys.add((table, date.fromisoformat(match.group(1))))
                if actual_keys != expected_keys or len(memberships) != len(expected_keys):
                    raise IdentitySourceError(
                        "bounded preview capability differs from its exact daily scope"
                    )
            else:
                raise IdentitySourceError("official S7 source capability scope is invalid")
            object.__setattr__(self, "artifact_memberships", MappingProxyType(memberships))
            object.__setattr__(self, "authorized_sessions", sessions)
        elif (
            self.data_root is not None
            or self.artifact_memberships
            or self.physical_scope != _TEST_PHYSICAL_SCOPE
            or self.authorized_sessions
            or self.preview_control_binding is not None
        ):
            raise IdentitySourceError("test S7 source capability cannot carry official membership")

    def require_artifact_membership(
        self,
        *,
        table: str,
        release_id: str,
        release_manifest_path: str,
        release_manifest_sha256: str,
        ref: ArtifactRef,
        path: Path,
    ) -> None:
        """Bind a physical artifact to the exact membership issued by the factory."""

        if not self.official:
            return
        expected = self.artifact_memberships.get((table, ref.path))
        if expected != (
            release_id,
            release_manifest_path,
            release_manifest_sha256,
            ref,
        ):
            raise IdentitySourceError("S7 artifact is outside its capability release membership")
        assert self.data_root is not None
        if safe_relative_path(self.data_root, ref.path) != path.resolve():
            raise IdentitySourceError("S7 artifact path differs from its capability membership")

    def require_factory_issued(self) -> None:
        if self.official and self not in _FACTORY_ISSUED_OFFICIAL_CAPABILITIES:
            raise IdentitySourceError("official S7 capability was not issued by the source factory")

    def require_approved_preview_scope(
        self,
        *,
        plan_id: str,
        plan_sha256: str,
        approval_id: str,
        approval_sha256: str,
        sessions: Sequence[date],
    ) -> None:
        self.require_factory_issued()
        if (
            not self.official
            or self.physical_scope != _PREVIEW_PHYSICAL_SCOPE
            or self.preview_control_binding
            != (plan_id, plan_sha256, approval_id, approval_sha256)
            or self.authorized_sessions != tuple(sessions)
        ):
            raise IdentitySourceError(
                "S7 source capability crosses its approved bounded preview scope"
            )

    def require_approved_directional_preview_scope(
        self,
        *,
        plan_id: str,
        plan_sha256: str,
        approval_id: str,
        approval_sha256: str,
        sessions: Sequence[date],
    ) -> None:
        """Require the dedicated exact directional Plan/Approval capability."""

        self.require_factory_issued()
        if (
            not self.official
            or self.physical_scope != _DIRECTIONAL_PREVIEW_PHYSICAL_SCOPE
            or self.preview_control_binding
            != (plan_id, plan_sha256, approval_id, approval_sha256)
            or self.authorized_sessions != tuple(sessions)
        ):
            raise IdentitySourceError(
                "S7 source capability crosses its approved directional preview scope"
            )


_FACTORY_ISSUED_OFFICIAL_CAPABILITIES: WeakSet[_IdentitySourceCapability] = WeakSet()


@dataclass(frozen=True, slots=True)
class IdentitySourcePin:
    table: str
    release_id: str
    release_manifest_sha256: str
    build_id: str
    artifact_count: int
    row_count: int
    evidence_only_s4: bool

    def __post_init__(self) -> None:
        if not _IDENTIFIER.fullmatch(self.table):
            raise IdentitySourceError("S7 source pin table is invalid")
        for label, value in (
            ("release_id", self.release_id),
            ("release_manifest_sha256", self.release_manifest_sha256),
            ("build_id", self.build_id),
        ):
            if not isinstance(value, str) or not _SHA256.fullmatch(value):
                raise IdentitySourceError(f"S7 source pin {label} is invalid")
        if type(self.artifact_count) is not int or self.artifact_count <= 0:
            raise IdentitySourceError("S7 source artifact count must be positive")
        if type(self.row_count) is not int or self.row_count < 0:
            raise IdentitySourceError("S7 source row count must be nonnegative")
        if type(self.evidence_only_s4) is not bool:
            raise IdentitySourceError("S7 evidence-only marker must be a native bool")


S7_SOURCE_PINS: Final[Mapping[str, IdentitySourcePin]] = MappingProxyType(
    {
        pin.table: pin
        for pin in (
            IdentitySourcePin(
                table="asset_observation_daily",
                release_id="26819530e50cb92cbe0ec833d4b731b959c8bd2463ee2197255c02994241d44c",
                release_manifest_sha256=(
                    "f5fb26e75f44382caddf980e8fdf88a77903465b55bfd367f8d9029852848084"
                ),
                build_id="9e3b5df531c01d1bcdd73cbd9cdf747bd30cdff459481b262e1ed7a23f40acc4",
                artifact_count=2_513,
                row_count=69_381_182,
                evidence_only_s4=True,
            ),
            IdentitySourcePin(
                table="asset_observation_version",
                release_id="b422fd05df859b33587b8ece80d078247dd972d01d272710ef49c3529b0e54be",
                release_manifest_sha256=(
                    "0ea30b7cf2338e6067b82eff455a3973c59fcc20b433a0de9ba486ec9d8deaf3"
                ),
                build_id="59708791dc897214d3151dfd7da6b15534800afabf0c36dd36c566bd8d01ef9a",
                artifact_count=2_513,
                row_count=9_706,
                evidence_only_s4=True,
            ),
            IdentitySourcePin(
                table="universe_source_daily",
                release_id="c7e0d9a75857cbca130ba8873a737411ccb2f11d3e711ee0c0b0d9d0e2f5c614",
                release_manifest_sha256=(
                    "6b2c6ca1b612c4c38ddc8e359c1402c177a4f19b0295604d42b78bcd5804596d"
                ),
                build_id="21921c72c4be79665d41077664f8f027a1beb9ac0600ff4c6610d4f40638b185",
                artifact_count=2_513,
                row_count=69_376_329,
                evidence_only_s4=True,
            ),
            IdentitySourcePin(
                table="ticker_event_request_status",
                release_id="afc63db6850fb50295daa8e6e499c52fe1c16b8290b7932b08aea67531ff98eb",
                release_manifest_sha256=(
                    "29a8c5dbe1de1fbdc819a8e8a08f998967cde2ea19c3bb56e94b34bdea9fdb11"
                ),
                build_id="7ff845634148274b61c2f515cb66cb9e94f8bb8a5e1abe47316343eaa9f22ca1",
                artifact_count=1,
                row_count=15_173,
                evidence_only_s4=False,
            ),
            IdentitySourcePin(
                table="ticker_change_event",
                release_id="18a7eb3dd6805b94151f5b6ce0167c19dbeb328f45bec7c2f806dac42b8a6350",
                release_manifest_sha256=(
                    "34cff4cdacbdace305f5ee541c101112a5a7f7fb4e572a3c2405509cf178ba50"
                ),
                build_id="7753688e3d4f19658ca5657b2dc5ccb9bf4c4b229b3c58dc68b255d5999735d2",
                artifact_count=1,
                row_count=12_895,
                evidence_only_s4=False,
            ),
            IdentitySourcePin(
                table="ticker_overview_safe",
                release_id="8715f90d0e01f990e9738b9266edfeb2830a76d59a00ae4fb7490d9f077092a5",
                release_manifest_sha256=(
                    "a830ad88706393db8b28534379538149aa676e254ca87fd9cbb046ce4d2b51fe"
                ),
                build_id="f9e66da7f8aa86f9a2eacff4ee745874776f52d62182d3554d99c7f9b5b90ec0",
                artifact_count=1,
                row_count=30_570,
                evidence_only_s4=False,
            ),
        )
    }
)


@dataclass(frozen=True, slots=True)
class IdentityPublishedSource:
    """One exact published source after its complete trust chain was verified."""

    pin: IdentitySourcePin
    published: PublishedRelease
    release_manifest_path: str
    release_manifest_sha256: str
    data_root: Path | None = None
    physical_artifacts: tuple[tuple[ArtifactRef, Path], ...] | None = None

    @property
    def artifact_bindings(self) -> tuple[tuple[ArtifactRef, Path], ...]:
        """Return only artifacts physically authorized by this source capability."""

        if self.physical_artifacts is not None:
            return tuple(self.physical_artifacts)
        outputs = tuple(self.published.release.outputs)
        paths = tuple(self.published.data_paths)
        if len(outputs) != len(paths):
            raise IdentitySourceError("S7 release artifact/path counts differ")
        return tuple(zip(outputs, paths, strict=True))

    @property
    def data_paths(self) -> tuple[Path, ...]:
        return tuple(path for _, path in self.artifact_bindings)


@dataclass(frozen=True, slots=True)
class IdentitySourceArtifact:
    """One physical DATA artifact inside an exact pinned Silver release."""

    table: str
    release_id: str
    release_manifest_path: str
    release_manifest_sha256: str
    ref: ArtifactRef
    path: Path
    _bundle_capability: _IdentitySourceCapability = field(repr=False)

    def __post_init__(self) -> None:
        if self.ref.table != self.table or self.ref.row_count is None:
            raise IdentitySourceError("S7 physical artifact has invalid table/row metadata")
        if not self.path.is_file() or self.path.is_symlink():
            raise IdentitySourceError("S7 physical source artifact is unavailable or a symlink")
        if not isinstance(self._bundle_capability, _IdentitySourceCapability):
            raise IdentitySourceError("S7 physical artifact has no bundle capability")
        self._bundle_capability.require_artifact_membership(
            table=self.table,
            release_id=self.release_id,
            release_manifest_path=self.release_manifest_path,
            release_manifest_sha256=self.release_manifest_sha256,
            ref=self.ref,
            path=self.path,
        )

    @property
    def official(self) -> bool:
        return self._bundle_capability.official

    def require_official(self) -> None:
        if not self.official:
            raise IdentitySourceError("test S7 source artifacts cannot attest production rows")
        self._bundle_capability.require_factory_issued()


@dataclass(frozen=True, slots=True)
class IdentitySourceBatch:
    """A bounded batch with an exact physical Parquet row locator."""

    artifact: IdentitySourceArtifact
    row_group: int
    row_index_in_group: int
    batch: pa.RecordBatch
    _bundle_capability: _IdentitySourceCapability = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.row_group) is not int or self.row_group < 0:
            raise IdentitySourceError("S7 row_group must be a nonnegative native int")
        if type(self.row_index_in_group) is not int or self.row_index_in_group < 0:
            raise IdentitySourceError("S7 row_index_in_group must be a nonnegative native int")
        if not isinstance(self.batch, pa.RecordBatch) or self.batch.num_rows <= 0:
            raise IdentitySourceError("S7 physical batch must contain rows")
        if self._bundle_capability is not self.artifact._bundle_capability:
            raise IdentitySourceError("S7 physical batch/artifact capabilities differ")

    @property
    def official(self) -> bool:
        return self._bundle_capability.official

    def require_official(self) -> None:
        if not self.official:
            raise IdentitySourceError("test S7 source batches cannot attest production rows")
        self._bundle_capability.require_factory_issued()


class IdentitySourceBundle:
    """Read-only six-release S7 input with bounded Parquet streaming."""

    def __init__(
        self,
        sources: Mapping[str, IdentityPublishedSource],
        *,
        _capability: _IdentitySourceCapability | None = None,
    ) -> None:
        if not isinstance(_capability, _IdentitySourceCapability):
            raise IdentitySourceError(
                "S7 source bundles must be opened by the official factory or _for_testing"
            )
        if set(sources) != set(S7_SOURCE_PINS):
            raise IdentitySourceError("S7 source bundle is not the exact six-table set")
        self._sources = MappingProxyType(dict(sources))
        self._capability = _capability
        self._artifact_cache: dict[str, tuple[IdentitySourceArtifact, ...]] = {}

    @classmethod
    def _for_testing(
        cls,
        sources: Mapping[str, IdentityPublishedSource],
    ) -> IdentitySourceBundle:
        """Build an explicitly non-official fixture bundle for unit tests only."""

        return cls(
            sources,
            _capability=_IdentitySourceCapability(
                official=False,
                data_root=None,
                artifact_memberships=MappingProxyType({}),
                physical_scope=_TEST_PHYSICAL_SCOPE,
                authorized_sessions=(),
                preview_control_binding=None,
                _seal=_TEST_CAPABILITY_SEAL,
            ),
        )

    @property
    def binding_id(self) -> str:
        return S7_SIX_RELEASE_BINDING_ID

    @property
    def official(self) -> bool:
        return self._capability.official

    @property
    def data_root(self) -> Path | None:
        """Return the exact official root; fixture bundles deliberately expose none."""

        return self._capability.data_root

    def require_official(self) -> None:
        if not self.official:
            raise IdentitySourceError("test S7 source bundles cannot attest production rows")
        self._capability.require_factory_issued()

    def require_approved_preview_scope(
        self,
        *,
        plan_id: str,
        plan_sha256: str,
        approval_id: str,
        approval_sha256: str,
        sessions: Sequence[date],
    ) -> None:
        """Fail unless this bundle is bound to the exact approved preview controls."""

        self._capability.require_approved_preview_scope(
            plan_id=plan_id,
            plan_sha256=plan_sha256,
            approval_id=approval_id,
            approval_sha256=approval_sha256,
            sessions=sessions,
        )

    def require_approved_directional_preview_scope(
        self,
        *,
        plan_id: str,
        plan_sha256: str,
        approval_id: str,
        approval_sha256: str,
        sessions: Sequence[date],
    ) -> None:
        self._capability.require_approved_directional_preview_scope(
            plan_id=plan_id,
            plan_sha256=plan_sha256,
            approval_id=approval_id,
            approval_sha256=approval_sha256,
            sessions=sessions,
        )

    @property
    def sources(self) -> Mapping[str, IdentityPublishedSource]:
        return self._sources

    def iter_batches(
        self,
        table: str,
        *,
        columns: Sequence[str] | None = None,
        batch_size: int = 65_536,
    ) -> Iterator[pa.RecordBatch]:
        """Yield bounded batches after validating requested columns against the contract."""

        if type(batch_size) is not int or batch_size <= 0 or batch_size > 1_000_000:
            raise IdentitySourceError("S7 batch_size must be in [1, 1000000]")
        for physical in self.iter_physical_batches(
            table,
            columns=columns,
            batch_size=batch_size,
        ):
            yield physical.batch

    def artifacts(self, table: str) -> tuple[IdentitySourceArtifact, ...]:
        """Return exact release DATA refs paired to their already verified paths."""

        cached = self._artifact_cache.get(table)
        if cached is not None:
            return cached
        try:
            source = self._sources[table]
        except KeyError as exc:
            raise IdentitySourceError(f"table is outside the S7 source binding: {table}") from exc
        artifacts: list[IdentitySourceArtifact] = []
        for ref, path in source.artifact_bindings:
            if source.data_root is not None:
                expected = safe_relative_path(source.data_root, ref.path)
                if expected != path.resolve():
                    raise IdentitySourceError("S7 release artifact path binding differs")
            artifacts.append(
                IdentitySourceArtifact(
                    table=table,
                    release_id=source.pin.release_id,
                    release_manifest_path=source.release_manifest_path,
                    release_manifest_sha256=source.release_manifest_sha256,
                    ref=ref,
                    path=path,
                    _bundle_capability=self._capability,
                )
            )
        result = tuple(artifacts)
        self._artifact_cache[table] = result
        return result

    def iter_physical_batches(
        self,
        table: str,
        *,
        columns: Sequence[str] | None = None,
        batch_size: int = 65_536,
        artifacts: Sequence[IdentitySourceArtifact] | None = None,
    ) -> Iterator[IdentitySourceBatch]:
        """Yield row-group-aware batches from exact release artifacts only."""

        if type(batch_size) is not int or batch_size <= 0 or batch_size > 1_000_000:
            raise IdentitySourceError("S7 batch_size must be in [1, 1000000]")
        source = self._sources.get(table)
        if source is None:
            raise IdentitySourceError(f"table is outside the S7 source binding: {table}")
        selected = None if columns is None else tuple(columns)
        if selected is not None:
            if not selected or len(set(selected)) != len(selected):
                raise IdentitySourceError("S7 selected columns must be nonempty and unique")
            allowed = set(source.published.contract.arrow_schema.names)
            unknown = sorted(set(selected) - allowed)
            if unknown:
                raise IdentitySourceError(f"S7 selected columns are not contracted: {unknown}")
        all_artifacts = self.artifacts(table)
        available = {item.ref.path: item for item in all_artifacts}
        if len(available) != len(all_artifacts):
            raise IdentitySourceError("S7 exact release contains duplicate artifact paths")
        requested = tuple(available.values()) if artifacts is None else tuple(artifacts)
        if len({item.ref.path for item in requested}) != len(requested):
            raise IdentitySourceError("S7 requested physical artifacts are duplicated")
        for artifact in requested:
            if available.get(artifact.ref.path) != artifact:
                raise IdentitySourceError("S7 requested artifact is outside the exact release")
            if (
                artifact.path.stat().st_size != artifact.ref.bytes
                or sha256_file(artifact.path) != artifact.ref.sha256
            ):
                raise IdentitySourceError(
                    f"S7 source artifact bytes changed after publication: {table}"
                )
            parquet = pq.ParquetFile(artifact.path)
            if not parquet.schema_arrow.equals(source.published.contract.arrow_schema):
                raise IdentitySourceError(f"S7 source schema changed after publication: {table}")
            if parquet.metadata.num_rows != artifact.ref.row_count:
                raise IdentitySourceError(f"S7 source row count changed after publication: {table}")
            for row_group in range(parquet.num_row_groups):
                offset = 0
                for batch in parquet.iter_batches(
                    batch_size=batch_size,
                    row_groups=(row_group,),
                    columns=selected,
                    use_threads=False,
                ):
                    yield IdentitySourceBatch(
                        artifact=artifact,
                        row_group=row_group,
                        row_index_in_group=offset,
                        batch=batch,
                        _bundle_capability=self._capability,
                    )
                    offset += batch.num_rows

    def daily_partition_artifacts(
        self,
        table: str,
        session_dates: Sequence[date],
    ) -> tuple[IdentitySourceArtifact, ...]:
        """Select one exact daily artifact per requested session without latest lookup."""

        sessions = tuple(session_dates)
        if not sessions or any(type(item) is not date for item in sessions):
            raise IdentitySourceError("S7 daily artifact scope requires native dates")
        if tuple(sorted(set(sessions))) != sessions:
            raise IdentitySourceError("S7 daily artifact sessions must be sorted and unique")
        by_session: dict[date, IdentitySourceArtifact] = {}
        for artifact in self.artifacts(table):
            match = _SESSION_PARTITION.search(artifact.ref.path)
            if match is None:
                raise IdentitySourceError("S7 daily release contains a non-session artifact")
            session = date.fromisoformat(match.group(1))
            if session in by_session:
                raise IdentitySourceError("S7 daily release contains duplicate session artifacts")
            by_session[session] = artifact
        missing = tuple(item for item in sessions if item not in by_session)
        if missing:
            raise IdentitySourceError(f"S7 daily release is missing scoped sessions: {missing!r}")
        return tuple(by_session[item] for item in sessions)

    def iter_bounce_observations(
        self,
        *,
        batch_size: int = 65_536,
    ) -> Iterator[IdentityObservation]:
        """Stream the exact S4 columns consumed by the discovery-only bounce detector."""

        # Local import keeps the generic release reader independent from detector logic.
        from ame_stocks_api.silver.identity_bounce import IdentityObservation

        columns = (
            "session_date",
            "ticker",
            "active_on_date",
            "composite_figi",
            "selected_source_record_id",
            "source_available_session",
        )
        for batch in self.iter_batches(
            "universe_source_daily",
            columns=columns,
            batch_size=batch_size,
        ):
            for row in batch.to_pylist():
                yield IdentityObservation(
                    session_date=row["session_date"],
                    ticker=row["ticker"],
                    active_on_date=row["active_on_date"],
                    observed_composite_figi=row["composite_figi"],
                    source_record_id=row["selected_source_record_id"],
                    source_available_session=row["source_available_session"],
                )


def open_approved_identity_directional_raw_preview_source_bundle(
    data_root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> IdentitySourceBundle:
    """Open only the exact twenty-two directional-preview artifacts in the Plan.

    This factory accepts no dates, tickers, ranges, artifacts or paths from the
    caller.  The independently recorded execution Approval is required, and the
    ordinary full-history and old detector-preview capabilities are not reused.
    """

    if not isinstance(data_root, Path):
        raise IdentitySourceError("approved directional preview data_root must be a Path")
    for label, value in (
        ("plan ID", plan_id),
        ("plan SHA-256", expected_plan_sha256),
        ("approval ID", approval_id),
        ("approval SHA-256", expected_approval_sha256),
    ):
        if not isinstance(value, str) or not _SHA256.fullmatch(value):
            raise IdentitySourceError(f"approved directional preview {label} is invalid")
    root = data_root.expanduser().resolve()
    if not root.is_dir() or root.is_symlink() or root != root.resolve():
        raise IdentitySourceError("approved directional preview root is unavailable or unsafe")

    from ame_stocks_api.silver.identity_directional_raw_preview_approval import (
        DirectionalRawPreviewExecutionApprovalStore,
        IdentityDirectionalRawPreviewExecutionApprovalError,
    )
    from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
        DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS,
    )
    from ame_stocks_api.silver.identity_directional_raw_preview_execution_plan import (
        DirectionalRawPreviewExecutionPlanStore,
        IdentityDirectionalRawPreviewExecutionPlanError,
    )
    from ame_stocks_api.silver.store import SilverStoreError

    try:
        plan_store = DirectionalRawPreviewExecutionPlanStore(root)
        plan, _ = plan_store.load_execution_plan(
            plan_id, expected_sha256=expected_plan_sha256
        )
        approval, _ = DirectionalRawPreviewExecutionApprovalStore(root).load_approval(
            approval_id, expected_sha256=expected_approval_sha256
        )
        if (
            approval.plan_id != plan.plan_id
            or approval.plan_sha256 != plan.sha256
            or approval.source_artifact_set_digest != plan.source_artifact_set_digest
            or approval.source_binding_manifest_id != plan.source_binding_manifest_id
            or approval.source_binding_manifest_sha256
            != plan.source_binding_manifest_sha256
            or approval.execution_data_root != str(root)
            or not approval.preview_execution_authorized
            or not approval.data_read_authorized
            or not approval.parquet_read_authorized
            or approval.source_discovery_authorized
            or approval.caller_scope_override_authorized
        ):
            raise IdentitySourceError("directional execution controls cross scope")
        source_pins = tuple(plan.source_artifacts)
        sessions = tuple(
            sorted({date.fromisoformat(item.session_date) for item in source_pins})
        )
        fixed_sessions = tuple(
            sorted(
                {
                    session
                    for _, case_sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
                    for session in case_sessions
                }
            )
        )
        if (
            len(source_pins) != 22
            or sessions != fixed_sessions
            or {
                (item.table, item.session_date) for item in source_pins
            }
            != {
                (table, session.isoformat())
                for table in ("asset_observation_daily", "universe_source_daily")
                for session in sessions
            }
        ):
            raise IdentitySourceError("directional Plan source spine is not exact")

        release_set_path = (
            "manifests/silver/release-sets/assets/"
            f"release_set_id={S7_S4_RELEASE_SET_ID}/manifest.json"
        )
        physical_release_set_path = safe_relative_path(root, release_set_path)
        if (
            not physical_release_set_path.is_file()
            or physical_release_set_path.is_symlink()
            or sha256_file(physical_release_set_path)
            != S7_S4_RELEASE_SET_MANIFEST_SHA256
        ):
            raise IdentitySourceError("S7 S4 release-set marker differs")

        store = SilverStore(root)
        selected_by_table: dict[str, tuple[ArtifactRef, ...]] = {}
        for table in ("asset_observation_daily", "universe_source_daily"):
            pin = S7_SOURCE_PINS[table]
            release, document = store.load_release(pin.release_id)
            _verify_release_pin_metadata(pin, release, document.path, document.sha256)
            selected = _select_daily_preview_outputs(
                release, table=table, sessions=sessions
            )
            expected_by_session = {
                date.fromisoformat(item.session_date): item
                for item in source_pins
                if item.table == table
            }
            for output in selected:
                match = _SESSION_PARTITION.search(output.path)
                if match is None:
                    raise IdentitySourceError("directional DATA path has no session")
                expected = expected_by_session[date.fromisoformat(match.group(1))]
                if (
                    output.path != expected.path
                    or output.sha256 != expected.sha256
                    or output.bytes != expected.bytes
                    or output.row_count != expected.row_count
                    or output.table != expected.table
                    or output.schema_digest != expected.schema_digest
                    or release.contract_id != expected.source_contract_id
                    or pin.release_id != expected.release_id
                    or pin.release_manifest_sha256 != expected.release_manifest_sha256
                ):
                    raise IdentitySourceError(
                        "directional source artifact differs from embedded Plan ref"
                    )
            selected_by_table[table] = selected
        _verify_directional_preview_metadata_caps(plan.resource_caps, selected_by_table)

        ordinary_reader = PublishedSilverReader(root)
        s4_reader = PublishedAssetEvidenceReader(root)
        sources: dict[str, IdentityPublishedSource] = {}
        for table, pin in S7_SOURCE_PINS.items():
            selected_outputs = selected_by_table.get(table, ())
            selected_paths = tuple(sorted(item.path for item in selected_outputs))
            if pin.evidence_only_s4:
                evidence, verified_outputs = (
                    s4_reader._inspect_selected_for_identity_preview(
                        pin.release_id, selected_paths
                    )
                )
                published = PublishedRelease(
                    release=evidence.release,
                    contract=evidence.contract,
                    build=evidence.build,
                    data_paths=evidence.data_paths,
                )
            else:
                published, release_set, verified_outputs = (
                    ordinary_reader._inspect_selected_for_identity_preview(
                        pin.release_id, selected_paths
                    )
                )
                if release_set is not None:
                    raise IdentitySourceError(
                        "non-S4 directional source unexpectedly has a release set"
                    )
            if tuple(verified_outputs) != tuple(
                sorted(selected_outputs, key=lambda item: item.path)
            ):
                raise IdentitySourceError("directional physical artifact set differs")
            release, document = store.load_release(pin.release_id)
            if release.to_dict() != published.release.to_dict():
                raise IdentitySourceError(f"S7 release changed on reread: {table}")
            _verify_pin(pin, published, document.path, document.sha256)
            _verify_declared_identity_source(pin, published)
            sources[table] = IdentityPublishedSource(
                pin=pin,
                published=published,
                release_manifest_path=document.path,
                release_manifest_sha256=document.sha256,
                data_root=root,
                physical_artifacts=tuple(
                    zip(verified_outputs, published.data_paths, strict=True)
                ),
            )
        return _build_official_identity_directional_preview_source_bundle(
            root,
            sources,
            plan_id=plan.plan_id,
            plan_sha256=plan.sha256,
            approval_id=approval.approval_id,
            approval_sha256=approval.sha256,
            sessions=sessions,
        )
    except IdentitySourceError:
        raise
    except (
        IdentityDirectionalRawPreviewExecutionPlanError,
        IdentityDirectionalRawPreviewExecutionApprovalError,
        SilverStoreError,
        OSError,
    ) as exc:
        raise IdentitySourceError(
            "approved directional preview source bundle cannot open"
        ) from exc


def _verify_directional_preview_metadata_caps(
    caps: object,
    selected_by_table: Mapping[str, tuple[ArtifactRef, ...]],
) -> None:
    asset = selected_by_table["asset_observation_daily"]
    universe = selected_by_table["universe_source_daily"]
    values = (
        (
            sum(int(item.row_count or 0) for item in asset),
            getattr(caps, "scanned_asset_row_hard_cap", -1),
            "asset rows",
        ),
        (
            sum(int(item.row_count or 0) for item in universe),
            getattr(caps, "scanned_universe_row_hard_cap", -1),
            "universe rows",
        ),
        (
            sum(int(item.row_count or 0) for item in (*asset, *universe)),
            getattr(caps, "scanned_total_row_hard_cap", -1),
            "total rows",
        ),
        (
            len(asset) + len(universe),
            getattr(caps, "expected_physical_artifact_count", -1),
            "artifact count",
        ),
        (
            sum(item.bytes for item in (*asset, *universe)),
            getattr(caps, "source_bytes_hard_cap", -1),
            "source bytes",
        ),
    )
    for observed, limit, label in values:
        if type(limit) is not int or limit <= 0 or observed > limit:
            raise IdentitySourceError(
                f"approved directional metadata exceeds {label} cap"
            )
    if len(asset) + len(universe) != getattr(
        caps, "expected_physical_artifact_count", -1
    ):
        raise IdentitySourceError("approved directional artifact count is not exact")


def _build_official_identity_directional_preview_source_bundle(
    data_root: Path,
    sources: Mapping[str, IdentityPublishedSource],
    *,
    plan_id: str,
    plan_sha256: str,
    approval_id: str,
    approval_sha256: str,
    sessions: tuple[date, ...],
) -> IdentitySourceBundle:
    root = data_root.expanduser().resolve()
    _verify_official_identity_preview_sources(root, sources, sessions=sessions)
    memberships = {
        (table, ref.path): (
            source.pin.release_id,
            source.release_manifest_path,
            source.release_manifest_sha256,
            ref,
        )
        for table, source in sources.items()
        for ref, _ in source.artifact_bindings
    }
    capability = _IdentitySourceCapability(
        official=True,
        data_root=root,
        artifact_memberships=MappingProxyType(memberships),
        physical_scope=_DIRECTIONAL_PREVIEW_PHYSICAL_SCOPE,
        authorized_sessions=sessions,
        preview_control_binding=(plan_id, plan_sha256, approval_id, approval_sha256),
        _seal=_OFFICIAL_CAPABILITY_SEAL,
    )
    bundle = IdentitySourceBundle(sources, _capability=capability)
    _FACTORY_ISSUED_OFFICIAL_CAPABILITIES.add(capability)
    bundle.require_approved_directional_preview_scope(
        plan_id=plan_id,
        plan_sha256=plan_sha256,
        approval_id=approval_id,
        approval_sha256=approval_sha256,
        sessions=sessions,
    )
    return bundle


def open_approved_identity_preview_source_bundle(
    data_root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> IdentitySourceBundle:
    """Open only physical S4 partitions authorized by one exact preview approval.

    All six release manifests, workflows, contracts, builds, approvals, and the complete
    S4 release-set control plane remain authenticated.  Unselected DATA artifacts are
    validated as immutable manifest metadata but are never resolved or read from disk.
    """

    if not isinstance(data_root, Path):
        raise IdentitySourceError("approved preview data_root must be a Path")
    for label, value in (
        ("plan ID", plan_id),
        ("plan SHA-256", expected_plan_sha256),
        ("approval ID", approval_id),
        ("approval SHA-256", expected_approval_sha256),
    ):
        if not isinstance(value, str) or not _SHA256.fullmatch(value):
            raise IdentitySourceError(f"approved preview {label} is invalid")
    root = data_root.expanduser().resolve()
    if not root.is_dir() or root.is_symlink() or root != root.resolve():
        raise IdentitySourceError("approved preview data_root is unavailable or unsafe")

    # Local imports avoid a module cycle: preview_plan pins the constants above.
    from ame_stocks_api.silver.calendar_artifact import (
        XNYSCalendarArtifactError,
        load_xnys_calendar_artifact,
    )
    from ame_stocks_api.silver.identity_preview_plan import (
        IdentityPreviewPlanError,
        IdentityPreviewPlanStore,
    )
    from ame_stocks_api.silver.store import SilverStoreError

    try:
        control_store = IdentityPreviewPlanStore(root)
        plan, _ = control_store.load_plan(plan_id, expected_sha256=expected_plan_sha256)
        approval, _ = control_store.load_approval(
            approval_id,
            expected_sha256=expected_approval_sha256,
        )
        calendar = load_xnys_calendar_artifact(
            root,
            calendar_artifact_id=plan.calendar_artifact_id,
            expected_sha256=plan.calendar_artifact_sha256,
        )
        if approval.plan_id != plan.plan_id or approval.plan_sha256 != plan.sha256:
            raise IdentitySourceError("approved preview controls cross plans")
        sessions = tuple(
            item.session_date
            for item in calendar.sessions
            if plan.start_session <= item.session_date <= plan.end_session
        )
        if (
            len(sessions) != plan.session_count
            or not sessions
            or sessions[0] != plan.start_session
            or sessions[-1] != plan.end_session
        ):
            raise IdentitySourceError("approved preview session spine does not reproduce")

        release_set_path = (
            "manifests/silver/release-sets/assets/"
            f"release_set_id={S7_S4_RELEASE_SET_ID}/manifest.json"
        )
        physical_release_set_path = safe_relative_path(root, release_set_path)
        if (
            not physical_release_set_path.is_file()
            or physical_release_set_path.is_symlink()
            or sha256_file(physical_release_set_path)
            != S7_S4_RELEASE_SET_MANIFEST_SHA256
        ):
            raise IdentitySourceError(
                "S7 S4 release-set marker differs from the reviewed binding"
            )

        store = SilverStore(root)
        selected_by_table: dict[str, tuple[ArtifactRef, ...]] = {}
        for table in ("asset_observation_daily", "universe_source_daily"):
            pin = S7_SOURCE_PINS[table]
            release, document = store.load_release(pin.release_id)
            _verify_release_pin_metadata(pin, release, document.path, document.sha256)
            selected_by_table[table] = _select_daily_preview_outputs(
                release,
                table=table,
                sessions=sessions,
            )
        _verify_preview_metadata_caps(plan.resource_caps, selected_by_table)

        ordinary_reader = PublishedSilverReader(root)
        s4_reader = PublishedAssetEvidenceReader(root)
        sources: dict[str, IdentityPublishedSource] = {}
        for table, pin in S7_SOURCE_PINS.items():
            selected_outputs = selected_by_table.get(table, ())
            selected_paths = tuple(sorted(item.path for item in selected_outputs))
            if pin.evidence_only_s4:
                evidence, verified_outputs = (
                    s4_reader._inspect_selected_for_identity_preview(
                        pin.release_id,
                        selected_paths,
                    )
                )
                published = PublishedRelease(
                    release=evidence.release,
                    contract=evidence.contract,
                    build=evidence.build,
                    data_paths=evidence.data_paths,
                )
            else:
                published, release_set, verified_outputs = (
                    ordinary_reader._inspect_selected_for_identity_preview(
                        pin.release_id,
                        selected_paths,
                    )
                )
                if release_set is not None:
                    raise IdentitySourceError(
                        f"non-S4 preview source unexpectedly requires a release set: {table}"
                    )
            if tuple(verified_outputs) != tuple(
                sorted(selected_outputs, key=lambda item: item.path)
            ):
                raise IdentitySourceError(
                    f"physically verified preview artifacts differ for {table}"
                )
            release, document = store.load_release(pin.release_id)
            if release.to_dict() != published.release.to_dict():
                raise IdentitySourceError(f"S7 release was inconsistent on reread: {table}")
            _verify_pin(pin, published, document.path, document.sha256)
            _verify_declared_identity_source(pin, published)
            physical_artifacts = tuple(
                zip(verified_outputs, published.data_paths, strict=True)
            )
            sources[table] = IdentityPublishedSource(
                pin=pin,
                published=published,
                release_manifest_path=document.path,
                release_manifest_sha256=document.sha256,
                data_root=root,
                physical_artifacts=physical_artifacts,
            )
        return _build_official_identity_preview_source_bundle(
            root,
            sources,
            plan_id=plan.plan_id,
            plan_sha256=plan.sha256,
            approval_id=approval.approval_id,
            approval_sha256=approval.sha256,
            sessions=sessions,
        )
    except IdentitySourceError:
        raise
    except (
        IdentityPreviewPlanError,
        XNYSCalendarArtifactError,
        SilverStoreError,
        OSError,
    ) as exc:
        raise IdentitySourceError("approved preview source bundle cannot be opened") from exc


def _select_daily_preview_outputs(
    release: ReleaseManifest,
    *,
    table: str,
    sessions: tuple[date, ...],
) -> tuple[ArtifactRef, ...]:
    requested = frozenset(sessions)
    by_session: dict[date, ArtifactRef] = {}
    for output in release.outputs:
        match = _SESSION_PARTITION.search(output.path)
        if match is None:
            raise IdentitySourceError(f"daily S7 release contains non-session DATA: {table}")
        session = date.fromisoformat(match.group(1))
        if session not in requested:
            continue
        if session in by_session:
            raise IdentitySourceError(f"daily S7 release duplicates a preview session: {table}")
        by_session[session] = output
    missing = tuple(session for session in sessions if session not in by_session)
    if missing:
        raise IdentitySourceError(f"daily S7 release misses preview sessions: {table}")
    return tuple(by_session[session] for session in sessions)


def _verify_preview_metadata_caps(
    caps: object,
    selected_by_table: Mapping[str, tuple[ArtifactRef, ...]],
) -> None:
    asset = selected_by_table["asset_observation_daily"]
    universe = selected_by_table["universe_source_daily"]
    asset_rows = sum(int(item.row_count or 0) for item in asset)
    universe_rows = sum(int(item.row_count or 0) for item in universe)
    total_rows = asset_rows + universe_rows
    total_artifacts = len(asset) + len(universe)
    total_bytes = sum(item.bytes for item in (*asset, *universe))
    checks = (
        (asset_rows, getattr(caps, "asset_parent_scanned_row_cap", -1), "asset rows"),
        (universe_rows, getattr(caps, "universe_scanned_row_cap", -1), "universe rows"),
        (total_rows, getattr(caps, "total_scanned_row_cap", -1), "total rows"),
        (total_artifacts, getattr(caps, "source_artifact_cap", -1), "artifacts"),
        (total_bytes, getattr(caps, "source_bytes_cap", -1), "bytes"),
    )
    for observed, limit, label in checks:
        if type(limit) is not int or limit <= 0 or observed > limit:
            raise IdentitySourceError(f"approved preview metadata exceeds its {label} cap")


def _build_official_identity_preview_source_bundle(
    data_root: Path,
    sources: Mapping[str, IdentityPublishedSource],
    *,
    plan_id: str,
    plan_sha256: str,
    approval_id: str,
    approval_sha256: str,
    sessions: tuple[date, ...],
) -> IdentitySourceBundle:
    root = data_root.expanduser().resolve()
    _verify_official_identity_preview_sources(root, sources, sessions=sessions)
    memberships = {
        (table, ref.path): (
            source.pin.release_id,
            source.release_manifest_path,
            source.release_manifest_sha256,
            ref,
        )
        for table, source in sources.items()
        for ref, _ in source.artifact_bindings
    }
    capability = _IdentitySourceCapability(
        official=True,
        data_root=root,
        artifact_memberships=MappingProxyType(memberships),
        physical_scope=_PREVIEW_PHYSICAL_SCOPE,
        authorized_sessions=sessions,
        preview_control_binding=(plan_id, plan_sha256, approval_id, approval_sha256),
        _seal=_OFFICIAL_CAPABILITY_SEAL,
    )
    bundle = IdentitySourceBundle(sources, _capability=capability)
    _FACTORY_ISSUED_OFFICIAL_CAPABILITIES.add(capability)
    return bundle


def _verify_official_identity_preview_sources(
    root: Path,
    sources: Mapping[str, IdentityPublishedSource],
    *,
    sessions: tuple[date, ...],
) -> None:
    if not root.is_dir() or root.is_symlink() or root != root.resolve():
        raise IdentitySourceError("official preview data_root is unavailable or unsafe")
    if set(sources) != set(S7_SOURCE_PINS):
        raise IdentitySourceError("official preview source set is not the exact six tables")
    store = SilverStore(root)
    expected_daily_keys = {
        (table, session)
        for table in ("asset_observation_daily", "universe_source_daily")
        for session in sessions
    }
    actual_daily_keys: set[tuple[str, date]] = set()
    for table, pin in S7_SOURCE_PINS.items():
        source = sources[table]
        if source.pin != pin or source.data_root != root:
            raise IdentitySourceError(f"official preview source binding differs for {table}")
        release, document = store.load_release(pin.release_id)
        if (
            release.to_dict() != source.published.release.to_dict()
            or document.path != source.release_manifest_path
            or document.sha256 != source.release_manifest_sha256
        ):
            raise IdentitySourceError(f"official preview release changed for {table}")
        _verify_pin(pin, source.published, document.path, document.sha256)
        _verify_declared_identity_source(pin, source.published)
        for ref, path in source.artifact_bindings:
            match = _SESSION_PARTITION.search(ref.path)
            if match is None:
                raise IdentitySourceError("preview physical scope contains non-daily DATA")
            actual_daily_keys.add((table, date.fromisoformat(match.group(1))))
            expected_path = safe_relative_path(root, ref.path)
            if path != expected_path or not path.is_file() or path.is_symlink():
                raise IdentitySourceError("preview physical artifact path changed")
    if actual_daily_keys != expected_daily_keys:
        raise IdentitySourceError("preview physical sources differ from approved sessions")


def _verify_declared_identity_source(
    pin: IdentitySourcePin,
    published: PublishedRelease,
) -> None:
    release = published.release
    contract = published.contract
    if (
        contract.table != pin.table
        or contract.domain != release.domain
        or contract.schema_version != release.schema_version
        or contract.contract_id != release.contract_id
    ):
        raise IdentitySourceError(f"S7 declared contract binding differs for {pin.table}")
    outputs = tuple(release.outputs)
    if len({item.path for item in outputs}) != len(outputs):
        raise IdentitySourceError(f"S7 declared DATA paths duplicate for {pin.table}")
    if any(
        item.role is not ArtifactRole.DATA
        or item.media_type != "application/vnd.apache.parquet"
        or item.table != pin.table
        or item.schema_digest != contract.schema_digest
        or item.row_count is None
        for item in outputs
    ):
        raise IdentitySourceError(f"S7 declared DATA metadata differs for {pin.table}")


def open_identity_source_bundle(data_root: Path) -> IdentitySourceBundle:
    """Open only the reviewed S7 six-release bundle; latest-release lookup is impossible."""

    root = data_root.expanduser().resolve()
    ordinary_reader = PublishedSilverReader(root)
    s4_reader = PublishedAssetEvidenceReader(root)
    store = SilverStore(root)
    release_set_path = (
        f"manifests/silver/release-sets/assets/release_set_id={S7_S4_RELEASE_SET_ID}/manifest.json"
    )
    physical_release_set_path = safe_relative_path(root, release_set_path)
    if (
        not physical_release_set_path.is_file()
        or sha256_file(physical_release_set_path) != S7_S4_RELEASE_SET_MANIFEST_SHA256
    ):
        raise IdentitySourceError("S7 S4 release-set marker differs from the reviewed binding")
    sources: dict[str, IdentityPublishedSource] = {}
    for table, pin in S7_SOURCE_PINS.items():
        if pin.evidence_only_s4:
            evidence = s4_reader.inspect(pin.release_id)
            published = PublishedRelease(
                release=evidence.release,
                contract=evidence.contract,
                build=evidence.build,
                data_paths=evidence.data_paths,
            )
        else:
            published = ordinary_reader.inspect(pin.release_id)
        release, document = store.load_release(pin.release_id)
        if release.to_dict() != published.release.to_dict():
            raise IdentitySourceError(f"S7 release was inconsistent on reread: {table}")
        _verify_pin(pin, published, document.path, document.sha256)
        sources[table] = IdentityPublishedSource(
            pin=pin,
            published=published,
            release_manifest_path=document.path,
            release_manifest_sha256=document.sha256,
            data_root=root,
        )
    return _build_official_identity_source_bundle(root, sources)


def _build_official_identity_source_bundle(
    data_root: Path,
    sources: Mapping[str, IdentityPublishedSource],
) -> IdentitySourceBundle:
    """Re-attest every physical binding before issuing an official capability."""

    root = data_root.expanduser().resolve()
    _verify_official_identity_sources(root, sources)
    memberships = {
        (table, ref.path): (
            source.pin.release_id,
            source.release_manifest_path,
            source.release_manifest_sha256,
            ref,
        )
        for table, source in sources.items()
        for ref, _ in source.artifact_bindings
    }
    capability = _IdentitySourceCapability(
        official=True,
        data_root=root,
        artifact_memberships=MappingProxyType(memberships),
        physical_scope=_FULL_PHYSICAL_SCOPE,
        authorized_sessions=(),
        preview_control_binding=None,
        _seal=_OFFICIAL_CAPABILITY_SEAL,
    )
    bundle = IdentitySourceBundle(
        sources,
        _capability=capability,
    )
    _FACTORY_ISSUED_OFFICIAL_CAPABILITIES.add(capability)
    return bundle


def _verify_official_identity_sources(
    root: Path,
    sources: Mapping[str, IdentityPublishedSource],
) -> None:
    """Verify exact pins, manifests, contracts, schemas and DATA paths again."""

    if not root.is_dir() or root.is_symlink() or root != root.resolve():
        raise IdentitySourceError("official S7 data_root is unavailable, unsafe, or inexact")
    if set(sources) != set(S7_SOURCE_PINS):
        raise IdentitySourceError("official S7 source bundle is not the exact six-table set")
    store = SilverStore(root)
    for table, expected_pin in S7_SOURCE_PINS.items():
        source = sources[table]
        if source.pin != expected_pin:
            raise IdentitySourceError(f"official S7 source pin object differs for {table}")
        if source.data_root is None or source.data_root != root:
            raise IdentitySourceError(f"official S7 source data_root differs for {table}")

        stored_release, document = store.load_release(expected_pin.release_id)
        if (
            stored_release.to_dict() != source.published.release.to_dict()
            or document.path != source.release_manifest_path
            or document.sha256 != source.release_manifest_sha256
        ):
            raise IdentitySourceError(f"official S7 release/manifest reread differs for {table}")
        manifest_path = safe_relative_path(root, source.release_manifest_path)
        if (
            not manifest_path.is_file()
            or manifest_path.is_symlink()
            or sha256_file(manifest_path) != expected_pin.release_manifest_sha256
        ):
            raise IdentitySourceError(f"official S7 release manifest differs for {table}")

        published = source.published
        release = published.release
        contract = published.contract
        _verify_pin(expected_pin, published, document.path, document.sha256)
        if (
            contract.table != table
            or contract.domain != release.domain
            or contract.schema_version != release.schema_version
            or contract.contract_id != release.contract_id
        ):
            raise IdentitySourceError(f"official S7 contract binding differs for {table}")

        outputs = release.outputs
        paths = published.data_paths
        if len(outputs) != len(paths) or len({item.path for item in outputs}) != len(outputs):
            raise IdentitySourceError(f"official S7 DATA artifact set differs for {table}")
        for ref, path in zip(outputs, paths, strict=True):
            expected_path = safe_relative_path(root, ref.path)
            if (
                ref.role is not ArtifactRole.DATA
                or ref.media_type != "application/vnd.apache.parquet"
                or ref.table != table
                or ref.schema_digest != contract.schema_digest
                or ref.row_count is None
                or path != expected_path
                or not path.is_file()
                or path.is_symlink()
            ):
                raise IdentitySourceError(f"official S7 DATA role/path differs for {table}")
            if path.stat().st_size != ref.bytes or sha256_file(path) != ref.sha256:
                raise IdentitySourceError(f"official S7 DATA bytes differ for {table}")
            parquet = pq.ParquetFile(path)
            if (
                not parquet.schema_arrow.equals(contract.arrow_schema)
                or parquet.metadata.num_rows != ref.row_count
            ):
                raise IdentitySourceError(f"official S7 DATA schema/rows differ for {table}")


def _verify_pin(
    pin: IdentitySourcePin,
    published: PublishedRelease,
    manifest_path: str,
    manifest_sha256: str,
) -> None:
    _verify_release_pin_metadata(
        pin,
        published.release,
        manifest_path,
        manifest_sha256,
    )
    if not manifest_path:
        raise IdentitySourceError(f"S7 release has no auditable manifest for {pin.table}")


def _verify_release_pin_metadata(
    pin: IdentitySourcePin,
    release: ReleaseManifest,
    manifest_path: str,
    manifest_sha256: str,
) -> None:
    if (
        release.table != pin.table
        or release.release_id != pin.release_id
        or release.build_id != pin.build_id
        or manifest_sha256 != pin.release_manifest_sha256
    ):
        raise IdentitySourceError(f"S7 source pin mismatch for {pin.table}")
    if len(release.outputs) != pin.artifact_count:
        raise IdentitySourceError(f"S7 artifact count mismatch for {pin.table}")
    if sum(output.row_count or 0 for output in release.outputs) != pin.row_count:
        raise IdentitySourceError(f"S7 row count mismatch for {pin.table}")


__all__ = [
    "S7_S4_RELEASE_SET_ID",
    "S7_S4_RELEASE_SET_MANIFEST_SHA256",
    "S7_SIX_RELEASE_BINDING_ID",
    "S7_SOURCE_PINS",
    "IdentityPublishedSource",
    "IdentitySourceArtifact",
    "IdentitySourceBatch",
    "IdentitySourceBundle",
    "IdentitySourceError",
    "IdentitySourcePin",
    "open_approved_identity_preview_source_bundle",
    "open_identity_source_bundle",
]
