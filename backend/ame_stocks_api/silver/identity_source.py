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
from ame_stocks_api.silver.contracts import ArtifactRef, ArtifactRole, SilverContractError
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
                    not isinstance(ref, ArtifactRef)
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
            if actual_counts != expected_counts:
                raise IdentitySourceError(
                    "official S7 source capability artifact membership is incomplete"
                )
            object.__setattr__(self, "artifact_memberships", MappingProxyType(memberships))
        elif self.data_root is not None or self.artifact_memberships:
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

    @property
    def data_paths(self) -> tuple[Path, ...]:
        return self.published.data_paths


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
        outputs = source.published.release.outputs
        paths = source.data_paths
        if len(outputs) != len(paths):
            raise IdentitySourceError("S7 release artifact/path counts differ")
        artifacts: list[IdentitySourceArtifact] = []
        for ref, path in zip(outputs, paths, strict=True):
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
        for ref in source.published.release.outputs
    }
    capability = _IdentitySourceCapability(
        official=True,
        data_root=root,
        artifact_memberships=MappingProxyType(memberships),
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
    release = published.release
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
    if not manifest_path or not published.data_paths:
        raise IdentitySourceError(f"S7 release has no auditable manifest/data for {pin.table}")
    if not all(path.is_file() for path in published.data_paths):
        raise IdentitySourceError(f"S7 verified source disappeared for {pin.table}")


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
    "open_identity_source_bundle",
]
