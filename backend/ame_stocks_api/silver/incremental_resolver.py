"""Pure, exact-pin resolver for S7.5 incremental Silver release chains.

The resolver deliberately knows nothing about a filesystem layout.  Callers provide an exact
top manifest pin and a loader that can dereference exactly one supplied pin.  This keeps reader
selection separate from discovery and makes it impossible for this module to implement an
implicit ``latest`` policy.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from itertools import pairwise
from types import MappingProxyType

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.incremental_contract import (
    ContentAttestedRelease,
    ControlValidatedCandidate,
    IncrementalContractError,
    IncrementalReleaseManifest,
    ManifestPin,
    PartitionReceipt,
    ReleaseType,
    RowVersionReceipt,
    ViewKind,
    _mint_content_attested_release_from_verified,
    verify_content_attested_release,
    verify_control_validated_candidate,
)

type PartitionKey = tuple[str, str]
type StableRowKey = tuple[str, str]
type RowVersionKey = tuple[str, str]


class IncrementalResolutionError(SilverContractError):
    """Raised when an exact incremental chain cannot be resolved unambiguously."""


@dataclass(frozen=True, slots=True)
class ResolvedIncrementalSnapshot:
    """Immutable logical snapshot selected from one exact release chain."""

    top_manifest_pin: ManifestPin
    view_kind: ViewKind
    cutoff_session: date
    release_chain: tuple[ManifestPin, ...]
    partition_receipts: Mapping[PartitionKey, PartitionReceipt]
    row_version_catalog: Mapping[RowVersionKey, RowVersionReceipt]
    audit_row_version_catalog: Mapping[RowVersionKey, RowVersionReceipt]
    row_versions_by_stable_key: Mapping[StableRowKey, tuple[RowVersionReceipt, ...]]
    terminal_row_versions: Mapping[StableRowKey, RowVersionReceipt]
    tombstoned_row_keys: tuple[StableRowKey, ...]
    compatibility_digests: Mapping[str, str]
    lineage_digest: str
    resolved_content_digest: str
    snapshot_digest: str


def _view_value(view_kind: object) -> str:
    value = getattr(view_kind, "value", view_kind)
    if value not in {"historical_as_known", "latest_reviewed_research"}:
        raise IncrementalResolutionError(f"unsupported resolved view: {value!r}")
    return str(value)


def _release_type_value(manifest: object) -> str:
    value = getattr(manifest.release_type, "value", manifest.release_type)
    if value not in {"base", "delta", "correction"}:
        raise IncrementalResolutionError(f"unsupported release type: {value!r}")
    return str(value)


def _pin_identity(pin: object) -> tuple[object, ...]:
    return (
        pin.release_id,
        getattr(pin, "manifest_path", getattr(pin, "path", None)),
        getattr(pin, "manifest_sha256", getattr(pin, "sha256", None)),
        getattr(pin, "manifest_bytes", getattr(pin, "bytes", None)),
        getattr(pin, "release_available_session", None),
    )


def _session(value: object, *, label: str) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise IncrementalResolutionError(f"{label} must be an ISO date") from exc
    raise IncrementalResolutionError(f"{label} must be a date")


def _compatibility(manifest: object) -> dict[str, str]:
    names = (
        "schema_digest",
        "transform_semantics_digest",
        "identity_policy_bundle_id",
        "calendar_digest",
    )
    return {name: getattr(manifest, name) for name in names}


def _manifest_availability(pin: object, manifest: object) -> date:
    pin_value = _session(
        getattr(pin, "release_available_session", None),
        label="manifest-pin release availability session",
    )
    body_value = _session(
        getattr(manifest, "release_available_session", None),
        label="manifest-body release availability session",
    )
    if pin_value != body_value:
        raise IncrementalResolutionError("manifest pin and body release availability differ")
    return body_value


def _manifest_cutoffs(manifest: object) -> tuple[date, date]:
    source_cutoff = _session(
        manifest.source_cutoff_session,
        label="manifest source_cutoff_session",
    )
    availability_cutoff = _session(
        manifest.availability_cutoff_session,
        label="manifest availability_cutoff_session",
    )
    if source_cutoff > availability_cutoff:
        raise IncrementalResolutionError("manifest source cutoff exceeds availability cutoff")
    return source_cutoff, availability_cutoff


def _added_rows(manifest: object) -> tuple[object, ...]:
    rows = getattr(manifest, "added_row_versions", None)
    if rows is None:
        rows = manifest.added_row_version_receipts
    return tuple(rows)


def _partition_key(receipt: object) -> PartitionKey:
    key = receipt.key
    return (str(key[0]), str(key[1]))


def _stable_row_key(receipt: object) -> StableRowKey:
    key = receipt.key
    return (str(key[0]), str(key[1]))


def _row_version_key(receipt: object) -> RowVersionKey:
    return (str(receipt.table_name), str(receipt.row_version_id))


def _available(receipt: object, *, view_value: str, cutoff: date) -> bool:
    if view_value == "latest_reviewed_research":
        return True
    return _session(receipt.availability_session, label="receipt availability_session") <= cutoff


def _as_digestable(value: object) -> object:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, tuple):
        return [_as_digestable(item) for item in value]
    if isinstance(value, date):
        return value.isoformat()
    return value


def validate_release_content_attestation(
    candidate: ControlValidatedCandidate,
    *,
    load_parent: Callable[[ManifestPin], ContentAttestedRelease | None],
) -> ContentAttestedRelease:
    """Validate a candidate's exact resolved receipt-graph attestation.

    Unlike a consumer query, this accepts no caller-selected view or cutoff. It
    resolves the exact validated candidate at its embedded publication
    availability, never at a freely supplied or backfilled date. This pure
    function does not verify storage bytes. Gate A also has no trusted
    append-only approval-event ledger, so correction candidates remain
    structurally reviewable but cannot receive this reader capability.
    A base/delta capability minted here is likewise metadata-only runtime
    state, not production publication or new-base cutover authority.
    """
    try:
        validated = verify_control_validated_candidate(candidate)
    except IncrementalContractError as exc:
        raise IncrementalResolutionError(
            "published content attestation requires a validated release capability"
        ) from exc
    if validated.manifest.release_type is ReleaseType.CORRECTION:
        raise IncrementalResolutionError(
            "Gate A correction approval-event attestation is unavailable"
        )
    top_manifest = validated.manifest
    top_manifest_pin = validated.manifest_pin
    top_identity = _pin_identity(top_manifest_pin)
    attested_cache: dict[tuple[object, ...], ContentAttestedRelease] = {}

    def cache_attested_chain(root: ContentAttestedRelease | None) -> None:
        cursor = root
        while cursor is not None:
            identity = _pin_identity(cursor.manifest_pin)
            existing = attested_cache.get(identity)
            if existing is not None:
                if existing.content_attestation_digest != cursor.content_attestation_digest:
                    raise IncrementalResolutionError(
                        "one exact parent pin has two content attestations"
                    )
                return
            attested_cache[identity] = cursor
            cursor = cursor.candidate.parent_release

    cache_attested_chain(validated.parent_release)

    def load_exact_chain(pin: ManifestPin) -> IncrementalReleaseManifest | None:
        if _pin_identity(pin) == top_identity:
            return validated.manifest
        cached = attested_cache.get(_pin_identity(pin))
        if cached is not None:
            return cached.manifest
        parent = load_parent(pin)
        if parent is None:
            return None
        try:
            attested_parent = verify_content_attested_release(parent)
        except IncrementalContractError as exc:
            raise IncrementalResolutionError(
                "content attestation parent is not publication-attested"
            ) from exc
        if attested_parent.manifest_pin != pin:
            raise IncrementalResolutionError(
                "content-attested parent does not match the requested exact pin"
            )
        cache_attested_chain(attested_parent)
        return attested_parent.manifest

    resolved = _resolve_manifest_snapshot(
        top_manifest_pin,
        view_kind=top_manifest.resolved_view,
        cutoff_session=top_manifest.release_available_session,
        load_parent=load_exact_chain,
    )
    if resolved.resolved_content_digest != top_manifest.resolved_content_digest:
        raise IncrementalResolutionError(
            "manifest resolved-content attestation does not match resolved release content"
        )
    try:
        return _mint_content_attested_release_from_verified(
            validated,
            resolved_content_digest=resolved.resolved_content_digest,
            snapshot_digest=resolved.snapshot_digest,
        )
    except IncrementalContractError as exc:  # pragma: no cover - digest checked above
        raise IncrementalResolutionError(
            "content-attested release capability could not be minted"
        ) from exc


def resolve_incremental_snapshot(
    top_manifest_pin: ManifestPin,
    *,
    view_kind: ViewKind,
    cutoff_session: date | str,
    load_parent: Callable[[ManifestPin], ContentAttestedRelease | None],
) -> ResolvedIncrementalSnapshot:
    """Resolve one exact chain of content-attested release capabilities."""

    attested_cache: dict[tuple[object, ...], ContentAttestedRelease] = {}

    def cache_attested_chain(root: ContentAttestedRelease) -> None:
        cursor: ContentAttestedRelease | None = root
        while cursor is not None:
            identity = _pin_identity(cursor.manifest_pin)
            existing = attested_cache.get(identity)
            if existing is not None:
                if existing.content_attestation_digest != cursor.content_attestation_digest:
                    raise IncrementalResolutionError(
                        "one exact release pin has two content attestations"
                    )
                return
            attested_cache[identity] = cursor
            cursor = cursor.candidate.parent_release

    def load_validated_body(pin: ManifestPin) -> IncrementalReleaseManifest | None:
        cached = attested_cache.get(_pin_identity(pin))
        if cached is not None:
            return cached.manifest
        value = load_parent(pin)
        if value is None:
            return None
        try:
            validated = verify_content_attested_release(value)
        except IncrementalContractError as exc:
            raise IncrementalResolutionError(
                "loader returned a release without content attestation"
            ) from exc
        if validated.manifest_pin != pin:
            raise IncrementalResolutionError(
                "validated release capability does not match the requested exact pin"
            )
        cache_attested_chain(validated)
        return validated.manifest

    top_manifest = load_validated_body(top_manifest_pin)
    if top_manifest is None:
        raise IncrementalResolutionError(
            f"missing exact content-attested release {top_manifest_pin.release_id}"
        )
    publication_snapshot = _resolve_manifest_snapshot(
        top_manifest_pin,
        view_kind=top_manifest.resolved_view,
        cutoff_session=top_manifest.release_available_session,
        load_parent=load_validated_body,
    )
    top_attested = attested_cache[_pin_identity(top_manifest_pin)]
    if (
        publication_snapshot.resolved_content_digest != top_manifest.resolved_content_digest
        or publication_snapshot.resolved_content_digest
        != top_attested.attested_resolved_content_digest
        or publication_snapshot.snapshot_digest != top_attested.attested_snapshot_digest
    ):
        raise IncrementalResolutionError(
            "content-attested release does not reproduce at publication cutoff"
        )

    return _resolve_manifest_snapshot(
        top_manifest_pin,
        view_kind=view_kind,
        cutoff_session=cutoff_session,
        load_parent=load_validated_body,
    )


def _resolve_manifest_snapshot(
    top_manifest_pin: ManifestPin,
    *,
    view_kind: ViewKind,
    cutoff_session: date | str,
    load_parent: Callable[[ManifestPin], IncrementalReleaseManifest | None],
) -> ResolvedIncrementalSnapshot:
    """Internal graph resolver after the control boundary has been proven.

    ``load_parent`` is intentionally the only source of manifest bodies.  It is invoked with the
    exact top pin first and then with each exact parent pin named by the loaded child.  The loader
    must verify the supplied path, byte count, and SHA-256 before returning the body.  A manifest
    body does not contain its own pin (which would make the manifest hash self-referential), but
    its logical ``release_id`` must reproduce the release ID carried by the external pin.
    Returning ``None`` is a missing-manifest error; the resolver never attempts fallback
    discovery.
    """

    cutoff = _session(cutoff_session, label="cutoff_session")
    view_value = _view_value(view_kind)
    newest_first: list[tuple[object, object]] = []
    seen_by_release_id: dict[str, tuple[object, ...]] = {}
    requested_pin: object | None = top_manifest_pin

    while requested_pin is not None:
        release_id = str(requested_pin.release_id)
        identity = _pin_identity(requested_pin)
        previous_identity = seen_by_release_id.get(release_id)
        if previous_identity is not None:
            if previous_identity != identity:
                raise IncrementalResolutionError(
                    f"release_id {release_id} appeared with two different manifest pins"
                )
            raise IncrementalResolutionError(f"release parent cycle detected at {release_id}")
        seen_by_release_id[release_id] = identity

        manifest = load_parent(requested_pin)
        if manifest is None:
            raise IncrementalResolutionError(f"missing exact manifest for release {release_id}")
        actual_release_id = str(manifest.release_id)
        if actual_release_id != release_id:
            raise IncrementalResolutionError(
                "loader returned a manifest body whose release ID does not match exact pin "
                f"{release_id}"
            )
        newest_first.append((requested_pin, manifest))

        release_type = _release_type_value(manifest)
        parent_pin = manifest.parent_release_pin
        if release_type == "base":
            if parent_pin is not None:
                raise IncrementalResolutionError("base release must not have a parent")
            break
        if parent_pin is None:
            raise IncrementalResolutionError(f"{release_type} release is missing its parent pin")
        requested_pin = parent_pin

    chain_nodes = tuple(reversed(newest_first))
    chain = tuple(manifest for _, manifest in chain_nodes)
    chain_pins = tuple(pin for pin, _ in chain_nodes)
    if not chain or _release_type_value(chain[0]) != "base":
        raise IncrementalResolutionError("release chain did not terminate at a base release")

    release_availabilities = tuple(
        _manifest_availability(pin, manifest) for pin, manifest in chain_nodes
    )
    release_cutoffs = tuple(_manifest_cutoffs(manifest) for manifest in chain)
    release_views = tuple(_view_value(manifest.resolved_view) for manifest in chain)
    for release_availability, (_, availability_cutoff) in zip(
        release_availabilities,
        release_cutoffs,
        strict=True,
    ):
        if release_availability < availability_cutoff:
            raise IncrementalResolutionError(
                "release availability precedes manifest availability cutoff"
            )
    for parent_availability, child_availability in pairwise(release_availabilities):
        if child_availability < parent_availability:
            raise IncrementalResolutionError(
                "child release availability precedes parent release availability"
            )
    for parent_release_availability, (_, child_availability_cutoff) in zip(
        release_availabilities[:-1],
        release_cutoffs[1:],
        strict=True,
    ):
        if child_availability_cutoff < parent_release_availability:
            raise IncrementalResolutionError(
                "child availability cutoff precedes parent release availability"
            )
    for (parent_source, parent_availability), (
        child_source,
        child_availability,
    ) in pairwise(release_cutoffs):
        if child_source < parent_source:
            raise IncrementalResolutionError("child source cutoff precedes parent source cutoff")
        if child_availability < parent_availability:
            raise IncrementalResolutionError(
                "child availability cutoff precedes parent availability cutoff"
            )
    for parent_view, child_view in pairwise(release_views):
        if child_view != parent_view:
            raise IncrementalResolutionError("non-base release changed its parent's resolved view")

    base = chain[0]
    if view_value == "historical_as_known":
        base_availability = release_availabilities[0]
        if base_availability > cutoff:
            raise IncrementalResolutionError(
                "historical_as_known cannot use a retrospective base newer than its cutoff"
            )

    # ``all_partitions`` validates the complete immutable chain.  The selected map is a
    # separately projected consumer view; a future release must never hide structural
    # corruption merely because it is beyond a historical cutoff.
    all_partitions: dict[PartitionKey, object] = {}
    selected_partitions: dict[PartitionKey, object] = {}
    partition_receipt_introductions: list[tuple[object, int]] = []
    all_rows: dict[RowVersionKey, object] = {}
    row_introduction_index: dict[RowVersionKey, int] = {}
    row_release_availability: dict[RowVersionKey, date] = {}
    successor_by_predecessor: dict[RowVersionKey, RowVersionKey] = {}
    chain_compatibility = _compatibility(base)
    resolved_compatibility = dict(chain_compatibility)
    max_partition_session: date | None = None

    for release_index, (_, manifest) in enumerate(chain_nodes):
        release_type = _release_type_value(manifest)
        source_cutoff, availability_cutoff = release_cutoffs[release_index]
        current_compatibility = _compatibility(manifest)
        if release_type != "base":
            immutable_names = (
                "schema_digest",
                "transform_semantics_digest",
                "calendar_digest",
            )
            changed_immutable = sorted(
                name
                for name in immutable_names
                if current_compatibility[name] != chain_compatibility[name]
            )
            if changed_immutable:
                raise IncrementalResolutionError(
                    "non-base release changed base-only compatibility contract: "
                    f"{', '.join(changed_immutable)}"
                )
            if (
                release_type == "delta"
                and current_compatibility["identity_policy_bundle_id"]
                != chain_compatibility["identity_policy_bundle_id"]
            ):
                raise IncrementalResolutionError("clean delta changed identity policy bundle")
        if release_type != "base":
            chain_compatibility = current_compatibility

        release_availability = release_availabilities[release_index]
        release_applies = view_value == "latest_reviewed_research" or release_availability <= cutoff
        if release_applies:
            resolved_compatibility = dict(current_compatibility)

        additions = tuple(manifest.added_partition_receipts)
        replacements = tuple(manifest.partition_replacements)
        if release_type == "delta" and replacements:
            raise IncrementalResolutionError("delta release cannot replace a partition")
        parent_max_partition_session = max_partition_session

        operation_keys: set[PartitionKey] = set()
        for receipt in additions:
            receipt_availability = _session(
                receipt.availability_session,
                label="partition receipt availability_session",
            )
            if receipt_availability > release_availability:
                raise IncrementalResolutionError(
                    "partition receipt availability exceeds its release availability"
                )
            key = _partition_key(receipt)
            partition_session = _session(key[1], label="partition key")
            if partition_session > receipt_availability:
                raise IncrementalResolutionError(
                    "partition session exceeds partition receipt availability"
                )
            if partition_session > source_cutoff:
                raise IncrementalResolutionError("partition session exceeds manifest source cutoff")
            if receipt_availability > availability_cutoff:
                raise IncrementalResolutionError(
                    "partition receipt availability exceeds manifest availability cutoff"
                )
            if key in operation_keys:
                raise IncrementalResolutionError(
                    f"duplicate partition operation for {key[0]}:{key[1]}"
                )
            operation_keys.add(key)
            if key in all_partitions:
                raise IncrementalResolutionError(
                    f"added partition already exists in parent snapshot: {key[0]}:{key[1]}"
                )
            if (
                release_type == "delta"
                and parent_max_partition_session is not None
                and partition_session <= parent_max_partition_session
            ):
                raise IncrementalResolutionError(
                    "delta added partition session is not strictly later than the parent "
                    "resolved maximum session; use a correction release"
                )
            all_partitions[key] = receipt
            if max_partition_session is None or partition_session > max_partition_session:
                max_partition_session = partition_session
            partition_receipt_introductions.append((receipt, release_index))
            if release_applies and _available(
                receipt,
                view_value=view_value,
                cutoff=cutoff,
            ):
                if key in selected_partitions:
                    raise IncrementalResolutionError(
                        f"selected-view partition addition already exists: {key[0]}:{key[1]}"
                    )
                selected_partitions[key] = receipt

        for replacement in replacements:
            replacement_receipt = replacement.replacement_receipt
            replacement_availability = _session(
                replacement_receipt.availability_session,
                label="replacement receipt availability_session",
            )
            if replacement_availability > release_availability:
                raise IncrementalResolutionError(
                    "replacement receipt availability exceeds its release availability"
                )
            key = replacement.key
            key = (str(key[0]), str(key[1]))
            replacement_session = _session(key[1], label="replacement partition key")
            if replacement_session > replacement_availability:
                raise IncrementalResolutionError(
                    "partition session exceeds replacement receipt availability"
                )
            if replacement_session > source_cutoff:
                raise IncrementalResolutionError(
                    "replacement partition session exceeds manifest source cutoff"
                )
            if replacement_availability > availability_cutoff:
                raise IncrementalResolutionError(
                    "replacement receipt availability exceeds manifest availability cutoff"
                )
            if key in operation_keys:
                raise IncrementalResolutionError(
                    f"duplicate partition operation for {key[0]}:{key[1]}"
                )
            operation_keys.add(key)
            if _partition_key(replacement_receipt) != key:
                raise IncrementalResolutionError("partition replacement crossed logical keys")
            expected = replacement.replaced_receipt
            if _partition_key(expected) != key:
                raise IncrementalResolutionError("replaced partition pin crossed logical keys")
            all_current = all_partitions.get(key)
            if all_current is None or all_current != expected:
                raise IncrementalResolutionError(
                    f"partition replacement did not name exact current receipt: {key[0]}:{key[1]}"
                )
            all_partitions[key] = replacement_receipt
            partition_receipt_introductions.append((replacement_receipt, release_index))
            if release_applies and _available(
                replacement_receipt,
                view_value=view_value,
                cutoff=cutoff,
            ):
                selected_current = selected_partitions.get(key)
                if selected_current is None or selected_current != expected:
                    raise IncrementalResolutionError(
                        "available partition replacement did not name exact selected-view "
                        f"receipt: {key[0]}:{key[1]}"
                    )
                selected_partitions[key] = replacement_receipt

        manifest_row_keys: set[StableRowKey] = set()
        manifest_version_keys: set[RowVersionKey] = set()
        manifest_rows = _added_rows(manifest)
        superseded_ids = tuple(str(item) for item in manifest.superseded_row_version_ids)
        if len(set(superseded_ids)) != len(superseded_ids):
            raise IncrementalResolutionError("duplicate superseded row version id")
        predecessor_ids = tuple(
            str(receipt.predecessor_row_version_id)
            for receipt in manifest_rows
            if receipt.predecessor_row_version_id is not None
        )
        if sorted(predecessor_ids) != sorted(superseded_ids):
            raise IncrementalResolutionError(
                "superseded row version ids do not exactly match successor predecessors"
            )
        for receipt in manifest_rows:
            row_availability = _session(
                receipt.availability_session,
                label="row-version receipt availability_session",
            )
            if row_availability > release_availability:
                raise IncrementalResolutionError(
                    "row-version receipt availability exceeds its release availability"
                )
            if row_availability > availability_cutoff:
                raise IncrementalResolutionError(
                    "row-version receipt availability exceeds manifest availability cutoff"
                )
            stable_key = _stable_row_key(receipt)
            version_key = _row_version_key(receipt)
            if stable_key in manifest_row_keys or version_key in manifest_version_keys:
                raise IncrementalResolutionError(
                    f"duplicate row-version operation for {stable_key[0]}:{stable_key[1]}"
                )
            manifest_row_keys.add(stable_key)
            manifest_version_keys.add(version_key)
            if version_key in all_rows:
                raise IncrementalResolutionError(
                    f"duplicate row version id for {version_key[0]}:{version_key[1]}"
                )
            if receipt.is_tombstone and not str(receipt.tombstone_reason or "").strip():
                raise IncrementalResolutionError("tombstone row version requires a reason")
            all_rows[version_key] = receipt
            row_introduction_index[version_key] = release_index
            row_release_availability[version_key] = release_availability

    # Validate the complete immutable row-version graph before selecting a view.  A later version
    # remains an auditable fact even when historical_as_known does not apply it.
    for version_key, receipt in all_rows.items():
        predecessor_id = receipt.predecessor_row_version_id
        if predecessor_id is None:
            continue
        predecessor_key = (str(receipt.table_name), str(predecessor_id))
        predecessor = all_rows.get(predecessor_key)
        if predecessor is None:
            raise IncrementalResolutionError(
                f"row version {version_key[1]} has a missing predecessor {predecessor_id}"
            )
        if _stable_row_key(predecessor) != _stable_row_key(receipt):
            raise IncrementalResolutionError("row-version predecessor crossed stable keys")
        if row_introduction_index[predecessor_key] > row_introduction_index[version_key]:
            raise IncrementalResolutionError(
                f"row version {version_key[1]} references predecessor {predecessor_id} "
                "introduced by a later release"
            )
        existing_successor = successor_by_predecessor.get(predecessor_key)
        if existing_successor is not None and existing_successor != version_key:
            raise IncrementalResolutionError(
                f"row-version fork from predecessor {predecessor_key[1]}"
            )
        successor_by_predecessor[predecessor_key] = version_key

    row_visit_state: dict[RowVersionKey, int] = {}
    for start_key in all_rows:
        if row_visit_state.get(start_key) == 2:
            continue
        path: list[RowVersionKey] = []
        cursor: RowVersionKey | None = start_key
        while cursor is not None:
            state = row_visit_state.get(cursor, 0)
            if state == 2:
                break
            if state == 1:
                raise IncrementalResolutionError(
                    f"row-version predecessor loop detected at {cursor[1]}"
                )
            row_visit_state[cursor] = 1
            path.append(cursor)
            predecessor_id = all_rows[cursor].predecessor_row_version_id
            cursor = (
                None
                if predecessor_id is None
                else (str(all_rows[cursor].table_name), str(predecessor_id))
            )
        for visited_key in path:
            row_visit_state[visited_key] = 2

    # Validate every partition FK against the complete chain and the release in which the
    # partition receipt first appeared.  Existence in a later child is not valid lineage.
    for receipt, release_index in partition_receipt_introductions:
        for reference in receipt.row_version_references:
            reference_key = (str(reference.table_name), str(reference.row_version_id))
            if reference_key not in all_rows:
                raise IncrementalResolutionError(
                    "partition references a missing historical row version: "
                    f"{reference_key[0]}:{reference_key[1]}"
                )
            if row_introduction_index[reference_key] > release_index:
                raise IncrementalResolutionError(
                    "partition references a row version introduced by a later release: "
                    f"{reference_key[0]}:{reference_key[1]}"
                )
            referenced_availability = _session(
                all_rows[reference_key].availability_session,
                label="referenced row-version availability_session",
            )
            partition_availability = _session(
                receipt.availability_session,
                label="referencing partition availability_session",
            )
            if referenced_availability > partition_availability:
                raise IncrementalResolutionError(
                    "partition availability precedes its referenced row version: "
                    f"{reference_key[0]}:{reference_key[1]}"
                )

    applicable_rows = {
        key: receipt
        for key, receipt in all_rows.items()
        if (
            view_value == "latest_reviewed_research"
            or (
                row_release_availability[key] <= cutoff
                and _available(receipt, view_value=view_value, cutoff=cutoff)
            )
        )
    }
    applicable_successors: dict[RowVersionKey, RowVersionKey] = {}
    applicable_rows_by_stable: dict[StableRowKey, list[object]] = {}
    for key, receipt in applicable_rows.items():
        applicable_rows_by_stable.setdefault(_stable_row_key(receipt), []).append(receipt)
        predecessor_id = receipt.predecessor_row_version_id
        if predecessor_id is not None:
            predecessor_key = (str(receipt.table_name), str(predecessor_id))
            if predecessor_key not in applicable_rows:
                raise IncrementalResolutionError(
                    f"available row version {key[1]} precedes its unavailable predecessor"
                )
            applicable_successors[predecessor_key] = key

    terminal: dict[StableRowKey, object] = {}
    for key, receipt in applicable_rows.items():
        if key in applicable_successors:
            continue
        stable_key = _stable_row_key(receipt)
        if stable_key in terminal:
            raise IncrementalResolutionError(
                f"row-version graph has multiple terminals for {stable_key[0]}:{stable_key[1]}"
            )
        terminal[stable_key] = receipt

    # Published membership may keep an old alias-resolution FK indefinitely.  The target need
    # only remain applicable in the selected catalog; it need not be the latest terminal version.
    for receipt in selected_partitions.values():
        for reference in receipt.row_version_references:
            reference_key = (str(reference.table_name), str(reference.row_version_id))
            if reference_key not in applicable_rows:
                raise IncrementalResolutionError(
                    "partition references a missing historical row version: "
                    f"{reference_key[0]}:{reference_key[1]}"
                )

    lineage_digest = stable_digest(
        {
            "cutoff_session": cutoff.isoformat(),
            "release_chain": [_as_digestable(pin) for pin in chain_pins],
            "view_kind": view_value,
        }
    )
    resolved_content_digest = stable_digest(
        {
            "compatibility_digests": resolved_compatibility,
            "cutoff_session": cutoff.isoformat(),
            "partition_receipts": [
                {
                    "key": list(key),
                    "receipt": _as_digestable(receipt),
                }
                for key, receipt in sorted(selected_partitions.items())
            ],
            "terminal_row_versions": [
                {
                    "key": list(key),
                    "receipt": _as_digestable(receipt),
                }
                for key, receipt in sorted(terminal.items())
            ],
            "view_kind": view_value,
        }
    )
    snapshot_digest = stable_digest(
        {
            "lineage_digest": lineage_digest,
            "resolved_content_digest": resolved_content_digest,
        }
    )
    frozen_rows_by_stable = {key: tuple(rows) for key, rows in applicable_rows_by_stable.items()}
    return ResolvedIncrementalSnapshot(
        top_manifest_pin=top_manifest_pin,
        view_kind=view_kind,
        cutoff_session=cutoff,
        release_chain=chain_pins,
        partition_receipts=MappingProxyType(dict(selected_partitions)),
        row_version_catalog=MappingProxyType(dict(applicable_rows)),
        audit_row_version_catalog=MappingProxyType(dict(all_rows)),
        row_versions_by_stable_key=MappingProxyType(frozen_rows_by_stable),
        terminal_row_versions=MappingProxyType(dict(terminal)),
        tombstoned_row_keys=tuple(
            sorted(key for key, receipt in terminal.items() if receipt.is_tombstone)
        ),
        compatibility_digests=MappingProxyType(resolved_compatibility),
        lineage_digest=lineage_digest,
        resolved_content_digest=resolved_content_digest,
        snapshot_digest=snapshot_digest,
    )
