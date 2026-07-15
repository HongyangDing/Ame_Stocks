from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ame_stocks_api.silver.calendar_artifact import (
    build_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.identity_preview_plan import (
    DETECTOR_PREVIEW_APPROVAL_STAGE,
    DETECTOR_PREVIEW_AUTHORIZED_ACTION,
    DETECTOR_PREVIEW_SCOPE,
    MAX_ASSET_PARENT_SCANNED_ROWS,
    MAX_BATCH_SIZE,
    MAX_CASES,
    MAX_PREVIEW_SESSIONS,
    MAX_PREVIEW_TICKERS,
    MAX_SELECTED_ROWS,
    MAX_SOURCE_ARTIFACTS,
    MAX_SOURCE_BYTES,
    MAX_TOTAL_SCANNED_ROWS,
    MAX_UNIVERSE_SCANNED_ROWS,
    IdentityPreviewPlanError,
    IdentityPreviewPlanStore,
    S7DetectorPreviewApprovalRequest,
    S7DetectorPreviewPlan,
    S7DetectorPreviewPlanApproval,
    S7DetectorPreviewResourceCaps,
    S7TickerAllowlist,
    StoredIdentityPreviewDocument,
    build_s7_ticker_allowlist,
    detector_preview_approval_request_path,
    detector_preview_plan_approval_path,
    detector_preview_plan_path,
    ticker_allowlist_path,
)
from ame_stocks_api.silver.identity_source import (
    S7_S4_RELEASE_SET_ID,
    S7_S4_RELEASE_SET_MANIFEST_SHA256,
    S7_SIX_RELEASE_BINDING_ID,
    S7_SOURCE_PINS,
)

CALENDAR = build_xnys_calendar_artifact(date(2024, 1, 2), date(2024, 2, 5))
CREATED_AT = datetime(2024, 1, 12, 18, 0, tzinfo=UTC)
REQUESTED_AT = datetime(2024, 1, 12, 18, 30, tzinfo=UTC)
APPROVED_AT = datetime(2024, 1, 12, 19, 0, tzinfo=UTC)
GIT_COMMIT = "a" * 40


def _caps(
    *,
    selected: int = 15,
    universe: int = 100_000,
    asset: int = 100_000,
    total: int = 200_000,
    artifacts: int = 30,
    source_bytes: int = 64 * 1024 * 1024,
    cases: int = 10,
    batch: int = 8_192,
) -> S7DetectorPreviewResourceCaps:
    return S7DetectorPreviewResourceCaps(
        selected_row_cap=selected,
        universe_scanned_row_cap=universe,
        asset_parent_scanned_row_cap=asset,
        total_scanned_row_cap=total,
        source_artifact_cap=artifacts,
        source_bytes_cap=source_bytes,
        case_cap=cases,
        batch_size=batch,
    )


def _prepare(
    root: Path,
    *,
    tickers: tuple[str, ...] = ("A", "BRK.B", "a"),
) -> tuple[
    IdentityPreviewPlanStore,
    S7TickerAllowlist,
    S7DetectorPreviewPlan,
    StoredIdentityPreviewDocument,
]:
    write_xnys_calendar_artifact(root, CALENDAR)
    store = IdentityPreviewPlanStore(root)
    allowlist = build_s7_ticker_allowlist(tickers)
    store.store_ticker_allowlist(allowlist)
    selected_sessions = tuple(
        item.session_date
        for item in CALENDAR.sessions
        if date(2024, 1, 2) <= item.session_date <= date(2024, 1, 8)
    )
    plan = S7DetectorPreviewPlan.create(
        created_by="s7-preview-planner",
        created_at_utc=CREATED_AT,
        git_commit=GIT_COMMIT,
        calendar_artifact_id=CALENDAR.calendar_artifact_id,
        calendar_artifact_sha256=CALENDAR.sha256,
        start_session=selected_sessions[0],
        end_session=selected_sessions[-1],
        session_count=len(selected_sessions),
        ticker_allowlist=allowlist,
        resource_caps=_caps(selected=len(selected_sessions) * len(tickers)),
    )
    stored = store.store_plan(plan)
    return store, allowlist, plan, stored


def _prepare_request(
    root: Path,
) -> tuple[
    IdentityPreviewPlanStore,
    S7DetectorPreviewPlan,
    S7DetectorPreviewApprovalRequest,
    StoredIdentityPreviewDocument,
]:
    store, _, plan, stored_plan = _prepare(root)
    request = S7DetectorPreviewApprovalRequest.create(
        plan,
        stored_plan,
        created_by="s7-preview-requester",
        created_at_utc=REQUESTED_AT,
    )
    stored_request = store.store_approval_request(request)
    return store, plan, request, stored_request


def test_ticker_allowlist_is_exact_case_sensitive_content_addressed_and_canonical() -> None:
    first = build_s7_ticker_allowlist(("A", "BRK.B", "a"))
    repeated = build_s7_ticker_allowlist(("A", "BRK.B", "a"))

    assert first == repeated
    assert first.ticker_count == 3
    assert first.tickers == ("A", "BRK.B", "a")
    assert first.relative_path == ticker_allowlist_path(first.ticker_allowlist_id)
    assert first.sha256 == hashlib.sha256(first.content).hexdigest()
    assert first.content.endswith(b"\n")
    assert b"\n " not in first.content
    document = json.loads(first.content)
    assert document["tickers"] == ["A", "BRK.B", "a"]
    assert document["ticker_count"] == 3
    assert "A" in document["tickers"] and "a" in document["tickers"]


@pytest.mark.parametrize(
    ("tickers", "message"),
    (
        ((), "cannot be empty"),
        (("B", "A"), "sorted unique"),
        (("A", "A"), "sorted unique"),
        ((" A",), "unsafe ticker"),
        (("A\n",), "unsafe ticker"),
        (("*",), "unsafe ticker"),
        ((".*",), "unsafe ticker"),
        (("X" * 65,), "unsafe ticker"),
        (tuple(f"T{index:03d}" for index in range(MAX_PREVIEW_TICKERS + 1)), "hard preview"),
    ),
)
def test_ticker_allowlist_rejects_ambiguous_or_unsafe_values(
    tickers: tuple[str, ...], message: str
) -> None:
    with pytest.raises(IdentityPreviewPlanError, match=message):
        build_s7_ticker_allowlist(tickers)
    with pytest.raises(IdentityPreviewPlanError, match="sequence"):
        build_s7_ticker_allowlist("AAPL")


def test_resource_caps_accept_every_hard_boundary() -> None:
    caps = _caps(
        selected=MAX_SELECTED_ROWS,
        universe=MAX_UNIVERSE_SCANNED_ROWS,
        asset=MAX_ASSET_PARENT_SCANNED_ROWS,
        total=2_000_000,
        artifacts=MAX_SOURCE_ARTIFACTS,
        source_bytes=MAX_SOURCE_BYTES,
        cases=MAX_CASES,
        batch=MAX_BATCH_SIZE,
    )
    assert caps.selected_row_cap == 6_250
    assert caps.to_dict()["source_bytes_cap"] == 512 * 1024 * 1024


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("selected", MAX_SELECTED_ROWS + 1),
        ("universe", MAX_UNIVERSE_SCANNED_ROWS + 1),
        ("asset", MAX_ASSET_PARENT_SCANNED_ROWS + 1),
        ("total", MAX_TOTAL_SCANNED_ROWS + 1),
        ("artifacts", MAX_SOURCE_ARTIFACTS + 1),
        ("source_bytes", MAX_SOURCE_BYTES + 1),
        ("cases", MAX_CASES + 1),
        ("batch", MAX_BATCH_SIZE + 1),
    ),
)
def test_resource_caps_reject_each_hard_limit(field: str, value: int) -> None:
    arguments = {
        "selected": 100,
        "universe": 1_000,
        "asset": 1_000,
        "total": 2_000,
        "artifacts": 10,
        "source_bytes": 1_000,
        "cases": 10,
        "batch": 100,
    }
    arguments[field] = value
    if field == "selected" and arguments["cases"] > value:
        arguments["cases"] = value
    if field == "universe":
        arguments["total"] = value
    if field == "asset":
        arguments["total"] = value
    with pytest.raises(IdentityPreviewPlanError, match="hard safety limit"):
        _caps(**arguments)


def test_resource_caps_reject_incoherent_component_and_case_bounds() -> None:
    with pytest.raises(IdentityPreviewPlanError, match="below either component"):
        _caps(universe=1_000, asset=2_000, total=1_500)
    with pytest.raises(IdentityPreviewPlanError, match="component-cap sum"):
        _caps(universe=1_000, asset=1_000, total=2_001)
    with pytest.raises(IdentityPreviewPlanError, match="case_cap"):
        _caps(selected=5, cases=6)
    with pytest.raises(IdentityPreviewPlanError, match="positive"):
        _caps(batch=0)


def test_plan_is_exact_content_addressed_non_executable_and_binds_six_pins(
    tmp_path: Path,
) -> None:
    _, allowlist, plan, stored = _prepare(tmp_path)

    assert plan.plan_id == json.loads(plan.content)["plan_id"]
    assert plan.sha256 == hashlib.sha256(plan.content).hexdigest()
    assert plan.relative_path == detector_preview_plan_path(plan.plan_id)
    assert stored.path == plan.relative_path
    assert plan.six_release_binding_id == S7_SIX_RELEASE_BINDING_ID
    assert plan.s4_release_set_id == S7_S4_RELEASE_SET_ID
    assert plan.s4_release_set_manifest_sha256 == S7_S4_RELEASE_SET_MANIFEST_SHA256
    assert [item.table for item in plan.source_pins] == sorted(S7_SOURCE_PINS)
    assert len(plan.source_pins) == 6
    assert sum(item.row_count for item in plan.source_pins) == 138_825_855
    assert sum(item.artifact_count for item in plan.source_pins) == 7_542
    assert plan.clean_checkout_required is True
    assert plan.execution_scope == DETECTOR_PREVIEW_SCOPE
    assert plan.plan_state == "awaiting_exact_plan_approval"
    assert plan.ticker_allowlist_id == allowlist.ticker_allowlist_id
    assert plan.ticker_allowlist_path == allowlist.relative_path
    document = json.loads(plan.content)
    assert document["selection"]["ticker_match_rule"] == ("exact_case_sensitive_allowlist_only")
    assert "approval_id" not in document
    assert "full_run" not in plan.execution_scope
    assert "publish" not in plan.execution_scope


def test_plan_store_round_trips_idempotently_by_exact_id_and_sha(tmp_path: Path) -> None:
    store, _, plan, first = _prepare(tmp_path)
    first_mtime = (tmp_path / first.path).stat().st_mtime_ns
    repeated = store.store_plan(plan)
    loaded, stored = store.load_plan(plan.plan_id, expected_sha256=plan.sha256)

    assert repeated == first == stored
    assert loaded == plan
    assert (tmp_path / first.path).stat().st_mtime_ns == first_mtime
    latest = tmp_path / "manifests/silver/identity/detector-preview-plans/latest.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text("{}\n", encoding="utf-8")
    assert store.load_plan(plan.plan_id, expected_sha256=plan.sha256)[0] == plan


def test_store_requires_exact_allowlist_and_calendar_before_plan_write(tmp_path: Path) -> None:
    allowlist = build_s7_ticker_allowlist(("A",))
    plan = S7DetectorPreviewPlan.create(
        created_by="planner",
        created_at_utc=CREATED_AT,
        git_commit=GIT_COMMIT,
        calendar_artifact_id=CALENDAR.calendar_artifact_id,
        calendar_artifact_sha256=CALENDAR.sha256,
        start_session=CALENDAR.sessions[0].session_date,
        end_session=CALENDAR.sessions[0].session_date,
        session_count=1,
        ticker_allowlist=allowlist,
        resource_caps=_caps(selected=1, cases=1),
    )
    store = IdentityPreviewPlanStore(tmp_path)
    with pytest.raises(IdentityPreviewPlanError, match=r"ticker allowlist.*missing"):
        store.store_plan(plan)
    store.store_ticker_allowlist(allowlist)
    with pytest.raises(IdentityPreviewPlanError, match="calendar binding"):
        store.store_plan(plan)
    assert not (tmp_path / plan.relative_path).exists()


def test_store_rejects_calendar_range_or_allowlist_count_mismatch(tmp_path: Path) -> None:
    store, _, plan, _ = _prepare(tmp_path)
    with pytest.raises(IdentityPreviewPlanError, match="date range/count"):
        store.store_plan(
            replace(
                plan,
                session_count=plan.session_count - 1,
                resource_caps=replace(
                    plan.resource_caps,
                    selected_row_cap=(plan.session_count - 1) * plan.ticker_count,
                ),
            )
        )
    with pytest.raises(IdentityPreviewPlanError, match="ticker allowlist binding"):
        store.store_plan(
            replace(
                plan,
                ticker_count=plan.ticker_count - 1,
                resource_caps=replace(
                    plan.resource_caps,
                    selected_row_cap=plan.session_count * (plan.ticker_count - 1),
                ),
            )
        )


def test_plan_rejects_any_source_git_scope_or_bound_drift(tmp_path: Path) -> None:
    _, _, plan, _ = _prepare(tmp_path)
    with pytest.raises(IdentityPreviewPlanError, match="six-release binding"):
        replace(plan, six_release_binding_id="0" * 64)
    with pytest.raises(IdentityPreviewPlanError, match="source pins"):
        replace(plan, source_pins=plan.source_pins[:-1])
    with pytest.raises(IdentityPreviewPlanError, match="release-set ID"):
        replace(plan, s4_release_set_id="0" * 64)
    with pytest.raises(IdentityPreviewPlanError, match="40-hex"):
        replace(plan, git_commit="0" * 39)
    with pytest.raises(IdentityPreviewPlanError, match="clean Git"):
        replace(plan, clean_checkout_required=False)
    with pytest.raises(IdentityPreviewPlanError, match="scope"):
        replace(plan, execution_scope="full_run_and_publish")
    with pytest.raises(IdentityPreviewPlanError, match="hard preview"):
        replace(plan, session_count=MAX_PREVIEW_SESSIONS + 1)
    with pytest.raises(IdentityPreviewPlanError, match="times ticker_count"):
        replace(
            plan,
            resource_caps=replace(
                plan.resource_caps,
                selected_row_cap=plan.session_count * plan.ticker_count + 1,
            ),
        )


def test_plan_and_approval_metadata_reject_sensitive_material(tmp_path: Path) -> None:
    _, _, plan, _ = _prepare(tmp_path)
    with pytest.raises(IdentityPreviewPlanError, match="sensitive"):
        replace(plan, created_by="api_key=do-not-store")
    store, _, request, stored_request = _prepare_request(tmp_path / "approval")
    assert (
        store.load_approval_request(request.request_event_id, expected_sha256=request.sha256)[0]
        == request
    )
    with pytest.raises(IdentityPreviewPlanError, match="sensitive"):
        S7DetectorPreviewPlanApproval.create(
            request,
            stored_request,
            approval_literal=request.canonical_approval_literal,
            approved_by="reviewer",
            approved_at_utc=APPROVED_AT,
            approval_note="authorization: bearer secret",
        )


def test_preapproval_request_is_deterministic_stored_first_and_exactly_plan_bound(
    tmp_path: Path,
) -> None:
    store, plan, request, stored_request = _prepare_request(tmp_path)
    loaded, repeated = store.load_approval_request(
        request.request_event_id,
        expected_sha256=request.sha256,
    )

    assert loaded == request
    assert repeated == stored_request
    assert request.relative_path == detector_preview_approval_request_path(request.request_event_id)
    assert request.plan_id == plan.plan_id
    assert request.plan_path == plan.relative_path
    assert request.plan_sha256 == plan.sha256
    assert request.resource_caps_digest == plan.resource_caps.digest
    assert request.created_at_utc == REQUESTED_AT
    assert request.authorized_action == DETECTOR_PREVIEW_AUTHORIZED_ACTION
    assert request.sha256 == hashlib.sha256(request.content).hexdigest()
    assert store.store_approval_request(request) == stored_request

    literal = json.loads(request.canonical_approval_literal)
    assert literal == {
        "authorized_action": DETECTOR_PREVIEW_AUTHORIZED_ACTION,
        "literal_version": "s7_detector_preview_approval_literal_v2",
        "plan_id": plan.plan_id,
        "plan_sha256": plan.sha256,
        "request_event_id": request.request_event_id,
        "request_event_sha256": request.sha256,
        "resource_caps_digest": plan.resource_caps.digest,
    }


def test_approval_is_independent_content_addressed_and_exactly_plan_bound(
    tmp_path: Path,
) -> None:
    store, plan, request, stored_request = _prepare_request(tmp_path)
    approval = S7DetectorPreviewPlanApproval.create(
        request,
        stored_request,
        approval_literal=request.canonical_approval_literal,
        approved_by="human-s7-reviewer",
        approved_at_utc=APPROVED_AT,
        approval_note="Approved only this bounded detector preview plan.",
    )
    stored = store.store_approval(approval)
    loaded, repeated = store.load_approval(
        approval.approval_id,
        expected_sha256=approval.sha256,
    )

    assert loaded == approval
    assert repeated == stored
    assert approval.relative_path == detector_preview_plan_approval_path(approval.approval_id)
    assert approval.plan_id == plan.plan_id
    assert approval.plan_path == plan.relative_path
    assert approval.plan_sha256 == plan.sha256
    assert approval.request_event_id == request.request_event_id
    assert approval.request_event_path == request.relative_path
    assert approval.request_event_sha256 == request.sha256
    assert approval.resource_caps_digest == plan.resource_caps.digest
    assert approval.approval_literal == request.canonical_approval_literal
    assert (
        approval.approval_literal_sha256
        == hashlib.sha256(request.canonical_approval_literal.encode("utf-8")).hexdigest()
    )
    assert approval.approval_stage == DETECTOR_PREVIEW_APPROVAL_STAGE
    assert approval.authorized_action == DETECTOR_PREVIEW_AUTHORIZED_ACTION
    assert approval.execution_scope == DETECTOR_PREVIEW_SCOPE
    assert approval.decision == "approved"
    document = json.loads(approval.content)
    assert document["schema_version"] == 2
    assert document["approval_stage"] not in {"full_run", "publish", "schema"}
    assert "adjudication" not in document["authorized_action"]
    assert "materialization" not in document["authorized_action"]


def test_request_and_approval_reject_wrong_receipts_literal_time_and_broader_stages(
    tmp_path: Path,
) -> None:
    store, plan, request, stored_request = _prepare_request(tmp_path)
    with pytest.raises(IdentityPreviewPlanError, match="stored plan receipt"):
        S7DetectorPreviewApprovalRequest.create(
            plan,
            replace(
                store.load_plan(plan.plan_id, expected_sha256=plan.sha256)[1],
                sha256="0" * 64,
            ),
            created_by="requester",
            created_at_utc=REQUESTED_AT,
        )
    with pytest.raises(IdentityPreviewPlanError, match="stored approval request receipt"):
        S7DetectorPreviewPlanApproval.create(
            request,
            replace(stored_request, sha256="0" * 64),
            approval_literal=request.canonical_approval_literal,
            approved_by="reviewer",
            approved_at_utc=APPROVED_AT,
        )
    with pytest.raises(IdentityPreviewPlanError, match="exact canonical literal"):
        S7DetectorPreviewPlanApproval.create(
            request,
            stored_request,
            approval_literal=request.canonical_approval_literal + " ",
            approved_by="reviewer",
            approved_at_utc=APPROVED_AT,
        )
    with pytest.raises(IdentityPreviewPlanError, match="must predate"):
        S7DetectorPreviewPlanApproval.create(
            request,
            stored_request,
            approval_literal=request.canonical_approval_literal,
            approved_by="reviewer",
            approved_at_utc=REQUESTED_AT,
        )
    approval = S7DetectorPreviewPlanApproval.create(
        request,
        stored_request,
        approval_literal=request.canonical_approval_literal,
        approved_by="reviewer",
        approved_at_utc=APPROVED_AT,
    )
    for stage in ("full_run", "publish", "schema"):
        with pytest.raises(IdentityPreviewPlanError, match="cannot use"):
            replace(approval, approval_stage=stage)
    with pytest.raises(IdentityPreviewPlanError, match="too broad"):
        replace(
            approval,
            authorized_action="run_preview_then_adjudicate",
            approval_literal=approval.approval_literal.replace(
                DETECTOR_PREVIEW_AUTHORIZED_ACTION, "run_preview_then_adjudicate"
            ),
            approval_literal_sha256=hashlib.sha256(
                approval.approval_literal.replace(
                    DETECTOR_PREVIEW_AUTHORIZED_ACTION, "run_preview_then_adjudicate"
                ).encode("utf-8")
            ).hexdigest(),
        )
    with pytest.raises(IdentityPreviewPlanError, match="too broad"):
        replace(approval, execution_scope="bounded_preview_and_materialization")

    forged_request = replace(request, resource_caps_digest="0" * 64)
    with pytest.raises(IdentityPreviewPlanError, match="exact plan and resource caps"):
        store.store_approval_request(forged_request)
    assert not (tmp_path / forged_request.relative_path).exists()


def test_approval_cannot_be_stored_before_exact_request_event(tmp_path: Path) -> None:
    store, _, plan, stored_plan = _prepare(tmp_path)
    request = S7DetectorPreviewApprovalRequest.create(
        plan,
        stored_plan,
        created_by="requester",
        created_at_utc=REQUESTED_AT,
    )
    not_actually_stored = StoredIdentityPreviewDocument(
        path=request.relative_path,
        sha256=request.sha256,
        bytes=len(request.content),
    )
    approval = S7DetectorPreviewPlanApproval.create(
        request,
        not_actually_stored,
        approval_literal=request.canonical_approval_literal,
        approved_by="reviewer",
        approved_at_utc=APPROVED_AT,
    )

    with pytest.raises(IdentityPreviewPlanError, match="approval request is missing"):
        store.store_approval(approval)
    assert not (tmp_path / approval.relative_path).exists()


def test_v1_approval_document_is_not_backward_compatible(tmp_path: Path) -> None:
    _, _, request, stored_request = _prepare_request(tmp_path)
    approval = S7DetectorPreviewPlanApproval.create(
        request,
        stored_request,
        approval_literal=request.canonical_approval_literal,
        approved_by="reviewer",
        approved_at_utc=APPROVED_AT,
    )
    legacy = json.loads(approval.content)
    legacy["schema_version"] = 1
    legacy["approval_rule_version"] = "s7_detector_preview_plan_approval_v1"
    for key in (
        "approval_literal",
        "approval_literal_sha256",
        "request_event_id",
        "request_event_path",
        "request_event_sha256",
        "resource_caps_digest",
    ):
        legacy.pop(key)

    with pytest.raises(IdentityPreviewPlanError, match="schema is not exact"):
        S7DetectorPreviewPlanApproval.from_dict(legacy)


def test_loaders_fail_closed_on_wrong_sha_noncanonical_bytes_and_symlinks(
    tmp_path: Path,
) -> None:
    store, allowlist, plan, _ = _prepare(tmp_path)
    with pytest.raises(IdentityPreviewPlanError, match="SHA-256 mismatch"):
        store.load_plan(plan.plan_id, expected_sha256="0" * 64)

    plan_path = tmp_path / plan.relative_path
    plan_path.chmod(0o644)
    pretty = (json.dumps(json.loads(plan.content), indent=2, sort_keys=True) + "\n").encode()
    plan_path.write_bytes(pretty)
    with pytest.raises(IdentityPreviewPlanError, match="not canonical"):
        store.load_plan(plan.plan_id, expected_sha256=hashlib.sha256(pretty).hexdigest())

    fresh = tmp_path / "fresh"
    fresh.mkdir()
    symlink_store = IdentityPreviewPlanStore(fresh)
    target = fresh / "target.json"
    target.write_bytes(allowlist.content)
    allowlist_path = fresh / allowlist.relative_path
    allowlist_path.parent.mkdir(parents=True)
    allowlist_path.symlink_to(target)
    with pytest.raises(IdentityPreviewPlanError, match="symlink"):
        symlink_store.load_ticker_allowlist(
            allowlist.ticker_allowlist_id,
            expected_sha256=allowlist.sha256,
        )


def test_loader_rejects_tampered_exact_schema_and_ids(tmp_path: Path) -> None:
    store, _, plan, _ = _prepare(tmp_path)
    forged = json.loads(plan.content)
    forged["unexpected"] = True
    content = json.dumps(forged, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    path = tmp_path / plan.relative_path
    path.chmod(0o644)
    path.write_bytes(content)
    with pytest.raises(IdentityPreviewPlanError, match="schema is not exact"):
        store.load_plan(plan.plan_id, expected_sha256=hashlib.sha256(content).hexdigest())


def test_control_store_creates_no_preview_candidate_adjudication_or_release_paths(
    tmp_path: Path,
) -> None:
    store, _, request, stored_request = _prepare_request(tmp_path)
    approval = S7DetectorPreviewPlanApproval.create(
        request,
        stored_request,
        approval_literal=request.canonical_approval_literal,
        approved_by="reviewer",
        approved_at_utc=APPROVED_AT,
    )
    store.store_approval(approval)

    assert not (tmp_path / "staging").exists()
    assert not (tmp_path / "silver").exists()
    assert not (tmp_path / "manifests/silver/identity-case-candidates").exists()
    assert not (tmp_path / "manifests/silver/identity/adjudication-plans").exists()
    assert not (tmp_path / "manifests/silver/identity/adjudication-registry").exists()
    assert not (tmp_path / "manifests/silver/releases").exists()
    assert not (tmp_path / "manifests/silver/approvals/full_run").exists()
    assert not (tmp_path / "manifests/silver/approvals/publish").exists()
