"""Freeze the exact metadata-only S4 base frontier for session appends."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ame_stocks_api.silver.asset_incremental import (
    derive_s4_base_frontier,
    load_current_s4_reference_binding,
    verify_s4_incremental_git_checkout,
    write_s4_base_frontier,
)
from ame_stocks_api.silver.incremental_contract import ArtifactPin


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Authenticate one exact published S4 release set and freeze its metadata-only "
            "base frontier. Historical S4 DATA Parquet is not read."
        )
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--release-set-id", required=True)
    parser.add_argument("--release-set-sha256", required=True)
    parser.add_argument("--release-set-bytes", type=int, required=True)
    parser.add_argument("--calendar-artifact-id", required=True)
    parser.add_argument("--calendar-artifact-sha256", required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    verify_s4_incremental_git_checkout(args.repo_root, args.git_commit)
    release_set_artifact = ArtifactPin(
        path=(
            "manifests/silver/release-sets/assets/"
            f"release_set_id={args.release_set_id}/manifest.json"
        ),
        sha256=args.release_set_sha256,
        bytes=args.release_set_bytes,
    )
    reference = load_current_s4_reference_binding(args.data_root)
    frontier = derive_s4_base_frontier(
        args.data_root,
        release_set_artifact=release_set_artifact,
        calendar_artifact_id=args.calendar_artifact_id,
        calendar_artifact_sha256=args.calendar_artifact_sha256,
        reference_binding=reference,
    )
    parent = write_s4_base_frontier(args.data_root, frontier)
    print(
        json.dumps(
            {
                "frontier": frontier.to_dict(),
                "parent_frontier": parent.to_dict(),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
