#!/usr/bin/env python3
"""Generate or verify byte-identical S7 relation-registry contract resources."""

from __future__ import annotations

import argparse
from pathlib import Path

from ame_stocks_api.silver.identity_relation_registry_contract import (
    RELATION_REGISTRY_CONTRACTS,
    contract_bytes,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    for table, contract in RELATION_REGISTRY_CONTRACTS.items():
        payload = contract_bytes(contract)
        paths = (
            root / "docs/silver/contracts/identity" / f"{table}.schema-v1.candidate.json",
            root / "backend/ame_stocks_api/silver/schema_resources" / f"{table}.schema-v1.json",
        )
        for path in paths:
            if args.check:
                if not path.is_file() or path.read_bytes() != payload:
                    raise SystemExit(f"contract resource differs: {path}")
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
