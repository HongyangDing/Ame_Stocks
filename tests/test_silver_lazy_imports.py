from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Sequence

from ame_stocks_api.silver.identity_market_inventory_execution_plan import (
    REQUIRED_EXECUTION_RUNTIME_PATHS,
)

SILVER_PUBLIC_API_COUNT = 200
SILVER_PUBLIC_API_SHA256 = "1e2ee0873181872a4408ac1486df05abb77545393dcfc992be78d54697eca816"


def _isolated_modules_after(statements: Sequence[str]) -> set[str]:
    script = "\n".join(
        [
            "import json",
            "import sys",
            *statements,
            "print(json.dumps(sorted(sys.modules)))",
        ]
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    return set(json.loads(completed.stdout))


def _forbidden_execution_modules(modules: set[str]) -> set[str]:
    forbidden: set[str] = set()
    for module in modules:
        if module.startswith("ame_stocks_api.providers"):
            forbidden.add(module)
        if module.startswith("ame_stocks_api.silver.asset_full"):
            forbidden.add(module)
        if module in {
            "ame_stocks_api.silver.asset_publish_plan",
            "ame_stocks_api.silver.asset_release_set",
        }:
            forbidden.add(module)
        if module.startswith("ame_stocks_api.silver.identity_resolution"):
            forbidden.add(module)
        if module.startswith(
            (
                "ame_stocks_api.silver.identity_adjudication",
                "ame_stocks_api.silver.identity_cross_market",
                "ame_stocks_api.silver.identity_preview_",
                "ame_stocks_api.silver.identity_provider_evidence",
                "ame_stocks_api.silver.identity_streaming_preview",
            )
        ):
            forbidden.add(module)
        if module.endswith("_lifecycle") or module.endswith("_release"):
            forbidden.add(module)
    return forbidden


def test_silver_package_preserves_public_api_without_eager_submodule_imports() -> None:
    script = "\n".join(
        [
            "import hashlib",
            "import json",
            "import sys",
            "import ame_stocks_api.silver as silver",
            (
                "payload = json.dumps(silver.__all__, ensure_ascii=False, "
                "separators=(',', ':')).encode()"
            ),
            "print(json.dumps({",
            "    'count': len(silver.__all__),",
            "    'digest': hashlib.sha256(payload).hexdigest(),",
            "    'modules': sorted(sys.modules),",
            "    'registry_matches': set(silver.__all__) == set(silver._EXPORTS),",
            "}))",
        ]
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    result = json.loads(completed.stdout)

    assert result["count"] == SILVER_PUBLIC_API_COUNT
    assert result["digest"] == SILVER_PUBLIC_API_SHA256
    assert result["registry_matches"] is True
    loaded_silver_children = {
        name for name in result["modules"] if name.startswith("ame_stocks_api.silver.")
    }
    assert loaded_silver_children == set()
    assert _forbidden_execution_modules(set(result["modules"])) == set()


def test_public_symbol_loads_only_its_defining_module_on_first_access() -> None:
    modules = _isolated_modules_after(
        [
            "import ame_stocks_api.silver as silver",
            "from ame_stocks_api.silver.contracts import TableContract",
            "assert silver.TableContract is TableContract",
            "assert silver.TableContract is TableContract",
        ]
    )

    assert "ame_stocks_api.silver.contracts" in modules
    assert _forbidden_execution_modules(modules) == set()


def test_every_existing_public_symbol_remains_importable() -> None:
    _isolated_modules_after(
        [
            "import ame_stocks_api.silver as silver",
            "resolved = {name: getattr(silver, name) for name in silver.__all__}",
            "assert set(resolved) == set(silver.__all__)",
        ]
    )


def test_direct_silver_submodule_import_does_not_activate_other_workflows() -> None:
    modules = _isolated_modules_after(
        ["import ame_stocks_api.silver.identity_market_inventory_contract"]
    )

    assert "ame_stocks_api.silver.identity_market_inventory_contract" in modules
    assert _forbidden_execution_modules(modules) == set()


def test_run_cli_import_has_no_provider_or_materialization_capability() -> None:
    modules = _isolated_modules_after(
        ["import ame_stocks_api.cli.silver_identity_market_inventory_run"]
    )

    assert "ame_stocks_api.silver.identity_market_inventory_runner" not in modules
    assert not any(name.startswith("ame_stocks_api.silver.") for name in modules)
    assert "ame_stocks_api.providers.massive" not in modules
    assert _forbidden_execution_modules(modules) == set()


def test_runner_transitive_repo_imports_are_all_runtime_pinned() -> None:
    script = "\n".join(
        [
            "import json",
            "import pathlib",
            "import sys",
            "root = pathlib.Path.cwd().resolve()",
            "from ame_stocks_api.cli.silver_identity_market_inventory_run import _load_runner",
            "_load_runner()",
            "paths = []",
            "for module in sys.modules.values():",
            "    raw = getattr(module, '__file__', None)",
            "    if raw is None:",
            "        continue",
            "    try:",
            "        path = pathlib.Path(raw).resolve().relative_to(root)",
            "    except (OSError, ValueError):",
            "        continue",
            "    if path.suffix == '.py' and str(path).startswith('backend/ame_stocks_api/'):",
            "        paths.append(str(path))",
            "print(json.dumps(sorted(set(paths))))",
        ]
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    loaded_repo_modules = set(json.loads(completed.stdout))

    assert loaded_repo_modules <= set(REQUIRED_EXECUTION_RUNTIME_PATHS)
    assert (
        _forbidden_execution_modules(
            _isolated_modules_after(
                [
                    "from ame_stocks_api.cli."
                    "silver_identity_market_inventory_run import _load_runner",
                    "_load_runner()",
                ]
            )
        )
        == set()
    )
