#!/usr/bin/env python3
"""Print ``[project.optional-dependencies] dev`` packages.

Used for CI / docs alignment checks. The GitHub Actions ``python-tests`` job
installs dependencies with an explicit
``pip install`` line in ``.github/workflows/_shared-ci.yml``. Keep that list in
sync with ``pyproject.toml`` ``[project.optional-dependencies] dev`` (same
versions where applicable). Run this script after editing either side:

    python scripts/ci_print_dev_extras.py

Exit 0 always; output is for human / review use.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_dev_extras() -> list[str]:
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python <3.11 in dev venv
        import tomli as tomllib  # type: ignore[no-redef,import-not-found]

    data = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    opt = data.get("project", {}).get("optional-dependencies", {})
    dev = opt.get("dev")
    if not dev:
        raise SystemExit("pyproject.toml: missing [project.optional-dependencies] dev")
    return list(dev)


def main() -> None:
    lines = _load_dev_extras()
    print("# Declared in pyproject.toml [project.optional-dependencies] dev:\n")
    for line in lines:
        print(line)
    print(
        "\n# CI installs a flattened subset via _shared-ci.yml "
        "(plus pyright; moltres-core/greenlet must match [dev] / [moltres]; "
        "entei-core/pymongo/beanie must match [dev] / [mongo])."
    )


if __name__ == "__main__":
    main()
