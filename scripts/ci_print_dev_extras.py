#!/usr/bin/env python3
"""Print ``[project.optional-dependencies] dev`` packages.

The GitHub Actions ``python-tests`` jobs install ``pip install -e ".[dev]"``
(plus ``raikou-core`` and ``pyspark`` for JVM Spark tests) after building the
native extension — see ``.github/workflows/_shared-ci.yml``. This script lists
what ``[dev]`` contains for review when editing extras.

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
        '\n# CI: maturin develop, then pip install -e ".[dev]" '
        "raikou-core pyspark (see _shared-ci.yml)."
    )


if __name__ == "__main__":
    main()
