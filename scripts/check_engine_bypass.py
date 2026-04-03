#!/usr/bin/env python3
"""Fail if ``pydantable._core`` is reached outside the engine allowlist.

See docs/ADR-engines.md. Allowed locations:
  - ``python/pydantable/engine/`` (entire package)
  - ``python/pydantable/_extension.py``
  - ``python/pydantable/rust_engine.py``
"""

from __future__ import annotations

import sys
from pathlib import Path

FORBIDDEN_SUBSTRINGS = (
    "import pydantable._core",
    "from pydantable import _core",
    "get_default_engine().rust_core",
)

ALLOWLIST_FILES = frozenset(
    {
        "_extension.py",
        "rust_engine.py",
    }
)


def _allowlisted(rel: Path) -> bool:
    posix = rel.as_posix()
    if posix.startswith("engine/"):
        return True
    return posix in ALLOWLIST_FILES


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    pkg = root / "python" / "pydantable"
    if not pkg.is_dir():
        print(f"check_engine_bypass: missing package dir {pkg}", file=sys.stderr)
        return 2

    violations: list[str] = []
    for path in sorted(pkg.rglob("*.py")):
        rel = path.relative_to(pkg)
        if _allowlisted(rel):
            continue
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), 1):
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            for bad in FORBIDDEN_SUBSTRINGS:
                if bad in line:
                    violations.append(f"{path}:{lineno}: contains {bad!r}")
                    break

    if violations:
        print(
            "Engine bypass check failed (native coupling outside allowlist):\n",
            file=sys.stderr,
        )
        for v in violations:
            print(v, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
