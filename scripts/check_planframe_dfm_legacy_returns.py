#!/usr/bin/env python3
"""Fail if legacy ``return self._from_dataframe(self._df...)`` reappears.

Targets ``python/pydantable/dataframe_model.py``. Those tails were unreachable dead
code after PlanFrame-backed transforms (Phase 4).
"""

from __future__ import annotations

import sys
from pathlib import Path

FORBIDDEN = "return self._from_dataframe(self._df"


def main() -> int:
    path = (
        Path(__file__).resolve().parents[1]
        / "python"
        / "pydantable"
        / "dataframe_model.py"
    )
    if not path.is_file():
        print(f"check_planframe_dfm_legacy_returns: missing {path}", file=sys.stderr)
        return 2
    text = path.read_text(encoding="utf-8")
    violations: list[int] = []
    for lineno, line in enumerate(text.splitlines(), 1):
        if FORBIDDEN in line:
            violations.append(lineno)
    if violations:
        print(
            "PlanFrame DataFrameModel legacy return check failed:\n"
            f"  {path} must not contain {FORBIDDEN!r} (lines {violations}).\n"
            "  Use PlanFrame _dfm_sync_pf paths only; engine-only work via "
            "to_dataframe().\n",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
