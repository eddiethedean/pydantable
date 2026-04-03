"""Run every ``docs/examples/**/*.py`` script (built native extension required).

Skips ``fastapi/service_layout`` (uvicorn app package; run manually from that folder).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
EXAMPLES = REPO / "docs" / "examples"
PY = REPO / "python"


def _iter_example_scripts() -> list[Path]:
    out: list[Path] = []
    for p in sorted(EXAMPLES.rglob("*.py")):
        if "service_layout" in p.parts:
            continue
        if p.name == "__init__.py":
            continue
        out.append(p)
    return out


@pytest.mark.parametrize(
    "script_path",
    _iter_example_scripts(),
    ids=lambda p: str(p.relative_to(REPO)),
)
def test_example_script_exits_zero(script_path: Path) -> None:
    pytest.importorskip("pydantable_native._core")
    if "sqlmodel" in script_path.parts or "sqlmodel" in script_path.name:
        pytest.importorskip("sqlmodel")
    env = {**os.environ, "PYTHONPATH": str(PY)}
    proc = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(REPO),
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert proc.returncode == 0, (
        f"{script_path}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
