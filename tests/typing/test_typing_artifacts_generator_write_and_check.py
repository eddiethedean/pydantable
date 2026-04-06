from __future__ import annotations

import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

from tests._support.paths import repo_root

if TYPE_CHECKING:
    from pathlib import Path


def _run(argv: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "scripts/generate_typing_artifacts.py", *argv],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


@pytest.mark.parametrize(
    "path_rel",
    [
        "typings/pydantable/__init__.pyi",
        "typings/pydantable/dataframe/__init__.pyi",
        "typings/pydantable/dataframe_model.pyi",
    ],
)
def test_generator_writes_and_check_detects_drift(
    tmp_path: Path, path_rel: str
) -> None:
    """
    Contract:
    - Write mode should repair drift in-place.
    - --check should fail when artifacts drift.

    We simulate drift by modifying one committed `typings/` file and then ensure
    `--check` fails, and write mode restores it.
    """
    repo = repo_root()
    target = repo / path_rel
    if not target.exists():
        pytest.skip(f"Missing target {path_rel}")

    original = target.read_text(encoding="utf-8")
    try:
        target.write_text(original + "\n# drift\n", encoding="utf-8")

        proc = _run(["--check"], cwd=repo)
        assert proc.returncode != 0
        assert "Typing artifacts are out of date" in proc.stdout
        assert path_rel in proc.stdout

        proc2 = _run([], cwd=repo)
        assert proc2.returncode == 0, (proc2.stdout, proc2.stderr)

        repaired = target.read_text(encoding="utf-8")
        assert repaired == original

        proc3 = _run(["--check"], cwd=repo)
        assert proc3.returncode == 0, (proc3.stdout, proc3.stderr)
    finally:
        target.write_text(original, encoding="utf-8")


def test_generator_verbose_lists_targets() -> None:
    repo = repo_root()
    proc = _run(["--check", "--verbose"], cwd=repo)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    # Spot-check a couple of known targets.
    assert "python/pydantable/__init__.pyi" in proc.stdout
    assert "typings/pydantable/__init__.pyi" in proc.stdout
