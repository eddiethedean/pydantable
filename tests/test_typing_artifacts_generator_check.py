from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


def test_typing_artifacts_generator_check_mode_passes() -> None:
    """
    Contract: `--check` should succeed in a clean tree (no drift).

    This is intentionally a subprocess call: it mirrors how CI/Make uses it.
    """
    repo = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "scripts/generate_typing_artifacts.py", "--check"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if proc.returncode != 0:
        pytest.fail(f"--check failed:\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")

