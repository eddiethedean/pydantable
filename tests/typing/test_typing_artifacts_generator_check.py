from __future__ import annotations

import subprocess
import sys

import pytest

from tests._support.paths import repo_root


def test_typing_artifacts_generator_check_mode_passes() -> None:
    """
    Contract: `--check` should succeed in a clean tree (no drift).

    This is intentionally a subprocess call: it mirrors how CI/Make uses it.
    """
    repo = repo_root()
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
