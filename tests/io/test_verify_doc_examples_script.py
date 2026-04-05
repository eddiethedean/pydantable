"""Run scripts/verify_doc_examples.py under pytest."""

from __future__ import annotations

import os
import subprocess
import sys

from tests._support.paths import repo_root

REPO = repo_root()


def test_verify_doc_examples_script() -> None:
    env = os.environ.copy()
    py_src = str(REPO / "python")
    prev = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = py_src if not prev else f"{py_src}{os.pathsep}{prev}"

    r = subprocess.run(
        [sys.executable, "scripts/verify_doc_examples.py"],
        cwd=str(REPO),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0, f"verify_doc_examples failed:\n{r.stderr}\n{r.stdout}"
