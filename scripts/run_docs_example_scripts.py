"""Run `docs/examples/**/*.py` and write captured outputs next to each script.

This is a docs-maintenance helper for keeping rendered documentation in sync with
the runnable example scripts. It is intentionally conservative:

- skips `docs/examples/fastapi/service_layout/` (meant to be run via uvicorn)
- skips non-scripts like `routers/__init__.py`
- writes `<script>.out.txt` and `<script>.err.txt` (empty stderr omitted)
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = REPO_ROOT / "docs" / "examples"


@dataclass(frozen=True)
class RunResult:
    path: Path
    returncode: int
    stdout: str
    stderr: str


def _iter_example_scripts() -> Iterable[Path]:
    for p in sorted(EXAMPLES_DIR.rglob("*.py")):
        rel = p.relative_to(REPO_ROOT)
        if "docs/examples/fastapi/service_layout" in str(rel).replace("\\", "/"):
            continue
        if p.name == "__init__.py":
            continue
        yield p


def _run_script(path: Path) -> RunResult:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT / "python") + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )

    proc = subprocess.run(
        [sys.executable, str(path)],
        cwd=str(REPO_ROOT),
        env=env,
        text=True,
        capture_output=True,
        timeout=120,
    )
    return RunResult(
        path=path,
        returncode=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
    )


def _write_outputs(res: RunResult) -> None:
    out_path = res.path.with_suffix(res.path.suffix + ".out.txt")
    err_path = res.path.with_suffix(res.path.suffix + ".err.txt")

    out_body = res.stdout.rstrip() + ("\n" if res.stdout else "")
    err_body = res.stderr.rstrip() + ("\n" if res.stderr else "")

    out_path.write_text(out_body, encoding="utf-8")
    if err_body:
        err_path.write_text(err_body, encoding="utf-8")
    else:
        if err_path.exists():
            err_path.unlink()


def main() -> int:
    scripts = list(_iter_example_scripts())
    if not scripts:
        print("No example scripts found.")
        return 1

    failed: list[RunResult] = []
    for p in scripts:
        rel = p.relative_to(REPO_ROOT)
        print(f"run: {rel}")
        res = _run_script(p)
        _write_outputs(res)
        if res.returncode != 0:
            failed.append(res)

    if failed:
        print("\nFAILED:")
        for res in failed:
            rel = res.path.relative_to(REPO_ROOT)
            print(f"- {rel} (code={res.returncode})")
        return 1

    print(f"\nOK: ran {len(scripts)} scripts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

