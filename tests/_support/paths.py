from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    """Return the repository root (directory containing ``tests/conftest.py``)."""
    here = Path(__file__).resolve()
    for p in [here, *here.parents]:
        if (p / "tests" / "conftest.py").is_file():
            return p
    raise RuntimeError("Could not locate repository root (tests/conftest.py not found)")
