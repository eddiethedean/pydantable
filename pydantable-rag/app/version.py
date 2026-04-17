from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path


def app_version() -> str:
    """
    Return the deployed app version.

    Prefer package metadata (when installed). Fall back to parsing the adjacent
    `pyproject.toml` (works in FastAPI Cloud even if not installed as a package).
    """
    try:
        return version("pydantable-rag")
    except PackageNotFoundError:
        pass

    try:
        import tomllib

        pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        return str(data["project"]["version"])
    except Exception:
        return "unknown"

