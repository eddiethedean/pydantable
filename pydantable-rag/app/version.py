from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version


def app_version() -> str:
    """
    Return the deployed app version.

    In FastAPI Cloud this project is installed from `pyproject.toml`, so the
    package metadata version should match what is actually running.
    """
    try:
        return version("pydantable-rag")
    except PackageNotFoundError:
        return "unknown"

