"""Test helpers for applications using pydantable (optional imports by submodule).

Re-exports FastAPI-oriented fixtures from :mod:`pydantable.testing.fastapi`.
"""

from __future__ import annotations

from pydantable.testing.fastapi import fastapi_app_with_executor, fastapi_test_client

__all__ = [
    "fastapi_app_with_executor",
    "fastapi_test_client",
]
