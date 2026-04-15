"""Compatibility re-export of :mod:`pydantable.mongo_dataframe_engine`."""

from __future__ import annotations

from pydantable.mongo_dataframe_engine import (
    MongoPydantableEngine,
    _amaterialize_root_data,
)

EnteiPydantableEngine = MongoPydantableEngine

__all__ = ["EnteiPydantableEngine", "MongoPydantableEngine", "_amaterialize_root_data"]
