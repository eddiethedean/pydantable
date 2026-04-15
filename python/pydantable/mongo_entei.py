"""Compatibility re-export of :mod:`pydantable.mongo_dataframe`."""

from __future__ import annotations

import warnings
from typing import Any

from pydantable.mongo_dataframe import (
    BeanieAsyncRoot,
    MongoDataFrame,
    MongoDataFrameModel,
)

EnteiDataFrame = MongoDataFrame
EnteiDataFrameModel = MongoDataFrameModel


def __getattr__(name: str) -> Any:
    import pydantable.mongo_dataframe as md

    if name == "EnteiPydantableEngine":
        warnings.warn(
            "EnteiPydantableEngine is deprecated; use MongoPydantableEngine.",
            DeprecationWarning,
            stacklevel=2,
        )
        return md.MongoPydantableEngine
    if name in (
        "MongoPydantableEngine",
        "MongoRoot",
    ):
        return getattr(md, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BeanieAsyncRoot",
    "EnteiDataFrame",
    "EnteiDataFrameModel",
    "EnteiPydantableEngine",
    "MongoDataFrame",
    "MongoDataFrameModel",
    "MongoPydantableEngine",
    "MongoRoot",
]
