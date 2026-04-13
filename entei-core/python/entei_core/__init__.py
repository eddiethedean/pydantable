"""MongoDB-oriented optional engine for pydantable (PyMongo)."""

from __future__ import annotations

from entei_core.dataframe import EnteiDataFrame, EnteiDataFrameModel
from entei_core.engine import EnteiPydantableEngine
from entei_core.mongo_root import MongoRoot

__all__ = [
    "EnteiDataFrame",
    "EnteiDataFrameModel",
    "EnteiPydantableEngine",
    "MongoRoot",
    "__version__",
]

__version__ = "1.16.1"
